/**
 * @file ViewManager.cpp
 *
 * @date Feb 6, 2017
 */

#include <arpa/inet.h>
#include <tuple>

#include <derecho/core/derecho_exception.hpp>
#include <derecho/core/detail/container_template_functions.hpp>
#include <derecho/core/detail/version_code.hpp>
#include <derecho/core/detail/view_manager.hpp>
#include <derecho/core/git_version.hpp>
#include <derecho/core/replicated.hpp>

#include <derecho/persistent/Persistent.hpp>
#include <derecho/utils/logger.hpp>

#include <mutils/macro_utils.hpp>

namespace derecho {

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;
using shared_lock_t = std::shared_lock<std::shared_timed_mutex>;

/* Leader/Restart Leader Constructor */
ViewManager::ViewManager(
        const SubgroupInfo& subgroup_info,
        const std::vector<std::type_index>& subgroup_type_order,
        const bool any_persistent_objects,
        const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
        ReplicatedObjectReferenceMap& object_reference_map,
        const persistence_manager_callbacks_t& _persistence_manager_callbacks,
        std::vector<view_upcall_t> _view_upcalls)
        : server_socket(getConfUInt16(CONF_DERECHO_GMS_PORT)),
          thread_shutdown(false),
          disable_partitioning_safety(getConfBoolean(CONF_DERECHO_DISABLE_PARTITIONING_SAFETY)),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          subgroup_type_order(subgroup_type_order),
          tcp_sockets(group_tcp_sockets),
          subgroup_objects(object_reference_map),
          any_persistent_objects(any_persistent_objects),
          active_leader(true),
          persistence_manager_callbacks(_persistence_manager_callbacks) {
    rls_default_info("Derecho library running version {}.{}.{} + {} commits",
                     derecho::MAJOR_VERSION, derecho::MINOR_VERSION, derecho::PATCH_VERSION,
                     derecho::COMMITS_AHEAD_OF_VERSION);
    if(any_persistent_objects) {
        //Attempt to load a saved View from disk, to see if one is there
        curr_view = persistent::loadObject<View>();
    }
    const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    if(curr_view) {
        in_total_restart = true;
        dbg_default_debug("Found view {} on disk", curr_view->vid);
        dbg_default_info("Logged View found on disk. Restarting in recovery mode.");
        //The subgroup_type_order can't be serialized, but it's constant across restarts
        curr_view->subgroup_type_order = subgroup_type_order;
        restart_state = std::make_unique<RestartState>();
        restart_state->load_ragged_trim(*curr_view);
        restart_leader_state_machine = std::make_unique<RestartLeaderState>(
                std::move(curr_view), *restart_state,
                subgroup_info, my_id);
        await_rejoining_nodes(my_id);
        setup_initial_tcp_connections(restart_leader_state_machine->get_restart_view(), my_id);
    } else {
        in_total_restart = false;
        curr_view = std::make_unique<View>(
                0, std::vector<node_id_t>{my_id},
                std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>>{
                        {getConfString(CONF_DERECHO_LOCAL_IP),
                         getConfUInt16(CONF_DERECHO_GMS_PORT),
                         getConfUInt16(CONF_DERECHO_RPC_PORT),
                         getConfUInt16(CONF_DERECHO_SST_PORT),
                         getConfUInt16(CONF_DERECHO_RDMC_PORT)}},
                std::vector<char>{0},
                std::vector<node_id_t>{}, std::vector<node_id_t>{},
                0, 0, subgroup_type_order);
        await_first_view(my_id);
        setup_initial_tcp_connections(*curr_view, my_id);
    }
}

/* Non-leader Constructor */
ViewManager::ViewManager(
        tcp::socket& leader_connection,
        const SubgroupInfo& subgroup_info,
        const std::vector<std::type_index>& subgroup_type_order,
        const bool any_persistent_objects,
        const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
        ReplicatedObjectReferenceMap& object_reference_map,
        const persistence_manager_callbacks_t& _persistence_manager_callbacks,
        std::vector<view_upcall_t> _view_upcalls)
        : server_socket(getConfUInt16(CONF_DERECHO_GMS_PORT)),
          thread_shutdown(false),
          disable_partitioning_safety(getConfBoolean(CONF_DERECHO_DISABLE_PARTITIONING_SAFETY)),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          subgroup_type_order(subgroup_type_order),
          tcp_sockets(group_tcp_sockets),
          subgroup_objects(object_reference_map),
          any_persistent_objects(any_persistent_objects),
          active_leader(false),
          persistence_manager_callbacks(_persistence_manager_callbacks) {
    rls_default_info("Derecho library running version {}.{}.{} + {} commits",
                     derecho::MAJOR_VERSION, derecho::MINOR_VERSION, derecho::PATCH_VERSION,
                     derecho::COMMITS_AHEAD_OF_VERSION);
    const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    receive_initial_view(my_id, leader_connection);
    //As soon as we have a tentative initial view, set up the TCP connections
    setup_initial_tcp_connections(*curr_view, my_id);
}

ViewManager::~ViewManager() {
    thread_shutdown = true;
    // force accept to return.
    tcp::socket s{"localhost", getConfUInt16(CONF_DERECHO_GMS_PORT)};
    if(client_listener_thread.joinable()) {
        client_listener_thread.join();
    }
    old_views_cv.notify_all();
    if(old_view_cleanup_thread.joinable()) {
        old_view_cleanup_thread.join();
    }
}

/* ----------  1. Constructor Components ------------- */
void ViewManager::receive_initial_view(node_id_t my_id, tcp::socket& leader_connection) {
    JoinResponse leader_response;
    bool leader_redirect;
    do {
        leader_redirect = false;
        uint64_t leader_version_hashcode;
        bool success;
        dbg_default_debug("Socket connected to leader, exchanging version codes.");
        success = leader_connection.exchange(my_version_hashcode, leader_version_hashcode);
        if(!success) throw derecho_exception("Failed to exchange version hashcodes with the leader! Leader has crashed.");
        if(leader_version_hashcode != my_version_hashcode) {
            throw derecho_exception("Unable to connect to Derecho leader because the leader is running on an incompatible platform or used an incompatible compiler.");
        }
        success = leader_connection.write(my_id);
        if(!success) throw derecho_exception("Failed to send ID to the leader! Leader has crashed.");
        success = leader_connection.read(leader_response);
        if(!success) throw derecho_exception("Failed to read initial response from leader! Leader has crashed.");
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Error! Leader refused connection because ID {} is already in use!", my_id);
            dbg_default_flush();
            throw derecho_exception("Leader rejected join, ID already in use.");
        }
        if(leader_response.code == JoinResponseCode::LEADER_REDIRECT) {
            std::size_t ip_addr_size;
            leader_connection.read(ip_addr_size);
            char buffer[ip_addr_size];
            leader_connection.read(buffer, ip_addr_size);
            ip_addr_t leader_ip(buffer);
            uint16_t leader_gms_port;
            leader_connection.read(leader_gms_port);
            dbg_default_info("That node was not the leader! Redirecting to {}", leader_ip);
            //Use move-assignment to reconnect the socket to the given IP address, and try again
            //(good thing that leader_connection reference is mutable)
            leader_connection = tcp::socket(leader_ip, leader_gms_port);
            leader_redirect = true;
        }
    } while(leader_redirect);

    in_total_restart = (leader_response.code == JoinResponseCode::TOTAL_RESTART);
    if(in_total_restart) {
        curr_view = persistent::loadObject<View>();
        dbg_default_debug("In restart mode, sending view {} to leader", curr_view->vid);
        bool success = leader_connection.write(mutils::bytes_size(*curr_view));
        if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
        auto leader_socket_write = [&leader_connection](const char* bytes, std::size_t size) {
            if(!leader_connection.write(bytes, size)) {
                throw derecho_exception("Restart leader crashed before sending a restart View!");
            }
        };
        mutils::post_object(leader_socket_write, *curr_view);
        //Restore this non-serializeable field to curr_view before using it
        curr_view->subgroup_type_order = subgroup_type_order;
        restart_state = std::make_unique<RestartState>();
        restart_state->load_ragged_trim(*curr_view);
        dbg_default_debug("In restart mode, sending {} ragged trims to leader", restart_state->logged_ragged_trim.size());
        /* Protocol: Send the number of RaggedTrim objects, then serialize each RaggedTrim */
        /* Since we know this node is only a member of one shard per subgroup,
         * the size of the outer map (subgroup IDs) is the number of RaggedTrims. */
        success = leader_connection.write(restart_state->logged_ragged_trim.size());
        if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
        for(const auto& id_to_shard_map : restart_state->logged_ragged_trim) {
            const std::unique_ptr<RaggedTrim>& ragged_trim = id_to_shard_map.second.begin()->second;  //The inner map has one entry
            success = leader_connection.write(mutils::bytes_size(*ragged_trim));
            if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
            mutils::post_object(leader_socket_write, *ragged_trim);
        }
    }
    leader_connection.write(getConfUInt16(CONF_DERECHO_GMS_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_RPC_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_SST_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_RDMC_PORT));

    receive_view_and_leaders(my_id, leader_connection);
    dbg_default_debug("Received initial view {} from leader.", curr_view->vid);
}

void ViewManager::receive_view_and_leaders(const node_id_t my_id, tcp::socket& leader_connection) {
    //The leader will first send the size of the necessary buffer, then the serialized View
    std::size_t size_of_view;
    bool success = leader_connection.read(size_of_view);
    if(!success) {
        throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
    }
    char buffer[size_of_view];
    success = leader_connection.read(buffer, size_of_view);
    if(!success) {
        throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
    }
    curr_view = mutils::from_bytes<View>(nullptr, buffer);
    if(in_total_restart) {
        //In total restart mode, the leader will also send the RaggedTrims it has collected
        dbg_default_debug("In restart mode, receiving ragged trim from leader");
        restart_state->logged_ragged_trim.clear();
        std::size_t num_of_ragged_trims;
        leader_connection.read(num_of_ragged_trims);
        for(std::size_t i = 0; i < num_of_ragged_trims; ++i) {
            std::size_t size_of_ragged_trim;
            leader_connection.read(size_of_ragged_trim);
            char buffer[size_of_ragged_trim];
            leader_connection.read(buffer, size_of_ragged_trim);
            std::unique_ptr<RaggedTrim> ragged_trim = mutils::from_bytes<RaggedTrim>(nullptr, buffer);
            //operator[] is intentional: Create an empty inner map at subgroup_id if one does not exist
            restart_state->logged_ragged_trim[ragged_trim->subgroup_id].emplace(
                    ragged_trim->shard_num, std::move(ragged_trim));
        }
    }
    //Next, the leader will send the list of nodes to do state transfer from
    prior_view_shard_leaders = *receive_vector2d<int64_t>(leader_connection);

    //Set up non-serialized fields of curr_view
    curr_view->subgroup_type_order = subgroup_type_order;
    curr_view->my_rank = curr_view->rank_of(my_id);
}

bool ViewManager::check_view_committed(tcp::socket& leader_connection) {
    CommitMessage commit_message;
    //The leader will first sent a Prepare message, then a Commit message if the
    //new was committed at all joining members. Either one of these could be Abort
    //if the leader detected a failure.
    bool success = leader_connection.read(commit_message);
    if(!success) {
        throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
    }
    if(commit_message == CommitMessage::PREPARE) {
        dbg_default_debug("Leader sent PREPARE");
        bool success = leader_connection.write(CommitMessage::ACK);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        //After a successful Prepare, replace commit_message with the second message,
        //which is either Commit or Abort
        success = leader_connection.read(commit_message);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
    }
    //This checks if either the first or the second message was Abort
    if(commit_message == CommitMessage::ABORT) {
        dbg_default_debug("Leader sent ABORT");
        const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
        //Wait for a new initial view and ragged trim to be sent,
        //so that when this method returns we can try state transfer again
        receive_view_and_leaders(my_id, leader_connection);
        //Update the TCP connections pool for any new/failed nodes,
        //so we can run state transfer again.
        reinit_tcp_connections(*curr_view, my_id);
    }
    //Unless the final message was Commit, we need to retry state transfer
    return (commit_message == CommitMessage::COMMIT);
}

void ViewManager::truncate_logs() {
    if(!in_total_restart) {
        return;
    }
    for(const auto& subgroup_and_map : restart_state->logged_ragged_trim) {
        for(const auto& shard_and_trim : subgroup_and_map.second) {
            persistent::saveObject(*shard_and_trim.second,
                                   ragged_trim_filename(subgroup_and_map.first, shard_and_trim.first).c_str());
        }
    }
    dbg_default_debug("Truncating persistent logs to conform to leader's ragged trim");
    truncate_persistent_logs(restart_state->logged_ragged_trim);
}

void ViewManager::initialize_multicast_groups(CallbackSet callbacks) {
    initialize_rdmc_sst();
    std::map<subgroup_id_t, SubgroupSettings> subgroup_settings_map;
    auto sizes = derive_subgroup_settings(*curr_view, subgroup_settings_map);
    uint32_t num_received_size = sizes.first;
    uint32_t slot_size = sizes.second;
    dbg_default_trace("Initial view is: {}", curr_view->debug_string());
    if(any_persistent_objects) {
        //Persist the initial View to disk as soon as possible, which is after my_subgroups has been initialized
        persistent::saveObject(*curr_view);
    }

    dbg_default_debug("Initializing SST and RDMC for the first time.");
    construct_multicast_group(callbacks, subgroup_settings_map, num_received_size, slot_size);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
}

void ViewManager::finish_setup() {
    //At this point curr_view has been committed by the leader
    if(in_total_restart) {
        //If we were doing total restart, it has completed successfully
        restart_state.reset();
        in_total_restart = false;
        //The restart leader now gives up its leader role to the "real" leader
        active_leader = curr_view->i_am_leader();
    }
    last_suspected = std::vector<bool>(curr_view->members.size());
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    dbg_default_debug("Done setting up initial SST and RDMC");

    if(curr_view->vid != 0 && curr_view->my_rank != curr_view->find_rank_of_leader()) {
        // If this node is joining an existing group with a non-initial view, copy the leader's num_changes, num_acked, and num_committed
        // Otherwise, you'll immediately think that there's a new proposed view change because gmsSST.num_changes[leader] > num_acked[my_rank]
        curr_view->gmsSST->init_local_change_proposals(curr_view->find_rank_of_leader());
        curr_view->gmsSST->put();
        dbg_default_debug("Joining node initialized its SST row from the leader");
    }
    create_threads();
    register_predicates();

    shared_lock_t lock(view_mutex);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }
}

void ViewManager::send_logs() {
    if(!in_total_restart) {
        return;
    }
    //The restart leader doesn't have curr_view
    const View& restart_view = curr_view ? *curr_view : restart_leader_state_machine->get_restart_view();
    /* If we're in total restart mode, prior_view_shard_leaders is equal
     * to restart_state->restart_shard_leaders */
    node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    for(subgroup_id_t subgroup_id = 0; subgroup_id < prior_view_shard_leaders.size(); ++subgroup_id) {
        for(uint32_t shard = 0; shard < prior_view_shard_leaders[subgroup_id].size(); ++shard) {
            if(my_id == prior_view_shard_leaders[subgroup_id][shard]) {
                dbg_default_debug("This node is the restart leader for subgroup {}, shard {}. Sending object data to shard members.", subgroup_id, shard);
                //Send object data to all shard members, since they will all be in receive_objects()
                for(node_id_t shard_member : restart_view.subgroup_shard_views[subgroup_id][shard].members) {
                    if(shard_member != my_id) {
                        send_subgroup_object(subgroup_id, shard_member);
                    }
                }
            }
        }
    }
}

void ViewManager::setup_initial_tcp_connections(const View& initial_view, node_id_t my_id) {
    //Establish TCP connections to each other member of the view in ascending order
    for(int i = 0; i < initial_view.num_members; ++i) {
        if(initial_view.members[i] != my_id) {
            tcp_sockets->add_node(initial_view.members[i],
                                  {std::get<0>(initial_view.member_ips_and_ports[i]),
                                   std::get<PORT_TYPE::RPC>(initial_view.member_ips_and_ports[i])});
            dbg_default_debug("Established a TCP connection to node {}", initial_view.members[i]);
        }
    }
}

void ViewManager::reinit_tcp_connections(const View& initial_view, node_id_t my_id) {
    //Delete sockets for failed members no longer in the view
    tcp_sockets->filter_to(initial_view.members);
    //Recheck the members list and establish connections to any new members
    for(int i = 0; i < initial_view.num_members; ++i) {
        if(initial_view.members[i] != my_id
           && !tcp_sockets->contains_node(initial_view.members[i])) {
            tcp_sockets->add_node(initial_view.members[i],
                                  {std::get<0>(initial_view.member_ips_and_ports[i]),
                                   std::get<PORT_TYPE::RPC>(initial_view.member_ips_and_ports[i])});
            dbg_default_debug("Established a TCP connection to node {}", initial_view.members[i]);
        }
    }
}

void ViewManager::start() {
    dbg_default_debug("Starting predicate evaluation");
    curr_view->gmsSST->start_predicate_evaluation();
}

void ViewManager::truncate_persistent_logs(const ragged_trim_map_t& logged_ragged_trim) {
    for(const auto& id_to_shard_map : logged_ragged_trim) {
        subgroup_id_t subgroup_id = id_to_shard_map.first;
        uint32_t my_shard_id;
        //On the restart leader, the proposed view is still in RestartLeaderState
        //On all the other nodes, it's been received and stored in curr_view
        const View& restart_view = curr_view ? *curr_view : restart_leader_state_machine->get_restart_view();
        const auto find_my_shard = restart_view.my_subgroups.find(subgroup_id);
        if(find_my_shard == restart_view.my_subgroups.end()) {
            continue;
        }
        my_shard_id = find_my_shard->second;
        const auto& my_shard_ragged_trim = id_to_shard_map.second.at(my_shard_id);
        persistent::version_t max_delivered_version = RestartState::ragged_trim_to_latest_version(
                my_shard_ragged_trim->vid, my_shard_ragged_trim->max_received_by_sender);
        dbg_default_trace("Truncating persistent log for subgroup {} to version {}", subgroup_id, max_delivered_version);
        dbg_default_flush();
        subgroup_objects.at(subgroup_id).get().truncate(max_delivered_version);
    }
}

void ViewManager::await_first_view(const node_id_t my_id) {
    std::map<node_id_t, tcp::socket> waiting_join_sockets;
    std::set<node_id_t> members_sent_view;
    curr_view->is_adequately_provisioned = false;
    bool joiner_failed = false;
    do {
        while(!curr_view->is_adequately_provisioned) {
            tcp::socket client_socket = server_socket.accept();
            uint64_t joiner_version_code;
            client_socket.exchange(my_version_hashcode, joiner_version_code);
            if(joiner_version_code != my_version_hashcode) {
                rls_default_warn("Rejected a connection from client at {}. Client was running on an incompatible platform or used an incompatible compiler.", client_socket.get_remote_ip());
                continue;
            }
            node_id_t joiner_id = 0;
            client_socket.read(joiner_id);
            if(curr_view->rank_of(joiner_id) != -1) {
                client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE, my_id});
                continue;
            }
            client_socket.write(JoinResponse{JoinResponseCode::OK, my_id});
            uint16_t joiner_gms_port = 0;
            client_socket.read(joiner_gms_port);
            uint16_t joiner_rpc_port = 0;
            client_socket.read(joiner_rpc_port);
            uint16_t joiner_sst_port = 0;
            client_socket.read(joiner_sst_port);
            uint16_t joiner_rdmc_port = 0;
            client_socket.read(joiner_rdmc_port);
            const ip_addr_t& joiner_ip = client_socket.get_remote_ip();
            ip_addr_t my_ip = client_socket.get_self_ip();
            //Construct a new view by appending this joiner to the previous view
            //None of these views are ever installed, so we don't use curr_view/next_view like normal
            curr_view = std::make_unique<View>(curr_view->vid,
                                               functional_append(curr_view->members, joiner_id),
                                               functional_append(curr_view->member_ips_and_ports,
                                                                 {joiner_ip, joiner_gms_port, joiner_rpc_port, joiner_sst_port, joiner_rdmc_port}),
                                               std::vector<char>(curr_view->num_members + 1, 0),
                                               functional_append(curr_view->joined, joiner_id),
                                               std::vector<node_id_t>{}, 0, 0,
                                               subgroup_type_order);
            make_subgroup_maps(subgroup_info, std::unique_ptr<View>(), *curr_view);
            waiting_join_sockets.emplace(joiner_id, std::move(client_socket));
            dbg_default_debug("Node {} connected from IP address {} and GMS port {}", joiner_id, joiner_ip, joiner_gms_port);
        }
        joiner_failed = false;
        for(auto waiting_sockets_iter = waiting_join_sockets.begin();
            waiting_sockets_iter != waiting_join_sockets.end();) {
            std::size_t view_buffer_size = mutils::bytes_size(*curr_view);
            char view_buffer[view_buffer_size];
            bool send_success;
            //Within this try block, any send that returns failure throws the ID of the node that failed
            try {
                //First send the View
                send_success = waiting_sockets_iter->second.write(view_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                mutils::to_bytes(*curr_view, view_buffer);
                send_success = waiting_sockets_iter->second.write(view_buffer, view_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                //Then send "0" as the size of the "old shard leaders" vector, since there are no old leaders
                send_success = waiting_sockets_iter->second.write(std::size_t{0});
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                members_sent_view.emplace(waiting_sockets_iter->first);
                waiting_sockets_iter++;
            } catch(node_id_t failed_joiner_id) {
                dbg_default_warn("Node {} failed after contacting the leader! Removing it from the initial view.", failed_joiner_id);
                //Remove the failed client and recompute the view
                std::vector<node_id_t> filtered_members(curr_view->members.size() - 1);
                std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>> filtered_ips_and_ports(curr_view->member_ips_and_ports.size() - 1);
                std::vector<node_id_t> filtered_joiners(curr_view->joined.size() - 1);
                std::remove_copy(curr_view->members.begin(), curr_view->members.end(),
                                 filtered_members.begin(), failed_joiner_id);
                std::remove_copy(curr_view->member_ips_and_ports.begin(),
                                 curr_view->member_ips_and_ports.end(),
                                 filtered_ips_and_ports.begin(),
                                 curr_view->member_ips_and_ports[curr_view->rank_of(failed_joiner_id)]);
                std::remove_copy(curr_view->joined.begin(), curr_view->joined.end(),
                                 filtered_joiners.begin(), failed_joiner_id);
                curr_view = std::make_unique<View>(0, filtered_members, filtered_ips_and_ports,
                                                   std::vector<char>(curr_view->num_members - 1, 0), filtered_joiners,
                                                   std::vector<node_id_t>{}, 0, 0,
                                                   subgroup_type_order);
                /* This will update curr_view->is_adequately_provisioned, so set joiner_failed to true
                 * to start over from the beginning and test if we need to wait for more joiners. */
                make_subgroup_maps(subgroup_info, std::unique_ptr<View>(), *curr_view);
                waiting_join_sockets.erase(waiting_sockets_iter);
                joiner_failed = true;
                break;
            }
        }  //for (waiting_join_sockets)

        if(joiner_failed) {
            for(const node_id_t& member_sent_view : members_sent_view) {
                dbg_default_debug("Sending view abort message to node {}", member_sent_view);
                waiting_join_sockets.at(member_sent_view).write(CommitMessage::ABORT);
            }
            members_sent_view.clear();
        }
    } while(joiner_failed);

    dbg_default_trace("Decided on initial view: {}", curr_view->debug_string());
    //At this point, we have successfully sent an initial view to all joining nodes, so we can commit it
    //There's no state-transfer step to do, so we don't have to wait for state transfer to complete
    //before committing this view, and the Group constructor's state-transfer methods will do nothing
    for(auto waiting_sockets_iter = waiting_join_sockets.begin();
        waiting_sockets_iter != waiting_join_sockets.end();) {
        dbg_default_debug("Sending prepare and commit messages to node {}", waiting_sockets_iter->first);
        waiting_sockets_iter->second.write(CommitMessage::PREPARE);
        waiting_sockets_iter->second.write(CommitMessage::COMMIT);
        waiting_sockets_iter = waiting_join_sockets.erase(waiting_sockets_iter);
    }
}

void ViewManager::await_rejoining_nodes(const node_id_t my_id) {
    bool quorum_achieved = false;
    while(!quorum_achieved) {
        restart_leader_state_machine->await_quorum(server_socket);
        dbg_default_debug("Reached a quorum of nodes from view {}, created view {}", restart_leader_state_machine->get_curr_view().vid, restart_leader_state_machine->get_restart_view().vid);
        quorum_achieved = true;
        //Compute a final ragged trim
        //Actually, I don't think there's anything to "compute" because
        //we only kept the latest ragged trim from each subgroup and shard
        //So just mark all of the RaggedTrims with the "restart leader" value to stamp them with our approval
        for(auto& subgroup_to_map : restart_state->logged_ragged_trim) {
            for(auto& shard_trim_pair : subgroup_to_map.second) {
                shard_trim_pair.second->leader_id = std::numeric_limits<node_id_t>::max();
            }
        }
        whendebug(restart_leader_state_machine->print_longest_logs(););

        //Send the restart view, ragged trim, and restart shard leaders to all the members
        int64_t failed_node_id = restart_leader_state_machine->send_restart_view();
        //If a node failed while waiting for the quorum, abort this restart view and try again
        if(failed_node_id != -1) {
            dbg_default_warn("Node {} failed while waiting for restart leader to reach a quorum!", failed_node_id);
            quorum_achieved = restart_leader_state_machine->resend_view_until_quorum_lost();
        }
    }
    dbg_default_debug("Successfully sent restart view {} to all restarted nodes", restart_leader_state_machine->get_restart_view().vid);
    prior_view_shard_leaders = restart_state->restart_shard_leaders;
    //Now control will return to Group to do state transfer before confirming this view
}

bool ViewManager::leader_prepare_initial_view(bool& leader_has_quorum) {
    if(restart_leader_state_machine) {
        dbg_default_trace("Sending prepare messages for restart View");
        int64_t failed_node_id = restart_leader_state_machine->send_prepare();
        if(failed_node_id != -1) {
            dbg_default_warn("Node {} failed when sending Prepare messages for the restart view!", failed_node_id);
            leader_has_quorum = restart_leader_state_machine->resend_view_until_quorum_lost();
            //If there was at least one failure, we (may) need to do state transfer again, so return false
            //The out-parameter will tell the leader if it also needs to wait for more joins
            return false;
        }
    }
    return true;
}

void ViewManager::leader_commit_initial_view() {
    if(restart_leader_state_machine) {
        dbg_default_trace("Decided on restart view: {}", restart_leader_state_machine->get_restart_view().debug_string());
        //Commit the restart view at all joining clients
        restart_leader_state_machine->send_commit();
        curr_view = restart_leader_state_machine->take_restart_view();
        //After sending the commit messages, it's safe to discard the restart state
        restart_leader_state_machine.reset();
    }
}

void ViewManager::initialize_rdmc_sst() {
    dbg_default_debug("Starting global initialization of RDMC and SST, including internal TCP connection setup");
    // construct member_ips
    auto member_ips_and_rdmc_ports_map = make_member_ips_and_ports_map<PORT_TYPE::RDMC>(*curr_view);
    if(!rdmc::initialize(member_ips_and_rdmc_ports_map,
                         curr_view->members[curr_view->my_rank])) {
        std::cout << "Global setup failed" << std::endl;
        exit(0);
    }
    auto member_ips_and_sst_ports_map = make_member_ips_and_ports_map<PORT_TYPE::SST>(*curr_view);

#ifdef USE_VERBS_API
    sst::verbs_initialize(member_ips_and_sst_ports_map,
                          curr_view->members[curr_view->my_rank]);
#else
    sst::lf_initialize(member_ips_and_sst_ports_map,
                       curr_view->members[curr_view->my_rank]);
#endif
}

void ViewManager::create_threads() {
    client_listener_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "client_thread");
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            dbg_default_debug("Background thread got a client connection from {}", client_socket.get_remote_ip());
            pending_join_sockets.locked().access.emplace_back(std::move(client_socket));
        }
    }};

    old_view_cleanup_thread = std::thread([this]() {
        pthread_setname_np(pthread_self(), "old_view");
        while(!thread_shutdown) {
            unique_lock_t old_views_lock(old_views_mutex);
            old_views_cv.wait(old_views_lock, [this]() {
                return !old_views.empty() || thread_shutdown;
            });
            if(!thread_shutdown) {
                old_views.front().reset();
                old_views.pop();
            }
        }
    });
}

void ViewManager::register_predicates() {
    /* Note that each trigger function must be wrapped in a lambda because it's
   * a member function, and lambdas are the only way to bind "this" to a member
   * function invocation. */
    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& sst) { new_suspicion(sst); };

    auto start_join_pred = [this](const DerechoSST& sst) {
        return active_leader && has_pending_join();
    };
    auto start_join_trig = [this](DerechoSST& sst) { leader_start_join(sst); };

    auto reject_join_pred = [this](const DerechoSST& sst) {
        return !active_leader && has_pending_join();
    };
    auto reject_join = [this](DerechoSST& sst) { redirect_join_attempt(sst); };

    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return active_leader
               && min_acked(gmsSST, curr_view->failed) > gmsSST.num_committed[curr_view->my_rank];
    };
    auto commit_change = [this](DerechoSST& sst) { leader_commit_change(sst); };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_changes[curr_view->find_rank_of_leader()]
               > gmsSST.num_acked[curr_view->my_rank];
    };
    auto ack_proposed_change = [this](DerechoSST& sst) {
        acknowledge_proposed_change(sst);
    };

    auto leader_committed_changes = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_committed[curr_view->find_rank_of_leader()]
               > gmsSST.num_installed[curr_view->my_rank];
    };
    auto view_change_trig = [this](DerechoSST& sst) { start_meta_wedge(sst); };

    if(!suspected_changed_handle.is_valid()) {
        suspected_changed_handle = curr_view->gmsSST->predicates.insert(
                suspected_changed, suspected_changed_trig,
                sst::PredicateType::RECURRENT);
    }
    if(!start_join_handle.is_valid()) {
        start_join_handle = curr_view->gmsSST->predicates.insert(
                start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
    }
    if(!reject_join_handle.is_valid()) {
        reject_join_handle = curr_view->gmsSST->predicates.insert(reject_join_pred, reject_join,
                                                                  sst::PredicateType::RECURRENT);
    }
    if(!change_commit_ready_handle.is_valid()) {
        change_commit_ready_handle = curr_view->gmsSST->predicates.insert(
                change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
    }
    if(!leader_proposed_handle.is_valid()) {
        leader_proposed_handle = curr_view->gmsSST->predicates.insert(
                leader_proposed_change, ack_proposed_change,
                sst::PredicateType::RECURRENT);
    }
    if(!leader_committed_handle.is_valid()) {
        leader_committed_handle = curr_view->gmsSST->predicates.insert(
                leader_committed_changes, view_change_trig,
                sst::PredicateType::ONE_TIME);
    }
}

/* ------------- 2. Predicate-Triggers That Implement View Management Logic ---------- */

void ViewManager::new_suspicion(DerechoSST& gmsSST) {
    // keep calm?
    if(bSilent) {
        return;
    }

    dbg_default_debug("Suspected[] changed");
    View& Vc = *curr_view;
    const int my_rank = curr_view->my_rank;
    //Cache this before changing failed[], so we can see if the leader changed
    const int old_leader_rank = curr_view->find_rank_of_leader();
    int num_left = 0;
    // Aggregate suspicions into gmsSST[myRank].Suspected;
    for(int r = 0; r < Vc.num_members; r++) {
        for(int who = 0; who < Vc.num_members; who++) {
            gmssst::set(gmsSST.suspected[my_rank][who],
                        gmsSST.suspected[my_rank][who] || gmsSST.suspected[r][who]);
        }
        if(gmsSST.rip[r]) {
            num_left++;
        }
    }

    for(int rank = 0; rank < Vc.num_members; rank++) {
        if(gmsSST.suspected[my_rank][rank] && !last_suspected[rank]) {
            // This is safer than copy_suspected, since suspected[] might change during this loop
            last_suspected[rank] = gmsSST.suspected[my_rank][rank];
            dbg_default_debug("Marking {} failed", Vc.members[rank]);

            if(!gmsSST.rip[my_rank] && Vc.num_failed != 0
               && (Vc.num_failed - num_left >= (Vc.num_members - num_left + 1) / 2)) {
                if(disable_partitioning_safety) {
                    dbg_default_warn("Potential partitioning event, but partitioning safety is disabled. num_failed - num_left = {} but num_members - num_left + 1 = {}",
                                     Vc.num_failed - num_left, Vc.num_members - num_left + 1);
                } else {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }
            }

            dbg_default_debug("GMS telling SST to freeze row {}", rank);
            gmsSST.freeze(rank);
            //These two lines are the same as Vc.wedge()
            Vc.multicast_group->wedge();
            gmssst::set(gmsSST.wedged[my_rank], true);
            //Synchronize Vc.failed with gmsSST.suspected
            Vc.failed[rank] = true;
            Vc.num_failed++;

            if(!gmsSST.rip[my_rank] && Vc.num_failed != 0
               && (Vc.num_failed - num_left >= (Vc.num_members - num_left + 1) / 2)) {
                if(disable_partitioning_safety) {
                    dbg_default_warn("Potential partitioning event, but partitioning safety is disabled. num_failed - num_left = {} but num_members - num_left + 1 = {}",
                                     Vc.num_failed - num_left, Vc.num_members - num_left + 1);
                } else {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }
            }

            // push change to gmsSST.suspected[myRank]
            gmsSST.put(gmsSST.suspected);
            // push change to gmsSST.wedged[myRank]
            gmsSST.put(gmsSST.wedged);
            const int new_leader_rank = Vc.find_rank_of_leader();
            //Only propose the change if there was no change in leadership
            if(my_rank == new_leader_rank && my_rank == old_leader_rank
               && !changes_contains(gmsSST, Vc.members[rank])) {
                const int next_change_index = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
                if(next_change_index == (int)gmsSST.changes.size()) {
                    throw derecho_exception("Ran out of room in the pending changes list!");
                }

                gmssst::set(gmsSST.changes[my_rank][next_change_index],
                            Vc.members[rank]);  // Reports the failure
                gmssst::increment(gmsSST.num_changes[my_rank]);
                dbg_default_debug("Leader proposed a change to remove failed node {}", Vc.members[rank]);
                gmsSST.put(gmsSST.changes, next_change_index);
                gmsSST.put(gmsSST.num_changes);
            }
        }
    }
    //Determine if the detected failures made me the new leader, and register the takeover predicate
    if(my_rank == Vc.find_rank_of_leader() && my_rank != old_leader_rank) {
        dbg_default_debug("The current leader failed, so this node will take over as leader");
        auto leader_change_finished = [this](const DerechoSST& sst) {
            return curr_view->i_am_leader() && previous_leaders_suspected(sst, *curr_view);
        };
        auto leader_change_trigger = [this](DerechoSST& sst) {
            new_leader_takeover(sst);
        };
        gmsSST.predicates.insert(leader_change_finished, leader_change_trigger,
                                 sst::PredicateType::ONE_TIME);
    }
}

void ViewManager::leader_start_join(DerechoSST& gmsSST) {
    dbg_default_debug("GMS handling a new client connection");
    if((gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_committed[curr_view->my_rank])
       == static_cast<int>(curr_view->members.size())) {
        dbg_default_debug("Delaying handling the new client, there are already {} pending changes", curr_view->members.size());
        return;
    }
    {
        //Hold the lock on pending_join_sockets while moving a socket into proposed_join_sockets
        auto pending_join_sockets_locked = pending_join_sockets.locked();
        proposed_join_sockets.splice(proposed_join_sockets.end(),
                                     pending_join_sockets_locked.access,
                                     pending_join_sockets_locked.access.begin());
    }
    bool success = receive_join(gmsSST, proposed_join_sockets.back());
    //If the join failed, close the socket
    if(!success) proposed_join_sockets.pop_back();
}

void ViewManager::redirect_join_attempt(DerechoSST& gmsSST) {
    tcp::socket client_socket;
    {
        auto pending_join_sockets_locked = pending_join_sockets.locked();
        client_socket = std::move(pending_join_sockets_locked.access.front());
        pending_join_sockets_locked.access.pop_front();
    }
    node_id_t joiner_id;
    client_socket.read(joiner_id);
    client_socket.write(JoinResponse{JoinResponseCode::LEADER_REDIRECT,
                                     curr_view->members[curr_view->my_rank]});
    //Send the client the IP address of the current leader
    const int rank_of_leader = curr_view->find_rank_of_leader();
    client_socket.write(mutils::bytes_size(std::get<0>(
            curr_view->member_ips_and_ports[rank_of_leader])));
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {
        client_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write,
                        std::get<0>(curr_view->member_ips_and_ports[rank_of_leader]));
    client_socket.write(std::get<PORT_TYPE::GMS>(
            curr_view->member_ips_and_ports[rank_of_leader]));
}

void ViewManager::new_leader_takeover(DerechoSST& gmsSST) {
    bool changes_copied = copy_prior_leader_proposals(gmsSST);
    dbg_default_debug("Taking over as the new leader; everyone suspects prior leaders.");
    //For each node that I suspect, make sure a change is proposed to remove it
    const unsigned int my_rank = gmsSST.get_local_index();
    for(int rank = 0; rank < curr_view->num_members; ++rank) {
        if(gmsSST.suspected[my_rank][rank]
           && !changes_contains(gmsSST, curr_view->members[rank])) {
            const int next_change_index = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
            if(next_change_index == (int)gmsSST.changes.size()) {
                throw derecho_exception("Ran out of room in the pending changes list!");
            }

            gmssst::set(gmsSST.changes[my_rank][next_change_index],
                        curr_view->members[rank]);
            gmssst::increment(gmsSST.num_changes[my_rank]);
            dbg_default_debug("Leader proposed a change to remove failed node {}", curr_view->members[rank]);
            //If changes were copied, we'll have to push the whole vector
            //otherwise we can just push the new element
            if(!changes_copied) {
                gmsSST.put(gmsSST.changes, next_change_index);
                gmsSST.put(gmsSST.num_changes);
            }
        }
    }
    if(changes_copied) {
        gmsSST.put(gmsSST.changes);
        gmsSST.put(gmsSST.num_changes);
    }
    //I am now "awake" as the leader and can take new actions
    active_leader = true;
}

void ViewManager::leader_commit_change(DerechoSST& gmsSST) {
    gmssst::set(gmsSST.num_committed[gmsSST.get_local_index()],
                min_acked(gmsSST, curr_view->failed));  // Leader commits a new request
    dbg_default_debug("Leader committing change proposal #{}", gmsSST.num_committed[gmsSST.get_local_index()]);
    gmsSST.put(gmsSST.num_committed);
}

void ViewManager::acknowledge_proposed_change(DerechoSST& gmsSST) {
    const int myRank = gmsSST.get_local_index();
    const int leader = curr_view->find_rank_of_leader();
    dbg_default_debug("Detected that leader proposed change #{}. Acknowledging.", gmsSST.num_changes[leader]);
    if(myRank != leader) {
        // Echo the count
        gmssst::set(gmsSST.num_changes[myRank], gmsSST.num_changes[leader]);

        // Echo (copy) the vector including the new changes
        gmssst::set(gmsSST.changes[myRank], gmsSST.changes[leader],
                    gmsSST.changes.size());
        // Echo the new member's IP and ports
        gmssst::set(gmsSST.joiner_ips[myRank], gmsSST.joiner_ips[leader],
                    gmsSST.joiner_ips.size());
        gmssst::set(gmsSST.joiner_gms_ports[myRank], gmsSST.joiner_gms_ports[leader],
                    gmsSST.joiner_gms_ports.size());
        gmssst::set(gmsSST.joiner_rpc_ports[myRank], gmsSST.joiner_rpc_ports[leader],
                    gmsSST.joiner_rpc_ports.size());
        gmssst::set(gmsSST.joiner_sst_ports[myRank], gmsSST.joiner_sst_ports[leader],
                    gmsSST.joiner_sst_ports.size());
        gmssst::set(gmsSST.joiner_rdmc_ports[myRank], gmsSST.joiner_rdmc_ports[leader],
                    gmsSST.joiner_rdmc_ports.size());
        gmssst::set(gmsSST.num_committed[myRank], gmsSST.num_committed[leader]);
    }

    // Notice a new request, acknowledge it
    gmssst::set(gmsSST.num_acked[myRank], gmsSST.num_changes[myRank]);
    /* breaking the above put statement into individual put calls, to be sure that
     * if we were relying on any ordering guarantees, we won't run into issue when
     * guarantees do not hold*/
    gmsSST.put(gmsSST.changes);
    //This pushes the contiguous set of joiner_xxx_ports fields all at once
    gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
    gmsSST.put(gmsSST.num_changes);
    gmsSST.put(gmsSST.num_committed);
    gmsSST.put(gmsSST.num_acked);
    gmsSST.put(gmsSST.num_installed);
    dbg_default_debug("Wedging current view.");
    curr_view->wedge();
    dbg_default_debug("Done wedging current view.");
}

void ViewManager::start_meta_wedge(DerechoSST& gmsSST) {
    dbg_default_debug("Meta-wedging view {}", curr_view->vid);
    // Disable all the other SST predicates, except suspected_changed and the
    // one I'm about to register
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(reject_join_handle);
    gmsSST.predicates.remove(change_commit_ready_handle);
    gmsSST.predicates.remove(leader_proposed_handle);

    curr_view->wedge();

    /* We now need to wait for all other nodes to wedge the current view,
     * which is called "meta-wedged." To do that, this predicate trigger
     * creates a new predicate that will fire when meta-wedged is true, and
     * registers the next epoch termination method as its trigger.
     */
    auto is_meta_wedged = [this](const DerechoSST& gmsSST) {
        for(unsigned int n = 0; n < gmsSST.get_num_rows(); ++n) {
            if(!curr_view->failed[n] && !gmsSST.wedged[n]) {
                return false;
            }
        }
        return true;
    };
    auto meta_wedged_continuation = [this](DerechoSST& gmsSST) {
        terminate_epoch(gmsSST);
    };
    gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::terminate_epoch(DerechoSST& gmsSST) {
    dbg_default_debug("MetaWedged is true; continuing epoch termination");
    // If this is the first time terminate_epoch() was called, next_view will still be null
    bool first_call = false;
    if(!next_view) {
        first_call = true;
    }
    std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);
    next_view = make_next_view(curr_view, gmsSST);
    dbg_default_debug("Checking provisioning of view {}", next_view->vid);
    make_subgroup_maps(subgroup_info, curr_view, *next_view);
    if(!next_view->is_adequately_provisioned) {
        dbg_default_debug("Next view would not be adequately provisioned, waiting for more joins.");
        if(first_call) {
            // Re-register the predicates for accepting and acknowledging joins
            register_predicates();
            // But remove the one for start_meta_wedge
            gmsSST.predicates.remove(leader_committed_handle);
        }
        // Construct a predicate that watches for any new committed change that is a join
        int curr_num_committed = gmsSST.num_committed[curr_view->find_rank_of_leader()];
        auto leader_committed_change = [this, curr_num_committed](const DerechoSST& gmsSST) {
            return gmsSST.num_committed[curr_view->find_rank_of_leader()] > curr_num_committed;
        };
        // Construct a trigger that will re-call terminate_epoch()
        auto retry_next_view = [this](DerechoSST& sst) {
            terminate_epoch(sst);
        };
        gmsSST.predicates.insert(leader_committed_change, retry_next_view,
                                 sst::PredicateType::ONE_TIME);
        return;
    }
    // If execution reached here, we have a valid next view

    // go through all subgroups first and acknowledge all messages received through SST
    for(const auto& shard_settings_pair :
        curr_view->multicast_group->get_subgroup_settings()) {
        const subgroup_id_t subgroup_id = shard_settings_pair.first;
        const auto& curr_subgroup_settings = shard_settings_pair.second;
        auto num_shard_members = curr_subgroup_settings.members.size();
        std::vector<int> shard_senders = curr_subgroup_settings.senders;
        auto num_shard_senders = curr_view->multicast_group->get_num_senders(shard_senders);
        std::map<uint32_t, uint32_t> shard_ranks_by_sender_rank;
        for(uint j = 0, l = 0; j < num_shard_members; ++j) {
            if(shard_senders[j]) {
                shard_ranks_by_sender_rank[l] = j;
                l++;
            }
        }
        // wait for all pending sst sends to finish
        dbg_default_debug("Waiting for pending SST sends to finish");
        while(curr_view->multicast_group->check_pending_sst_sends(subgroup_id)) {
        }
        gmsSST.put_with_completion();
        gmsSST.sync_with_members(
                curr_view->multicast_group->get_shard_sst_indices(subgroup_id));
        while(curr_view->multicast_group->receiver_predicate(
                curr_subgroup_settings, shard_ranks_by_sender_rank,
                num_shard_senders, gmsSST)) {
            auto sst_receive_handler_lambda =
                    [this, subgroup_id, curr_subgroup_settings,
                     shard_ranks_by_sender_rank, num_shard_senders](
                            uint32_t sender_rank, volatile char* data, uint32_t size) {
                        curr_view->multicast_group->sst_receive_handler(
                                subgroup_id, curr_subgroup_settings, shard_ranks_by_sender_rank,
                                num_shard_senders, sender_rank, data, size);
                    };
            curr_view->multicast_group->receiver_function(
                    subgroup_id, curr_subgroup_settings, shard_ranks_by_sender_rank,
                    num_shard_senders, gmsSST,
                    curr_subgroup_settings.profile.window_size, sst_receive_handler_lambda);
        }
    }

    gmsSST.put_with_completion();
    dbg_default_debug("Doing an SST sync_with_members");
    gmsSST.sync_with_members();

    // For subgroups in which I'm the shard leader, do RaggedEdgeCleanup for the leader
    auto follower_subgroups_and_shards = std::make_shared<std::map<subgroup_id_t, uint32_t>>();
    for(const auto& shard_settings_pair : curr_view->multicast_group->get_subgroup_settings()) {
        const subgroup_id_t subgroup_id = shard_settings_pair.first;
        const uint32_t shard_num = shard_settings_pair.second.shard_num;
        const SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
        uint num_shard_senders = 0;
        for(auto v : shard_view.is_sender) {
            if(v) num_shard_senders++;
        }
        if(num_shard_senders) {
            if(shard_view.my_rank == curr_view->subview_rank_of_shard_leader(subgroup_id, shard_num)) {
                leader_ragged_edge_cleanup(
                        subgroup_id,
                        shard_settings_pair.second.num_received_offset, shard_view.members,
                        num_shard_senders);
            } else {
                // Keep track of which subgroups I'm a non-leader in, and what my
                // corresponding shard ID is
                follower_subgroups_and_shards->emplace(subgroup_id, shard_num);
            }
        }
    }

    // Wait for the shard leaders of subgroups I'm not a leader in to post
    // global_min_ready before continuing.
    auto leader_global_mins_are_ready = [this, follower_subgroups_and_shards](const DerechoSST& gmsSST) {
        for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
            const SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_shard_pair.first)
                                                .at(subgroup_shard_pair.second);
            node_id_t shard_leader = shard_view.members.at(
                    curr_view->subview_rank_of_shard_leader(subgroup_shard_pair.first,
                                                            subgroup_shard_pair.second));
            if(!gmsSST.global_min_ready[curr_view->rank_of(shard_leader)][subgroup_shard_pair.first]) {
                return false;
            }
        }
        return true;
    };

    auto global_min_ready_continuation = [this, follower_subgroups_and_shards](DerechoSST& gmsSST) {
        echo_ragged_trim(follower_subgroups_and_shards, gmsSST);
    };

    gmsSST.predicates.insert(leader_global_mins_are_ready, global_min_ready_continuation,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::echo_ragged_trim(
        std::shared_ptr<std::map<subgroup_id_t, uint32_t>> follower_subgroups_and_shards,
        DerechoSST& gmsSST) {
    dbg_default_debug("GlobalMins are ready for all {} subgroup leaders this node is waiting on", follower_subgroups_and_shards->size());
    // Call RaggedEdgeCleanup for subgroups in which I'm not the leader
    for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
        const subgroup_id_t subgroup_id = subgroup_shard_pair.first;
        const uint32_t shard_num = subgroup_shard_pair.second;
        SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
        uint num_shard_senders = 0;
        for(auto v : shard_view.is_sender) {
            if(v) num_shard_senders++;
        }
        node_id_t shard_leader = shard_view.members[curr_view->subview_rank_of_shard_leader(
                subgroup_id, shard_num)];
        follower_ragged_edge_cleanup(
                subgroup_id,
                curr_view->rank_of(shard_leader),
                curr_view->multicast_group->get_subgroup_settings().at(subgroup_id).num_received_offset,
                num_shard_senders);
    }

    //Now, for all subgroups I'm in (leader or not), wait for everyone to have echoed the leader's
    //global_min_ready before delivering any messages; this means they have seen and logged the ragged trim
    auto everyone_echoed_pred = [this](const DerechoSST& gmsSST) {
        for(const auto& subgroup_shard_pair : curr_view->my_subgroups) {
            const SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_shard_pair.first)
                                                .at(subgroup_shard_pair.second);
            for(const node_id_t shard_member : shard_view.members) {
                int shard_member_rank = curr_view->rank_of(shard_member);
                //Always check failed before reading an SST row
                if(!curr_view->failed[shard_member_rank]
                   && !gmsSST.global_min_ready[shard_member_rank][subgroup_shard_pair.first]) {
                    return false;
                }
            }
        }
        return true;
    };

    auto deliver_ragged_trim_trig = [this](DerechoSST& gmsSST) {
        deliver_ragged_trim(gmsSST);
    };

    gmsSST.predicates.insert(everyone_echoed_pred, deliver_ragged_trim_trig,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::deliver_ragged_trim(DerechoSST& gmsSST) {
    dbg_default_debug("GlobalMin has been echoed by everyone for all {} subgroups this node is in", curr_view->my_subgroups.size());
    for(const auto& subgroup_shard_pair : curr_view->my_subgroups) {
        const subgroup_id_t subgroup_id = subgroup_shard_pair.first;
        const uint32_t shard_num = subgroup_shard_pair.second;
        const SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
        node_id_t shard_leader = shard_view.members.at(
                curr_view->subview_rank_of_shard_leader(subgroup_id, shard_num));
        uint num_shard_senders = 0;
        for(auto v : shard_view.is_sender) {
            if(v) num_shard_senders++;
        }
        deliver_in_order(curr_view->rank_of(shard_leader), subgroup_id,
                         curr_view->multicast_group->get_subgroup_settings()
                                 .at(subgroup_id)
                                 .num_received_offset,
                         shard_view.members, num_shard_senders);
    }

    // Wait for persistence to finish for messages delivered in RaggedEdgeCleanup before continuing
    auto persistence_finished_pred = [this](const DerechoSST& gmsSST) {
        // For each subgroup/shard that this node is a member of...
        for(const auto& subgroup_shard_pair : curr_view->my_subgroups) {
            const subgroup_id_t subgroup_id = subgroup_shard_pair.first;
            const uint32_t shard_num = subgroup_shard_pair.second;
            if(curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num).mode == Mode::UNORDERED) {
                // Skip non-ordered subgroups, they never do persistence
                continue;
            }
            message_id_t last_delivered_seq_num = gmsSST.delivered_num[curr_view->my_rank][subgroup_id];
            // For each member of that shard...
            for(const node_id_t& shard_member :
                curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num).members) {
                uint member_row = curr_view->rank_of(shard_member);
                // Check to see if the member persisted up to the ragged edge trim
                if(!curr_view->failed[member_row]
                   && persistent::unpack_version<int32_t>(
                              gmsSST.persisted_num[member_row][subgroup_id])
                                      .second
                              < last_delivered_seq_num) {
                    return false;
                }
            }
        }
        return true;
    };

    auto finish_view_change_trig = [this](DerechoSST& gmsSST) {
        finish_view_change(gmsSST);
    };

    gmsSST.predicates.insert(persistence_finished_pred, finish_view_change_trig,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::finish_view_change(DerechoSST& gmsSST) {
    std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);

    // Disable all the other SST predicates, except suspected_changed
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(reject_join_handle);
    gmsSST.predicates.remove(change_commit_ready_handle);
    gmsSST.predicates.remove(leader_proposed_handle);

    // Now that the next_view won't change any more, calculate its subgroup settings
    std::map<subgroup_id_t, SubgroupSettings> next_subgroup_settings;
    auto sizes = derive_subgroup_settings(*next_view, next_subgroup_settings);
    uint32_t new_num_received_size = sizes.first;
    uint32_t new_slot_size = sizes.second;

    dbg_default_debug("Ready to transition to the next View: {}", next_view->debug_string());
    // Determine the shard leaders in the old view and re-index them by new subgroup IDs
    vector_int64_2d old_shard_leaders_by_id = old_shard_leaders_by_new_ids(
            *curr_view, *next_view);

    std::list<tcp::socket> joiner_sockets;
    if(active_leader && next_view->joined.size() > 0) {
        // If j joins have been committed, pop the next j sockets off
        // proposed_join_sockets and send them the new View and old shard
        // leaders list
        for(std::size_t c = 0; c < next_view->joined.size(); ++c) {
            send_view(*next_view, proposed_join_sockets.front());
            std::size_t size_of_vector = mutils::bytes_size(old_shard_leaders_by_id);
            proposed_join_sockets.front().write(size_of_vector);
            mutils::post_object([this](const char* bytes, std::size_t size) {
                proposed_join_sockets.front().write(bytes, size);
            },
                                old_shard_leaders_by_id);
            // save the socket for the commit step
            joiner_sockets.emplace_back(std::move(proposed_join_sockets.front()));
            proposed_join_sockets.pop_front();
        }
    }

    node_id_t my_id = next_view->members[next_view->my_rank];

    // Set up TCP connections to the joined nodes
    update_tcp_connections(*next_view);
    // After doing that, shard leaders can send them RPC objects
    send_objects_to_new_members(*next_view, old_shard_leaders_by_id);

    // Re-initialize this node's RPC objects, which includes receiving them
    // from shard leaders if it is newly a member of a subgroup
    dbg_default_debug("Receiving state for local Replicated Objects");
    initialize_subgroup_objects(my_id, *next_view, old_shard_leaders_by_id);

    // Once state transfer completes, we can tell joining clients to commit the view
    if(active_leader) {
        for(auto& joiner_socket : joiner_sockets) {
            //Eventually, we could check for success here and abort the view if a node failed
            joiner_socket.write(CommitMessage::PREPARE);
        }
        for(auto& joiner_socket : joiner_sockets) {
            joiner_socket.write(CommitMessage::COMMIT);
        }
        joiner_sockets.clear();
    }

    // Delete the last two GMS predicates from the old SST in preparation for deleting it
    gmsSST.predicates.remove(leader_committed_handle);
    gmsSST.predicates.remove(suspected_changed_handle);

    dbg_default_debug("Starting creation of new SST and DerechoGroup for view {}", next_view->vid);
    for(const node_id_t failed_node_id : next_view->departed) {
        dbg_default_debug("Removing global TCP connections for failed node {} from RDMC and SST", failed_node_id);
#ifdef USE_VERBS_API
        rdma::impl::verbs_remove_connection(failed_node_id);
#else
        rdma::impl::lf_remove_connection(failed_node_id);
#endif
        sst::remove_node(failed_node_id);
    }
    // if new members have joined, add their RDMA connections to SST and RDMC
    for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
        // The new members will be the last joined.size() elements of the members lists
        int joiner_rank = next_view->num_members - next_view->joined.size() + i;
        dbg_default_debug("Adding RDMC connection to node {}, at IP {} and port {}", next_view->members[joiner_rank], std::get<0>(next_view->member_ips_and_ports[joiner_rank]), std::get<PORT_TYPE::RDMC>(next_view->member_ips_and_ports[joiner_rank]));

#ifdef USE_VERBS_API
        rdma::impl::verbs_add_connection(next_view->members[joiner_rank],
                                         next_view->member_ips_and_ports[joiner_rank], my_id);
#else
        rdma::impl::lf_add_connection(
                next_view->members[joiner_rank],
                std::pair<ip_addr_t, uint16_t>{
                        std::get<0>(next_view->member_ips_and_ports[joiner_rank]),
                        std::get<PORT_TYPE::RDMC>(
                                next_view->member_ips_and_ports[joiner_rank])});
#endif
    }
    for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
        int joiner_rank = next_view->num_members - next_view->joined.size() + i;
        sst::add_node(next_view->members[joiner_rank],
                      std::pair<ip_addr_t, uint16_t>{
                              std::get<0>(next_view->member_ips_and_ports[joiner_rank]),
                              std::get<PORT_TYPE::SST>(
                                      next_view->member_ips_and_ports[joiner_rank])});
    }

    // This will block until everyone responds to SST/RDMC initial handshakes
    transition_multicast_group(next_subgroup_settings, new_num_received_size, new_slot_size);

    // New members can now proceed to view_manager.start(), which will call sync()
    next_view->gmsSST->put();
    next_view->gmsSST->sync_with_members();
    dbg_default_debug("Done setting up SST and MulticastGroup for view {}", next_view->vid);
    {
        lock_guard_t old_views_lock(old_views_mutex);
        old_views.push(std::move(curr_view));
        old_views_cv.notify_all();
    }
    curr_view = std::move(next_view);

    if(any_persistent_objects) {
        // Write the new view to disk before using it
        persistent::saveObject(*curr_view);
    }

    // Re-initialize last_suspected (suspected[] has been reset to all false in the new view)
    last_suspected.assign(curr_view->members.size(), false);

    // Register predicates in the new view
    register_predicates();

    // First task with my new view...
    if(curr_view->i_am_new_leader()) {
        dbg_default_debug("i_am_new_leader() was true, calling merge_changes()");
        curr_view->merge_changes();  // Create a combined list of Changes
        active_leader = true;
    }

    // Announce the new view to the application
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }

    curr_view->gmsSST->start_predicate_evaluation();
    view_change_cv.notify_all();
    dbg_default_debug("Done with view change to view {}", curr_view->vid);
}

/* ------------- 3. Helper Functions for Predicates and Triggers ------------- */

void ViewManager::construct_multicast_group(CallbackSet callbacks,
                                            const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                            const uint32_t num_received_size,
                                            const uint32_t slot_size) {
    const auto num_subgroups = curr_view->subgroup_shard_views.size();

    curr_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(curr_view->members, curr_view->members[curr_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); },
                           curr_view->failed, false),
            num_subgroups, num_received_size, slot_size);

    curr_view->multicast_group = std::make_unique<MulticastGroup>(
            curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, callbacks, num_subgroups, subgroup_settings,
            view_max_sender_timeout,
            [this](const subgroup_id_t& subgroup_id, const persistent::version_t& ver, const uint64_t& msg_ts) {
                assert(subgroup_objects.find(subgroup_id) != subgroup_objects.end());
                subgroup_objects.at(subgroup_id).get().post_next_version(ver, msg_ts);
            },
            persistence_manager_callbacks, curr_view->failed);
}

void ViewManager::transition_multicast_group(
        const std::map<subgroup_id_t, SubgroupSettings>& new_subgroup_settings,
        const uint32_t new_num_received_size, const uint32_t new_slot_size) {
    const auto num_subgroups = next_view->subgroup_shard_views.size();

    next_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(next_view->members, next_view->members[next_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); },
                           next_view->failed, false),
            num_subgroups, new_num_received_size, new_slot_size);

    next_view->multicast_group = std::make_unique<MulticastGroup>(
            next_view->members, next_view->members[next_view->my_rank],
            next_view->gmsSST, std::move(*curr_view->multicast_group), num_subgroups,
            new_subgroup_settings,
            [this](const subgroup_id_t& subgroup_id, const persistent::version_t& ver, const uint64_t& msg_ts) {
                assert(subgroup_objects.find(subgroup_id) != subgroup_objects.end());
                subgroup_objects.at(subgroup_id).get().post_next_version(ver, msg_ts);
            },
            persistence_manager_callbacks, next_view->failed);

    curr_view->multicast_group.reset();

    // Initialize this node's row in the new SST
    int changes_installed = next_view->joined.size() + next_view->departed.size();
    next_view->gmsSST->init_local_row_from_previous(
            (*curr_view->gmsSST), curr_view->my_rank, changes_installed);
    gmssst::set(next_view->gmsSST->vid[next_view->my_rank], next_view->vid);
}

bool ViewManager::receive_join(DerechoSST& gmsSST, tcp::socket& client_socket) {
    struct in_addr joiner_ip_packed;
    inet_aton(client_socket.get_remote_ip().c_str(), &joiner_ip_packed);

    uint64_t joiner_version_code;
    client_socket.exchange(my_version_hashcode, joiner_version_code);
    if(joiner_version_code != my_version_hashcode) {
        rls_default_warn("Rejected a connection from client at {}. Client was running on an incompatible platform or used an incompatible compiler.",
                         client_socket.get_remote_ip());
        return false;
    }
    node_id_t joining_client_id = 0;
    client_socket.read(joining_client_id);

    if(curr_view->rank_of(joining_client_id) != -1) {
        dbg_default_warn("Joining node at IP {} announced it has ID {}, which is already in the View!", client_socket.get_remote_ip(), joining_client_id);
        client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE,
                                         curr_view->members[curr_view->my_rank]});
        return false;
    }
    client_socket.write(JoinResponse{JoinResponseCode::OK, curr_view->members[curr_view->my_rank]});

    uint16_t joiner_gms_port = 0;
    client_socket.read(joiner_gms_port);
    uint16_t joiner_rpc_port = 0;
    client_socket.read(joiner_rpc_port);
    uint16_t joiner_sst_port = 0;
    client_socket.read(joiner_sst_port);
    uint16_t joiner_rdmc_port = 0;
    client_socket.read(joiner_rdmc_port);

    dbg_default_debug("Proposing change to add node {}", joining_client_id);
    size_t next_change = gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_installed[curr_view->my_rank];
    gmssst::set(gmsSST.changes[curr_view->my_rank][next_change],
                joining_client_id);
    gmssst::set(gmsSST.joiner_ips[curr_view->my_rank][next_change],
                joiner_ip_packed.s_addr);
    gmssst::set(gmsSST.joiner_gms_ports[curr_view->my_rank][next_change],
                joiner_gms_port);
    gmssst::set(gmsSST.joiner_rpc_ports[curr_view->my_rank][next_change],
                joiner_rpc_port);
    gmssst::set(gmsSST.joiner_sst_ports[curr_view->my_rank][next_change],
                joiner_sst_port);
    gmssst::set(gmsSST.joiner_rdmc_ports[curr_view->my_rank][next_change],
                joiner_rdmc_port);

    gmssst::increment(gmsSST.num_changes[curr_view->my_rank]);

    dbg_default_debug("Wedging view {}", curr_view->vid);
    curr_view->wedge();
    // gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(),
    // gmsSST.num_committed.get_base() - gmsSST.changes.get_base());
    /* breaking the above put statement into individual put calls, to be sure
     * that if we were relying on any ordering guarantees, we won't run into
     * issue when guarantees do not hold*/
    gmsSST.put(gmsSST.changes);
    gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
    gmsSST.put(gmsSST.num_changes);
    return true;
}

void ViewManager::send_view(const View& new_view, tcp::socket& client_socket) {
    dbg_default_debug("Sending client the new view");
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {
        client_socket.write(bytes, size);
    };
    std::size_t size_of_view = mutils::bytes_size(new_view);
    client_socket.write(size_of_view);
    mutils::post_object(bind_socket_write, new_view);
}

void ViewManager::send_objects_to_new_members(const View& new_view, const vector_int64_2d& old_shard_leaders) {
    node_id_t my_id = new_view.members[new_view.my_rank];
    for(subgroup_id_t subgroup_id = 0; subgroup_id < old_shard_leaders.size(); ++subgroup_id) {
        for(uint32_t shard = 0; shard < old_shard_leaders[subgroup_id].size(); ++shard) {
            //if I was the leader of the shard in the old view...
            if(my_id == old_shard_leaders[subgroup_id][shard]) {
                //send its object state to the new members
                for(node_id_t shard_joiner : new_view.subgroup_shard_views[subgroup_id][shard].joined) {
                    if(shard_joiner != my_id) {
                        send_subgroup_object(subgroup_id, shard_joiner);
                    }
                }
            }
        }
    }
}

/* Note for the future: Since this "send" requires first receiving the log tail length,
 * it's really a blocking receive-then-send. Since all nodes call send_subgroup_object
 * before initialize_subgroup_objects, there's a small chance of a deadlock: node A could
 * be attempting to send an object to node B at the same time as B is attempting to send a
 * different object to A, and neither node will be able to send the log tail length that
 * the other one is waiting on. */
void ViewManager::send_subgroup_object(subgroup_id_t subgroup_id, node_id_t new_node_id) {
    LockedReference<std::unique_lock<std::mutex>, tcp::socket> joiner_socket = tcp_sockets->get_socket(new_node_id);
    ReplicatedObject& subgroup_object = subgroup_objects.at(subgroup_id);
    if(subgroup_object.is_persistent()) {
        //First, read the log tail length sent by the joining node
        int64_t persistent_log_length = 0;
        joiner_socket.get().read(persistent_log_length);
        persistent::PersistentRegistry::setEarliestVersionToSerialize(persistent_log_length);
        dbg_default_debug("Got log tail length {}", persistent_log_length);
    }
    dbg_default_debug("Sending Replicated Object state for subgroup {} to node {}", subgroup_id, new_node_id);
    subgroup_object.send_object(joiner_socket.get());
}

void ViewManager::update_tcp_connections(const View& new_view) {
    for(const node_id_t& removed_id : new_view.departed) {
        dbg_default_debug("Removing TCP connection for failed node {}", removed_id);
        tcp_sockets->delete_node(removed_id);
    }
    for(const node_id_t& joiner_id : new_view.joined) {
        tcp_sockets->add_node(joiner_id,
                              {std::get<0>(new_view.member_ips_and_ports[new_view.rank_of(joiner_id)]),
                               std::get<PORT_TYPE::RPC>(new_view.member_ips_and_ports[new_view.rank_of(joiner_id)])});
        dbg_default_debug("Established a TCP connection to node {}", joiner_id);
    }
}

uint32_t ViewManager::compute_num_received_size(const View& view) {
    uint32_t num_received_size = 0;
    for(subgroup_id_t subgroup_num = 0; subgroup_num < view.subgroup_shard_views.size(); ++subgroup_num) {
        uint32_t max_shard_senders = 0;
        for(uint32_t shard_num = 0; shard_num < view.subgroup_shard_views[subgroup_num].size(); ++shard_num) {
            std::size_t shard_size = view.subgroup_shard_views[subgroup_num][shard_num].members.size();
            uint32_t num_shard_senders = view.subgroup_shard_views[subgroup_num][shard_num].num_senders();
            if(num_shard_senders > max_shard_senders) {
                max_shard_senders = shard_size;
            }
        }
        num_received_size += max_shard_senders;
    }
    return num_received_size;
}

void ViewManager::make_subgroup_maps(const SubgroupInfo& subgroup_info,
                                     const std::unique_ptr<View>& prev_view, View& curr_view) {
    int32_t initial_next_unassigned_rank = curr_view.next_unassigned_rank;
    curr_view.subgroup_shard_views.clear();
    curr_view.subgroup_ids_by_type_id.clear();
    subgroup_allocation_map_t subgroup_allocations;
    try {
        auto temp = subgroup_info.subgroup_membership_function(curr_view.subgroup_type_order,
                                                               prev_view, curr_view);
        //Hack to ensure RVO works even though subgroup_allocations had to be declared outside this scope
        subgroup_allocations = std::move(temp);
    } catch(subgroup_provisioning_exception& ex) {
        // Mark the view as inadequate and roll back everything done by allocation functions
        curr_view.is_adequately_provisioned = false;
        curr_view.next_unassigned_rank = initial_next_unassigned_rank;
        curr_view.subgroup_shard_views.clear();
        curr_view.subgroup_ids_by_type_id.clear();
        return;
    }
    /* Now that all the subgroups are fully provisioned, use subgroup_allocations to initialize
     * curr_view's subgroup_ids_by_type_id, my_subgroups, and subgroup_shard_views
     */
    for(subgroup_type_id_t subgroup_type_id = 0;
        subgroup_type_id < curr_view.subgroup_type_order.size();
        ++subgroup_type_id) {
        const std::type_index& subgroup_type = curr_view.subgroup_type_order[subgroup_type_id];
        subgroup_shard_layout_t& curr_type_subviews = subgroup_allocations[subgroup_type];
        std::size_t num_subgroups = curr_type_subviews.size();
        curr_view.subgroup_ids_by_type_id.emplace(subgroup_type_id, std::vector<subgroup_id_t>(num_subgroups));
        for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
            // Assign this (type, index) pair a new unique subgroup ID
            subgroup_id_t curr_subgroup_id = curr_view.subgroup_shard_views.size();
            curr_view.subgroup_ids_by_type_id[subgroup_type_id][subgroup_index] = curr_subgroup_id;
            uint32_t num_shards = curr_type_subviews[subgroup_index].size();
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                SubView& shard_view = curr_type_subviews[subgroup_index][shard_num];
                shard_view.my_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
                if(shard_view.my_rank != -1) {
                    // Initialize my_subgroups
                    curr_view.my_subgroups[curr_subgroup_id] = shard_num;
                }
                if(prev_view) {
                    // Initialize this shard's SubView.joined and SubView.departed
                    subgroup_id_t prev_subgroup_id = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)
                                                             .at(subgroup_index);
                    SubView& prev_shard_view = prev_view->subgroup_shard_views[prev_subgroup_id][shard_num];
                    shard_view.init_joined_departed(prev_shard_view);
                }
            }  // for(shard_num)
            /* Pull this shard->SubView mapping out of the subgroup allocation
             * and save it under its subgroup ID (which was subgroup_shard_views.size()).
             * This deletes it from the subgroup_shard_layout_t's outer vector. */
            curr_view.subgroup_shard_views.emplace_back(std::move(
                    subgroup_allocations[subgroup_type][subgroup_index]));
        }  //for(subgroup_index)
    }
}

std::pair<uint32_t, uint32_t> ViewManager::derive_subgroup_settings(View& view,
                                                                    std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings) {
    uint32_t num_received_offset = 0;
    uint32_t slot_offset = 0;
    view.my_subgroups.clear();
    for(subgroup_id_t subgroup_id = 0; subgroup_id < view.subgroup_shard_views.size(); ++subgroup_id) {
        uint32_t num_shards = view.subgroup_shard_views.at(subgroup_id).size();
        uint32_t max_shard_senders = 0;
        uint32_t slot_size_for_subgroup = 0;
        uint64_t max_payload_size = 0;

        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            SubView& shard_view = view.subgroup_shard_views.at(subgroup_id).at(shard_num);
            max_shard_senders = std::max(shard_view.num_senders(), max_shard_senders);

            const DerechoParams& profile = DerechoParams::from_profile(shard_view.profile);
            uint32_t slot_size_for_shard = profile.window_size * (profile.sst_max_msg_size + 2 * sizeof(uint64_t));
            uint64_t payload_size = profile.max_msg_size - sizeof(header);
            max_payload_size = std::max(payload_size, max_payload_size);
            slot_size_for_subgroup = std::max(slot_size_for_shard, slot_size_for_subgroup);
            view_max_window_size = std::max(profile.window_size, view_max_window_size);
            view_max_sender_timeout = std::max(profile.heartbeat_ms, view_max_sender_timeout);

            //Initialize my_rank in the SubView for this node's ID
            shard_view.my_rank = shard_view.rank_of(view.members[view.my_rank]);
            if(shard_view.my_rank != -1) {
                //Initialize my_subgroups
                view.my_subgroups[subgroup_id] = shard_num;
                //Save the settings for MulticastGroup
                subgroup_settings[subgroup_id] = {
                        shard_num,
                        (uint32_t)shard_view.my_rank,
                        shard_view.members,
                        shard_view.is_sender,
                        shard_view.sender_rank_of(shard_view.my_rank),
                        num_received_offset,
                        slot_offset,
                        shard_view.mode,
                        profile,
                };
            }
        }  // for(shard_num)
        num_received_offset += max_shard_senders;
        slot_offset += slot_size_for_subgroup;
        max_payload_sizes[subgroup_id] = max_payload_size;
        view_max_payload_size = std::max(max_payload_size, view_max_payload_size);
    }  // for(subgroup_id)

    return {num_received_offset, slot_offset};
}

std::map<subgroup_id_t, uint64_t> ViewManager::get_max_payload_sizes() {
    return max_payload_sizes;
}

std::unique_ptr<View> ViewManager::make_next_view(const std::unique_ptr<View>& curr_view,
                                                  const DerechoSST& gmsSST) {
    int myRank = curr_view->my_rank;
    std::set<int> leave_ranks;
    std::vector<int> join_indexes;
    // Look through pending changes up to num_committed and filter the joins and leaves
    const int committed_count = gmsSST.num_committed[curr_view->find_rank_of_leader()]
                                - gmsSST.num_installed[curr_view->find_rank_of_leader()];
    for(int change_index = 0; change_index < committed_count; change_index++) {
        node_id_t change_id = gmsSST.changes[myRank][change_index];
        int change_rank = curr_view->rank_of(change_id);
        if(change_rank != -1) {
            // Might as well save the rank, since we'll need it again
            leave_ranks.emplace(change_rank);
        } else {
            join_indexes.emplace_back(change_index);
        }
    }

    int next_num_members = curr_view->num_members - leave_ranks.size() + join_indexes.size();
    // Initialize the next view
    std::vector<node_id_t> joined, members(next_num_members), departed;
    std::vector<char> failed(next_num_members);
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>> member_ips_and_ports(next_num_members);
    int next_unassigned_rank = curr_view->next_unassigned_rank;
    for(std::size_t i = 0; i < join_indexes.size(); ++i) {
        const int join_index = join_indexes[i];
        node_id_t joiner_id = gmsSST.changes[myRank][join_index];
        struct in_addr joiner_ip_packed;
        joiner_ip_packed.s_addr = gmsSST.joiner_ips[myRank][join_index];
        char* joiner_ip_cstr = inet_ntoa(joiner_ip_packed);
        std::string joiner_ip(joiner_ip_cstr);

        joined.emplace_back(joiner_id);
        // New members go at the end of the members list, but it may shrink in the new view
        int new_member_rank = curr_view->num_members - leave_ranks.size() + i;
        members[new_member_rank] = joiner_id;
        member_ips_and_ports[new_member_rank] = std::tuple{joiner_ip,
                                                           gmsSST.joiner_gms_ports[myRank][join_index],
                                                           gmsSST.joiner_rpc_ports[myRank][join_index],
                                                           gmsSST.joiner_sst_ports[myRank][join_index],
                                                           gmsSST.joiner_rdmc_ports[myRank][join_index]};
        dbg_default_debug("Next view will add new member with ID {}", joiner_id);
    }
    for(const auto& leaver_rank : leave_ranks) {
        departed.emplace_back(curr_view->members[leaver_rank]);
        // Decrement next_unassigned_rank for every failure,
        // unless the failed node wasn't assigned to a subgroup anyway
        if(leaver_rank <= curr_view->next_unassigned_rank) {
            next_unassigned_rank--;
        }
    }
    dbg_default_debug("Next view will exclude {} failed members.", leave_ranks.size());

    // Copy member information, excluding the members that have failed
    int new_rank = 0;
    for(int old_rank = 0; old_rank < curr_view->num_members; old_rank++) {
        // This is why leave_ranks needs to be a set
        if(leave_ranks.find(old_rank) == leave_ranks.end()) {
            members[new_rank] = curr_view->members[old_rank];
            member_ips_and_ports[new_rank] = curr_view->member_ips_and_ports[old_rank];
            failed[new_rank] = curr_view->failed[old_rank];
            ++new_rank;
        }
    }

    // Initialize my_rank in next_view
    int32_t my_new_rank = -1;
    node_id_t myID = curr_view->members[myRank];
    for(int i = 0; i < next_num_members; ++i) {
        if(members[i] == myID) {
            my_new_rank = i;
            break;
        }
    }
    if(my_new_rank == -1) {
        throw derecho_exception("Some other node reported that I failed.  Node " + std::to_string(myID) + " terminating.");
    }

    auto next_view = std::make_unique<View>(
            curr_view->vid + 1, members, member_ips_and_ports, failed, joined,
            departed, my_new_rank, next_unassigned_rank,
            curr_view->subgroup_type_order);
    next_view->i_know_i_am_leader = curr_view->i_know_i_am_leader;
    return next_view;
}

vector_int64_2d ViewManager::old_shard_leaders_by_new_ids(const View& curr_view,
                                                          const View& next_view) {
    std::vector<std::vector<int64_t>> old_shard_leaders_by_new_id(next_view.subgroup_shard_views.size());
    for(const auto& type_to_old_ids : curr_view.subgroup_ids_by_type_id) {
        for(uint32_t subgroup_index = 0; subgroup_index < type_to_old_ids.second.size(); ++subgroup_index) {
            subgroup_id_t old_subgroup_id = type_to_old_ids.second[subgroup_index];
            //The subgroup is uniquely identified by (type ID, subgroup index) in both old and new views
            subgroup_id_t new_subgroup_id = next_view.subgroup_ids_by_type_id.at(type_to_old_ids.first)
                                                    .at(subgroup_index);
            std::size_t new_num_shards = next_view.subgroup_shard_views[new_subgroup_id].size();
            old_shard_leaders_by_new_id[new_subgroup_id].resize(new_num_shards, -1);
            for(uint32_t shard_num = 0; shard_num < new_num_shards; ++shard_num) {
                int64_t old_shard_leader = -1;
                //Raw subgroups don't have any state to send to new members, so they have no leaders
                if(curr_view.subgroup_type_order.at(type_to_old_ids.first)
                   != std::type_index(typeid(RawObject))) {
                    int old_shard_leader_rank = curr_view.subview_rank_of_shard_leader(old_subgroup_id, shard_num);
                    if(old_shard_leader_rank >= 0) {
                        old_shard_leader = curr_view.subgroup_shard_views[old_subgroup_id][shard_num]
                                                   .members[old_shard_leader_rank];
                    }
                }
                old_shard_leaders_by_new_id[new_subgroup_id][shard_num] = old_shard_leader;
            }  // for(shard_num)
        }      // for(subgroup_index)
    }          // for(type_to_old_ids)
    return old_shard_leaders_by_new_id;
}

bool ViewManager::suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old) {
    for(unsigned int r = 0; r < gmsSST.get_num_rows(); r++) {
        for(size_t who = 0; who < gmsSST.suspected.size(); who++) {
            if(gmsSST.suspected[r][who] && !old[who]) {
                return true;
            }
        }
    }
    return false;
}

void ViewManager::copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old) {
    for(size_t who = 0; who < gmsSST.suspected.size(); ++who) {
        old[who] = gmsSST.suspected[gmsSST.get_local_index()][who];
    }
}

bool ViewManager::changes_contains(const DerechoSST& gmsSST, const node_id_t q) {
    int myRow = gmsSST.get_local_index();
    for(int p_index = 0;
        p_index < gmsSST.num_changes[myRow] - gmsSST.num_installed[myRow];
        p_index++) {
        const node_id_t p(const_cast<node_id_t&>(gmsSST.changes[myRow][p_index]));
        if(p == q) {
            return true;
        }
    }
    return false;
}

int ViewManager::min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed) {
    int myRank = gmsSST.get_local_index();
    int min_num_acked = gmsSST.num_acked[myRank];
    for(size_t n = 0; n < failed.size(); n++) {
        if(!failed[n]) {
            // copy to avoid race condition and non-volatile based optimizations
            int num_acked_copy = gmsSST.num_acked[n];
            min_num_acked = std::min(min_num_acked, num_acked_copy);
        }
    }

    return min_num_acked;
}

bool ViewManager::previous_leaders_suspected(const DerechoSST& gmsSST, const View& curr_view) {
    const int rank_of_leader = curr_view.find_rank_of_leader();
    //For each non-failed member, check if that node suspects all ranks lower than the current leader
    for(uint row = 0; row < gmsSST.get_num_rows(); ++row) {
        if(!curr_view.failed[row]) {
            for(int previous_leader_rank = 0;
                previous_leader_rank < rank_of_leader;
                ++previous_leader_rank) {
                if(!gmsSST.suspected[row][previous_leader_rank]) {
                    return false;
                }
            }
        }
    }
    return true;
}

bool ViewManager::copy_prior_leader_proposals(DerechoSST& gmsSST) {
    const int my_rank = gmsSST.get_local_index();
    const int my_changes_length = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
    bool prior_changes_found = false;
    int prior_leader_rank = my_rank;
    while(!prior_changes_found && prior_leader_rank > 0) {
        prior_leader_rank--;
        const int changes_length = gmsSST.num_changes[prior_leader_rank]
                                   - gmsSST.num_installed[prior_leader_rank];
        if(changes_length > my_changes_length) {
            prior_changes_found = true;
        } else {
            //Check each element of changes, in case this node acknowledged a different leader's change
            for(int i = 0; i < changes_length; ++i) {
                if(gmsSST.changes[prior_leader_rank][i] != gmsSST.changes[my_rank][i]) {
                    prior_changes_found = true;
                    break;
                }
            }
        }
    }
    //Copy the prior leader's changes over mine;
    //this function is called before proposing any changes as the new leader
    if(prior_changes_found) {
        dbg_default_debug("Re-proposing changes from prior leader at rank {}. Num_changes is now {}", prior_leader_rank, gmsSST.num_changes[prior_leader_rank]);
        gmssst::set(gmsSST.changes[my_rank], gmsSST.changes[prior_leader_rank],
                    gmsSST.changes.size());
        gmssst::set(gmsSST.num_changes[my_rank], gmsSST.num_changes[prior_leader_rank]);
        gmssst::set(gmsSST.joiner_ips[my_rank], gmsSST.joiner_ips[prior_leader_rank],
                    gmsSST.joiner_ips.size());
        gmssst::set(gmsSST.joiner_gms_ports[my_rank], gmsSST.joiner_gms_ports[prior_leader_rank],
                    gmsSST.joiner_gms_ports.size());
        gmssst::set(gmsSST.joiner_rpc_ports[my_rank], gmsSST.joiner_rpc_ports[prior_leader_rank],
                    gmsSST.joiner_rpc_ports.size());
        gmssst::set(gmsSST.joiner_sst_ports[my_rank], gmsSST.joiner_sst_ports[prior_leader_rank],
                    gmsSST.joiner_sst_ports.size());
        gmssst::set(gmsSST.joiner_rdmc_ports[my_rank], gmsSST.joiner_rdmc_ports[prior_leader_rank],
                    gmsSST.joiner_rdmc_ports.size());
    }
    return prior_changes_found;
}

void ViewManager::leader_ragged_edge_cleanup(const subgroup_id_t subgroup_num,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members,
                                             uint num_shard_senders) {
    dbg_default_debug("Running leader RaggedEdgeCleanup for subgroup {}", subgroup_num);
    View& Vc = *curr_view;
    int myRank = Vc.my_rank;
    bool found = false;
    //Look to see if another node (i.e. a previous leader) has already set global_min_ready
    for(uint n = 0; n < shard_members.size() && !found; n++) {
        const auto node_id = shard_members[n];
        const auto node_rank = Vc.rank_of(node_id);
        if(Vc.gmsSST->global_min_ready[node_rank][subgroup_num]) {
            //Copy this shard's slice of global_min, starting at num_received_offset
            gmssst::set(&Vc.gmsSST->global_min[myRank][num_received_offset],
                        &Vc.gmsSST->global_min[node_rank][num_received_offset], num_shard_senders);
            found = true;
        }
    }

    if(!found) {
        //Compute the global_min for this shard
        for(uint n = 0; n < num_shard_senders; n++) {
            int min_num_received = Vc.gmsSST->num_received[myRank][num_received_offset + n];
            for(uint r = 0; r < shard_members.size(); r++) {
                auto node_rank = Vc.rank_of(shard_members[r]);
                if(!Vc.failed[node_rank]) {
                    int num_received_copy = Vc.gmsSST->num_received[node_rank][num_received_offset + n];
                    min_num_received = std::min(min_num_received, num_received_copy);
                }
            }

            gmssst::set(Vc.gmsSST->global_min[myRank][num_received_offset + n], min_num_received);
        }
    }

    dbg_default_debug("Shard leader for subgroup {} finished computing global_min", subgroup_num);
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   Vc.gmsSST->global_min_ready, subgroup_num);

    if(any_persistent_objects) {
        log_ragged_trim(myRank, subgroup_num, num_received_offset, num_shard_senders);
    }
}

void ViewManager::follower_ragged_edge_cleanup(
        const subgroup_id_t subgroup_num, uint shard_leader_rank,
        const uint32_t num_received_offset,
        uint num_shard_senders) {
    const View& Vc = *curr_view;
    int myRank = Vc.my_rank;
    dbg_default_debug("Running follower RaggedEdgeCleanup for subgroup {}", subgroup_num);
    // Learn the leader's ragged trim, log it, and echo it before acting upon it
    if(any_persistent_objects) {
        log_ragged_trim(shard_leader_rank, subgroup_num, num_received_offset, num_shard_senders);
    }
    //Copy this shard's slice of global_min, starting at num_received_offset
    gmssst::set(&Vc.gmsSST->global_min[myRank][num_received_offset],
                &Vc.gmsSST->global_min[shard_leader_rank][num_received_offset],
                num_shard_senders);
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   Vc.gmsSST->global_min_ready, subgroup_num);
}

void ViewManager::deliver_in_order(const int shard_leader_rank,
                                   const uint32_t subgroup_num, const uint32_t num_received_offset,
                                   const std::vector<node_id_t>& shard_members, uint num_shard_senders) {
    // Ragged cleanup is finished, deliver in the implied order
    const View& Vc = *curr_view;
    std::vector<int32_t> max_received_indices(num_shard_senders);
    whenlog(std::stringstream delivery_order);
    whenlog(if(LoggerFactory::getDefaultLogger()->should_log(spdlog::level::debug)) {
        delivery_order << "Subgroup " << subgroup_num
                       << ", shard " << Vc.my_subgroups.at(subgroup_num)
                       << " ";
    });
    for(uint sender_rank = 0; sender_rank < num_shard_senders; sender_rank++) {
        whenlog(if(LoggerFactory::getDefaultLogger()->should_log(spdlog::level::debug)) {
            //This only works if every member is a sender, otherwise the rank will be wrong
            delivery_order << shard_members[sender_rank]
                           << ":0..."
                           << Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + sender_rank]
                           << " ";
        });
        max_received_indices[sender_rank]
                = Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + sender_rank];
    }
    dbg_default_debug("Delivering ragged-edge messages in order: {}", delivery_order.str());
    Vc.multicast_group->deliver_messages_upto(max_received_indices, subgroup_num, num_shard_senders);
}

void ViewManager::log_ragged_trim(const int shard_leader_rank,
                                  const subgroup_id_t subgroup_num,
                                  const uint32_t num_received_offset,
                                  const uint num_shard_senders) {
    //Copy this shard's slice of global_min into a new vector
    std::vector<int32_t> max_received_indices(num_shard_senders);
    for(uint sender_rank = 0; sender_rank < num_shard_senders; sender_rank++) {
        max_received_indices[sender_rank]
                = curr_view->gmsSST->global_min[shard_leader_rank][num_received_offset + sender_rank];
    }
    uint32_t shard_num = curr_view->my_subgroups.at(subgroup_num);
    RaggedTrim trim_log{subgroup_num, shard_num, curr_view->vid,
                        static_cast<int32_t>(curr_view->members[curr_view->find_rank_of_leader()]),
                        max_received_indices};
    persistent::saveObject(trim_log, ragged_trim_filename(subgroup_num, shard_num).c_str());
    dbg_default_debug("Done logging ragged trim to disk for subgroup {}", subgroup_num);
}

/* ------------- 4. Public-Interface methods of ViewManager ------------- */

void ViewManager::report_failure(const node_id_t who) {
    // keep calm
    if(bSilent) {
        return;
    }
    const int failed_rank = curr_view->rank_of(who);
    dbg_default_debug("Node ID {} failure reported; marking suspected[{}]", who, failed_rank);
    gmssst::set(curr_view->gmsSST->suspected[curr_view->my_rank][failed_rank], true);
    int failed_cnt = 0;
    int rip_cnt = 0;
    for(std::size_t r = 0; r < curr_view->gmsSST->suspected.size(); r++) {
        if(curr_view->gmsSST->rip[r]) {
            ++rip_cnt;
        } else if(curr_view->gmsSST->suspected[curr_view->my_rank][r]) {
            ++failed_cnt;
        }
    }

    if(!curr_view->gmsSST->rip[curr_view->my_rank]
       && failed_cnt != 0 && (failed_cnt >= (curr_view->num_members - rip_cnt + 1) / 2)) {
        if(disable_partitioning_safety) {
            dbg_default_warn("Potential partitioning event, but partitioning safety is disabled. failed_cnt = {} but num_members - rip_cnt + 1 = {}",
                             failed_cnt, curr_view->num_members - rip_cnt + 1);
        } else {
            throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
        }
    }
    curr_view->gmsSST->put(curr_view->gmsSST->suspected, failed_rank);
}

void ViewManager::silence() {
    bSilent = true;
}

void ViewManager::leave() {
    shared_lock_t lock(view_mutex);
    dbg_default_debug("Cleanly leaving the group.");
    curr_view->multicast_group->wedge();
    curr_view->gmsSST->predicates.clear();
    gmssst::set(curr_view->gmsSST->suspected[curr_view->my_rank][curr_view->my_rank], true);
    curr_view->gmsSST->put(curr_view->gmsSST->suspected, curr_view->my_rank);
    curr_view->gmsSST->rip[curr_view->my_rank] = true;
    curr_view->gmsSST->put_with_completion(curr_view->gmsSST->rip.get_base() - curr_view->gmsSST->getBaseAddress(),
                                           sizeof(curr_view->gmsSST->rip[0]));
    thread_shutdown = true;
}

void ViewManager::send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
                       const std::function<void(char* buf)>& msg_generator, bool cooked_send) {
    shared_lock_t lock(view_mutex);
    view_change_cv.wait(lock, [&]() {
        return curr_view->multicast_group->send(subgroup_num, payload_size,
                                                msg_generator, cooked_send);
    });
}

const uint64_t ViewManager::compute_global_stability_frontier(subgroup_id_t subgroup_num) {
    shared_lock_t lock(view_mutex);
    return curr_view->multicast_group->compute_global_stability_frontier(subgroup_num);
}

void ViewManager::add_view_upcall(const view_upcall_t& upcall) {
    view_upcalls.emplace_back(upcall);
}

std::vector<node_id_t> ViewManager::get_members() {
    shared_lock_t read_lock(view_mutex);
    return curr_view->members;
}

int32_t ViewManager::get_my_rank() {
    shared_lock_t read_lock(view_mutex);
    return curr_view->my_rank;
}

std::vector<std::vector<node_id_t>> ViewManager::get_subgroup_members(subgroup_type_id_t subgroup_type, uint32_t subgroup_index) {
    shared_lock_t read_lock(view_mutex);
    subgroup_id_t subgroup_id = curr_view->subgroup_ids_by_type_id.at(subgroup_type).at(subgroup_index);
    std::vector<std::vector<node_id_t>> subgroup_members;
    for(const auto& shard_view : curr_view->subgroup_shard_views.at(subgroup_id)) {
        subgroup_members.push_back(shard_view.members);
    }
    return subgroup_members;
}

int32_t ViewManager::get_my_shard(subgroup_type_id_t subgroup_type, uint32_t subgroup_index) {
    shared_lock_t read_lock(view_mutex);
    subgroup_id_t subgroup_id = curr_view->subgroup_ids_by_type_id.at(subgroup_type).at(subgroup_index);
    auto find_id_result = curr_view->my_subgroups.find(subgroup_id);
    if(find_id_result == curr_view->my_subgroups.end()) {
        return -1;
    } else {
        return find_id_result->second;
    }
}

void ViewManager::barrier_sync() {
    shared_lock_t read_lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

SharedLockedReference<View> ViewManager::get_current_view() {
    assert(curr_view);
    return SharedLockedReference<View>(*curr_view, view_mutex);
}

SharedLockedReference<const View> ViewManager::get_current_view_const() {
    if(restart_leader_state_machine) {
        return SharedLockedReference<const View>(restart_leader_state_machine->get_curr_view(), view_mutex);
    } else {
        return SharedLockedReference<const View>(*curr_view, view_mutex);
    }
}

void ViewManager::debug_print_status() const {
    std::cout << "curr_view = " << curr_view->debug_string() << std::endl;
}
} /* namespace derecho */
