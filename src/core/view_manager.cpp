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

ViewManager::ViewManager(
        const SubgroupInfo& subgroup_info,
        const std::vector<std::type_index>& subgroup_type_order,
        const bool any_persistent_objects,
        ReplicatedObjectReferenceMap& object_reference_map,
        const persistence_manager_callbacks_t& _persistence_manager_callbacks,
        std::vector<view_upcall_t> _view_upcalls)
        : server_socket(getConfUInt16(CONF_DERECHO_GMS_PORT)),
          thread_shutdown(false),
          disable_partitioning_safety(getConfBoolean(CONF_DERECHO_DISABLE_PARTITIONING_SAFETY)),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          subgroup_type_order(subgroup_type_order),
          tcp_sockets(getConfUInt32(CONF_DERECHO_LOCAL_ID),
                      {{getConfUInt32(CONF_DERECHO_LOCAL_ID),
                        {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_RPC_PORT)}}}),
          subgroup_objects(object_reference_map),
          any_persistent_objects(any_persistent_objects),
          persistence_manager_callbacks(_persistence_manager_callbacks) {
    rls_default_info("Derecho library running version {}.{}.{} + {} commits",
                     derecho::MAJOR_VERSION, derecho::MINOR_VERSION, derecho::PATCH_VERSION,
                     derecho::COMMITS_AHEAD_OF_VERSION);
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
    tcp_sockets.destroy();
}

/* ----------  1. Constructor Components ------------- */
bool ViewManager::first_init() {
    if(any_persistent_objects) {
        //Attempt to load a saved View from disk, to see if one is there
        curr_view = persistent::loadObject<View>();
    }
    //The presence of a logged View on disk means this node is restarting after a crash
    if(curr_view) {
        dbg_default_debug("Found view {} on disk", curr_view->vid);
        restart_state = std::make_unique<RestartState>();
        restart_state->restart_leader_ips = split_string(getConfString(CONF_DERECHO_RESTART_LEADERS));
        restart_state->restart_leader_ports = [&]() {
            //"Apply std::stoi over the result of split_string(getConfString(...))"
            auto port_list = split_string(getConfString(CONF_DERECHO_RESTART_LEADER_PORTS));
            std::vector<uint16_t> ports;
            for(const auto& port_str : port_list) {
                ports.emplace_back((uint16_t)std::stoi(port_str));
            }
            return ports;
        }();
        restart_state->num_leader_failures = 0;
        restart_to_initial_view();
    } else {
        startup_to_first_view();
    }
    return in_total_restart;
}

void ViewManager::startup_to_first_view() {
    const ip_addr_t my_ip = getConfString(CONF_DERECHO_LOCAL_IP);
    const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const uint16_t my_gms_port = getConfUInt16(CONF_DERECHO_GMS_PORT);
    in_total_restart = false;
    //Determine if I am the initial leader for a new group
    if(my_ip == getConfString(CONF_DERECHO_LEADER_IP) && my_gms_port == getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT)) {
        curr_view = std::make_unique<View>(
                0, std::vector<node_id_t>{my_id},
                std::vector<IpAndPorts>{
                        {getConfString(CONF_DERECHO_LOCAL_IP),
                         getConfUInt16(CONF_DERECHO_GMS_PORT),
                         getConfUInt16(CONF_DERECHO_RPC_PORT),
                         getConfUInt16(CONF_DERECHO_SST_PORT),
                         getConfUInt16(CONF_DERECHO_RDMC_PORT),
                         getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}},
                std::vector<char>{0},
                std::vector<node_id_t>{}, std::vector<node_id_t>{},
                0, 0, subgroup_type_order);
        active_leader = true;
        await_first_view();
        setup_initial_tcp_connections(*curr_view, my_id);
    } else {
        active_leader = false;
        leader_connection = std::make_unique<tcp::socket>(getConfString(CONF_DERECHO_LEADER_IP),
                                                          getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT));
        bool success = receive_initial_view();
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        setup_initial_tcp_connections(*curr_view, my_id);
    }
}

bool ViewManager::restart_to_initial_view() {
    const ip_addr_t my_ip = getConfString(CONF_DERECHO_LOCAL_IP);
    const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const uint16_t my_gms_port = getConfUInt16(CONF_DERECHO_GMS_PORT);
    const bool enable_backup_restart_leaders = getConfBoolean(CONF_DERECHO_ENABLE_BACKUP_RESTART_LEADERS);

    bool got_initial_view = false;
    while(!got_initial_view) {
        //Determine if I am the current restart leader
        if(my_ip == restart_state->restart_leader_ips[restart_state->num_leader_failures]
           && my_gms_port == restart_state->restart_leader_ports[restart_state->num_leader_failures]) {
            in_total_restart = true;
            active_leader = true;
            dbg_default_info("Logged View {} found on disk. Restarting in recovery mode as the leader.", curr_view->vid);
            //The subgroup_type_order can't be serialized, but it's constant across restarts
            curr_view->subgroup_type_order = subgroup_type_order;
            //Set up restart state and await rejoining nodes as the leader
            restart_state->load_ragged_trim(*curr_view);
            restart_leader_state_machine = std::make_unique<RestartLeaderState>(
                    std::move(curr_view), *restart_state,
                    subgroup_info, my_id);
            await_rejoining_nodes(my_id);
            setup_initial_tcp_connections(restart_leader_state_machine->get_restart_view(), my_id);
            got_initial_view = true;
        } else {
            //If I am not a restart leader, we may or may not be in total restart;
            //in_total_restart will be set when the leader responds in receive_initial_view
            using namespace std::chrono;
            leader_connection = std::make_unique<tcp::socket>();
            //Heuristic: Wait for half the leader's restart timeout before concluding the leader isn't responding
            int time_remaining_micro = getConfUInt32(CONF_DERECHO_RESTART_TIMEOUT_MS) * 1000 / 2;
            int connect_status = -1;
            //Annoyingly, try_connect will return immediately if the connection is refused,
            //which is the usual result while waiting for the leader to start up.
            while(time_remaining_micro > 0 && connect_status != 0) {
                auto start_time = high_resolution_clock::now();
                connect_status = leader_connection->try_connect(restart_state->restart_leader_ips[restart_state->num_leader_failures],
                                                                restart_state->restart_leader_ports[restart_state->num_leader_failures],
                                                                time_remaining_micro / 1000);
                auto end_time = high_resolution_clock::now();
                microseconds time_waited = duration_cast<microseconds>(end_time - start_time);
                time_remaining_micro -= time_waited.count();
            }
            if(connect_status == 0) {
                active_leader = false;
                got_initial_view = receive_initial_view();
                if(got_initial_view) {
                    setup_initial_tcp_connections(*curr_view, my_id);
                } else {
                    if(!enable_backup_restart_leaders) {
                        throw derecho_exception("Restart leader crashed before sending the View, and backup restart leaders are disabled.");
                    }
                    restart_state->num_leader_failures++;
                    dbg_default_debug("Restart leader failed, moving to leader #{}", restart_state->num_leader_failures);
                }
            } else if(enable_backup_restart_leaders) {
                dbg_default_warn("Couldn't connect to restart leader at {}", restart_state->restart_leader_ips[restart_state->num_leader_failures]);
                restart_state->num_leader_failures++;
                //If backup_restart_leaders is disabled, keep num_leader_failures at 0 and just keep retrying
            }
            if(!got_initial_view && restart_state->num_leader_failures >= restart_state->restart_leader_ips.size()) {
                throw derecho_exception("All configured restart leaders have failed! Giving up on restart.");
            }
        }
    }
    return in_total_restart;
}

bool ViewManager::receive_initial_view() {
    assert(leader_connection);
    const node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    JoinResponse leader_response;
    bool leader_redirect;
    do {
        leader_redirect = false;
        uint64_t leader_version_hashcode;
        dbg_default_debug("Socket connected to leader, exchanging version codes.");
        try {
            leader_connection->exchange(my_version_hashcode, leader_version_hashcode);
            if(leader_version_hashcode != my_version_hashcode) {
                throw derecho_exception("Unable to connect to Derecho leader because the leader is running on an incompatible platform or used an incompatible compiler.");
            }
            leader_connection->write(JoinRequest{my_id, false});
            leader_connection->read(leader_response);
        } catch(tcp::socket_error& e) {
            return false;
        }
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Error! Leader refused connection because ID {} is already in use!", my_id);
            dbg_default_flush();
            throw derecho_exception("Leader rejected join, ID already in use.");
        }
        if(leader_response.code == JoinResponseCode::LEADER_REDIRECT) {
            //Receive the size of the IP address, then the IP address, then the port (which is a fixed size)
            std::size_t ip_addr_size;
            leader_connection->read(ip_addr_size);
            char buffer[ip_addr_size];
            leader_connection->read(buffer, ip_addr_size);
            ip_addr_t leader_ip(buffer);
            uint16_t leader_gms_port;
            leader_connection->read(leader_gms_port);
            dbg_default_info("That node was not the leader! Redirecting to {}:{}", leader_ip, leader_gms_port);
            //Use move-assignment to reconnect the socket to the given IP address, and try again
            leader_connection = std::make_unique<tcp::socket>(leader_ip, leader_gms_port);
            leader_redirect = true;
        }
    } while(leader_redirect);

    in_total_restart = (leader_response.code == JoinResponseCode::TOTAL_RESTART);
    if(in_total_restart) {
        dbg_default_info("Logged state found on disk. Restarting in recovery mode.");
        dbg_default_debug("Sending view {} to leader", curr_view->vid);
        auto leader_socket_write = [this](const char* bytes, std::size_t size) {
            leader_connection->write(bytes, size);
        };
        try {
            leader_connection->write(mutils::bytes_size(*curr_view));
            mutils::post_object(leader_socket_write, *curr_view);
            //Restore this non-serializeable field to curr_view before using it
            curr_view->subgroup_type_order = subgroup_type_order;
            //Now that we know we need them, load ragged trims from disk
            restart_state->load_ragged_trim(*curr_view);
            dbg_default_debug("In restart mode, sending {} ragged trims to leader", restart_state->logged_ragged_trim.size());
            /* Protocol: Send the number of RaggedTrim objects, then serialize each RaggedTrim */
            /* Since we know this node is only a member of one shard per subgroup,
             * the size of the outer map (subgroup IDs) is the number of RaggedTrims. */
            leader_connection->write(restart_state->logged_ragged_trim.size());
            for(const auto& id_to_shard_map : restart_state->logged_ragged_trim) {
                assert(id_to_shard_map.second.size() == 1);  //The inner map has one entry
                const std::unique_ptr<RaggedTrim>& ragged_trim = id_to_shard_map.second.begin()->second;
                leader_connection->write(mutils::bytes_size(*ragged_trim));
                mutils::post_object(leader_socket_write, *ragged_trim);
            }
        } catch(tcp::socket_error& e) {
            //If any of the leader socket operations throws an error, stop and return false
            return false;
        }
    } else {
        //This might have been constructed even though we don't need it
        restart_state.reset();
    }
    try {
        leader_connection->write(getConfUInt16(CONF_DERECHO_GMS_PORT));
        leader_connection->write(getConfUInt16(CONF_DERECHO_RPC_PORT));
        leader_connection->write(getConfUInt16(CONF_DERECHO_SST_PORT));
        leader_connection->write(getConfUInt16(CONF_DERECHO_RDMC_PORT));
        leader_connection->write(getConfUInt16(CONF_DERECHO_EXTERNAL_PORT));
    } catch(tcp::socket_error& e) {
        return false;
    }
    if(receive_view_and_leaders()) {
        dbg_default_debug("Received initial view {} from leader: {}", curr_view->vid, curr_view->debug_string());
        return true;
    } else {
        return false;
    }
}

bool ViewManager::receive_view_and_leaders() {
    //This try block is to handle TCP socket errors
    try {
        //The leader will first send the size of the necessary buffer, then the serialized View
        std::size_t size_of_view;
        leader_connection->read(size_of_view);
        char buffer[size_of_view];
        leader_connection->read(buffer, size_of_view);
        curr_view = mutils::from_bytes<View>(nullptr, buffer);
        if(in_total_restart) {
            //In total restart mode, the leader will also send the RaggedTrims it has collected
            restart_state->logged_ragged_trim.clear();
            std::size_t num_of_ragged_trims;
            leader_connection->read(num_of_ragged_trims);
            dbg_default_debug("In restart mode, receiving {} ragged trims from leader", num_of_ragged_trims);
            for(std::size_t i = 0; i < num_of_ragged_trims; ++i) {
                std::size_t size_of_ragged_trim;
                leader_connection->read(size_of_ragged_trim);
                char buffer[size_of_ragged_trim];
                leader_connection->read(buffer, size_of_ragged_trim);
                std::unique_ptr<RaggedTrim> ragged_trim = mutils::from_bytes<RaggedTrim>(nullptr, buffer);
                //operator[] is intentional: Create an empty inner map at subgroup_id if one does not exist
                restart_state->logged_ragged_trim[ragged_trim->subgroup_id].emplace(
                        ragged_trim->shard_num, std::move(ragged_trim));
            }
        }
        //Next, the leader will send the list of nodes to do state transfer from
        prior_view_shard_leaders = *receive_vector2d<int64_t>(*leader_connection);
    } catch(tcp::socket_error& e) {
        return false;
    }

    //Set up non-serialized fields of curr_view
    curr_view->subgroup_type_order = subgroup_type_order;
    curr_view->my_rank = curr_view->rank_of(getConfUInt32(CONF_DERECHO_LOCAL_ID));
    return true;
}

void ViewManager::check_view_committed(bool& view_confirmed, bool& leader_failed) {
    assert(leader_connection);
    view_confirmed = false;
    leader_failed = false;
    CommitMessage commit_message;
    /* The leader will first sent a Prepare message, then a Commit message if the
     * new was committed at all joining members. Either one of these could be Abort
     * if the leader detected a failure.
     */
    try {
        leader_connection->read(commit_message);
        if(commit_message == CommitMessage::PREPARE) {
            dbg_default_debug("Leader sent PREPARE");
            leader_connection->write(CommitMessage::ACK);
            //After a successful Prepare, replace commit_message with the second message,
            //which is either Commit or Abort
            leader_connection->read(commit_message);
        }
        //This checks if either the first or the second message was Abort
        if(commit_message == CommitMessage::ABORT) {
            dbg_default_debug("Leader sent ABORT");
            const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
            //Wait for a new initial view and ragged trim to be sent,
            //so that when this method returns we can try state transfer again
            leader_failed = !receive_view_and_leaders();
            if(leader_failed) throw tcp::socket_error("Leader crashed!");
            //Update the TCP connections pool for any new/failed nodes,
            //so we can run state transfer again.
            reinit_tcp_connections(*curr_view, my_id);
        }
    } catch(tcp::socket_error&) {
        leader_failed = true;
        if(restart_state && getConfBoolean(CONF_DERECHO_ENABLE_BACKUP_RESTART_LEADERS)) {
            restart_state->num_leader_failures++;
            //Throw out the curr_view the leader sent and revert to the logged one on disk
            curr_view = persistent::loadObject<View>();
        }
        return;
    }
    //Unless the final message was Commit, we need to retry state transfer
    view_confirmed = (commit_message == CommitMessage::COMMIT);
}

void ViewManager::truncate_logs() {
    assert(in_total_restart);
    for(const auto& subgroup_and_map : restart_state->logged_ragged_trim) {
        for(const auto& shard_and_trim : subgroup_and_map.second) {
            persistent::saveObject(*shard_and_trim.second,
                                   ragged_trim_filename(subgroup_and_map.first, shard_and_trim.first).c_str());
        }
    }
    dbg_default_debug("Truncating persistent logs to conform to leader's ragged trim");

    const node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);

    for(const auto& id_to_shard_map : restart_state->logged_ragged_trim) {
        subgroup_id_t subgroup_id = id_to_shard_map.first;
        uint32_t my_shard_id;
        //On the restart leader, the proposed view is still in RestartLeaderState
        //On all the other nodes, it's been received and stored in curr_view
        const View& restart_view = curr_view ? *curr_view : restart_leader_state_machine->get_restart_view();
        //Determine which shard, if any, this node belongs to in subgroup subgroup_id.
        //At this point, initialize_multicast_groups has not yet been called, so
        //my_subgroups has not yet been initialized in the restart view.
        bool shard_found = false;
        for(my_shard_id = 0; my_shard_id < restart_view.subgroup_shard_views.at(subgroup_id).size();
            ++my_shard_id) {
            if(restart_view.subgroup_shard_views.at(subgroup_id).at(my_shard_id).rank_of(my_id) != -1) {
                shard_found = true;
                break;
            }
        }
        if(!shard_found) {
            continue;
        }
        const auto& my_shard_ragged_trim = id_to_shard_map.second.at(my_shard_id);
        persistent::version_t max_delivered_version = RestartState::ragged_trim_to_latest_version(
                my_shard_ragged_trim->vid, my_shard_ragged_trim->max_received_by_sender);
        dbg_default_trace("Truncating persistent log for subgroup {} to version {}", subgroup_id, max_delivered_version);
        dbg_default_flush();
        subgroup_objects.at(subgroup_id).get().truncate(max_delivered_version);
    }
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
    //Close the initial TCP socket connection to the leader (by deleting it)
    leader_connection.reset();

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
    assert(in_total_restart);
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

void ViewManager::setup_initial_tcp_connections(const View& initial_view, const node_id_t my_id) {
    //Establish TCP connections to each other member of the view in ascending order
    for(int i = 0; i < initial_view.num_members; ++i) {
        if(initial_view.members[i] != my_id) {
            tcp_sockets.add_node(initial_view.members[i],
                                 {initial_view.member_ips_and_ports[i].ip_address,
                                  initial_view.member_ips_and_ports[i].rpc_port});
            dbg_default_debug("Established a TCP connection to node {}", initial_view.members[i]);
        }
    }
}

void ViewManager::reinit_tcp_connections(const View& initial_view, const node_id_t my_id) {
    //Delete sockets for failed members no longer in the view
    tcp_sockets.filter_to(initial_view.members);
    //Recheck the members list and establish connections to any new members
    for(int i = 0; i < initial_view.num_members; ++i) {
        if(initial_view.members[i] != my_id
           && !tcp_sockets.contains_node(initial_view.members[i])) {
            tcp_sockets.add_node(initial_view.members[i],
                                 {initial_view.member_ips_and_ports[i].ip_address,
                                  initial_view.member_ips_and_ports[i].rpc_port});
            dbg_default_debug("Established a TCP connection to node {}", initial_view.members[i]);
        }
    }
}

bool ViewManager::is_starting_leader() const {
    if(in_total_restart) {
        return active_leader;
    } else {
        return getConfString(CONF_DERECHO_LOCAL_IP) == getConfString(CONF_DERECHO_LEADER_IP)
               && getConfUInt16(CONF_DERECHO_GMS_PORT) == getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT);
    }
}

void ViewManager::start() {
    dbg_default_debug("Starting predicate evaluation");
    curr_view->gmsSST->start_predicate_evaluation();
}

void ViewManager::await_first_view() {
    const node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
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
            JoinRequest join_request;
            client_socket.read(join_request);
            node_id_t joiner_id = join_request.joiner_id;
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
            uint16_t joiner_external_port = 0;
            client_socket.read(joiner_external_port);
            const ip_addr_t& joiner_ip = client_socket.get_remote_ip();
            ip_addr_t my_ip = client_socket.get_self_ip();
            //Construct a new view by appending this joiner to the previous view
            //None of these views are ever installed, so we don't use curr_view/next_view like normal
            curr_view = std::make_unique<View>(curr_view->vid,
                                               functional_append(curr_view->members, joiner_id),
                                               functional_append(curr_view->member_ips_and_ports,
                                                                 {joiner_ip, joiner_gms_port, joiner_rpc_port, joiner_sst_port, joiner_rdmc_port, joiner_external_port}),
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
            try {
                //First send the View
                waiting_sockets_iter->second.write(view_buffer_size);
                mutils::to_bytes(*curr_view, view_buffer);
                waiting_sockets_iter->second.write(view_buffer, view_buffer_size);
                //Then send "0" as the size of the "old shard leaders" vector, since there are no old leaders
                waiting_sockets_iter->second.write(std::size_t{0});
                members_sent_view.emplace(waiting_sockets_iter->first);
                waiting_sockets_iter++;
            } catch(tcp::socket_error& e) {
                //If any socket operation failed, assume the joining node failed
                node_id_t failed_joiner_id = waiting_sockets_iter->first;
                dbg_default_warn("Node {} failed after contacting the leader! Removing it from the initial view.", failed_joiner_id);
                //Remove the failed client and recompute the view
                std::vector<node_id_t> filtered_members(curr_view->members.size() - 1);
                std::vector<IpAndPorts> filtered_ips_and_ports(curr_view->member_ips_and_ports.size() - 1);
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

void ViewManager::leader_prepare_initial_view(bool& view_confirmed, bool& leader_has_quorum) {
    view_confirmed = true;
    assert(restart_leader_state_machine);
    dbg_default_trace("Sending prepare messages for restart View");
    int64_t failed_node_id = restart_leader_state_machine->send_prepare();
    if(failed_node_id != -1) {
        dbg_default_warn("Node {} failed when sending Prepare messages for the restart view!", failed_node_id);
        restart_leader_state_machine->send_abort();
        leader_has_quorum = restart_leader_state_machine->resend_view_until_quorum_lost();
        //If there was at least one failure, we (may) need to do state transfer again, so set view to unconfirmed
        view_confirmed = false;
        return;
    }
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
    auto member_ips_and_rdmc_ports_map = make_member_ips_and_ports_map(*curr_view, PortType::RDMC);
    if(!rdmc::initialize(member_ips_and_rdmc_ports_map,
                         curr_view->members[curr_view->my_rank])) {
        std::cout << "Global setup failed" << std::endl;
        exit(0);
    }
    auto member_ips_and_sst_ports_map = make_member_ips_and_ports_map(*curr_view, PortType::SST);
    node_id_t my_id = curr_view->members[curr_view->my_rank];
    const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>> self_ip_and_port_map = {
            {my_id,
             {getConfString(CONF_DERECHO_LOCAL_IP),
              getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}};

#ifdef USE_VERBS_API
    sst::verbs_initialize(member_ips_and_sst_ports_map,
                          self_ip_and_port_map,
                          my_id);
#else
    sst::lf_initialize(member_ips_and_sst_ports_map,
                       self_ip_and_port_map,
                       my_id);
#endif
}

void ViewManager::create_threads() {
    client_listener_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "client_thread");
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            dbg_default_debug("Background thread got a client connection from {}", client_socket.get_remote_ip());
            pending_new_sockets.locked().access.emplace_back(std::move(client_socket));
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

    auto leader_suspected_changed = [this](const DerechoSST& sst) {
        return active_leader && suspected_not_equal(sst, last_suspected);
    };
    auto nonleader_suspected_changed = [this](const DerechoSST& sst) {
        return !active_leader && suspected_not_equal(sst, last_suspected);
    };
    auto new_suspicion_trig = [this](DerechoSST& sst) { new_suspicion(sst); };

    auto start_join_pred = [this](const DerechoSST& sst) {
        return active_leader && has_pending_join();
    };
    auto propose_changes_trig = [this](DerechoSST& sst) { propose_changes(sst); };

    auto new_sockets_pred = [this](const DerechoSST& sst) {
        return has_pending_new();
    };
    auto new_sockets = [this](DerechoSST& sst) { process_new_sockets(); };

    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return active_leader
               && min_acked(gmsSST, curr_view->failed) > gmsSST.num_committed[curr_view->my_rank];
    };
    auto commit_change = [this](DerechoSST& sst) { leader_commit_change(sst); };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_changes[curr_view->find_rank_of_leader()]
               > gmsSST.num_acked[curr_view->my_rank];
    };
    auto ack_proposed_change = [this](DerechoSST& sst) { acknowledge_proposed_change(sst); };

    auto leader_committed_changes = [this](const DerechoSST& gmsSST) {
        const int leader_rank = curr_view->find_rank_of_leader();
        return gmsSST.num_committed[leader_rank]
                       > gmsSST.num_installed[curr_view->my_rank]
               && changes_includes_end_of_view(gmsSST, leader_rank);
    };
    auto view_change_trig = [this](DerechoSST& sst) { start_meta_wedge(sst); };
    /* This predicate detects if there are pending changes that are not yet part of a batch,
     * and I am the leader. It should run once at the beginning of each view to see if we just
     * installed a new view that already has pending changes (but not more often than that).
     */
    auto new_view_has_changes = [this](const DerechoSST& sst) {
        return active_leader
               && sst.num_changes[curr_view->my_rank] > sst.num_installed[curr_view->my_rank]
               && !changes_includes_end_of_view(sst, curr_view->my_rank);
    };
    curr_view->gmsSST->predicates.insert(new_view_has_changes,
                                         propose_changes_trig,
                                         sst::PredicateType::ONE_TIME);

    if(!leader_suspicion_handle.is_valid()) {
        leader_suspicion_handle = curr_view->gmsSST->predicates.insert(
                leader_suspected_changed, propose_changes_trig,
                sst::PredicateType::RECURRENT);
    }
    if(!follower_suspicion_handle.is_valid()) {
        follower_suspicion_handle = curr_view->gmsSST->predicates.insert(
                nonleader_suspected_changed, new_suspicion_trig, sst::PredicateType::RECURRENT);
    }
    if(!start_join_handle.is_valid()) {
        start_join_handle = curr_view->gmsSST->predicates.insert(
                start_join_pred, propose_changes_trig, sst::PredicateType::RECURRENT);
    }
    if(!new_sockets_handle.is_valid()) {
        new_sockets_handle = curr_view->gmsSST->predicates.insert(new_sockets_pred, new_sockets,
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
    const int my_rank = curr_view->my_rank;
    //Cache this before changing failed[], so we can see if the leader changed
    const int old_leader_rank = curr_view->find_rank_of_leader();

    process_suspicions(gmsSST);

    //Determine if the detected failures made me the new leader, and register the takeover predicate if so
    if(my_rank == curr_view->find_rank_of_leader() && my_rank != old_leader_rank) {
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

void ViewManager::propose_changes(DerechoSST& gmsSST) {
    const int my_rank = curr_view->my_rank;
    bool done_with_joins = !has_pending_join();
    while(!done_with_joins || suspected_not_equal(gmsSST, last_suspected)) {
        //First, check for failures
        const std::vector<int> failed_ranks = process_suspicions(gmsSST);
        for(const int failed_rank : failed_ranks) {
            if(!changes_contains(gmsSST, curr_view->members[failed_rank])) {
                const int next_change_index = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
                if(next_change_index == static_cast<int>(gmsSST.changes.size())) {
                    throw derecho_exception("Too many changes at once! Encountered a failure, but ran out of room in the pending changes list.");
                }

                gmssst::set(gmsSST.changes[my_rank][next_change_index],
                            make_change_proposal(curr_view->members[my_rank], curr_view->members[failed_rank]));  // Reports the failure
                gmssst::increment(gmsSST.num_changes[my_rank]);
                dbg_default_debug("Leader proposed a change to remove failed node {}", curr_view->members[failed_rank]);
                //Note: The proposed change has not actually been pushed yet
            }
            //If the next view has already been proposed, update it with the new failures
            if(next_view && changes_includes_end_of_view(gmsSST, my_rank)) {
                const int failed_rank_in_next_view = next_view->rank_of(curr_view->members[failed_rank]);
                next_view->failed[failed_rank_in_next_view] = true;
                next_view->num_failed++;
            }
        }

        //Second, check for joins
        while(!done_with_joins) {
            dbg_default_debug("Leader handling a new member connection");
            if((gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_committed[curr_view->my_rank])
               == static_cast<int>(curr_view->members.size())) {
                dbg_default_debug("Delaying handling the new member, there are already {} pending changes", curr_view->members.size());
                done_with_joins = true;
                continue;
            }

            proposed_join_sockets.splice(proposed_join_sockets.end(),
                                         pending_join_sockets,
                                         pending_join_sockets.begin());

            bool success = receive_join(gmsSST, proposed_join_sockets.back().first, proposed_join_sockets.back().second);
            //If the join failed, close the socket
            if(!success) proposed_join_sockets.pop_back();

            done_with_joins = !has_pending_join();
        }
    }

    //If the next view has already been proposed, these changes will have to wait until it's installed
    if(changes_includes_end_of_view(gmsSST, my_rank)) {
        return;
    }
    //Attempt to provision the next view, and see if it will be adequate
    const int last_change_index = gmsSST.num_changes[my_rank]
                                  - gmsSST.num_installed[my_rank] - 1;
    gmssst::set(gmsSST.changes[my_rank][last_change_index].end_of_view, true);
    next_view = make_next_view(curr_view, gmsSST);
    dbg_default_debug("Checking provisioning of view {}", next_view->vid);
    make_subgroup_maps(subgroup_info, curr_view, *next_view);
    if(!next_view->is_adequately_provisioned) {
        //Don't push the proposed changes yet; the view will stay wedged but view-change won't start
        dbg_default_debug("Next view would not be adequately provisioned, waiting for more joins.");
        gmssst::set(gmsSST.changes[my_rank][last_change_index].end_of_view, false);
    } else {
        //Push all the proposed changes, including joiner information if any joins were proposed
        gmsSST.put(gmsSST.changes);
        if(!proposed_join_sockets.empty()) {
            gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
                       gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
        }
        gmsSST.put(gmsSST.num_changes);
    }
}

void ViewManager::redirect_join_attempt(tcp::socket& client_socket) {
    client_socket.write(JoinResponse{JoinResponseCode::LEADER_REDIRECT,
                                     curr_view->members[curr_view->my_rank]});
    //Send the client the IP address of the current leader
    const int rank_of_leader = curr_view->find_rank_of_leader();
    client_socket.write(mutils::bytes_size(
            curr_view->member_ips_and_ports[rank_of_leader].ip_address));
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {
        client_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write,
                        curr_view->member_ips_and_ports[rank_of_leader].ip_address);
    client_socket.write(curr_view->member_ips_and_ports[rank_of_leader].gms_port);
}

void ViewManager::process_new_sockets() {
    tcp::socket client_socket;
    {
        auto pending_new_sockets_locked = pending_new_sockets.locked();
        client_socket = std::move(pending_new_sockets_locked.access.front());
        pending_new_sockets_locked.access.pop_front();
    }
    //Exchange version codes; close the socket if the client has an incompatible version
    uint64_t joiner_version_code;
    client_socket.exchange(my_version_hashcode, joiner_version_code);
    if(joiner_version_code != my_version_hashcode) {
        rls_default_warn("Rejected a connection from client at {}. Client was running on an incompatible platform or used an incompatible compiler.",
                         client_socket.get_remote_ip());
        return;
    }
    JoinRequest join_request;
    client_socket.read(join_request);
    if(join_request.is_external) {
        dbg_default_info("The join request is an external request from {}.", join_request.joiner_id);
        external_join_handler(client_socket, join_request.joiner_id);
    } else {
        if(active_leader) {
            pending_join_sockets.emplace_back(join_request.joiner_id, std::move(client_socket));
        } else {
            redirect_join_attempt(client_socket);
        }
    }
    return;
}
void ViewManager::external_join_handler(tcp::socket& client_socket, const node_id_t& joiner_id) {
    if(curr_view->rank_of(joiner_id) != -1) {
        // external can't have same id as any member
        client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE, getConfUInt32(CONF_DERECHO_LOCAL_ID)});
    }
    client_socket.write(JoinResponse{JoinResponseCode::OK, getConfUInt32(CONF_DERECHO_LOCAL_ID)});
    ExternalClientRequest request;
    client_socket.read(request);
    if(request == ExternalClientRequest::GET_VIEW) {
        send_view(*curr_view, client_socket);
    } else if(request == ExternalClientRequest::ESTABLISH_P2P) {
        uint16_t external_client_external_port = 0;
        client_socket.read(external_client_external_port);
        sst::add_external_node(joiner_id, {client_socket.get_remote_ip(),
                                           external_client_external_port});
        add_external_connection_upcall({joiner_id});
    }
}

void ViewManager::new_leader_takeover(DerechoSST& gmsSST) {
    bool prior_changes_found = copy_prior_leader_proposals(gmsSST);
    dbg_default_debug("Taking over as the new leader; everyone suspects prior leaders.");
    const unsigned int my_rank = gmsSST.get_local_index();
    const node_id_t my_id = curr_view->members[my_rank];
    //For any changes proposed by a previous leader but after the last end-of-view,
    //re-propose them as my own changes
    if(prior_changes_found) {
        const int my_changes_length = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
        std::size_t change_index = my_changes_length - 1;
        bool end_of_view_found = gmsSST.changes[my_rank][change_index].end_of_view;
        while(!end_of_view_found) {
            gmsSST.changes[my_rank][change_index].leader_id = my_id;
            --change_index;
            end_of_view_found = gmsSST.changes[my_rank][change_index].end_of_view;
        }
    }
    bool new_changes_proposed = false;
    //For each node that I suspect, make sure a change is proposed to remove it
    for(int rank = 0; rank < curr_view->num_members; ++rank) {
        if(gmsSST.suspected[my_rank][rank]
           && !changes_contains(gmsSST, curr_view->members[rank])) {
            last_suspected[rank] = gmsSST.suspected[my_rank][rank];
            const int next_change_index = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
            if(next_change_index == (int)gmsSST.changes.size()) {
                throw derecho_exception("Ran out of room in the pending changes list!");
            }

            gmssst::set(gmsSST.changes[my_rank][next_change_index],
                        make_change_proposal(my_id, curr_view->members[rank]));
            gmssst::increment(gmsSST.num_changes[my_rank]);
            dbg_default_debug("Leader proposed a change to remove failed node {}", curr_view->members[rank]);
            new_changes_proposed = true;
        }
    }
    if(new_changes_proposed && !prior_changes_found) {
        /* Ensure the "check for unfinished batch" predicate will run as soon as this
         * predicate is done, even if it already fired once during the current View
         * for some reason. This will take care of checking to see if the new changes
         * will make an adequate view or not. */
        auto new_view_has_changes = [this](const DerechoSST& sst) {
            return active_leader
                   && sst.num_changes[curr_view->my_rank] > sst.num_installed[curr_view->my_rank]
                   && !changes_includes_end_of_view(sst, curr_view->my_rank);
        };
        auto propose_changes_trig = [this](DerechoSST& sst) { propose_changes(sst); };
        gmsSST.predicates.insert(new_view_has_changes,
                                 propose_changes_trig,
                                 sst::PredicateType::ONE_TIME);
    }
    if(prior_changes_found) {
        //Immediately start the view-change process, since the last view proposed by the
        //old leader needs to be installed. At this point, we really hope it was adequate.
        next_view = make_next_view(curr_view, gmsSST);
        make_subgroup_maps(subgroup_info, curr_view, *next_view);
        //Push the entire new changes vector and the associated joiner_ip vectors
        gmsSST.put(gmsSST.changes);
        gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
                   gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
        gmsSST.put(gmsSST.num_changes);
    }
    //Allow this node to advance num_committed as the active leader
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
        gmssst::set(gmsSST.joiner_external_ports[myRank], gmsSST.joiner_external_ports[leader],
                    gmsSST.joiner_external_ports.size());
        gmssst::set(gmsSST.num_committed[myRank], gmsSST.num_committed[leader]);
    }

    // Acknowledge the proposed changes
    gmssst::set(gmsSST.num_acked[myRank], gmsSST.num_changes[myRank]);
    if(myRank != leader) {
        /* The leader should not push the changes information here, since it may
         * not be ready to push a batch of changes (if the next view would be
         * inadequate). Non-leaders should push the fields in order in separate
         * put() calls, to preserve ordering between dependent fields.
         */
        gmsSST.put(gmsSST.changes);
        //This pushes the contiguous set of joiner_xxx_ports fields all at once
        gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
                   gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
        gmsSST.put(gmsSST.num_changes);
        gmsSST.put(gmsSST.num_committed);
    }
    gmsSST.put(gmsSST.num_acked);
    dbg_default_debug("Wedging current view.");
    curr_view->wedge();
    dbg_default_debug("Done wedging current view.");
}

void ViewManager::start_meta_wedge(DerechoSST& gmsSST) {
    dbg_default_debug("Meta-wedging view {}", curr_view->vid);
    // Disable all the other SST predicates, except suspected_changed and the
    // one I'm about to register
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(new_sockets_handle);
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
                    dbg_default_debug("Waiting for node {} to finish persisting update {}", shard_member, last_delivered_seq_num);
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
    dbg_default_debug("Ragged trim messages are persisted, finishing view change");
    std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);

    // Disable all the other SST predicates, except suspected_changed
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(new_sockets_handle);
    gmsSST.predicates.remove(change_commit_ready_handle);
    gmsSST.predicates.remove(leader_proposed_handle);

    const node_id_t my_id = curr_view->members[curr_view->my_rank];

    //Send the new view from the leader to non-leaders in the old view using the TCP sockets
    if(active_leader) {
        for(int i = 0; i < curr_view->num_members; ++i) {
            if(i != curr_view->my_rank && !curr_view->failed[i]) {
                LockedReference<std::unique_lock<std::mutex>, tcp::socket> member_socket
                        = tcp_sockets.get_socket(curr_view->members[i]);
                send_view(*next_view, member_socket.get());
            }
        }
    } else {
        //Standard procedure for receiving a View, copied from receive_view_and_leaders
        const node_id_t leader_id = curr_view->members[curr_view->find_rank_of_leader()];
        std::size_t size_of_view;
        tcp_sockets.read(leader_id, reinterpret_cast<char*>(&size_of_view), sizeof(size_of_view));
        char buffer[size_of_view];
        tcp_sockets.read(leader_id, buffer, size_of_view);
        next_view = mutils::from_bytes<View>(nullptr, buffer);
        next_view->subgroup_type_order = subgroup_type_order;
        next_view->my_rank = next_view->rank_of(my_id);
    }

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
            send_view(*next_view, proposed_join_sockets.front().second);
            std::size_t size_of_vector = mutils::bytes_size(old_shard_leaders_by_id);
            proposed_join_sockets.front().second.write(size_of_vector);
            mutils::post_object([this](const char* bytes, std::size_t size) {
                proposed_join_sockets.front().second.write(bytes, size);
            },
                                old_shard_leaders_by_id);
            // save the socket for the commit step
            joiner_sockets.emplace_back(std::move(proposed_join_sockets.front().second));
            proposed_join_sockets.pop_front();
        }
    }

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

    // Delete the last three GMS predicates from the old SST in preparation for deleting it
    gmsSST.predicates.remove(leader_committed_handle);
    gmsSST.predicates.remove(leader_suspicion_handle);
    gmsSST.predicates.remove(follower_suspicion_handle);

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
        dbg_default_debug("Adding RDMC connection to node {}, at IP {} and port {}", next_view->members[joiner_rank], next_view->member_ips_and_ports[joiner_rank].ip_address, next_view->member_ips_and_ports[joiner_rank].rdmc_port);

#ifdef USE_VERBS_API
        // rdma::impl::verbs_add_connection(next_view->members[joiner_rank],
        //                                  next_view->member_ips_and_ports[joiner_rank], my_id);
        rdma::impl::verbs_add_connection(
                next_view->members[joiner_rank],
                {next_view->member_ips_and_ports[joiner_rank].ip_address,
                 next_view->member_ips_and_ports[joiner_rank].rdmc_port});
#else
        rdma::impl::lf_add_connection(
                next_view->members[joiner_rank],
                {next_view->member_ips_and_ports[joiner_rank].ip_address,
                 next_view->member_ips_and_ports[joiner_rank].rdmc_port});
#endif
    }
    for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
        int joiner_rank = next_view->num_members - next_view->joined.size() + i;
        sst::add_node(next_view->members[joiner_rank],
                      {next_view->member_ips_and_ports[joiner_rank].ip_address,
                       next_view->member_ips_and_ports[joiner_rank].sst_port});
    }

    // This will block until everyone responds to SST/RDMC initial handshakes
    transition_multicast_group(next_subgroup_settings, new_num_received_size, new_slot_size);
    dbg_default_debug("Done setting up SST and MulticastGroup for view {}; about to do a sync_with_members()", next_view->vid);

    // New members can now proceed to view_manager.start(), which will call sync()
    next_view->gmsSST->put();
    next_view->gmsSST->sync_with_members();
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

    // If there are already some failed members in the new view, we'll need to
    // immediately start another view change; don't let any multicasts start
    if(std::find(curr_view->failed.begin(), curr_view->failed.end(), true)
       != curr_view->failed.end()) {
        curr_view->wedge();
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
            sst::SSTParams(
                    curr_view->members, curr_view->members[curr_view->my_rank],
                    [this](const uint32_t node_id) { report_failure(node_id); },
                    curr_view->failed, false),
            num_subgroups, num_received_size, slot_size);

    curr_view->multicast_group = std::make_unique<MulticastGroup>(
            curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, callbacks, num_subgroups, subgroup_settings,
            getConfUInt32(CONF_DERECHO_HEARTBEAT_MS),
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
            sst::SSTParams(
                    next_view->members, next_view->members[next_view->my_rank],
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

bool ViewManager::receive_join(DerechoSST& gmsSST, const node_id_t joiner_id, tcp::socket& client_socket) {
    struct in_addr joiner_ip_packed;
    inet_aton(client_socket.get_remote_ip().c_str(), &joiner_ip_packed);

    const node_id_t my_id = curr_view->members[curr_view->my_rank];

    if(curr_view->rank_of(joiner_id) != -1) {
        dbg_default_warn("Joining node at IP {} announced it has ID {}, which is already in the View!", client_socket.get_remote_ip(), joiner_id);
        client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE, my_id});
        return false;
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
    uint16_t joiner_external_port = 0;
    client_socket.read(joiner_external_port);

    dbg_default_debug("Proposing change to add node {}", joiner_id);
    size_t next_change_index = gmsSST.num_changes[curr_view->my_rank]
                               - gmsSST.num_installed[curr_view->my_rank];
    if(next_change_index == gmsSST.changes.size()) {
        // This shouldn't happen because leader_start_join already checked that
        // num_changes - num_committed < members.size(), but we should check
        // anyway in case num_committed is much larger than num_installed
        throw derecho_exception("Too many changes at once! Processing a join, but ran out of room in the pending changes list.");
    }
    gmssst::set(gmsSST.changes[curr_view->my_rank][next_change_index],
                make_change_proposal(my_id, joiner_id));
    gmssst::set(gmsSST.joiner_ips[curr_view->my_rank][next_change_index],
                joiner_ip_packed.s_addr);
    gmssst::set(gmsSST.joiner_gms_ports[curr_view->my_rank][next_change_index],
                joiner_gms_port);
    gmssst::set(gmsSST.joiner_rpc_ports[curr_view->my_rank][next_change_index],
                joiner_rpc_port);
    gmssst::set(gmsSST.joiner_sst_ports[curr_view->my_rank][next_change_index],
                joiner_sst_port);
    gmssst::set(gmsSST.joiner_rdmc_ports[curr_view->my_rank][next_change_index],
                joiner_rdmc_port);
    gmssst::set(gmsSST.joiner_external_ports[curr_view->my_rank][next_change_index],
                joiner_external_port);

    gmssst::increment(gmsSST.num_changes[curr_view->my_rank]);
    //Don't actually push the proposed join yet, because we don't know if it's the last change in the batch

    if(!curr_view->is_wedged()) {
        dbg_default_debug("Wedging view {}", curr_view->vid);
        curr_view->wedge();
    }
    return true;
}

std::vector<int> ViewManager::process_suspicions(DerechoSST& gmsSST) {
    std::vector<int> failed_ranks;
    const int my_rank = curr_view->my_rank;
    int num_departed = 0;
    // Aggregate suspicions into gmsSST[myRank].Suspected;
    for(int r = 0; r < curr_view->num_members; r++) {
        for(int who = 0; who < curr_view->num_members; who++) {
            gmssst::set(gmsSST.suspected[my_rank][who],
                        gmsSST.suspected[my_rank][who] || gmsSST.suspected[r][who]);
        }
        if(gmsSST.rip[r]) {
            num_departed++;
        }
    }

    for(int curr_rank = 0; curr_rank < curr_view->num_members; curr_rank++) {
        //Check if the node at curr_rank is newly suspected
        if(gmsSST.suspected[my_rank][curr_rank] && !last_suspected[curr_rank]) {
            // This is safer than copy_suspected, since suspected[] might change during this loop
            last_suspected[curr_rank] = gmsSST.suspected[my_rank][curr_rank];
            dbg_default_debug("Marking {} failed", curr_view->members[curr_rank]);
            failed_ranks.emplace_back(curr_rank);

            if(!gmsSST.rip[my_rank] && curr_view->num_failed != 0
               && (curr_view->num_failed - num_departed >= (curr_view->num_members - num_departed + 1) / 2)) {
                if(disable_partitioning_safety) {
                    dbg_default_warn("Potential partitioning event, but partitioning safety is disabled. num_failed - num_departed = {} but num_members - num_departed + 1 = {}",
                                     curr_view->num_failed - num_departed, curr_view->num_members - num_departed + 1);
                } else {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }
            }

            dbg_default_debug("GMS telling SST to freeze row {}", curr_rank);
            gmsSST.freeze(curr_rank);
            //These two lines are the same as Vc.wedge()
            curr_view->multicast_group->wedge();
            gmssst::set(gmsSST.wedged[my_rank], true);
            //Synchronize Vc.failed with gmsSST.suspected
            curr_view->failed[curr_rank] = true;
            curr_view->num_failed++;

            if(!gmsSST.rip[my_rank] && curr_view->num_failed != 0
               && (curr_view->num_failed - num_departed >= (curr_view->num_members - num_departed + 1) / 2)) {
                if(disable_partitioning_safety) {
                    dbg_default_warn("Potential partitioning event, but partitioning safety is disabled. num_failed - num_left = {} but num_members - num_departed + 1 = {}",
                                     curr_view->num_failed - num_departed, curr_view->num_members - num_departed + 1);
                } else {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }
            }

            // push change to gmsSST.suspected[myRank]
            gmsSST.put(gmsSST.suspected);
            // push change to gmsSST.wedged[myRank]
            gmsSST.put(gmsSST.wedged);
        }
    }
    return failed_ranks;
}

void ViewManager::send_view(const View& new_view, tcp::socket& client_socket) {
    dbg_default_debug("Sending node at {} the new view", client_socket.get_remote_ip());
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
    LockedReference<std::unique_lock<std::mutex>, tcp::socket> joiner_socket = tcp_sockets.get_socket(new_node_id);
    assert(subgroup_objects.find(subgroup_id) != subgroup_objects.end());
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
        tcp_sockets.delete_node(removed_id);
    }
    for(const node_id_t& joiner_id : new_view.joined) {
        tcp_sockets.add_node(joiner_id,
                             {new_view.member_ips_and_ports[new_view.rank_of(joiner_id)].ip_address,
                              new_view.member_ips_and_ports[new_view.rank_of(joiner_id)].rpc_port});
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
            view_max_rpc_reply_payload_size = std::max(
                    profile.max_reply_msg_size - sizeof(header),
                    view_max_rpc_reply_payload_size);
            slot_size_for_subgroup = std::max(slot_size_for_shard, slot_size_for_subgroup);
            view_max_rpc_window_size = std::max(profile.window_size, view_max_rpc_window_size);

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
    }  // for(subgroup_id)

    return {num_received_offset, slot_offset};
}

std::map<subgroup_id_t, uint64_t> ViewManager::get_max_payload_sizes() {
    return max_payload_sizes;
}

std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>
ViewManager::make_member_ips_and_ports_map(const View& view, const PortType port) {
    std::map<node_id_t, std::pair<ip_addr_t, uint16_t>> member_ips_and_ports_map;
    size_t num_members = view.members.size();
    for(uint i = 0; i < num_members; ++i) {
        if(!view.failed[i]) {
            switch(port) {
                case PortType::GMS:
                    member_ips_and_ports_map[view.members[i]] = {
                            view.member_ips_and_ports[i].ip_address,
                            view.member_ips_and_ports[i].gms_port};
                    break;
                case PortType::RPC:
                    member_ips_and_ports_map[view.members[i]] = {
                            view.member_ips_and_ports[i].ip_address,
                            view.member_ips_and_ports[i].rpc_port};
                    break;
                case PortType::SST:
                    member_ips_and_ports_map[view.members[i]] = {
                            view.member_ips_and_ports[i].ip_address,
                            view.member_ips_and_ports[i].sst_port};
                    break;
                case PortType::RDMC:
                    member_ips_and_ports_map[view.members[i]] = {
                            view.member_ips_and_ports[i].ip_address,
                            view.member_ips_and_ports[i].rdmc_port};
                    break;
                case PortType::EXTERNAL:
                    member_ips_and_ports_map[view.members[i]] = {
                            view.member_ips_and_ports[i].ip_address,
                            view.member_ips_and_ports[i].external_port};
            }
        }
    }
    return member_ips_and_ports_map;
}

std::unique_ptr<View> ViewManager::make_next_view(const std::unique_ptr<View>& curr_view,
                                                  const DerechoSST& gmsSST) {
    const int32_t my_rank = curr_view->my_rank;
    const int32_t leader_rank = curr_view->find_rank_of_leader();
    std::set<int> leave_ranks;
    std::set<int> next_leave_ranks;
    std::vector<int> join_indexes;
    // Find out which changes to install by searching through the list of committed
    // changes and sorting them into joins and leaves. Changes after the end_of_view
    // marker won't be installed yet, but leaves should be marked as "failed" in the
    // next view so that the SST doesn't attempt to set up connections with them.
    const int valid_changes = gmsSST.num_changes[leader_rank]
                              - gmsSST.num_installed[leader_rank];
    bool end_of_view_found = false;
    for(int change_index = 0; change_index < valid_changes; change_index++) {
        node_id_t change_id = gmsSST.changes[my_rank][change_index].change_id;
        int change_rank = curr_view->rank_of(change_id);
        if(change_rank != -1) {
            if(end_of_view_found) {
                next_leave_ranks.emplace(change_rank);
            } else {
                leave_ranks.emplace(change_rank);
            }
        } else if(!end_of_view_found) {
            join_indexes.emplace_back(change_index);
        }
        if(gmsSST.changes[my_rank][change_index].end_of_view) {
            end_of_view_found = true;
        }
    }

    int next_num_members = curr_view->num_members - leave_ranks.size() + join_indexes.size();
    // Initialize the next view
    std::vector<node_id_t> joined, members(next_num_members), departed;
    std::vector<char> failed(next_num_members);
    std::vector<IpAndPorts> member_ips_and_ports(next_num_members);
    int next_unassigned_rank = curr_view->next_unassigned_rank;
    for(std::size_t i = 0; i < join_indexes.size(); ++i) {
        const int join_index = join_indexes[i];
        node_id_t joiner_id = gmsSST.changes[my_rank][join_index].change_id;
        struct in_addr joiner_ip_packed;
        joiner_ip_packed.s_addr = gmsSST.joiner_ips[my_rank][join_index];
        char* joiner_ip_cstr = inet_ntoa(joiner_ip_packed);
        std::string joiner_ip(joiner_ip_cstr);

        joined.emplace_back(joiner_id);
        // New members go at the end of the members list, but it may shrink in the new view
        int new_member_rank = curr_view->num_members - leave_ranks.size() + i;
        members[new_member_rank] = joiner_id;
        member_ips_and_ports[new_member_rank] = {joiner_ip,
                                                 gmsSST.joiner_gms_ports[my_rank][join_index],
                                                 gmsSST.joiner_rpc_ports[my_rank][join_index],
                                                 gmsSST.joiner_sst_ports[my_rank][join_index],
                                                 gmsSST.joiner_rdmc_ports[my_rank][join_index],
                                                 gmsSST.joiner_external_ports[my_rank][join_index]};
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
    dbg_default_debug("Next view will exclude {} failed members. Also, {} members have already failed in the next view", leave_ranks.size(), next_leave_ranks.size());

    // Copy member information, excluding the members that have been removed
    int new_rank = 0;
    for(int old_rank = 0; old_rank < curr_view->num_members; ++old_rank) {
        // This is why leave_ranks needs to be a set
        if(leave_ranks.find(old_rank) == leave_ranks.end()) {
            members[new_rank] = curr_view->members[old_rank];
            member_ips_and_ports[new_rank] = curr_view->member_ips_and_ports[old_rank];
            failed[new_rank] = curr_view->failed[old_rank];
            //Ensure that members that are about to be removed are marked as failed
            if(next_leave_ranks.find(old_rank) != next_leave_ranks.end()) {
                failed[new_rank] = true;
            }
            ++new_rank;
        }
    }

    // Initialize my_rank in next_view
    int32_t my_new_rank = -1;
    node_id_t myID = curr_view->members[my_rank];
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
        const node_id_t p(const_cast<uint16_t&>(gmsSST.changes[myRow][p_index].change_id));
        if(p == q) {
            return true;
        }
    }
    return false;
}

bool ViewManager::changes_includes_end_of_view(const DerechoSST& gmsSST, const int rank_of_leader) {
    for(int i = 0; i < gmsSST.num_changes[rank_of_leader] - gmsSST.num_installed[rank_of_leader]; ++i) {
        if(gmsSST.changes[rank_of_leader][i].end_of_view) {
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

/*
 * Note that if a prior leader proposed some changes but the change marked
 * "end of view" didn't reach any live node, we might not use those proposed
 * changes because we only copy proposals from another row if we find the "end
 * of view" marker. This should very rarely happen, since the prior leader will
 * wait to propose an entire batch of changes at once, including the "end of
 * view" change; it will only happen if the SST push of the changes vector only
 * partially finishes before the leader crashes.
 */
bool ViewManager::copy_prior_leader_proposals(DerechoSST& gmsSST) {
    const int my_rank = gmsSST.get_local_index();
    const int my_changes_length = gmsSST.num_changes[my_rank] - gmsSST.num_installed[my_rank];
    bool prior_changes_found = false;
    int prior_leader_rank = -1;
    int longest_changes_rank = my_rank;
    int longest_changes_length = my_changes_length;
    //Look through the SST in ascending rank order to see if anyone has
    //acknowledged a change from a prior leader that I do not have
    for(uint check_rank = 0; check_rank < gmsSST.get_num_rows(); ++check_rank) {
        const int changes_length = gmsSST.num_changes[check_rank]
                                   - gmsSST.num_installed[check_rank];
        bool first_prior_change_found = false;
        bool end_of_view_found = false;
        for(int i = 0; i < changes_length; ++i) {
            if(!first_prior_change_found
               && (i >= longest_changes_length
                   || (gmsSST.changes[check_rank][i].change_id != gmsSST.changes[longest_changes_rank][i].change_id
                       && gmsSST.changes[check_rank][i].leader_id >= prior_leader_rank))) {
                first_prior_change_found = true;
                prior_leader_rank = gmsSST.changes[check_rank][i].leader_id;
                dbg_default_debug("Found changes in row {} proposed by leader {}", check_rank, prior_leader_rank);
            }
            if(first_prior_change_found && !end_of_view_found
               && gmsSST.changes[check_rank][i].end_of_view) {
                end_of_view_found = true;
                //We found an entire batch of changes proposed by the previous leader, so we can use this row
                prior_changes_found = true;
                dbg_default_debug("Found end-of-view marker in row {}. Setting longest_changes_rank = {}, longest_changes_length = {}", check_rank, check_rank, changes_length);
                longest_changes_rank = check_rank;
                longest_changes_length = changes_length;
            } else if(first_prior_change_found && end_of_view_found
                      && gmsSST.changes[check_rank][i].leader_id >= prior_leader_rank) {
                //There might be changes from a succession of leaders, so we still
                //need to update prior_leader_rank after the first end-of-view is found
                prior_leader_rank = gmsSST.changes[check_rank][i].leader_id;
            }
        }
    }
    if(prior_changes_found) {
        //Note that this function is called before proposing any changes as the new leader
        //so it's OK to clobber the entire local changes vector
        dbg_default_debug("Re-proposing changes from prior leader at rank {}, found in row {}. Num_changes is now {}", prior_leader_rank, longest_changes_rank, gmsSST.num_changes[prior_leader_rank]);
        gmssst::set(gmsSST.changes[my_rank], gmsSST.changes[longest_changes_rank],
                    gmsSST.changes.size());
        gmssst::set(gmsSST.num_changes[my_rank], gmsSST.num_changes[longest_changes_rank]);
        gmssst::set(gmsSST.joiner_ips[my_rank], gmsSST.joiner_ips[longest_changes_rank],
                    gmsSST.joiner_ips.size());
        gmssst::set(gmsSST.joiner_gms_ports[my_rank], gmsSST.joiner_gms_ports[longest_changes_rank],
                    gmsSST.joiner_gms_ports.size());
        gmssst::set(gmsSST.joiner_rpc_ports[my_rank], gmsSST.joiner_rpc_ports[longest_changes_rank],
                    gmsSST.joiner_rpc_ports.size());
        gmssst::set(gmsSST.joiner_sst_ports[my_rank], gmsSST.joiner_sst_ports[longest_changes_rank],
                    gmsSST.joiner_sst_ports.size());
        gmssst::set(gmsSST.joiner_rdmc_ports[my_rank], gmsSST.joiner_rdmc_ports[longest_changes_rank],
                    gmsSST.joiner_rdmc_ports.size());
        gmssst::set(gmsSST.joiner_external_ports[my_rank], gmsSST.joiner_external_ports[longest_changes_rank],
                    gmsSST.joiner_external_ports.size());
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

std::size_t ViewManager::get_number_of_shards_in_subgroup(subgroup_type_id_t subgroup_type, uint32_t subgroup_index) {
    shared_lock_t read_lock(view_mutex);
    subgroup_id_t subgroup_id = curr_view->subgroup_ids_by_type_id.at(subgroup_type).at(subgroup_index);
    return curr_view->subgroup_shard_views.at(subgroup_id).size();
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

View& ViewManager::unsafe_get_current_view() {
    assert(curr_view);
    return *curr_view;
}

SharedLockedReference<const View> ViewManager::get_current_or_restart_view() {
    if(restart_leader_state_machine) {
        //If this node is the restart leader, the "current view" that it's trying to set up is actually
        //the restart view from RestartLeaderState, not curr_view.
        return SharedLockedReference<const View>(restart_leader_state_machine->get_restart_view(), view_mutex);
    } else {
        return SharedLockedReference<const View>(*curr_view, view_mutex);
    }
}

LockedReference<std::unique_lock<std::mutex>, tcp::socket> ViewManager::get_transfer_socket(node_id_t member_id) {
    return tcp_sockets.get_socket(member_id);
}

void ViewManager::debug_print_status() const {
    std::cout << "curr_view = " << curr_view->debug_string() << std::endl;
}
} /* namespace derecho */
