#include <optional>

#include <derecho/core/detail/container_template_functions.hpp>
#include <derecho/core/detail/restart_state.hpp>
#include <derecho/core/detail/version_code.hpp>
#include <derecho/utils/logger.hpp>
//This code needs access to ViewManager's static methods
#include <derecho/core/detail/view_manager.hpp>

#include <derecho/persistent/Persistent.hpp>

namespace derecho {

void RestartState::load_ragged_trim(const View& curr_view) {
    /* Iterate through all subgroups by type, rather than iterating through my_subgroups,
     * so that I have access to the type ID. This wastes time, but I don't have a map
     * from subgroup ID to subgroup_type_id within curr_view. */
    for(const auto& type_id_and_indices : curr_view.subgroup_ids_by_type_id) {
        for(uint32_t subgroup_index = 0; subgroup_index < type_id_and_indices.second.size(); ++subgroup_index) {
            subgroup_id_t subgroup_id = type_id_and_indices.second.at(subgroup_index);
            //We only care if the subgroup's ID is in my_subgroups
            auto subgroup_shard_ptr = curr_view.my_subgroups.find(subgroup_id);
            if(subgroup_shard_ptr != curr_view.my_subgroups.end()) {
                //If the subgroup ID is in my_subgroups, its value is this node's shard number
                uint32_t shard_num = subgroup_shard_ptr->second;
                std::unique_ptr<RaggedTrim> ragged_trim = persistent::loadObject<RaggedTrim>(
                        ragged_trim_filename(subgroup_id, shard_num).c_str());
                //If there was a logged ragged trim from an obsolete View, it's the same as not having a logged ragged trim
                if(ragged_trim == nullptr || ragged_trim->vid < curr_view.vid) {
                    dbg_default_debug("No ragged trim information found for subgroup {}, synthesizing it from logs", subgroup_id);
                    //Get the latest persisted version number from this subgroup's object's log
                    //(this requires converting the type ID to a std::type_index)
                    persistent::version_t last_persisted_version = persistent::getMinimumLatestPersistedVersion(curr_view.subgroup_type_order.at(type_id_and_indices.first),
                                                                                                                subgroup_index, shard_num);
                    if(last_persisted_version == persistent::INVALID_VERSION) {
                        //There was no persistent file for this object; it must have been a volatile subgroup
                        continue;
                    }
                    int32_t last_vid, last_seq_num;
                    std::tie(last_vid, last_seq_num) = persistent::unpack_version<int32_t>(last_persisted_version);
                    //Divide the sequence number into sender rank and message counter
                    uint32_t num_shard_senders = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num).num_senders();
                    int32_t last_message_counter = last_seq_num / num_shard_senders;
                    uint32_t last_sender = last_seq_num % num_shard_senders;
                    /* Fill max_received_by_sender: In round-robin order, all senders ranked below
                     * the last sender delivered last_message_counter, while all senders ranked above
                     * the last sender have only delivered last_message_counter-1. */
                    std::vector<int32_t> max_received_by_sender(num_shard_senders);
                    for(uint sender_rank = 0; sender_rank <= last_sender; ++sender_rank) {
                        max_received_by_sender[sender_rank] = last_message_counter;
                    }
                    for(uint sender_rank = last_sender + 1; sender_rank < num_shard_senders; ++sender_rank) {
                        max_received_by_sender[sender_rank] = last_message_counter - 1;
                    }
                    ragged_trim = std::make_unique<RaggedTrim>(subgroup_id, shard_num, last_vid, -1, max_received_by_sender);
                }
                //operator[] is intentional: default-construct an inner std::map at subgroup_id
                //Note that the inner map will only one entry, except on the restart leader where it will have one for every shard
                logged_ragged_trim[subgroup_id].emplace(shard_num, std::move(ragged_trim));
            }  // if(subgroup_shard_ptr != curr_view->my_subgroups.end())
        }      // for(subgroup_index)
    }
}

persistent::version_t RestartState::ragged_trim_to_latest_version(const int32_t view_id,
                                                                  const std::vector<int32_t>& max_received_by_sender) {
    uint32_t num_shard_senders = max_received_by_sender.size();
    //Determine the last deliverable sequence number using the same logic as deliver_messages_upto
    int32_t max_seq_num = 0;
    for(uint sender = 0; sender < num_shard_senders; sender++) {
        max_seq_num = std::max(max_seq_num,
                               static_cast<int32_t>(max_received_by_sender[sender] * num_shard_senders + sender));
    }
    //Make the corresponding version number using the same logic as version_message
    return persistent::combine_int32s(view_id, max_seq_num);
}

RestartLeaderState::RestartLeaderState(std::unique_ptr<View> _curr_view, RestartState& restart_state,
                                       const SubgroupInfo& subgroup_info,
                                       const node_id_t my_id)
        : curr_view(std::move(_curr_view)),
          restart_state(restart_state),
          subgroup_info(subgroup_info),
          last_known_view_members(curr_view->members.begin(), curr_view->members.end()),
          longest_log_versions(curr_view->subgroup_shard_views.size()),
          nodes_with_longest_log(curr_view->subgroup_shard_views.size()),
          my_id(my_id) {
    rejoined_node_ids.emplace(my_id);
    for(subgroup_id_t subgroup = 0; subgroup < curr_view->subgroup_shard_views.size(); ++subgroup) {
        longest_log_versions[subgroup].resize(curr_view->subgroup_shard_views[subgroup].size(), 0);
        nodes_with_longest_log[subgroup].resize(curr_view->subgroup_shard_views[subgroup].size(), -1);
    }
    //Initialize longest_logs with the RaggedTrims known locally -
    //this node will only have RaggedTrims for subgroups it belongs to
    for(const auto& subgroup_map_pair : restart_state.logged_ragged_trim) {
        for(const auto& shard_and_trim : subgroup_map_pair.second) {
            nodes_with_longest_log[subgroup_map_pair.first][shard_and_trim.first] = my_id;
            longest_log_versions[subgroup_map_pair.first][shard_and_trim.first]
                    = RestartState::ragged_trim_to_latest_version(shard_and_trim.second->vid,
                                                                  shard_and_trim.second->max_received_by_sender);
            dbg_default_trace("Latest logged persistent version for subgroup {}, shard {} initialized to {}",
                              subgroup_map_pair.first, shard_and_trim.first, longest_log_versions[subgroup_map_pair.first][shard_and_trim.first]);
        }
    }
}

void RestartLeaderState::await_quorum(tcp::connection_listener& server_socket) {
    bool ready_to_restart = false;
    int time_remaining_ms = RESTART_LEADER_TIMEOUT;
    while(time_remaining_ms > 0) {
        using namespace std::chrono;
        auto start_time = high_resolution_clock::now();
        std::optional<tcp::socket> client_socket = server_socket.try_accept(time_remaining_ms);
        auto end_time = high_resolution_clock::now();
        milliseconds time_waited = duration_cast<milliseconds>(end_time - start_time);
        time_remaining_ms -= time_waited.count();
        if(client_socket) {
            uint64_t joiner_version_code;
            client_socket->exchange(my_version_hashcode, joiner_version_code);
            if(joiner_version_code != my_version_hashcode) {
                rls_default_warn("Rejected a connection from client at {}. Client was running on an incompatible platform or used an incompatible compiler.", client_socket->get_remote_ip());
                continue;
            }
            JoinRequest join_request;
            client_socket->read(join_request);
            client_socket->write(JoinResponse{JoinResponseCode::TOTAL_RESTART, my_id});
            dbg_default_debug("Node {} rejoined", join_request.joiner_id);
            if(join_request.is_external) {
                dbg_default_debug("Rejected request from external client {} during total restart", join_request.joiner_id);
                continue;
            }
            rejoined_node_ids.emplace(join_request.joiner_id);
            //Receive and process the joining node's logs of the last known View and RaggedTrim
            receive_joiner_logs(join_request.joiner_id, *client_socket);

            //Receive the joining node's ports - this is part of the standard join logic
            uint16_t joiner_gms_port = 0;
            client_socket->read(joiner_gms_port);
            uint16_t joiner_rpc_port = 0;
            client_socket->read(joiner_rpc_port);
            uint16_t joiner_sst_port = 0;
            client_socket->read(joiner_sst_port);
            uint16_t joiner_rdmc_port = 0;
            client_socket->read(joiner_rdmc_port);
            uint16_t joiner_external_port = 0;
            client_socket->read(joiner_external_port);
            const ip_addr_t& joiner_ip = client_socket->get_remote_ip();
            rejoined_node_ips_and_ports[join_request.joiner_id] = {joiner_ip, joiner_gms_port,
                                                                   joiner_rpc_port, joiner_sst_port, joiner_rdmc_port, joiner_external_port};
            //Done receiving from this socket (for now), so store it in waiting_join_sockets for later
            waiting_join_sockets.emplace(join_request.joiner_id, std::move(*client_socket));
            //Check for quorum
            ready_to_restart = has_restart_quorum();
            //If all the members have rejoined, no need to keep waiting
            if(std::includes(rejoined_node_ids.begin(), rejoined_node_ids.end(),
                             last_known_view_members.begin(), last_known_view_members.end())) {
                return;
            }
        } else if(!ready_to_restart) {
            //Accept timed out, but we haven't heard from enough nodes yet, so reset the timer
            time_remaining_ms = RESTART_LEADER_TIMEOUT;
        }
    }
}

bool RestartLeaderState::has_restart_quorum() {
    //Compute rejoined_node_ids.intersect(last_known_view_members)
    //but with a lot of unnecessary repetitive boilerplate because it's the STL
    std::set<node_id_t> intersection_of_ids;
    std::set_intersection(rejoined_node_ids.begin(), rejoined_node_ids.end(),
                          last_known_view_members.begin(), last_known_view_members.end(),
                          std::inserter(intersection_of_ids, intersection_of_ids.end()));
    if(intersection_of_ids.size() < (last_known_view_members.size() / 2) + 1 || !contains_at_least_one_member_per_subgroup(rejoined_node_ids, *curr_view)) {
        return false;
    }
    //If we have a sufficient number of members, attempt to compute a restart view
    //If that fails, we know we don't have a quorum, but if it succeeds, we
    //both have a quorum and know the restart view
    return compute_restart_view();
}

void RestartLeaderState::receive_joiner_logs(const node_id_t& joiner_id, tcp::socket& client_socket) {
    //Receive the joining node's saved View
    std::size_t size_of_view;
    client_socket.read(size_of_view);
    char view_buffer[size_of_view];
    client_socket.read(view_buffer, size_of_view);
    std::unique_ptr<View> client_view = mutils::from_bytes<View>(nullptr, view_buffer);

    if(client_view->vid > curr_view->vid) {
        dbg_default_trace("Node {} had newer view {}, replacing view {} and discarding ragged trim",
                          joiner_id, client_view->vid, curr_view->vid);
        //The joining node has a newer View, so discard any ragged trims that are not longest-log records
        for(auto& subgroup_to_map : restart_state.logged_ragged_trim) {
            auto trim_map_iterator = subgroup_to_map.second.begin();
            while(trim_map_iterator != subgroup_to_map.second.end()) {
                if(trim_map_iterator->second->leader_id != -1) {
                    trim_map_iterator = subgroup_to_map.second.erase(trim_map_iterator);
                } else {
                    ++trim_map_iterator;
                }
            }
        }
    }
    //Receive the joining node's RaggedTrims
    std::size_t num_of_ragged_trims;
    client_socket.read(num_of_ragged_trims);
    for(std::size_t i = 0; i < num_of_ragged_trims; ++i) {
        std::size_t size_of_ragged_trim;
        client_socket.read(size_of_ragged_trim);
        char buffer[size_of_ragged_trim];
        client_socket.read(buffer, size_of_ragged_trim);
        std::unique_ptr<RaggedTrim> ragged_trim = mutils::from_bytes<RaggedTrim>(nullptr, buffer);
        dbg_default_trace("Received ragged trim for subgroup {}, shard {} from node {}",
                          ragged_trim->subgroup_id, ragged_trim->shard_num, joiner_id);
        /* If the joining node has an obsolete View, we only care about the
         * "ragged trims" if they are actually longest-log records and from
         * a newer view than any ragged trims we have for this subgroup. */
        if(client_view->vid < curr_view->vid && ragged_trim->leader_id != -1) {  //-1 means the RaggedTrim is a log report
            continue;
        }
        /* Determine if this node might end up being the "restart leader" for its subgroup
         * because it has the longest log. Note that comparing log versions implicitly
         * compares VIDs, so a ragged trim from a newer View is always "longer" */
        persistent::version_t ragged_trim_log_version = RestartState::ragged_trim_to_latest_version(ragged_trim->vid, ragged_trim->max_received_by_sender);
        if(ragged_trim_log_version > longest_log_versions[ragged_trim->subgroup_id][ragged_trim->shard_num]) {
            dbg_default_trace("Latest logged persistent version for subgroup {}, shard {} is now {}, which is at node {}",
                              ragged_trim->subgroup_id, ragged_trim->shard_num, ragged_trim_log_version, joiner_id);
            longest_log_versions[ragged_trim->subgroup_id][ragged_trim->shard_num] = ragged_trim_log_version;
            nodes_with_longest_log[ragged_trim->subgroup_id][ragged_trim->shard_num] = joiner_id;
        }
        if(client_view->vid <= curr_view->vid) {
            //In both of these cases, only keep the ragged trim if it is newer than anything we have
            auto existing_ragged_trim = restart_state.logged_ragged_trim[ragged_trim->subgroup_id].find(ragged_trim->shard_num);
            if(existing_ragged_trim == restart_state.logged_ragged_trim[ragged_trim->subgroup_id].end()) {
                dbg_default_trace("Adding node {}'s ragged trim to map, because we don't have one for shard ({}, {})",
                                  joiner_id, ragged_trim->subgroup_id, ragged_trim->shard_num);
                //operator[] is intentional: Default-construct an inner std::map if one doesn't exist at this ID
                restart_state.logged_ragged_trim[ragged_trim->subgroup_id].emplace(ragged_trim->shard_num,
                                                                                   std::move(ragged_trim));
            } else if(existing_ragged_trim->second->vid <= ragged_trim->vid) {
                existing_ragged_trim->second = std::move(ragged_trim);
            }
        } else {
            //The client had a newer View, so accept everything it sends
            restart_state.logged_ragged_trim[ragged_trim->subgroup_id].emplace(ragged_trim->shard_num,
                                                                               std::move(ragged_trim));
        }
    }
    //Replace curr_view if the client's view was newer
    if(client_view->vid > curr_view->vid) {
        client_view->subgroup_type_order = curr_view->subgroup_type_order;
        curr_view.swap(client_view);
        //Remake the std::set version of curr_view->members
        last_known_view_members.clear();
        last_known_view_members.insert(curr_view->members.begin(), curr_view->members.end());
    }
}

bool RestartLeaderState::compute_restart_view() {
    restart_view = update_curr_and_next_restart_view();
    ViewManager::make_subgroup_maps(subgroup_info, curr_view, *restart_view);
    return restart_view->is_adequately_provisioned;
}

int64_t RestartLeaderState::send_restart_view() {
    members_sent_restart_view.clear();
    for(auto waiting_sockets_iter = waiting_join_sockets.begin();
        waiting_sockets_iter != waiting_join_sockets.end();) {
        std::size_t view_buffer_size = mutils::bytes_size(*restart_view);
        std::size_t leaders_buffer_size = mutils::bytes_size(nodes_with_longest_log);
        char view_buffer[view_buffer_size];
        char leaders_buffer[leaders_buffer_size];
        bool send_success;
        //Within this try block, any send that returns failure throws the ID of the node that failed
        try {
            dbg_default_debug("Sending post-recovery view {} to node {}", restart_view->vid, waiting_sockets_iter->first);
            send_success = waiting_sockets_iter->second.write(view_buffer_size);
            if(!send_success) {
                throw waiting_sockets_iter->first;
            }
            mutils::to_bytes(*restart_view, view_buffer);
            send_success = waiting_sockets_iter->second.write(view_buffer, view_buffer_size);
            if(!send_success) {
                throw waiting_sockets_iter->first;
            }
            dbg_default_debug("Sending ragged-trim information to node {}", waiting_sockets_iter->first);
            std::size_t num_ragged_trims = multimap_size(restart_state.logged_ragged_trim);
            send_success = waiting_sockets_iter->second.write(num_ragged_trims);
            if(!send_success) {
                throw waiting_sockets_iter->first;
            }
            //Unroll the maps and send each RaggedTrim individually, since it contains its subgroup_id and shard_num
            for(const auto& subgroup_to_shard_map : restart_state.logged_ragged_trim) {
                for(const auto& shard_trim_pair : subgroup_to_shard_map.second) {
                    std::size_t trim_buffer_size = mutils::bytes_size(*shard_trim_pair.second);
                    char trim_buffer[trim_buffer_size];
                    send_success = waiting_sockets_iter->second.write(trim_buffer_size);
                    if(!send_success) {
                        throw waiting_sockets_iter->first;
                    }
                    mutils::to_bytes(*shard_trim_pair.second, trim_buffer);
                    send_success = waiting_sockets_iter->second.write(trim_buffer, trim_buffer_size);
                    if(!send_success) {
                        throw waiting_sockets_iter->first;
                    }
                }
            }
            dbg_default_debug("Sending longest-log locations to node {}", waiting_sockets_iter->first);
            send_success = waiting_sockets_iter->second.write(leaders_buffer_size);
            if(!send_success) {
                throw waiting_sockets_iter->first;
            }
            mutils::to_bytes(nodes_with_longest_log, leaders_buffer);
            send_success = waiting_sockets_iter->second.write(leaders_buffer, leaders_buffer_size);
            if(!send_success) {
                throw waiting_sockets_iter->first;
            }
            members_sent_restart_view.emplace(waiting_sockets_iter->first);
            waiting_sockets_iter++;
        } catch(node_id_t failed_node) {
            //All send failures will end up here.
            //Close the failed socket, delete it from rejoined_node_ids, and return the ID of the failed node.
            waiting_join_sockets.erase(waiting_sockets_iter);
            rejoined_node_ips_and_ports.erase(failed_node);
            rejoined_node_ids.erase(failed_node);
            return failed_node;
        }
    }  //for (waiting_join_sockets)

    //Save this to a class member so that we still have it in send_objects_if_total_restart()
    restart_state.restart_shard_leaders = nodes_with_longest_log;
    //Return -1 to indicate success: no node failed.
    return -1;
}

bool RestartLeaderState::resend_view_until_quorum_lost() {
    bool success = false;
    //This method is only called after at least one failure, so first recompute the view
    bool can_retry = has_restart_quorum();
    while(can_retry) {
        int64_t failed_node_id = send_restart_view();
        if(failed_node_id != -1) {
            dbg_default_warn("Recomputed View would still have been adequate, but node {} failed while sending it!", failed_node_id);
            send_abort();
            //Recompute the restart view again, and try again if it's still adequate
            can_retry = has_restart_quorum();
        } else {
            //Successfully sent the recomputed View to all remaining nodes, so we can stop retrying
            success = true;
            break;
        }
    }
    //If we reached this point and success is still false, we lost the quorum
    return success;
}

void RestartLeaderState::send_abort() {
    for(const node_id_t& member_sent_view : members_sent_restart_view) {
        dbg_default_debug("Sending view abort message to node {}", member_sent_view);
        waiting_join_sockets.at(member_sent_view).write(CommitMessage::ABORT);
    }
}

int64_t RestartLeaderState::send_prepare() {
    for(auto waiting_sockets_iter = waiting_join_sockets.begin();
        waiting_sockets_iter != waiting_join_sockets.end();) {
        bool socket_success;
        try {
            dbg_default_debug("Sending view prepare message to node {}", waiting_sockets_iter->first);
            socket_success = waiting_sockets_iter->second.write(CommitMessage::PREPARE);
            if(!socket_success) {
                throw waiting_sockets_iter->first;
            }
            //Wait for an acknowledgment, to make sure the node has finished state transfer
            CommitMessage response;
            socket_success = waiting_sockets_iter->second.read(response);
            if(!socket_success) {
                throw waiting_sockets_iter->first;
            }
            if(response == CommitMessage::ACK) {
                dbg_default_debug("Node {} acknowledged Prepare", waiting_sockets_iter->first);
            } else {
                dbg_default_warn("Node {} responded to Prepare with something other than Ack!", waiting_sockets_iter->first);
                throw waiting_sockets_iter->first;
            }
        } catch(node_id_t failed_node) {
            waiting_join_sockets.erase(waiting_sockets_iter);
            rejoined_node_ips_and_ports.erase(failed_node);
            rejoined_node_ids.erase(failed_node);
            return failed_node;
        }
        waiting_sockets_iter++;
    }
    return -1;
}

void RestartLeaderState::send_commit() {
    for(auto waiting_sockets_iter = waiting_join_sockets.begin();
        waiting_sockets_iter != waiting_join_sockets.end();) {
        dbg_default_debug("Sending view commit message to node {}", waiting_sockets_iter->first);
        waiting_sockets_iter->second.write(CommitMessage::COMMIT);
        waiting_sockets_iter = waiting_join_sockets.erase(waiting_sockets_iter);
    }
}

void RestartLeaderState::print_longest_logs() const {
    std::ostringstream leader_list;
    for(subgroup_id_t subgroup = 0; subgroup < longest_log_versions.size(); ++subgroup) {
        for(uint32_t shard = 0; shard < longest_log_versions.at(subgroup).size(); ++shard) {
            leader_list << "Subgroup (" << subgroup << "," << shard << "): node "
                        << nodes_with_longest_log.at(subgroup).at(shard) << " with log length "
                        << longest_log_versions.at(subgroup).at(shard) << ". ";
        }
    }
    dbg_default_debug("Restart subgroup/shard leaders: {}", leader_list.str());
}

std::unique_ptr<View> RestartLeaderState::update_curr_and_next_restart_view() {
    //Nodes that were not in the last view but have restarted will immediately "join" in the new view
    std::vector<node_id_t> nodes_to_add_in_next_view;
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>> ips_and_ports_to_add_in_next_view;
    for(const auto& id_socket_pair : waiting_join_sockets) {
        node_id_t joiner_id = id_socket_pair.first;
        int joiner_rank = curr_view->rank_of(joiner_id);
        if(joiner_rank == -1) {
            nodes_to_add_in_next_view.emplace_back(joiner_id);
            ips_and_ports_to_add_in_next_view.emplace_back(rejoined_node_ips_and_ports.at(joiner_id));
            //If this node had been marked as failed, but was still in the view, un-fail it
        } else if(curr_view->failed[joiner_rank] == true) {
            curr_view->failed[joiner_rank] = false;
            curr_view->num_failed--;
        }
    }
    //Ensure the restart leader itself will be in the next view
    if(curr_view->rank_of(my_id) == -1) {
        nodes_to_add_in_next_view.emplace_back(my_id);
        ips_and_ports_to_add_in_next_view.emplace_back(getConfString(CONF_DERECHO_LOCAL_IP),
                                                       getConfUInt16(CONF_DERECHO_GMS_PORT),
                                                       getConfUInt16(CONF_DERECHO_RPC_PORT),
                                                       getConfUInt16(CONF_DERECHO_SST_PORT),
                                                       getConfUInt16(CONF_DERECHO_RDMC_PORT),
                                                       getConfUInt16(CONF_DERECHO_EXTERNAL_PORT));
    }
    //Mark any nodes from the last view that haven't yet responded as failed
    for(std::size_t rank = 0; rank < curr_view->members.size(); ++rank) {
        if(rejoined_node_ids.count(curr_view->members[rank]) == 0
           && !curr_view->failed[rank]) {
            curr_view->failed[rank] = true;
            curr_view->num_failed++;
        }
    }

    //Compute the next view, which will include all the members currently rejoining and remove the failed ones
    return make_next_view(curr_view, nodes_to_add_in_next_view, ips_and_ports_to_add_in_next_view);
}

std::unique_ptr<View> RestartLeaderState::make_next_view(const std::unique_ptr<View>& curr_view,
                                                         const std::vector<node_id_t>& joiner_ids,
                                                         const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& joiner_ips_and_ports) {
    int next_num_members = curr_view->num_members - curr_view->num_failed + joiner_ids.size();
    std::vector<node_id_t> members(next_num_members), departed;
    std::vector<char> failed(next_num_members);
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>> member_ips_and_ports(next_num_members);
    int next_unassigned_rank = curr_view->next_unassigned_rank;
    std::set<int> leave_ranks;
    for(std::size_t rank = 0; rank < curr_view->failed.size(); ++rank) {
        if(curr_view->failed[rank]) {
            leave_ranks.emplace(rank);
        }
    }
    for(std::size_t i = 0; i < joiner_ids.size(); ++i) {
        int new_member_rank = curr_view->num_members - leave_ranks.size() + i;
        members[new_member_rank] = joiner_ids[i];
        member_ips_and_ports[new_member_rank] = joiner_ips_and_ports[i];
        dbg_default_debug("Restarted next view will add new member with id {}", joiner_ids[i]);
    }
    for(const auto& leaver_rank : leave_ranks) {
        departed.emplace_back(curr_view->members[leaver_rank]);
        //Decrement next_unassigned_rank for every failure, unless the failure wasn't assigned to a subgroup anyway
        if(leaver_rank <= curr_view->next_unassigned_rank) {
            next_unassigned_rank--;
        }
    }
    dbg_default_debug("Next view will exclude {} failed members.", leave_ranks.size());
    //Copy member information, excluding the members that have failed
    int new_rank = 0;
    for(int old_rank = 0; old_rank < curr_view->num_members; ++old_rank) {
        //This is why leave_ranks needs to be a set
        if(leave_ranks.find(old_rank) == leave_ranks.end()) {
            members[new_rank] = curr_view->members[old_rank];
            member_ips_and_ports[new_rank] = curr_view->member_ips_and_ports[old_rank];
            failed[new_rank] = curr_view->failed[old_rank];
            ++new_rank;
        }
    }

    //Initialize my_rank in next_view
    //Note that the restart leader might not be a member of curr_view, so we can't use curr_view->my_rank
    int32_t my_new_rank = -1;
    const uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    for(int i = 0; i < next_num_members; ++i) {
        if(members[i] == my_id) {
            my_new_rank = i;
            break;
        }
    }
    if(my_new_rank == -1) {
        dbg_default_flush();
        throw derecho_exception("Recovery leader wasn't in the next view it computed?!?!");
    }

    auto next_view = std::make_unique<View>(curr_view->vid + 1, members, member_ips_and_ports, failed,
                                            joiner_ids, departed, my_new_rank, next_unassigned_rank,
                                            curr_view->subgroup_type_order);
    return next_view;
}

bool RestartLeaderState::contains_at_least_one_member_per_subgroup(std::set<node_id_t> rejoined_node_ids, const View& last_view) {
    for(const auto& shard_view_vector : last_view.subgroup_shard_views) {
        for(const SubView& shard_view : shard_view_vector) {
            //If none of the former members of this shard are in the restart set, it is insufficient
            bool shard_member_restarted = false;
            for(const node_id_t member_node : shard_view.members) {
                if(rejoined_node_ids.find(member_node) != rejoined_node_ids.end()) {
                    shard_member_restarted = true;
                }
            }
            if(!shard_member_restarted) {
                return false;
            }
        }
    }
    return true;
}

} /* namespace derecho */
