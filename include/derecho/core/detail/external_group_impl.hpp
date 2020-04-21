#include "../external_group.hpp"
#include "version_code.hpp"
namespace derecho {

template <typename T, typename ExternalGroupType>
ExternalClientCaller<T, ExternalGroupType>::ExternalClientCaller(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, ExternalGroupType& group)
        : node_id(nid),
          subgroup_id(subgroup_id),
          group(group),
          wrapped_this(rpc::make_remote_invoker<T>(nid, type_id, subgroup_id,
                                                   T::register_functions(), *group.receivers)) {}

template <typename T, typename ExternalGroupType>
template <rpc::FunctionTag tag, typename... Args>
auto ExternalClientCaller<T, ExternalGroupType>::p2p_send(node_id_t dest_node, Args&&... args) {
    if (!group.p2p_connections->contains_node(dest_node)) {
        dbg_default_info("p2p connection to {} is not establised yet, establishing right now.", dest_node);
        int rank = group.curr_view->rank_of(dest_node);
        if(rank == -1) {
            throw invalid_node_exception("Cannot send a p2p request to node "
                                        + std::to_string(dest_node) + ": it is not a member of the Group.");
        }
        tcp::socket sock(group.curr_view->member_ips_and_ports[rank].ip_address, group.curr_view->member_ips_and_ports[rank].gms_port);

        JoinResponse leader_response;
        bool success;
        uint64_t leader_version_hashcode;
        success = sock.exchange(my_version_hashcode, leader_version_hashcode);
        if(!success) throw derecho_exception("Failed to exchange version hashcodes with the leader! Leader has crashed.");
        if(leader_version_hashcode != my_version_hashcode) {
            throw derecho_exception("Unable to connect to Derecho leader because the leader is running on an incompatible platform or used an incompatible compiler.");
        }
        success = sock.write(JoinRequest{node_id, true});
        success = sock.read(leader_response);
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Error! Leader refused connection because ID {} is already in use!", group.my_id);
            dbg_default_flush();
            throw derecho_exception("Leader rejected join, ID already in use.");
        }
        success = sock.write(ExternalClientRequest::ESTABLISH_P2P);
        success = sock.write(getConfUInt16(CONF_DERECHO_EXTERNAL_PORT));
        if (!success) {
            throw derecho_exception("Failed to establish P2P connection.");
        }

        assert(dest_node != node_id);
        sst::add_external_node(dest_node, {group.curr_view->member_ips_and_ports[rank].ip_address,
                group.curr_view->member_ips_and_ports[rank].external_port});
        group.p2p_connections->add_connections({dest_node});
    }

    auto return_pair = wrapped_this->template send<tag>(
            [this, &dest_node](size_t size) -> char* {
                const std::size_t max_p2p_request_payload_size = getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
                if(size <= max_p2p_request_payload_size) {
                    return (char*)group.get_sendbuffer_ptr(dest_node,
                                                           sst::REQUEST_TYPE::P2P_REQUEST);
                } else {
                    throw derecho_exception("The size of serialized args exceeds the maximum message size (CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE).");
                }
            },
            std::forward<Args>(args)...);
    group.finish_p2p_send(dest_node, subgroup_id, return_pair.pending);
    return std::move(return_pair.results);
}

template <typename... ReplicatedTypes>
ExternalGroup<ReplicatedTypes...>::ExternalGroup(IDeserializationContext* deserialization_context)
        : my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          receivers(new std::decay_t<decltype(*receivers)>()) {
    if(deserialization_context != nullptr) {
        rdv.push_back(deserialization_context);
    }
#ifdef USE_VERBS_API
    sst::verbs_initialize({},
                          std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{
                            my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                          my_id);
#else
    sst::lf_initialize({},
                       std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{
                           my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                       my_id);
#endif

    if (!get_view(INVALID_NODE_ID)) throw derecho_exception("Failed to contact the leader to request very first view.");

    uint64_t view_max_rpc_reply_payload_size = 0;
    uint32_t view_max_rpc_window_size = 0;

    for(subgroup_id_t subgroup_id = 0; subgroup_id < curr_view->subgroup_shard_views.size(); ++subgroup_id) {
        uint64_t max_payload_size = 0;
        uint32_t num_shards = curr_view->subgroup_shard_views.at(subgroup_id).size();
        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
            const DerechoParams& profile = DerechoParams::from_profile(shard_view.profile);
            uint64_t payload_size = profile.max_msg_size - sizeof(header);
            max_payload_size = std::max(payload_size, max_payload_size);
            view_max_rpc_reply_payload_size = std::max(
                    profile.max_reply_msg_size - sizeof(header),
                    view_max_rpc_reply_payload_size);
            view_max_rpc_window_size = std::max(profile.window_size, view_max_rpc_window_size);
        }
        max_payload_sizes[subgroup_id] = max_payload_size;
    }

    p2p_connections = std::make_unique<sst::P2PConnectionManager>(sst::P2PParams{
            my_id,
            getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE),
            view_max_rpc_window_size,
            getConfUInt64(CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE) + sizeof(header),
            getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE) + sizeof(header),
            view_max_rpc_reply_payload_size + sizeof(header),
            true,
            NULL});

    rpc_thread = std::thread(&ExternalGroup<ReplicatedTypes...>::p2p_receive_loop, this);
}  // namespace derecho

template <typename... ReplicatedTypes>
ExternalGroup<ReplicatedTypes...>::~ExternalGroup() {
    thread_shutdown = true;
    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
}

template <typename... ReplicatedTypes>
bool ExternalGroup<ReplicatedTypes...>::get_view(const node_id_t nid) {
    try {
        tcp::socket sock = (nid == INVALID_NODE_ID) ? 
        tcp::socket(getConfString(CONF_DERECHO_LEADER_IP), getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT)):
        tcp::socket(curr_view->member_ips_and_ports[curr_view->rank_of(nid)].ip_address,
                    curr_view->member_ips_and_ports[curr_view->rank_of(nid)].gms_port,
                    false);
        
        JoinResponse leader_response;
        bool success;
        uint64_t leader_version_hashcode;
        success = sock.exchange(my_version_hashcode, leader_version_hashcode);
        if(!success) return false;
        if(leader_version_hashcode != my_version_hashcode) {
            return false;
        }
        success = sock.write(JoinRequest{my_id, true});
        success = sock.read(leader_response);
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Error! Leader refused connection because ID {} is already in use!", my_id);
            dbg_default_flush();
            return false;
        }
        success = sock.write(ExternalClientRequest::GET_VIEW);

        std::size_t size_of_view;
        success = sock.read(size_of_view);
        if(!success) {
            return false;
        }
        char buffer[size_of_view];
        success = sock.read(buffer, size_of_view);
        if(!success) {
            return false;
        }
        prev_view = std::move(curr_view);
        curr_view = mutils::from_bytes<View>(nullptr, buffer);
        return success;
    } catch(tcp::connection_failure) {
        dbg_default_error("Failed to connect to group member {} when reqeusting new view.", nid);
        dbg_default_flush();
        return false;
    }
}

// template <typename... ReplicatedTypes>
// tcp::socket& ExternalGroup<ReplicatedTypes...>::get_socket(node_id_t nid) {
//     int rank = curr_view->rank_of(nid);
//     return tcp::socket(curr_view->member_ips_and_ports[rank].ip_address, curr_view->member_ips_and_ports[rank].external_port);
// }

template <typename... ReplicatedTypes>
void ExternalGroup<ReplicatedTypes...>::clean_up() {
    p2p_connections->filter_to(curr_view->members);
    sst::filter_external_to(curr_view->members);

    for(auto& fulfilled_pending_results_pair : fulfilled_pending_results) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        //For each PendingResults in this subgroup, check the departed list of each shard in
        //the subgroup, and call set_exception_for_removed_node for the departed nodes
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            //Garbage-collect PendingResults references that are obsolete
            if(pending_results_iter->get().all_responded()) {
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            } else {
                for(uint32_t shard_num = 0;
                    shard_num < curr_view->subgroup_shard_views[subgroup_id].size();
                    ++shard_num) {
                    for(auto removed_id : curr_view->subgroup_shard_views[subgroup_id][shard_num].departed) {
                        //This will do nothing if removed_id was never in the
                        //shard this PendingResult corresponds to
                        dbg_default_debug("Setting exception for removed node {} on PendingResults for subgroup {}, shard {}", removed_id, subgroup_id, shard_num);
                        pending_results_iter->get().set_exception_for_removed_node(removed_id);
                    }
                }
                pending_results_iter++;
            }
        }
    }
}

template <typename... ReplicatedTypes>
bool ExternalGroup<ReplicatedTypes...>::update_view() {
    for(auto& nid : curr_view->members) {
        if(get_view(nid)) {
            dbg_default_debug("Successfully got new view from {} ", nid);
            clean_up();
            return true;
        }
    }
    return false;
}
template <typename... ReplicatedTypes>
std::vector<node_id_t> ExternalGroup<ReplicatedTypes...>::get_members() {
    return curr_view->members;
}
template <typename... ReplicatedTypes>
std::vector<node_id_t> ExternalGroup<ReplicatedTypes...>::get_shard_members(uint32_t subgroup_id, uint32_t shard_num) {
    return curr_view->subgroup_shard_views[subgroup_id][shard_num].members;
}
template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<node_id_t> ExternalGroup<ReplicatedTypes...>::get_shard_members(uint32_t subgroup_index, uint32_t shard_num) {
    const subgroup_type_id_t subgroup_type_id = get_index_of_type(typeid(SubgroupType));
    const auto& subgroup_ids = curr_view->subgroup_ids_by_type_id.at(subgroup_type_id);
    const subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
    return get_shard_members(subgroup_id, shard_num);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ExternalClientCaller<SubgroupType, ExternalGroup<ReplicatedTypes...>>& ExternalGroup<ReplicatedTypes...>::get_subgroup_caller(uint32_t subgroup_index) {
    if(external_callers.template get<SubgroupType>().find(subgroup_index) == external_callers.template get<SubgroupType>().end()) {
        const subgroup_type_id_t subgroup_type_id = get_index_of_type(typeid(SubgroupType));
        const auto& subgroup_ids = curr_view->subgroup_ids_by_type_id.at(subgroup_type_id);
        const subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        external_callers.template get<SubgroupType>().emplace(
                subgroup_index, ExternalClientCaller<SubgroupType, ExternalGroup<ReplicatedTypes...>>(subgroup_type_id, my_id, subgroup_id, *this));
    }
    return external_callers.template get<SubgroupType>().at(subgroup_index);
}

template <typename... ReplicatedTypes>
volatile char* ExternalGroup<ReplicatedTypes...>::get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type) {
    volatile char* buf;
    do {
        try {
            buf = p2p_connections->get_sendbuffer_ptr(dest_id, type);
        } catch(std::out_of_range& map_error) {
            throw node_removed_from_group_exception(dest_id);
        }

    } while(!buf);
    return buf;
}

template <typename... ReplicatedTypes>
void ExternalGroup<ReplicatedTypes...>::finish_p2p_send(node_id_t dest_id, subgroup_id_t dest_subgroup_id, rpc::PendingBase& pending_results_handle) {
    try {
        p2p_connections->send(dest_id);
    } catch(std::out_of_range& map_error) {
        throw node_removed_from_group_exception(dest_id);
    }
    pending_results_handle.fulfill_map({dest_id});
    fulfilled_pending_results[dest_subgroup_id].push_back(pending_results_handle);
}

template <typename... ReplicatedTypes>
std::exception_ptr ExternalGroup<ReplicatedTypes...>::receive_message(
        const rpc::Opcode& indx, const node_id_t& received_from, char const* const buf,
        std::size_t payload_size, const std::function<char*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    assert(payload_size);
    auto receiver_function_entry = receivers->find(indx);
    if(receiver_function_entry == receivers->end()) {
        dbg_default_error("Received an RPC message with an invalid RPC opcode! Opcode was ({}, {}, {}, {}).",
                          indx.class_id, indx.subgroup_id, indx.function_id, indx.is_reply);
        //TODO: We should reply with some kind of "no such method" error in this case
        return std::exception_ptr{};
    }
    std::size_t reply_header_size = header_space();
    recv_ret reply_return = receiver_function_entry->second(
            &rdv, received_from, buf,
            [&out_alloc, &reply_header_size](std::size_t size) {
                return out_alloc(size + reply_header_size) + reply_header_size;
            });
    auto* reply_buf = reply_return.payload;
    if(reply_buf) {
        reply_buf -= reply_header_size;
        const auto id = reply_return.opcode;
        const auto size = reply_return.size;
        populate_header(reply_buf, size, id, my_id, 0);
    }
    return reply_return.possible_exception;
}

template <typename... ReplicatedTypes>
void ExternalGroup<ReplicatedTypes...>::p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size) {
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    retrieve_header(nullptr, msg_buf, payload_size, indx, received_from, flags);
    size_t reply_size = 0;
    if(indx.is_reply) {
        // REPLYs can be handled here because they do not block.
        receive_message(indx, received_from, msg_buf + header_size, payload_size,
                        [this, &buffer_size, &reply_size, &sender_id](size_t _size) -> char* {
                            reply_size = _size;
                            if(reply_size <= buffer_size) {
                                return (char*)p2p_connections->get_sendbuffer_ptr(
                                        sender_id, sst::REQUEST_TYPE::P2P_REPLY);
                            }
                            return nullptr;
                        });
        if(reply_size > 0) {
            p2p_connections->send(sender_id);
        }
    } else if(RPC_HEADER_FLAG_TST(flags, CASCADE)) {
        // TODO: what is the lifetime of msg_buf? discuss with Sagar to make
        // sure the buffers are safely managed.
        // for cascading messages, we create a new thread.
        throw derecho::derecho_exception("Cascading P2P Send/Queries to be implemented!");
    } else {
        // send to fifo queue.
        std::unique_lock<std::mutex> lock(fifo_queue_mutex);
        fifo_queue.emplace(sender_id, msg_buf, buffer_size);
        fifo_queue_cv.notify_one();
    }
}

template <typename... ReplicatedTypes>
void ExternalGroup<ReplicatedTypes...>::fifo_worker() {
    pthread_setname_np(pthread_self(), "fifo_thread");
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    size_t reply_size = 0;
    fifo_req request;

    while(!thread_shutdown) {
        {
            std::unique_lock<std::mutex> lock(fifo_queue_mutex);
            fifo_queue_cv.wait(lock, [&]() { return !fifo_queue.empty() || thread_shutdown; });
            if(thread_shutdown) {
                break;
            }
            request = fifo_queue.front();
            fifo_queue.pop();
        }
        retrieve_header(nullptr, request.msg_buf, payload_size, indx, received_from, flags);
        if(indx.is_reply || RPC_HEADER_FLAG_TST(flags, CASCADE)) {
            dbg_default_error("Invalid rpc message in fifo queue: is_reply={}, is_cascading={}",
                              indx.is_reply, RPC_HEADER_FLAG_TST(flags, CASCADE));
            throw derecho::derecho_exception("invalid rpc message in fifo queue...crash.");
        }
        receive_message(indx, received_from, request.msg_buf + header_size, payload_size,
                        [this, &reply_size, &request](size_t _size) -> char* {
                            reply_size = _size;
                            if(reply_size <= request.buffer_size) {
                                return (char*)p2p_connections->get_sendbuffer_ptr(
                                        request.sender_id, sst::REQUEST_TYPE::P2P_REPLY);
                            }
                            return nullptr;
                        });
        if(reply_size > 0) {
            p2p_connections->send(request.sender_id);
        } else {
            // hack for now to "simulate" a reply for p2p_sends to functions that do not generate a reply
            char* buf = p2p_connections->get_sendbuffer_ptr(request.sender_id, sst::REQUEST_TYPE::P2P_REPLY);
            buf[0] = 0;
            p2p_connections->send(request.sender_id);
        }
    }
}

template <typename... ReplicatedTypes>
void ExternalGroup<ReplicatedTypes...>::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "rpc_thread");

    uint64_t max_payload_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    // set the thread local rpc_handler context
    // _in_rpc_handler = true;

    fifo_worker_thread = std::thread(&ExternalGroup<ReplicatedTypes...>::fifo_worker, this);

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    // loop event
    while(!thread_shutdown) {
        std::unique_lock<std::mutex> connections_lock(p2p_connections_mutex);
        auto optional_reply_pair = p2p_connections->probe_all();
        if(optional_reply_pair) {
            auto reply_pair = optional_reply_pair.value();
            if (reply_pair.first != INVALID_NODE_ID) {
                p2p_message_handler(reply_pair.first, (char*)reply_pair.second, max_payload_size);
                p2p_connections->update_incoming_seq_num();
            }

            // update last time
            clock_gettime(CLOCK_REALTIME, &last_time);
        } else {
            clock_gettime(CLOCK_REALTIME, &cur_time);
            // check if the system has been inactive for enough time to induce sleep
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                        + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            if(time_elapsed_in_ms > 1) {
                connections_lock.unlock();
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
                connections_lock.lock();
            }
        }
    }
    // stop fifo worker.
    fifo_queue_cv.notify_one();
    fifo_worker_thread.join();
}

template <typename... ReplicatedTypes>
uint32_t ExternalGroup<ReplicatedTypes...>::get_index_of_type(const std::type_info& ti) {
    assert_always((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)} || ... || false));
    return (((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)}) ?  //
                     (index_of_type<ReplicatedTypes, ReplicatedTypes...>)
                                                                               : 0)
            + ... + 0);
    //return index_of_type<SubgroupType, ReplicatedTypes...>;
}
}  // namespace derecho
