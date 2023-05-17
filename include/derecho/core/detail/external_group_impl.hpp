#include "../external_group.hpp"
#include "version_code.hpp"

namespace derecho {

template <typename T, typename ExternalGroupType>
ExternalClientCaller<T, ExternalGroupType>::ExternalClientCaller(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, ExternalGroupType& group_client)
        : node_id(nid),
          subgroup_id(subgroup_id),
          group_client(group_client),
          wrapped_this(rpc::make_remote_invoker<T>(nid, type_id, subgroup_id,
                                                   T::register_functions(), *group_client.receivers)) {
    this->client_stub_mutex = std::make_unique<std::mutex>();
}

template <typename T, typename ExternalGroupType>
template <typename CopyOfT>
std::enable_if_t<std::is_base_of_v<derecho::NotificationSupport, CopyOfT>>
ExternalClientCaller<T, ExternalGroupType>::register_notification_handler(const notification_handler_t& func) {
    std::lock_guard<std::mutex> lck(*client_stub_mutex);
    if(client_stub == nullptr) {
        // Create the support pointer
        client_stub = group_client.factories.template get<T>()();

        // We have to store this pointer in ExternalClientCaller, although it is of no use to us in the future. This is to
        // keep it as well as the lambda inside alive throughout the entire program
        remote_invocable_ptr = mutils::callFunc([&](const auto&... unpacked_functions) {
            return build_remote_invocable_class<T>(
                    node_id,
                    group_client.template get_index_of_type<T>(),
                    subgroup_id,
                    *group_client.receivers,
                    bind_to_instance(&client_stub, unpacked_functions)...);
        },
                                                T::register_functions());
    }
    // set handler
    client_stub->set_notification_handler(func);
}

template <typename T, typename ExternalGroupType>
template <typename CopyOfT>
std::enable_if_t<std::is_base_of_v<derecho::NotificationSupport, CopyOfT>>
ExternalClientCaller<T, ExternalGroupType>::unregister_notification() {
    std::lock_guard<std::mutex> lck(*client_stub_mutex);
    if(client_stub != nullptr) {
        client_stub->remove_notification_handler();
    }
}

// Factor out add_p2p_connections out of p2p_send()
template <typename T, typename ExternalGroupType>
void ExternalClientCaller<T, ExternalGroupType>::add_p2p_connection(node_id_t dest_node) {
    if(group_client.p2p_connections->contains_node(dest_node)) {
        return;
    }
    dbg_default_info("p2p connection to {} is not established yet, establishing right now.", dest_node);
    int rank = group_client.curr_view->rank_of(dest_node);
    if(rank == -1) {
        throw invalid_node_exception("Cannot send a p2p request to node "
                                     + std::to_string(dest_node) + ": it is not a member of the Group.");
    }
    tcp::socket sock(group_client.curr_view->member_ips_and_ports[rank].ip_address,
                     group_client.curr_view->member_ips_and_ports[rank].gms_port);

    JoinResponse response;
    uint64_t node_version_hashcode;
    try {
        sock.exchange(my_version_hashcode, node_version_hashcode);
        if(node_version_hashcode != my_version_hashcode) {
            throw derecho_exception("Unable to connect to Derecho member because the node is running on an incompatible platform or used an incompatible compiler.");
        }
        sock.write(JoinRequest{node_id, true});
        sock.read(response);
        if(response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Error! Derecho member refused connection because ID {} is already in use!", group_client.my_id);
            dbg_default_flush();
            throw derecho_exception("Leader rejected join, ID already in use.");
        }
        sock.write(ExternalClientRequest::ESTABLISH_P2P);
        sock.write(getConfUInt16(CONF_DERECHO_EXTERNAL_PORT));
    } catch(tcp::socket_error&) {
        throw derecho_exception("Failed to establish P2P connection: socket error while sending join request.");
    }

    assert(dest_node != node_id);
    if(!sst::add_external_node(dest_node, {group_client.curr_view->member_ips_and_ports[rank].ip_address,
                                           group_client.curr_view->member_ips_and_ports[rank].external_port})) {
        dbg_default_error("Failed to set up a TCP connection to {} on {}:{}", dest_node, group_client.curr_view->member_ips_and_ports[rank].ip_address, group_client.curr_view->member_ips_and_ports[rank].external_port);
        throw derecho_exception("Failed to establish P2P connection: sst::add_external_node failed");
    }
    group_client.p2p_connections->add_connections({dest_node});
}

template <typename T, typename ExternalGroupType>
template <rpc::FunctionTag tag, typename... Args>
auto ExternalClientCaller<T, ExternalGroupType>::p2p_send(node_id_t dest_node, Args&&... args) {
    add_p2p_connection(dest_node);

    uint64_t message_seq_num;
    auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
            [this, &dest_node, &message_seq_num](size_t size) -> uint8_t* {
                const std::size_t max_p2p_request_payload_size = getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
                if(size <= max_p2p_request_payload_size) {
                    auto buffer_handle = group_client.get_sendbuffer_ptr(dest_node,
                                                                         sst::MESSAGE_TYPE::P2P_REQUEST);
                    message_seq_num = buffer_handle.seq_num;
                    return buffer_handle.buf_ptr;
                } else {
                    throw derecho_exception("The size of serialized args exceeds the maximum message size (CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE).");
                }
            },
            std::forward<Args>(args)...);
    group_client.send_p2p_message(dest_node, subgroup_id, message_seq_num, return_pair.pending);
    return std::move(*return_pair.results);
}

template <typename... ReplicatedTypes>
void ExternalGroupClient<ReplicatedTypes...>::initialize_p2p_connections() {
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
}

template <typename... ReplicatedTypes>
ExternalGroupClient<ReplicatedTypes...>::ExternalGroupClient()
        : my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          receivers(new std::decay_t<decltype(*receivers)>()),
          // ExternalGroupClient needs to create the RPC logger since P2PConnectionManager uses it (but there is no RPCManager to create it)
          rpc_logger(LoggerFactory::createIfAbsent(LoggerFactory::RPC_LOGGER_NAME, getConfString(CONF_LOGGER_RPC_LOG_LEVEL))),
          busy_wait_before_sleep_ms(getConfUInt64(CONF_DERECHO_P2P_LOOP_BUSY_WAIT_BEFORE_SLEEP_MS)) {
    RpcLoggerPtr::initialize();
#ifdef USE_VERBS_API
    sst::verbs_initialize({},
                          std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                          my_id);
#else
    sst::lf_initialize({},
                       std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                       my_id);
#endif

    if(!get_view(INVALID_NODE_ID)) throw derecho_exception("Failed to contact the leader to request very first view.");

    initialize_p2p_connections();

    rpc_listener_thread = std::thread(&ExternalGroupClient<ReplicatedTypes...>::p2p_receive_loop, this);
}

template <typename... ReplicatedTypes>
ExternalGroupClient<ReplicatedTypes...>::ExternalGroupClient(std::function<std::unique_ptr<ReplicatedTypes>()>... factories)
        : ExternalGroupClient({}, factories...) {}

template <typename... ReplicatedTypes>
ExternalGroupClient<ReplicatedTypes...>::ExternalGroupClient(
        std::vector<DeserializationContext*> deserialization_contexts,
        std::function<std::unique_ptr<ReplicatedTypes>()>... factories)
        : my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          receivers(new std::decay_t<decltype(*receivers)>()),
#if __GNUC__ < 9
          factories(make_kind_map(factories...)),
#else
          factories(make_kind_map<NoArgFactory>(factories...)),
#endif
          rpc_logger(LoggerFactory::createIfAbsent(LoggerFactory::RPC_LOGGER_NAME, getConfString(CONF_LOGGER_RPC_LOG_LEVEL))),
          busy_wait_before_sleep_ms(getConfUInt64(CONF_DERECHO_P2P_LOOP_BUSY_WAIT_BEFORE_SLEEP_MS)) {
    RpcLoggerPtr::initialize();
    for(auto dc : deserialization_contexts) {
        rdv.push_back(dc);
    }
#ifdef USE_VERBS_API
    sst::verbs_initialize({},
                          std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                          my_id);
#else
    sst::lf_initialize({},
                       std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_EXTERNAL_PORT)}}},
                       my_id);
#endif

    dbg_default_debug("External Client startup: contacting Derecho group leader to request a view");
    if(!get_view(INVALID_NODE_ID)) throw derecho_exception("Failed to contact the leader to request very first view.");

    initialize_p2p_connections();

    rpc_listener_thread = std::thread(&ExternalGroupClient<ReplicatedTypes...>::p2p_receive_loop, this);
}

template <typename... ReplicatedTypes>
ExternalGroupClient<ReplicatedTypes...>::~ExternalGroupClient() {
    thread_shutdown = true;
    if(rpc_listener_thread.joinable()) {
        rpc_listener_thread.join();
    }
}

template <typename... ReplicatedTypes>
bool ExternalGroupClient<ReplicatedTypes...>::get_view(const node_id_t nid) {
    try {
        tcp::socket sock = (nid == INVALID_NODE_ID)
                                   ? tcp::socket(getConfString(CONF_DERECHO_LEADER_IP), getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT))
                                   : tcp::socket(curr_view->member_ips_and_ports[curr_view->rank_of(nid)].ip_address,
                                                 curr_view->member_ips_and_ports[curr_view->rank_of(nid)].gms_port, false);

        JoinResponse leader_response;
        uint64_t leader_version_hashcode;
        sock.exchange(my_version_hashcode, leader_version_hashcode);
        if(leader_version_hashcode != my_version_hashcode) {
            dbg_default_error("Leader refused connection because Derecho or compiler version did not match! Local version hashcode = {}, leader version hashcode = {}", my_version_hashcode, leader_version_hashcode);
            dbg_default_flush();
            return false;
        }
        sock.write(JoinRequest{my_id, true});
        sock.read(leader_response);
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            dbg_default_error("Leader refused connection because ID {} is already in use!", my_id);
            dbg_default_flush();
            return false;
        }
        sock.write(ExternalClientRequest::GET_VIEW);

        std::size_t size_of_view;
        sock.read(size_of_view);
        uint8_t buffer[size_of_view];
        sock.read(buffer, size_of_view);
        prev_view = std::move(curr_view);
        curr_view = mutils::from_bytes<View>(nullptr, buffer);
    } catch(tcp::connection_failure&) {
        dbg_default_error("Failed to connect to group member {} when requesting new view.", nid);
        dbg_default_flush();
        return false;
    } catch(tcp::socket_error&) {
        return false;
    }
    return true;
}

// template <typename... ReplicatedTypes>
// tcp::socket& ExternalGroup<ReplicatedTypes...>::get_socket(node_id_t nid) {
//     int rank = curr_view->rank_of(nid);
//     return tcp::socket(curr_view->member_ips_and_ports[rank].ip_address, curr_view->member_ips_and_ports[rank].external_port);
// }

template <typename... ReplicatedTypes>
void ExternalGroupClient<ReplicatedTypes...>::clean_up() {
    p2p_connections->filter_to(curr_view->members);
    sst::filter_external_to(curr_view->members);

    for(auto& fulfilled_pending_results_pair : fulfilled_pending_results) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        // For each PendingResults in this subgroup, check the departed list of each shard in
        // the subgroup, and call set_exception_for_removed_node for the departed nodes
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->lock();
            if(live_pending_results && !live_pending_results->all_responded()) {
                for(uint32_t shard_num = 0;
                    shard_num < curr_view->subgroup_shard_views[subgroup_id].size();
                    ++shard_num) {
                    for(auto removed_id : curr_view->subgroup_shard_views[subgroup_id][shard_num].departed) {
                        // This will do nothing if removed_id was never in the
                        // shard this PendingResult corresponds to
                        dbg_debug(rpc_logger, "Setting exception for removed node {} on PendingResults for subgroup {}, shard {}", removed_id, subgroup_id, shard_num);
                        live_pending_results->set_exception_for_removed_node(removed_id);
                    }
                }
                pending_results_iter++;
            } else {
                // Garbage-collect PendingResults pointers that are obsolete
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            }
        }
    }
}

template <typename... ReplicatedTypes>
bool ExternalGroupClient<ReplicatedTypes...>::update_view() {
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
std::vector<node_id_t> ExternalGroupClient<ReplicatedTypes...>::get_members() const {
    return curr_view->members;
}
template <typename... ReplicatedTypes>
std::vector<node_id_t> ExternalGroupClient<ReplicatedTypes...>::get_shard_members(uint32_t subgroup_id, uint32_t shard_num) const {
    return curr_view->subgroup_shard_views[subgroup_id][shard_num].members;
}
template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<node_id_t> ExternalGroupClient<ReplicatedTypes...>::get_shard_members(uint32_t subgroup_index, uint32_t shard_num) const {
    const subgroup_type_id_t subgroup_type_id = get_index_of_type(typeid(SubgroupType));
    const auto& subgroup_ids = curr_view->subgroup_ids_by_type_id.at(subgroup_type_id);
    const subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
    return get_shard_members(subgroup_id, shard_num);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ExternalClientCaller<SubgroupType, ExternalGroupClient<ReplicatedTypes...>>& ExternalGroupClient<ReplicatedTypes...>::get_subgroup_caller(uint32_t subgroup_index) {
    // If there is not yet an ExternalClientCaller for this subgroup type, create one now
    if(external_callers.template get<SubgroupType>().find(subgroup_index) == external_callers.template get<SubgroupType>().end()) {
        const subgroup_type_id_t subgroup_type_id = get_index_of_type(typeid(SubgroupType));
        const auto& subgroup_ids = curr_view->subgroup_ids_by_type_id.at(subgroup_type_id);
        const subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        external_callers.template get<SubgroupType>().emplace(
                subgroup_index, ExternalClientCaller<SubgroupType, ExternalGroupClient<ReplicatedTypes...>>(subgroup_type_id, my_id, subgroup_id, *this));
    }
    return external_callers.template get<SubgroupType>().at(subgroup_index);
}

template <typename... ReplicatedTypes>
sst::P2PBufferHandle ExternalGroupClient<ReplicatedTypes...>::get_sendbuffer_ptr(uint32_t dest_id, sst::MESSAGE_TYPE type) {
    std::optional<sst::P2PBufferHandle> buffer;
    do {
        try {
            buffer = p2p_connections->get_sendbuffer_ptr(dest_id, type);
        } catch(std::out_of_range& map_error) {
            throw node_removed_from_group_exception(dest_id);
        }

    } while(!buffer);
    return *buffer;
}

template <typename... ReplicatedTypes>
void ExternalGroupClient<ReplicatedTypes...>::send_p2p_message(node_id_t dest_id, subgroup_id_t dest_subgroup_id, uint64_t sequence_num, std::weak_ptr<rpc::AbstractPendingResults> pending_results_handle) {
    try {
        p2p_connections->send(dest_id, sst::MESSAGE_TYPE::P2P_REQUEST, sequence_num);
    } catch(std::out_of_range& map_error) {
        throw node_removed_from_group_exception(dest_id);
    }
    std::shared_ptr<AbstractPendingResults> pending_results = pending_results_handle.lock();
    if(pending_results) {
        pending_results->fulfill_map({dest_id});
        fulfilled_pending_results[dest_subgroup_id].push_back(pending_results_handle);
    }
}

template <typename... ReplicatedTypes>
std::exception_ptr ExternalGroupClient<ReplicatedTypes...>::receive_message(
        const rpc::Opcode& indx, const node_id_t& received_from, uint8_t const* const buf,
        std::size_t payload_size, const std::function<uint8_t*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    assert(payload_size);
    auto receiver_function_entry = receivers->find(indx);
    if(receiver_function_entry == receivers->end()) {
        dbg_error(rpc_logger, "In External Group, Received an RPC message with an invalid RPC opcode! Opcode was ({}, {}, {}, {}).",
                  indx.class_id, indx.subgroup_id, indx.function_id, indx.is_reply);
        // TODO: We should reply with some kind of "no such method" error in this case
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
void ExternalGroupClient<ReplicatedTypes...>::p2p_message_handler(node_id_t sender_id, uint8_t* msg_buf) {
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    retrieve_header(nullptr, msg_buf, payload_size, indx, received_from, flags);
    if(indx.is_reply) {
        // REPLYs can be handled here because they do not block.
        receive_message(indx, received_from, msg_buf + header_size, payload_size,
                        [](size_t _size) -> uint8_t* {
                            throw derecho::derecho_exception("A P2P reply message attempted to generate another reply");
                        });
    } else if(RPC_HEADER_FLAG_TST(flags, CASCADE)) {
        // TODO: what is the lifetime of msg_buf? discuss with Sagar to make
        // sure the buffers are safely managed.
        // for cascading messages, we create a new thread.
        throw derecho::derecho_exception("Cascading P2P Send/Queries to be implemented!");
    } else {
        // send to fifo queue.
        std::unique_lock<std::mutex> lock(request_queue_mutex);
        p2p_request_queue.emplace(sender_id, msg_buf);
        request_queue_cv.notify_one();
    }
}

template <typename... ReplicatedTypes>
void ExternalGroupClient<ReplicatedTypes...>::p2p_request_worker() {
    pthread_setname_np(pthread_self(), "eg_req_wkr");
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    size_t reply_size = 0;
    p2p_req request;

    while(!thread_shutdown) {
        {
            std::unique_lock<std::mutex> lock(request_queue_mutex);
            request_queue_cv.wait(lock, [&]() { return !p2p_request_queue.empty() || thread_shutdown; });
            if(thread_shutdown) {
                break;
            }
            request = p2p_request_queue.front();
            p2p_request_queue.pop();
        }
        retrieve_header(nullptr, request.msg_buf, payload_size, indx, received_from, flags);
        if(indx.is_reply || RPC_HEADER_FLAG_TST(flags, CASCADE)) {
            dbg_error(rpc_logger, "Invalid rpc message in fifo queue: is_reply={}, is_cascading={}",
                      indx.is_reply, RPC_HEADER_FLAG_TST(flags, CASCADE));
            throw derecho::derecho_exception("invalid rpc message in fifo queue...crash.");
        }
        // Note: In practice, ExternalGroupClient should never receive a P2P message that produces
        // a reply, since it should never need to send a reply back to a group member.
        reply_size = 0;
        uint64_t reply_seq_num = 0;
        receive_message(indx, received_from, request.msg_buf + header_size, payload_size,
                        [this, &reply_size, &reply_seq_num, &request](size_t _size) {
                            reply_size = _size;
                            if(reply_size <= p2p_connections->get_max_p2p_reply_size()) {
                                auto buffer_handle = p2p_connections->get_sendbuffer_ptr(
                                        request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY);
                                if(!buffer_handle)
                                    throw derecho_exception("Failed to allocate a buffer for a P2P reply because the send window was full!");
                                reply_seq_num = buffer_handle->seq_num;
                                return buffer_handle->buf_ptr;
                            } else {
                                throw buffer_overflow_exception("Size of a P2P reply exceeds the maximum P2P reply size.");
                            }
                        });
        if(reply_size > 0) {
            p2p_connections->send(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY, reply_seq_num);
        } else {
            // hack for now to "simulate" a reply for p2p_sends to functions that do not generate a reply
            auto buffer_handle = p2p_connections->get_sendbuffer_ptr(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY);
            assert(buffer_handle);
            dbg_trace(rpc_logger, "Sending a null reply to node {} for a void P2P call", request.sender_id);
            reinterpret_cast<size_t*>(buffer_handle->buf_ptr)[0] = 0;
            p2p_connections->send(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY, buffer_handle->seq_num);
        }
    }
}

template <typename... ReplicatedTypes>
void ExternalGroupClient<ReplicatedTypes...>::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "eg_rpc_lsnr");

    request_worker_thread = std::thread(&ExternalGroupClient<ReplicatedTypes...>::p2p_request_worker, this);

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    // loop event
    while(!thread_shutdown) {
        // No need to get a View lock here, since ExternalGroupClient doesn't have a ViewManager or view-change events
        auto optional_message = p2p_connections->probe_all();
        if(optional_message) {
            auto message_handle = optional_message.value();
            // Invalid ID means the message was empty (a null reply)
            if(message_handle.sender_id != INVALID_NODE_ID) {
                p2p_message_handler(message_handle.sender_id, message_handle.buf);
                p2p_connections->increment_incoming_seq_num(message_handle.sender_id, message_handle.type);
            }

            // update last time
            clock_gettime(CLOCK_REALTIME, &last_time);
        } else {
            clock_gettime(CLOCK_REALTIME, &cur_time);
            // check if the system has been inactive for enough time to induce sleep
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                        + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            if(time_elapsed_in_ms > busy_wait_before_sleep_ms) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
            }
        }
    }
    // stop fifo worker.
    request_queue_cv.notify_one();
    request_worker_thread.join();
}

template <typename... ReplicatedTypes>
uint32_t ExternalGroupClient<ReplicatedTypes...>::get_index_of_type(const std::type_info& ti) const {
    assert_always(((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)}) || ... || false));
    return (((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)}) ?  //
                     (index_of_type<ReplicatedTypes, ReplicatedTypes...>)
                                                                               : 0)
            + ... + 0);
    // return index_of_type<SubgroupType, ReplicatedTypes...>;
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
uint32_t ExternalGroupClient<ReplicatedTypes...>::get_index_of_type() const {
    return get_index_of_type(typeid(SubgroupType));
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
uint32_t ExternalGroupClient<ReplicatedTypes...>::get_number_of_subgroups() const {
    uint32_t type_idx = this->template get_index_of_type<SubgroupType>();
    if(curr_view->subgroup_ids_by_type_id.find(type_idx) != curr_view->subgroup_ids_by_type_id.end()) {
        return curr_view->subgroup_ids_by_type_id.at(type_idx).size();
    }
    return 0;
}

template <typename... ReplicatedTypes>
uint32_t ExternalGroupClient<ReplicatedTypes...>::get_number_of_shards(uint32_t subgroup_id) const {
    if(subgroup_id < curr_view->subgroup_shard_views.size()) {
        return curr_view->subgroup_shard_views[subgroup_id].size();
    }
    return 0;
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
uint32_t ExternalGroupClient<ReplicatedTypes...>::get_number_of_shards(uint32_t subgroup_index) const {
    if(subgroup_index < this->template get_number_of_subgroups<SubgroupType>()) {
        return get_number_of_shards(curr_view->subgroup_ids_by_type_id.at(this->template get_index_of_type<SubgroupType>())[subgroup_index]);
    }
    return 0;
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<std::vector<node_id_t>> ExternalGroupClient<ReplicatedTypes...>::get_subgroup_members(uint32_t subgroup_index) const {
    std::vector<std::vector<node_id_t>> ret;
    if(subgroup_index < this->template get_number_of_subgroups<SubgroupType>()) {
        for (const auto& sv: curr_view->subgroup_shard_views[
                curr_view->subgroup_ids_by_type_id.at(this->template get_index_of_type<SubgroupType>())[subgroup_index]]) {
            ret.push_back(sv.members);
        }
    }
    return ret;
}
}  // namespace derecho
