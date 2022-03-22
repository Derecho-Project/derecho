/**
 * @file rpc_manager.cpp
 *
 * @date Feb 7, 2017
 */

#include "derecho/core/detail/rpc_manager.hpp"
#include "derecho/core/detail/view_manager.hpp"

#include <cassert>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

namespace derecho {

namespace rpc {

thread_local bool _in_rpc_handler = false;

RPCManager::~RPCManager() {
    thread_shutdown = true;
    if(rpc_listener_thread.joinable()) {
        rpc_listener_thread.join();
    }
}

void RPCManager::report_failure(const node_id_t who) {
    //Simultaneously test if the ID is in external_client_ids and, if so, erase it
    if(external_client_ids.erase(who) != 0) {
        // external client
        dbg_default_debug("External client with id {} failed, doing cleanup", who);
        connections->remove_connections({who});
        sst::remove_node(who);
    } else {
        // internal member
        view_manager.report_failure(who);
    }
}

void RPCManager::create_connections() {
    connections = std::make_unique<sst::P2PConnectionManager>(sst::P2PParams{
            nid,
            getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE),
            view_manager.view_max_rpc_window_size,
            getConfUInt64(CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE) + sizeof(header),
            getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE) + sizeof(header),
            view_manager.view_max_rpc_reply_payload_size + sizeof(header),
            false,
            [this](const uint32_t node_id) { report_failure(node_id); }});
}

void RPCManager::destroy_remote_invocable_class(uint32_t instance_id) {
    //Delete receiver functions that were added by this class/subgroup
    for(auto receivers_iterator = receivers->begin();
        receivers_iterator != receivers->end();) {
        if(receivers_iterator->first.subgroup_id == instance_id) {
            receivers_iterator = receivers->erase(receivers_iterator);
        } else {
            receivers_iterator++;
        }
    }
    //Deliver a node_removed_from_shard_exception to the QueryResults for this class
    //Important: This only works because the Replicated destructor runs before the
    //wrapped_this member is destroyed; otherwise the PendingResults we're referencing
    //would already have been deleted.
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    while(!pending_results_to_fulfill[instance_id].empty()) {
        std::shared_ptr<AbstractPendingResults> pending_results = pending_results_to_fulfill[instance_id].front().lock();
        if(pending_results) {
            pending_results->set_exception_for_caller_removed();
        }
        pending_results_to_fulfill[instance_id].pop();
    }
    for(auto& pending_results_pair : results_awaiting_local_persistence[instance_id]) {
        std::shared_ptr<AbstractPendingResults> pending_results = pending_results_pair.second.lock();
        if(pending_results) {
            pending_results->set_exception_for_caller_removed();
        }
    }
    results_awaiting_local_persistence[instance_id].clear();
}

void RPCManager::start_listening() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}

std::exception_ptr RPCManager::receive_message(
        const Opcode& indx, const node_id_t& received_from, uint8_t const* const buf,
        std::size_t payload_size, const std::function<uint8_t*(int)>& out_alloc) {
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
    //Pass through the provided out_alloc function, but add space for the reply header
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
        populate_header(reply_buf, size, id, nid, 0);
    }
    return reply_return.possible_exception;
}

std::exception_ptr RPCManager::parse_and_receive(uint8_t* buf, std::size_t size,
                                                 const std::function<uint8_t*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    assert(size >= header_space());
    std::size_t payload_size = size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    retrieve_header(&rdv, buf, payload_size, indx, received_from, flags);
    return receive_message(indx, received_from, buf + header_space(),
                           payload_size, out_alloc);
}

void RPCManager::rpc_message_handler(subgroup_id_t subgroup_id, node_id_t sender_id,
                                     persistent::version_t version, uint64_t timestamp,
                                     uint8_t* msg_buf, uint32_t buffer_size) {
    // WARNING: This assumes the current view doesn't change during execution!
    // (It accesses curr_view without a lock).

    // set the thread local rpc_handler context
    _in_rpc_handler = true;

    // Use the reply-buffer allocation lambda to detect whether parse_and_receive generated a reply
    size_t reply_size = 0;
    std::optional<sst::P2PBufferHandle> reply_buffer;
    parse_and_receive(msg_buf, buffer_size,
                      [this, &reply_buffer, &reply_size, &sender_id](size_t size) -> uint8_t* {
                          reply_size = size;
                          if(reply_size <= connections->get_max_rpc_reply_size()) {
                              reply_buffer = connections->get_sendbuffer_ptr(
                                      sender_id, sst::MESSAGE_TYPE::RPC_REPLY);
                              if(!reply_buffer)
                                  throw derecho_exception("Failed to allocate a buffer for a P2P reply because the send window was full!");
                              return reply_buffer->buf_ptr;
                          } else {
                              // the reply size is too large - not part of the design to handle it
                              throw buffer_overflow_exception("Size of a P2P reply exceeds the maximum P2P reply message size");
                          }
                      });
    if(sender_id == nid) {
        //This is a self-receive of an RPC message I sent, so I have a reply-map that needs fulfilling
        const uint32_t my_shard = view_manager.unsafe_get_current_view().my_subgroups.at(subgroup_id);
        {
            whenlog(int32_t msg_seq_num = persistent::unpack_version<int32_t>(version).second);
            dbg_default_trace("RPCManager got a self-receive for message {}", msg_seq_num);
            std::unique_lock<std::mutex> lock(pending_results_mutex);
            // because of a race condition, pending_results_to_fulfill can genuinely be empty
            // so before accessing it we should sleep on a condition variable and let the main
            // thread that called the orderedSend signal us
            // although the race condition is infinitely rare
            pending_results_cv.wait(lock, [&]() { return !pending_results_to_fulfill[subgroup_id].empty(); });
            std::shared_ptr<AbstractPendingResults> pending_results = pending_results_to_fulfill[subgroup_id].front().lock();
            if(pending_results) {
                //We now know the membership of "all nodes in my shard of the subgroup" in the current view
                pending_results->fulfill_map(
                        view_manager.unsafe_get_current_view().subgroup_shard_views.at(subgroup_id).at(my_shard).members);
                pending_results->set_persistent_version(version, timestamp);
                //Move the fulfilled PendingResults to either the "completed" list or the "awaiting persistence" list
                //(but move the weak_ptr, not the shared_ptr)
                if(view_manager.subgroup_is_persistent(subgroup_id)) {
                    results_awaiting_local_persistence[subgroup_id].emplace(version,
                                                                            pending_results_to_fulfill[subgroup_id].front());
                } else {
                    completed_pending_results[subgroup_id].emplace_back(pending_results_to_fulfill[subgroup_id].front());
                }
            } else {
                dbg_default_debug("Did not fulfill the PendingResults for message {} because it was already gone", msg_seq_num);
            }
            //Regardless of whether the weak_ptr was valid, delete the entry because we're done with it
            pending_results_to_fulfill[subgroup_id].pop();
        }  //release pending_results_mutex
        if(reply_size > 0) {
            // Since this was a self-receive, the reply also goes to myself
            // WARNING: This gets a buffer from connections->get_sendbuffer_ptr() but never
            // sends that buffer with connections->send(); it just passes the outgoing buffer
            // directly to the receive_message handler.
            parse_and_receive(
                    reply_buffer->buf_ptr, reply_size,
                    [](size_t size) -> uint8_t* { assert_always(false); });
        }
    } else if(reply_size > 0) {
        // Otherwise, the only thing to do is send the reply (if there was one)
        connections->send(sender_id, sst::MESSAGE_TYPE::RPC_REPLY, reply_buffer->seq_num);
    }

    // clear the thread local rpc_handler context
    _in_rpc_handler = false;
}

void RPCManager::p2p_message_handler(node_id_t sender_id, uint8_t* msg_buf) {
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    uint32_t flags;
    retrieve_header(nullptr, msg_buf, payload_size, indx, received_from, flags);
    dbg_default_trace("Handling a P2P message: function_id = {}, is_reply = {}, received_from = {}, payload_size = {}, invocation_id = {}",
                      indx.function_id, indx.is_reply, received_from, payload_size, ((long*)(msg_buf + header_size))[0]);
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

//This is always called while holding a write lock on view_manager.view_mutex
void RPCManager::new_view_callback(const View& new_view) {
    connections->remove_connections(new_view.departed);
    connections->add_connections(new_view.members);
    dbg_default_debug("Created new connections among the new view members");
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto& fulfilled_pending_results_pair : results_awaiting_local_persistence) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        //For each PendingResults in this subgroup, check the departed list of each shard in
        //the subgroup, and call set_exception_for_removed_node for the departed nodes
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->second.lock();
            if(live_pending_results) {
                if(!live_pending_results->all_responded()) {
                    for(uint32_t shard_num = 0;
                        shard_num < new_view.subgroup_shard_views[subgroup_id].size();
                        ++shard_num) {
                        for(auto removed_id : new_view.subgroup_shard_views[subgroup_id][shard_num].departed) {
                            //This will do nothing if removed_id was never in the
                            //shard this PendingResult corresponds to
                            dbg_default_debug("Setting exception for removed node {} on PendingResults for subgroup {}, shard {}", removed_id, subgroup_id, shard_num);
                            live_pending_results->set_exception_for_removed_node(removed_id);
                        }
                    }
                    //If the PendingResults was only waiting on responses from failed nodes,
                    //all_responded() could become true after setting the exceptions.
                    //If so, we need to delete RemoteInvoker's heap-allocated shared_ptr here,
                    //since RemoteInvoker won't get any more replies and won't get a chance to delete it
                    if(live_pending_results->all_responded()) {
                        dbg_default_trace("In new_view_callback, calling delete_self_ptr on PendingResults for version {}", pending_results_iter->first);
                        live_pending_results->delete_self_ptr();
                    }
                }
                pending_results_iter++;
            } else {
                //The PendingResults is gone, so don't bother keeping this entry
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            }
        }
    }
    //Do the same departed-node check on PendingResults in the awaiting_global_persistence map
    for(auto& fulfilled_pending_results_pair : results_awaiting_global_persistence) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->second.lock();
            if(live_pending_results) {
                if(!live_pending_results->all_responded()) {
                    for(uint32_t shard_num = 0;
                        shard_num < new_view.subgroup_shard_views[subgroup_id].size();
                        ++shard_num) {
                        for(auto removed_id : new_view.subgroup_shard_views[subgroup_id][shard_num].departed) {
                            dbg_default_debug("Setting exception for removed node {} on PendingResults for subgroup {}, shard {}", removed_id, subgroup_id, shard_num);
                            live_pending_results->set_exception_for_removed_node(removed_id);
                        }
                    }
                    if(live_pending_results->all_responded()) {
                        dbg_default_trace("In new_view_callback, calling delete_self_ptr on PendingResults for version {}", pending_results_iter->first);
                        live_pending_results->delete_self_ptr();
                    }
                }
                pending_results_iter++;
            } else {
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            }
        }
    }

    //No need to check any entries in the awaiting_signature map - if an update has reached global
    //persistence, then all of the replicas must have responded to the RPC message

    //Do the same check on completed_pending_results, but remove them if they are now finished
    for(auto& fulfilled_pending_results_pair : completed_pending_results) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->lock();
            if(live_pending_results && !live_pending_results->all_responded()) {
                for(uint32_t shard_num = 0;
                    shard_num < new_view.subgroup_shard_views[subgroup_id].size();
                    ++shard_num) {
                    for(auto removed_id : new_view.subgroup_shard_views[subgroup_id][shard_num].departed) {
                        //This will do nothing if removed_id was never in the
                        //shard this PendingResult corresponds to
                        dbg_default_debug("Setting exception for removed node {} on PendingResults for subgroup {}, shard {}", removed_id, subgroup_id, shard_num);
                        live_pending_results->set_exception_for_removed_node(removed_id);
                    }
                }
                if(live_pending_results->all_responded()) {
                    dbg_default_trace("In new_view_callback, calling delete_self_ptr on a completed PendingResults for subgroup {}", subgroup_id);
                    live_pending_results->delete_self_ptr();
                }
                pending_results_iter++;
            } else {
                //The PendingResults is gone, or it exists but all_responded() is true
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            }
        }
    }
}

void RPCManager::notify_persistence_finished(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_trace("RPCManager: Got a local persistence callback for version {}", version);
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    //PendingResults in each per-subgroup map are ordered by version number, so all entries before
    //the argument version number have been persisted and need to be notified
    for(auto pending_results_iter = results_awaiting_local_persistence[subgroup_id].begin();
        pending_results_iter != results_awaiting_local_persistence[subgroup_id].upper_bound(version);) {
        dbg_default_trace("RPCManager: Setting local persistence on version {}", pending_results_iter->first);
        std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->second.lock();
        if(live_pending_results) {
            live_pending_results->set_local_persistence();
            //If the PendingResults still exists, move the pointer to results_awaiting_global_persistence
            results_awaiting_global_persistence[subgroup_id].emplace(*pending_results_iter);
        }
        pending_results_iter = results_awaiting_local_persistence[subgroup_id].erase(pending_results_iter);
    }
}

void RPCManager::notify_global_persistence_finished(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_trace("RPCManager: Got a global persistence callback for version {}", version);
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    //PendingResults in each per-subgroup map are ordered by version number, so all entries before
    //the argument version number have been persisted and need to be notified
    for(auto pending_results_iter = results_awaiting_global_persistence[subgroup_id].begin();
        pending_results_iter != results_awaiting_global_persistence[subgroup_id].upper_bound(version);) {
        dbg_default_trace("RPCManager: Setting global persistence on version {}", pending_results_iter->first);
        std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->second.lock();
        if(live_pending_results) {
            live_pending_results->set_global_persistence();
            //If the subgroup needs signatures, move the pointer to results_awaiting_signature
            if(view_manager.subgroup_is_signed(subgroup_id)) {
                results_awaiting_signature[subgroup_id].emplace(*pending_results_iter);
            }
            //If not, no need to put the pointer in completed_pending_results, since all the replicas have
            //responded by now, and completed_pending_results is only used for set_exception_for_removed_node
        }
        pending_results_iter = results_awaiting_global_persistence[subgroup_id].erase(pending_results_iter);
    }
}

void RPCManager::notify_verification_finished(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_trace("RPCManager: Got a global verification callback for version {}", version);
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto pending_results_iter = results_awaiting_signature[subgroup_id].begin();
        pending_results_iter != results_awaiting_signature[subgroup_id].upper_bound(version);) {
        dbg_default_trace("RPCManager: Setting signature verification on version {}", pending_results_iter->first);
        std::shared_ptr<AbstractPendingResults> live_pending_results = pending_results_iter->second.lock();
        if(live_pending_results) {
            live_pending_results->set_signature_verified();
        }
        //Either way, delete the weak_ptr, since it won't be needed by completed_pending_results
        pending_results_iter = results_awaiting_signature[subgroup_id].erase(pending_results_iter);
    }
}

void RPCManager::add_external_connection(node_id_t node_id) {
    external_client_ids.emplace(node_id);
    connections->add_connections({node_id});
}

void RPCManager::register_rpc_results(subgroup_id_t subgroup_id, std::weak_ptr<AbstractPendingResults> pending_results_handle) {
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    pending_results_to_fulfill[subgroup_id].push(pending_results_handle);
    pending_results_cv.notify_all();
}

sst::P2PBufferHandle RPCManager::get_sendbuffer_ptr(uint32_t dest_id, sst::MESSAGE_TYPE type) {
    std::optional<sst::P2PBufferHandle> buffer;
    int curr_vid = -1;
    do {
        //ViewManager's view_mutex also prevents connections from being removed (because
        //that happens in new_view_callback)
        SharedLockedReference<View> view_and_lock = view_manager.get_current_view();
        //Check to see if the view changed between iterations of the loop, and re-get the rank
        if(curr_vid != view_and_lock.get().vid) {
            curr_vid = view_and_lock.get().vid;
        }
        try {
            buffer = connections->get_sendbuffer_ptr(dest_id, type);
        } catch(std::out_of_range& map_error) {
            throw node_removed_from_group_exception(dest_id);
        }

    } while(!buffer);
    return *buffer;
}

void RPCManager::send_p2p_message(node_id_t dest_id, subgroup_id_t dest_subgroup_id, uint64_t sequence_num,
                                  std::weak_ptr<AbstractPendingResults> pending_results_handle) {
    try {
        // ViewManager's view_mutex also prevents connections from being removed (because
        // that happens in new_view_callback)
        SharedLockedReference<View> view_and_lock = view_manager.get_current_view();
        // The type of message being sent here is always a P2P request, not a reply
        connections->send(dest_id, sst::MESSAGE_TYPE::P2P_REQUEST, sequence_num);
    } catch(std::out_of_range& map_error) {
        throw node_removed_from_group_exception(dest_id);
    }
    std::shared_ptr<AbstractPendingResults> pending_results = pending_results_handle.lock();
    if(pending_results) {
        pending_results->fulfill_map({dest_id});
        std::lock_guard<std::mutex> lock(pending_results_mutex);
        // These PendingResults don't need to have ReplyMaps fulfilled, and they
        // won't ever get version numbers or persistence notifications (since P2P sends are read-only)
        completed_pending_results[dest_subgroup_id].push_back(pending_results_handle);
    }
}

void RPCManager::p2p_request_worker() {
    pthread_setname_np(pthread_self(), "p2p_req_wkr");
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
            dbg_default_error("Invalid rpc message in fifo queue: is_reply={}, is_cascading={}",
                              indx.is_reply, RPC_HEADER_FLAG_TST(flags, CASCADE));
            throw derecho::derecho_exception("invalid rpc message in fifo queue...crash.");
        }
        reply_size = 0;
        uint64_t reply_seq_num = 0;
        receive_message(indx, received_from, request.msg_buf + header_size, payload_size,
                        [this, &reply_size, &reply_seq_num, &request](size_t _size) -> uint8_t* {
                            reply_size = _size;
                            if(reply_size <= connections->get_max_p2p_reply_size()) {
                                auto buffer_handle = connections->get_sendbuffer_ptr(
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
            dbg_default_trace("Sending a P2P reply to node {} for invocation ID {} of function {}",
                              request.sender_id, ((long*)(request.msg_buf + header_size))[0], indx.function_id);
            connections->send(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY, reply_seq_num);
        } else {
            // hack for now to "simulate" a reply for p2p_sends to functions that do not generate a reply
            auto buffer_handle = connections->get_sendbuffer_ptr(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY);
            if(buffer_handle) {
                dbg_default_trace("Sending a null reply to node {} for a void P2P call", request.sender_id);
                reinterpret_cast<size_t*>(buffer_handle->buf_ptr)[0] = 0;
                connections->send(request.sender_id, sst::MESSAGE_TYPE::P2P_REPLY, buffer_handle->seq_num);
            }
        }
    }
}

void RPCManager::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "rpc_lsnr");

    // set the thread local rpc_handler context
    _in_rpc_handler = true;

    while(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    dbg_default_debug("P2P listening thread started");
    // start the fifo worker thread
    request_worker_thread = std::thread(&RPCManager::p2p_request_worker, this);

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    // loop event
    while(!thread_shutdown) {
        bool message_received = false;
        // This scope contains a lock on the View, which prevents view changes during
        // delivery of a P2P message (otherwise, a View change could occur between a
        // successful probe_all() and the call to p2p_message_handler)
        {
            SharedLockedReference<View> locked_view = view_manager.get_current_view();
            auto optional_message = connections->probe_all();
            if(optional_message) {
                message_received = true;
                auto message_handle = optional_message.value();
                // Invalid ID means the message was empty (a null reply)
                if(message_handle.sender_id != INVALID_NODE_ID) {
                    p2p_message_handler(message_handle.sender_id, message_handle.buf);
                    connections->increment_incoming_seq_num(message_handle.sender_id, message_handle.type);
                }
                // update last time
                clock_gettime(CLOCK_REALTIME, &last_time);
            }
        }
        //Release the View lock before going to sleep if no messages were received
        if(!message_received) {
            clock_gettime(CLOCK_REALTIME, &cur_time);
            // check if the system has been inactive for enough time to induce sleep
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                        + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            if(time_elapsed_in_ms > 250) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
            }
        }
    }
    // stop fifo worker.
    request_queue_cv.notify_one();
    request_worker_thread.join();
}

bool in_rpc_handler() {
    return _in_rpc_handler;
}
}  // namespace rpc
}  // namespace derecho
