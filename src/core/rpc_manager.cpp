/**
 * @file rpc_manager.cpp
 *
 * @date Feb 7, 2017
 */

#include <cassert>
#include <iostream>

#include <derecho/core/detail/rpc_manager.hpp>

namespace derecho {

namespace rpc {

thread_local bool _in_rpc_handler = false;

RPCManager::~RPCManager() {
    thread_shutdown = true;
    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
}

void RPCManager::create_connections() {
    connections = std::make_unique<sst::P2PConnections>(sst::P2PParams{
            nid,
            {nid},
	    getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE),
            view_manager.view_max_rpc_window_size,
	    getConfUInt64(CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE) + sizeof(header),
	    getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE) + sizeof(header),
            view_manager.view_max_rpc_reply_payload_size + sizeof(header)});
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
        pending_results_to_fulfill[instance_id].front().get().set_exception_for_caller_removed();
        pending_results_to_fulfill[instance_id].pop();
    }
    while(!fulfilled_pending_results[instance_id].empty()) {
        fulfilled_pending_results[instance_id].front().get().set_exception_for_caller_removed();
        fulfilled_pending_results[instance_id].pop_front();
    }
}

void RPCManager::start_listening() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}

std::exception_ptr RPCManager::receive_message(
        const Opcode& indx, const node_id_t& received_from, char const* const buf,
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
        populate_header(reply_buf, size, id, nid, 0);
    }
    return reply_return.possible_exception;
}

std::exception_ptr RPCManager::parse_and_receive(char* buf, std::size_t size,
                                                 const std::function<char*(int)>& out_alloc) {
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
                                     char* msg_buf, uint32_t buffer_size) {
    // WARNING: This assumes the current view doesn't change during execution!
    // (It accesses curr_view without a lock).

    // set the thread local rpc_handler context
    _in_rpc_handler = true;

    //Use the reply-buffer allocation lambda to detect whether parse_and_receive generated a reply
    size_t reply_size = 0;
    char* reply_buf;
    parse_and_receive(msg_buf, buffer_size,
                      [this, &reply_buf, &reply_size, &sender_id](size_t size) -> char* {
                          reply_size = size;
                          if(reply_size <= connections->get_max_p2p_reply_size()) {
                              reply_buf = (char*)connections->get_sendbuffer_ptr(
                                      connections->get_node_rank(sender_id), sst::REQUEST_TYPE::RPC_REPLY);
                              return reply_buf;
                          } else {
                              // the reply size is too large - not part of the design to handle it
                              return nullptr;
                          }
                      });
    if(sender_id == nid) {
        //This is a self-receive of an RPC message I sent, so I have a reply-map that needs fulfilling
        const uint32_t my_shard = view_manager.curr_view->my_subgroups.at(subgroup_id);
        {
            std::unique_lock<std::mutex> lock(pending_results_mutex);
            // because of a race condition, pending_results_to_fulfill can genuinely be empty
            // so before accessing it we should sleep on a condition variable and let the main
            // thread that called the orderedSend signal us
            // although the race condition is infinitely rare
            pending_results_cv.wait(lock, [&]() { return !pending_results_to_fulfill[subgroup_id].empty(); });
            //We now know the membership of "all nodes in my shard of the subgroup" in the current view
            pending_results_to_fulfill[subgroup_id].front().get().fulfill_map(
                    view_manager.curr_view->subgroup_shard_views.at(subgroup_id).at(my_shard).members);
            fulfilled_pending_results[subgroup_id].push_back(
                    std::move(pending_results_to_fulfill[subgroup_id].front()));
            pending_results_to_fulfill[subgroup_id].pop();
        }  //release pending_results_mutex
        if(reply_size > 0) {
            //Since this was a self-receive, the reply also goes to myself
            parse_and_receive(
                    reply_buf, reply_size,
                    [](size_t size) -> char* { assert_always(false); });
        }
    } else if(reply_size > 0) {
        //Otherwise, the only thing to do is send the reply (if there was one)
        connections->send(connections->get_node_rank(sender_id));
    }

    // clear the thread local rpc_handler context
    _in_rpc_handler = false;
}

void RPCManager::p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size) {
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
                                return (char*)connections->get_sendbuffer_ptr(
                                        connections->get_node_rank(sender_id), sst::REQUEST_TYPE::P2P_REPLY);
                            }
                            return nullptr;
                        });
        if(reply_size > 0) {
            connections->send(connections->get_node_rank(sender_id));
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

//This is always called while holding a write lock on view_manager.view_mutex
void RPCManager::new_view_callback(const View& new_view) {
    {
        std::lock_guard<std::mutex> connections_lock(p2p_connections_mutex);
        connections = std::make_unique<sst::P2PConnections>(std::move(*connections), new_view.members);
    }
    dbg_default_debug("Created new connections among the new view members");
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto& fulfilled_pending_results_pair : fulfilled_pending_results) {
        const subgroup_id_t subgroup_id = fulfilled_pending_results_pair.first;
        //For each PendingResults in this subgroup, check the departed list of each shard
        //the subgroup, and call set_exception_for_removed_node for the departed nodes
        for(auto pending_results_iter = fulfilled_pending_results_pair.second.begin();
            pending_results_iter != fulfilled_pending_results_pair.second.end();) {
            //Garbage-collect PendingResults references that are obsolete
            if(pending_results_iter->get().all_responded()) {
                pending_results_iter = fulfilled_pending_results_pair.second.erase(pending_results_iter);
            } else {
                for(uint32_t shard_num = 0;
                    shard_num < new_view.subgroup_shard_views[subgroup_id].size();
                    ++shard_num) {
                    for(auto removed_id : new_view.subgroup_shard_views[subgroup_id][shard_num].departed) {
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

bool RPCManager::finish_rpc_send(subgroup_id_t subgroup_id, PendingBase& pending_results_handle) {
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    pending_results_to_fulfill[subgroup_id].push(pending_results_handle);
    pending_results_cv.notify_all();
    return true;
}

volatile char* RPCManager::get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type) {
    volatile char* buf;
    int curr_vid = -1;
    uint32_t dest_rank = 0;
    do {
        //This lock also prevents connections from being reassigned (because that happens
        //in new_view_callback), so we don't need p2p_connections_mutex
        std::shared_lock<std::shared_timed_mutex> view_read_lock(view_manager.view_mutex);
        //Check to see if the view changed between iterations of the loop, and re-get the rank
        if(curr_vid != view_manager.curr_view->vid) {
            try {
                dest_rank = connections->get_node_rank(dest_id);
            } catch(std::out_of_range& map_error) {
                throw node_removed_from_group_exception(dest_id);
            }
            curr_vid = view_manager.curr_view->vid;
        }
        buf = connections->get_sendbuffer_ptr(dest_rank, type);
    } while(!buf);
    return buf;
}

void RPCManager::finish_p2p_send(node_id_t dest_id, subgroup_id_t dest_subgroup_id, PendingBase& pending_results_handle) {
    try {
        //This lock also prevents connections from being reassigned (because that happens
        //in new_view_callback), so we don't need p2p_connections_mutex
        std::shared_lock<std::shared_timed_mutex> view_read_lock(view_manager.view_mutex);
        connections->send(connections->get_node_rank(dest_id));
    } catch(std::out_of_range& map_error) {
        throw node_removed_from_group_exception(dest_id);
    }
    pending_results_handle.fulfill_map({dest_id});
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    fulfilled_pending_results[dest_subgroup_id].push_back(pending_results_handle);
}

void RPCManager::fifo_worker() {
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
                                return (char*)connections->get_sendbuffer_ptr(
                                        connections->get_node_rank(request.sender_id), sst::REQUEST_TYPE::P2P_REPLY);
                            }
                            return nullptr;
                        });
        if(reply_size > 0) {
            connections->send(connections->get_node_rank(request.sender_id));
        } else {
            // hack for now to "simulate" a reply for p2p_sends to functions that do not generate a reply
            char* buf = connections->get_sendbuffer_ptr(connections->get_node_rank(request.sender_id), sst::REQUEST_TYPE::P2P_REPLY);
            buf[0] = 0;
            connections->send(connections->get_node_rank(request.sender_id));
        }
    }
}

void RPCManager::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "rpc_thread");

    uint64_t max_payload_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    // set the thread local rpc_handler context
    _in_rpc_handler = true;

    while(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    dbg_default_debug("P2P listening thread started");
    // start the fifo worker thread
    fifo_worker_thread = std::thread(&RPCManager::fifo_worker, this);

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    // loop event
    while(!thread_shutdown) {
        std::unique_lock<std::mutex> connections_lock(p2p_connections_mutex);
        auto optional_reply_pair = connections->probe_all();
        if(optional_reply_pair) {
            auto reply_pair = optional_reply_pair.value();
            p2p_message_handler(reply_pair.first, (char*)reply_pair.second, max_payload_size);
            connections->update_incoming_seq_num();

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

bool in_rpc_handler() {
    return _in_rpc_handler;
}
}  // namespace rpc
}  // namespace derecho
