/**
 * @file rpc_manager.cpp
 *
 * @date Feb 7, 2017
 */

#include <cassert>
#include <iostream>

#include "rpc_manager.h"

namespace derecho {

namespace rpc {

RPCManager::~RPCManager() {
    thread_shutdown = true;
    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
    tcp_connections.destroy();
}

void RPCManager::start_listening() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}

LockedReference<std::unique_lock<std::mutex>, tcp::socket> RPCManager::get_socket(node_id_t node) {
    return tcp_connections.get_socket(node);
}

std::exception_ptr RPCManager::receive_message(
        const Opcode& indx, const node_id_t& received_from, char const* const buf,
        std::size_t payload_size, const std::function<char*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    assert(payload_size);
//    int offset = indx.is_reply ? 1 : 0;
//    long int invocation_id = ((long int*)(buf + offset))[0];
//    logger->trace("Received an RPC message from {} with opcode: {{ class_id=typeinfo for {}, subgroup_id={}, function_id={}, is_reply={} }}, invocation id: {}",
//                  received_from, indx.class_id.name(), indx.subgroup_id, indx.function_id, indx.is_reply, invocation_id);
    auto reply_header_size = header_space();
    //TODO: Check that the given Opcode is actually in our receivers map,
    //and reply with a "no such method error" if it is not
    recv_ret reply_return = receivers->at(indx)(
            &rdv, received_from, buf,
            [&out_alloc, &reply_header_size](std::size_t size) {
                return out_alloc(size + reply_header_size) + reply_header_size;
            });
    auto* reply_buf = reply_return.payload;
    if(reply_buf) {
        reply_buf -= reply_header_size;
        const auto id = reply_return.opcode;
        const auto size = reply_return.size;
        populate_header(reply_buf, size, id, nid);
    }
    return reply_return.possible_exception;
}

std::exception_ptr RPCManager::parse_and_receive(char* buf, std::size_t size,
                                                 const std::function<char*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    std::size_t payload_size = size;
    Opcode indx;
    node_id_t received_from;
    retrieve_header(&rdv, buf, payload_size, indx, received_from);
    return receive_message(indx, received_from, buf + header_space(),
                           payload_size, out_alloc);
}

void RPCManager::rpc_message_handler(subgroup_id_t subgroup_id, node_id_t sender_id, char* msg_buf, uint32_t payload_size) {
    // WARNING: This assumes the current view doesn't change during execution! (It accesses curr_view without a lock).
    // extract the destination vector
    size_t dest_size = ((size_t*)msg_buf)[0];
    msg_buf += sizeof(size_t);
    payload_size -= sizeof(size_t);
    bool in_dest = false;
    for(size_t i = 0; i < dest_size; ++i) {
        auto n = ((node_id_t*)msg_buf)[0];
        msg_buf += sizeof(node_id_t);
        payload_size -= sizeof(node_id_t);
        if(n == nid) {
            in_dest = true;
        }
    }
    if(in_dest || dest_size == 0) {
        //Use the reply-buffer allocation lambda to detect whether handle_receive generated a reply
        size_t reply_size = 0;
        char* reply_buf;
        parse_and_receive(msg_buf, payload_size, [this, &reply_buf, &reply_size, &sender_id](size_t size) -> char* {
	    reply_size = size;
            if(reply_size <= connections->get_max_p2p_size()) {
	      reply_buf = (char*)connections->get_sendbuffer_ptr(
							    connections->get_node_rank(sender_id), sst::REQUEST_TYPE::RPC_REPLY);
                return reply_buf;
            } else {
                // the reply size is too large - not part of the design to handle it
                struct matthew_made_this_an_exception : public std::exception{};
                throw matthew_made_this_an_exception{};
            }
        });
        if(reply_size > 0) {
            if(sender_id == nid) {
                //The RPC message expects replies, and I was the sender, so I might have a reply-map that needs fulfilling
                if(dest_size == 0) {
                    //Destination was "all nodes in my shard of the subgroup"
                    int my_shard = view_manager.curr_view->multicast_group->get_subgroup_settings().at(subgroup_id).shard_num;
                    std::lock_guard<std::mutex> lock(pending_results_mutex);
                    assert(!toFulfillQueue.empty());
                    // logger->trace("Calling fulfill_map on toFulfillQueue.front(), its size is {}", toFulfillQueue.size());
                    toFulfillQueue.front().get().fulfill_map(
                            view_manager.curr_view->subgroup_shard_views.at(subgroup_id).at(my_shard).members);
                    fulfilledList.push_back(std::move(toFulfillQueue.front()));
                    toFulfillQueue.pop();
//                    logger->trace("Popped a PendingResults from toFulfillQueue, size is now {}", toFulfillQueue.size());
                }
                //Immediately handle the reply to myself
                parse_and_receive(
                        reply_buf, reply_size,
                        [](size_t size) -> char* { assert_always(false); });
            } else {
                connections->send(connections->get_node_rank(sender_id));
            }
        } else {
            logger->trace("RPC message handled, no reply necessary.");
            if(sender_id == nid && dest_size == 0) {
                std::lock_guard<std::mutex> lock(pending_results_mutex);
                assert(!toFulfillQueue.empty());
                toFulfillQueue.pop();
//                logger->trace("Deleted a useless PendingResults from toFulfillQueue, size is now {}", toFulfillQueue.size());
            }
        }
    }
}

void RPCManager::p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size) {
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    retrieve_header(nullptr, msg_buf, payload_size, indx, received_from);
    size_t reply_size = 0;
    receive_message(indx, received_from, msg_buf + header_size, payload_size,
                    [this, &msg_buf, &buffer_size, &reply_size, &sender_id](size_t _size) -> char* {
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
}

void RPCManager::new_view_callback(const View& new_view) {
    if(std::find(new_view.joined.begin(), new_view.joined.end(), nid) != new_view.joined.end()) {
        //If this node is in the joined list, we need to set up a connection to everyone
        for(int i = 0; i < new_view.num_members; ++i) {
            if(new_view.members[i] != nid) {
                tcp_connections.add_node(new_view.members[i], new_view.member_ips[i]);
                logger->debug("Established a TCP connection to node {}", new_view.members[i]);
            }
        }
    } else {
        //This node is already a member, so we already have connections to the previous view's members
        for(const node_id_t& joiner_id : new_view.joined) {
            tcp_connections.add_node(joiner_id,
                                 new_view.member_ips[new_view.rank_of(joiner_id)]);
            logger->debug("Established a TCP connection to node {}", joiner_id);
        }
        for(const node_id_t& removed_id : new_view.departed) {
            logger->debug("Removing TCP connection for failed node {}", removed_id);
            tcp_connections.delete_node(removed_id);
        }
    }

    std::lock_guard<std::mutex> connections_lock(p2p_connections_mutex);
    connections = std::make_unique<sst::P2PConnections>(std::move(*connections), new_view.members);
    logger->debug("Created new connections among the new view members");

    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto& pending : fulfilledList) {
        for(auto removed_id : new_view.departed) {
            pending.get().set_exception_for_removed_node(removed_id);
        }
    }
}

int RPCManager::populate_nodelist_header(const std::vector<node_id_t>& dest_nodes, char* buffer,
                                         std::size_t& max_payload_size) {
    int header_size = 0;
    // Put the list of destination nodes in another layer of "header"
    ((size_t*)buffer)[0] = dest_nodes.size();
    buffer += sizeof(size_t);
    header_size += sizeof(size_t);
    for(auto& node_id : dest_nodes) {
        ((node_id_t*)buffer)[0] = node_id;
        buffer += sizeof(node_id_t);
        header_size += sizeof(node_id_t);
    }
    //Two return values: the size of the header we just created,
    //and the maximum payload size based on that
    max_payload_size = view_manager.curr_view->multicast_group->max_msg_size - sizeof(derecho::header) - header_size;
    return header_size;
}

bool RPCManager::finish_rpc_send(uint32_t subgroup_id, const std::vector<node_id_t>& dest_nodes, PendingBase& pending_results_handle) {
    if(!view_manager.curr_view->multicast_group->send(subgroup_id)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    if(dest_nodes.size() != 0) {
        pending_results_handle.fulfill_map(dest_nodes);
        fulfilledList.push_back(pending_results_handle);
    } else {
        toFulfillQueue.push(pending_results_handle);
//        logger->trace("finish_rpc_send pushed a PendingResults onto toFulfillQueue, size is now {}", toFulfillQueue.size());
    }
    return true;
}

volatile char* RPCManager::get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type) {
    auto dest_rank = connections->get_node_rank(dest_id);
    volatile char* buf;
    do {
        buf = connections->get_sendbuffer_ptr(dest_rank, type);
    } while(!buf);
    return buf;
}

void RPCManager::finish_p2p_send(node_id_t dest_id, PendingBase& pending_results_handle) {
    connections->send(connections->get_node_rank(dest_id));
    pending_results_handle.fulfill_map({dest_id});
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    fulfilledList.push_back(pending_results_handle);
}

void RPCManager::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "rpc_thread");
    auto max_payload_size = view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
    while(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    logger->debug("P2P listening thread started");
    while(!thread_shutdown) {
        std::lock_guard<std::mutex> connections_lock(p2p_connections_mutex);
        auto optional_reply_pair = connections->probe_all();
        if(optional_reply_pair) {
            auto reply_pair = optional_reply_pair.value();
            p2p_message_handler(reply_pair.first, (char*)reply_pair.second, max_payload_size);
        }
    }
}
}  // namespace rpc
}  // namespace derecho
