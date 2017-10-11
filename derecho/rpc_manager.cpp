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
    connections.destroy();
}

void RPCManager::start_listening() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}

LockedReference<std::unique_lock<std::mutex>, tcp::socket> RPCManager::get_socket(node_id_t node) {
    return connections.get_socket(node);
}

std::exception_ptr RPCManager::handle_receive(
        const Opcode& indx, const node_id_t& received_from, char const* const buf,
        std::size_t payload_size, const std::function<char*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    assert(payload_size);
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

std::exception_ptr RPCManager::handle_receive(
        char* buf, std::size_t size,
        const std::function<char*(int)>& out_alloc) {
    using namespace remote_invocation_utilities;
    std::size_t payload_size = size;
    Opcode indx;
    node_id_t received_from;
    retrieve_header(&rdv, buf, payload_size, indx, received_from);
    return handle_receive(indx, received_from, buf + header_space(),
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
        auto max_payload_size = view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
        size_t reply_size = 0;
        handle_receive(msg_buf, payload_size, [this, &reply_size, &max_payload_size](size_t size) -> char* {
            reply_size = size;
            if(reply_size <= max_payload_size) {
                return replySendBuffer.get();
            } else {
                return nullptr;
            }
        });
        if(reply_size > 0) {
            if(sender_id == nid) {
                handle_receive(
                        replySendBuffer.get(), reply_size,
                        [](size_t size) -> char* { assert(false); });
                if(dest_size == 0) {
                    //Destination was "all nodes in my shard of the subgroup"
                    int my_shard = view_manager.curr_view->multicast_group->get_subgroup_settings().at(subgroup_id).shard_num;
                    std::lock_guard<std::mutex> lock(pending_results_mutex);
                    toFulfillQueue.front().get().fulfill_map(
                            view_manager.curr_view->subgroup_shard_views.at(subgroup_id).at(my_shard).members);
                    fulfilledList.push_back(std::move(toFulfillQueue.front()));
                    toFulfillQueue.pop();
                }
            } else {
                connections.write(sender_id, replySendBuffer.get(), reply_size);
            }
        }
    }
}

void RPCManager::p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size) {
    using namespace remote_invocation_utilities;
    const std::size_t header_size = header_space();
    connections.read(sender_id, msg_buf, header_size);
    std::size_t payload_size;
    Opcode indx;
    node_id_t received_from;
    retrieve_header(nullptr, msg_buf, payload_size, indx, received_from);
    connections.read(sender_id, msg_buf + header_size, payload_size);
    size_t reply_size = 0;
    handle_receive(indx, received_from, msg_buf + header_size, payload_size,
                   [&msg_buf, &buffer_size, &reply_size](size_t _size) -> char* {
                       reply_size = _size;
                       if(reply_size <= buffer_size) {
                           return msg_buf;
                       } else {
                           return nullptr;
                       }
                   });
    if(reply_size > 0) {
        connections.write(received_from, msg_buf, reply_size);
    }
}

void RPCManager::new_view_callback(const View& new_view) {
    if(std::find(new_view.joined.begin(), new_view.joined.end(), nid) != new_view.joined.end()) {
        //If this node is in the joined list, we need to set up a connection to everyone
        for(int i = 0; i < new_view.num_members; ++i) {
            if(new_view.members[i] != nid) {
                connections.add_node(new_view.members[i], new_view.member_ips[i]);
                logger->debug("Established a TCP connection to node {}", new_view.members[i]);
            }
        }
    } else {
        //This node is already a member, so we already have connections to the previous view's members
        for(const node_id_t& joiner_id : new_view.joined) {
            connections.add_node(joiner_id,
                                 new_view.member_ips[new_view.rank_of(joiner_id)]);
            logger->debug("Established a TCP connection to node {}", joiner_id);
        }
        for(const node_id_t& removed_id : new_view.departed) {
            logger->debug("Removing TCP connection for failed node {}", removed_id);
            connections.delete_node(removed_id);
        }
    }

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

void RPCManager::finish_rpc_send(uint32_t subgroup_id, const std::vector<node_id_t>& dest_nodes, PendingBase& pending_results_handle) {
    while(!view_manager.curr_view->multicast_group->send(subgroup_id)) {
    }
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    if(dest_nodes.size()) {
        pending_results_handle.fulfill_map(dest_nodes);
        fulfilledList.push_back(pending_results_handle);
    } else {
        toFulfillQueue.push(pending_results_handle);
    }
}

void RPCManager::finish_p2p_send(node_id_t dest_node, char* msg_buf, std::size_t size, PendingBase& pending_results_handle) {
    connections.write(dest_node, msg_buf, size);
    pending_results_handle.fulfill_map({dest_node});
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    fulfilledList.push_back(pending_results_handle);
}

void RPCManager::p2p_receive_loop() {
    pthread_setname_np(pthread_self(), "rpc_thread");
    auto max_payload_size = view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
    std::unique_ptr<char[]> rpcBuffer = std::unique_ptr<char[]>(new char[max_payload_size]);
    while(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    logger->debug("P2P listening thread started");
    while(!thread_shutdown) {
        auto other_id = connections.probe_all();
        if(other_id < 0) {
            continue;
        }
        p2p_message_handler(other_id, rpcBuffer.get(), max_payload_size);
    }
}
}
}
