/**
 * @file replicated.h
 *
 * @date Feb 3, 2017
 * @author edward
 */

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <utility>


#include "mutils-serialization/SerializationSupport.hpp"
#include "tcp/tcp.h"

#include "rpc_utils.h"
#include "rpc_manager.h"
#include "remote_invocable.h"
#include "derecho_exception.h"

namespace derecho {

template<typename T>
using Factory = std::function<std::unique_ptr<T>(void)>;


struct empty_reference_exception : public derecho_exception {
    empty_reference_exception(const std::string& message) : derecho_exception(message) {}
};

template<typename T>
class Replicated {
private:
    /** The user-provided state object with some RPC methods */
    std::unique_ptr<T> object;
    const node_id_t node_id;
    const subgroup_id_t subgroup_id;
    /** Non-owning, non-managed pointer to Group's RPCManager - really a
     * reference, but needs to be lazily initialized */
    rpc::RPCManager* group_rpc_manager;
    /** The actual implementation of Replicated<T>, hiding its ugly template parameters. */
    rpc::RemoteInvocableOf<T> wrapped_this;
    /** Buffer for replying to P2P messages, cached here so it doesn't need to be
     * created in every p2p_send call. */
    std::unique_ptr<char[]> p2pSendBuffer;


    template<rpc::FunctionTag tag, typename... Args>
    auto ordered_send_or_query(const std::vector<node_id_t>& destination_nodes,
                     Args&&... args) {
        if(is_valid()) {
            char* buffer;
            while(!(buffer = group_rpc_manager->view_manager.get_sendbuffer_ptr(subgroup_id, 0, 0, true))) {
            };
            std::unique_lock<std::mutex> view_lock(group_rpc_manager->view_manager.view_mutex);

            std::size_t max_payload_size;
            int buffer_offset = group_rpc_manager->populate_nodelist_header(destination_nodes,
                    buffer, max_payload_size);
            buffer += buffer_offset;
            auto send_return_struct = wrapped_this->template send<tag>(
                    [&buffer, &max_payload_size](size_t size) -> char* {
                if(size <= max_payload_size) {
                    return buffer;
                } else {
                    return nullptr;
                }
            },
            std::forward<Args>(args)...);

            group_rpc_manager->finish_rpc_send(subgroup_id, destination_nodes, send_return_struct.pending);
            return std::move(send_return_struct.results);
        } else {
            throw derecho::empty_reference_exception();
        }
    }

    template<rpc::FunctionTag tag, typename... Args>
    auto p2p_send_or_query(node_id_t dest_node, Args&&... args) {
        if(is_valid()) {
            assert(dest_node != node_id);
            size_t size;
            auto max_payload_size = group_rpc_manager->view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
            auto return_pair = wrapped_this->template send<tag>(
                    [this, &max_payload_size, &size](size_t _size) -> char* {
                size = _size;
                if(size <= max_payload_size) {
                    return p2pSendBuffer.get();
                } else {
                    return nullptr;
                }
            },
            std::forward<Args>(args)...);
            group_rpc_manager->finish_p2p_send(dest_node, p2pSendBuffer.get(), size, return_pair.pending);
            return std::move(return_pair.results);
        } else {
            throw derecho::empty_reference_exception();
        }
    }

public:
    Replicated(node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager,
               Factory<T> client_object_factory) :
        object(client_object_factory()),
        node_id(nid),
        subgroup_id(subgroup_id),
        group_rpc_manager(&group_rpc_manager),
        wrapped_this(object->register_functions(group_rpc_manager, &object)),
        p2pSendBuffer(new char[group_rpc_manager.view_manager.derecho_params.max_payload_size]) {}

    Replicated() :
        object(nullptr),
        node_id(0),
        subgroup_id(0),
        group_rpc_manager(nullptr),
        wrapped_this(nullptr),
        p2pSendBuffer(new char[1]) {}

    Replicated(Replicated&&) = default;
    Replicated(Replicated&) = delete;

    bool is_valid() const {
        return object && wrapped_this && group_rpc_manager;
    }

    template<rpc::FunctionTag tag, typename... Args>
    void ordered_send(const std::vector<node_id_t>& destination_nodes,
                      Args&&... args) {
        ordered_send_or_query(destination_nodes, std::forward<Args>(args)...);
    }

    template<rpc::FunctionTag tag, typename... Args>
    void ordered_send(Args&&... args) {
        // empty nodes means that the destination is the entire group
        ordered_send<tag>({}, std::forward<Args>(args)...);
    }

    template<rpc::FunctionTag tag, typename... Args>
    auto ordered_query(const std::vector<node_id_t>& destination_nodes,
                      Args&&... args) {
        return ordered_send_or_query(destination_nodes, std::forward<Args>(args)...);
    }

    template<rpc::FunctionTag tag, typename... Args>
    auto ordered_query(Args&&... args) {
        // empty nodes means that the destination is the entire group
        return ordered_query({}, std::forward<Args>(args)...);
    }

    template<rpc::FunctionTag tag, typename... Args>
    void p2p_send(node_id_t dest_node, Args&&... args) {
        p2p_send_or_query(dest_node, std::forward<Args>(args)...);
    }

    template<rpc::FunctionTag tag, typename... Args>
    auto p2p_query(node_id_t dest_node, Args&&... args) {
        return p2p_send_or_query(dest_node, std::forward<Args>(args)...);
    }

    /**
     * Gets a pointer into the send buffer for this subgroup, for the purpose of
     * doing a "raw send" (not an RPC send).
     * @param payload_size The size of the payload that the caller intends to
     * send, in bytes.
     * @param pause_sending_turns
     * @return
     */
    char* get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns) {
        return group_rpc_manager->view_manager.get_sendbuffer_ptr(subgroup_id,
                payload_size, pause_sending_turns, false);
    }

    /**
     * Submits the contents of the send buffer to be multicast to the subgroup,
     * assuming it has been previously filled with a call to get_sendbuffer_ptr().
     */
    void raw_send() {
        group_rpc_manager->view_manager.send(subgroup_id);
    }

    /**
     * @return The serialized size of the object, of type T, that holds the
     * state of this Replicated<T>.
     */
    std::size_t object_size() {
        return mutils::bytes_size(*object);
    }

    /**
     * Serializes and sends the state of the "wrapped" object (of type T) for
     * this Replicated<T> over the given socket.
     * @param receiver_socket
     */
    void send_object(tcp::socket& receiver_socket) {
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
            receiver_socket.write(bytes, size); };
        mutils::post_object(bind_socket_write, object_size());
        send_object_raw(receiver_socket);
    }

    /**
     * Serializes and sends the state of the "wrapped" object (of type T) for
     * this Replicated<T> over the given socket *without* first sending its size.
     * Should only be used when sending a list of objects, preceded by their
     * total size, otherwise the recipient will have no way of knowing how large
     * a buffer to allocate for this object.
     * @param receiver_socket
     */
    void send_object_raw(tcp::socket& receiver_socket) {
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
            receiver_socket.write(bytes, size); };
        mutils::post_object(bind_socket_write, *object);

    }

    /**
     * Updates the state of the "wrapped" object by replacing it with the object
     * serialized in a buffer. Returns the number of bytes read from the buffer,
     * in case the caller needs to know.
     * @param dsm A DeserializationManager to use for deserializing the object
     * in the buffer
     * @param buffer A buffer containing a serialized T, which will replace this
     * Replicated<T>'s wrapped T
     * @return The number of bytes read from the buffer.
     */
    std::size_t receive_object(char* buffer) {
        object = std::move(mutils::from_bytes<T>(&group_rpc_manager->dsm, buffer));
        return mutils::bytes_size(*object);
    }
};

}


