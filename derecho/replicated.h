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
#include <type_traits>

#include "mutils-serialization/SerializationSupport.hpp"
#include "tcp/tcp.h"

#include "rpc_utils.h"
#include "rpc_manager.h"
#include "remote_invocable.h"
#include "derecho_exception.h"

namespace derecho {

template <typename T>
using Factory = std::function<std::unique_ptr<T>(void)>;

/**
 * Common interface for all types of Replicated<T>, specifying the methods to
 * send and receive object state. This allows the Group to send object state
 * without knowing the full type of a subgroup.
 */
class ReplicatedObject {
public:
    virtual ~ReplicatedObject() = default;
    virtual bool is_valid() const = 0;
    virtual std::size_t object_size() const = 0;
    virtual void send_object(tcp::socket& receiver_socket) const = 0;
    virtual void send_object_raw(tcp::socket& receiver_socket) const = 0;
    virtual std::size_t receive_object(char* buffer) = 0;
};

template <typename T>
class Replicated : public ReplicatedObject {
private:
    /** The user-provided state object with some RPC methods. Stored by
     * pointer-to-pointer because it must stay pinned at a specific location
     * in memory, and otherwise Replicated<T> would be unmoveable. */
    std::unique_ptr<std::unique_ptr<T>> user_object_ptr;
    const node_id_t node_id;
    subgroup_id_t subgroup_id;
    /** Non-owning, non-managed pointer to Group's RPCManager - really a
     * reference, but needs to be lazily initialized */
    rpc::RPCManager* group_rpc_manager;
    /** The actual implementation of Replicated<T>, hiding its ugly template parameters. */
    std::unique_ptr<rpc::RemoteInvocableOf<T>> wrapped_this;
    /** Buffer for replying to P2P messages, cached here so it doesn't need to be
     * created in every p2p_send call. */
    std::unique_ptr<char[]> p2pSendBuffer;

    template <rpc::FunctionTag tag, typename... Args>
    auto ordered_send_or_query(const std::vector<node_id_t>& destination_nodes,
                               Args&&... args) {
        if(is_valid()) {
            char* buffer;
            while(!(buffer = group_rpc_manager->view_manager.get_sendbuffer_ptr(subgroup_id, 0, 0, true))) {
            };
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager->view_manager.view_mutex);

            std::size_t max_payload_size;
            int buffer_offset = group_rpc_manager->populate_nodelist_header(destination_nodes,
                                                                            buffer, max_payload_size);
            buffer += buffer_offset;
            std::cout << "Replicated: doing ordered send/query for function tagged " << tag << std::endl;
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
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send_or_query(node_id_t dest_node, Args&&... args) {
        if(is_valid()) {
            assert(dest_node != node_id);
            //Ensure a view change isn't in progress
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager->view_manager.view_mutex);
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
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

public:
    /**
     * Constructs a Replicated<T> that enables RPC function calls for an object
     * of type T.
     * @param nid The ID of the node on which this Replicated<T> is running.
     * @param subgroup_id The internally-generated subgroup ID for the subgroup
     * that participates in replicating this object
     * @param group_rpc_manager A reference to the RPCManager for the Group
     * that owns this Replicated<T>
     * @param client_object_factory A factory functor that can create instances
     * of T.
     */
    Replicated(node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager,
               Factory<T> client_object_factory)
            : user_object_ptr(std::make_unique<std::unique_ptr<T>>(client_object_factory())),
              node_id(nid),
              subgroup_id(subgroup_id),
              group_rpc_manager(&group_rpc_manager),
              wrapped_this(T::register_functions(group_rpc_manager, user_object_ptr.get())),
              p2pSendBuffer(new char[group_rpc_manager.view_manager.derecho_params.max_payload_size]) {}

    Replicated(node_id_t nid, rpc::RPCManager& group_rpc_manager)
            : user_object_ptr(std::make_unique<std::unique_ptr<T>>(nullptr)),
              node_id(nid),
              subgroup_id(0),
              group_rpc_manager(&group_rpc_manager),
              wrapped_this(T::register_functions(group_rpc_manager, user_object_ptr.get())),
              p2pSendBuffer(new char[group_rpc_manager.view_manager.derecho_params.max_payload_size]) {}

    Replicated(Replicated&&) = default;
    Replicated(const Replicated&) = delete;
    virtual ~Replicated() = default;

    /**
     * @return True if this Replicated<T> actually contains a reference to a
     * replicated object, false if it is "empty" because this node is not a
     * member of the subgroup that replicates T.
     */
    bool is_valid() const {
        return *user_object_ptr && true;
    }

    /**
     * Sends a multicast to only some members of the subgroup that replicates this
     * Replicated<T>, invoking the RPC function identified by the FunctionTag
     * template parameter, but does not wait for a response. This should only be
     * used for RPC functions whose return type is void.
     * @param args The arguments to the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    void ordered_send(const std::vector<node_id_t>& destination_nodes,
                      Args&&... args) {
        ordered_send_or_query<tag>(destination_nodes, std::forward<Args>(args)...);
    }

    /**
     * Sends a multicast to the entire subgroup that replicates this Replicated<T>,
     * invoking the RPC function identified by the FunctionTag template parameter,
     * but does not wait for a response. This should only be used for RPC functions
     * whose return type is void.
     * @param args The arguments to the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    void ordered_send(Args&&... args) {
        // empty nodes means that the destination is the entire group
        ordered_send<tag>({}, std::forward<Args>(args)...);
    }

    /**
     * Sends a multicast to only some members of the subgroup that replicates
     * this Replicated<T>, invoking the RPC function identified by the
     * FunctionTag template parameter.
     * @param destination_nodes The IDs of the nodes that should be sent the
     * RPC message
     * @param args The arguments to the RPC function
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked.
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto ordered_query(const std::vector<node_id_t>& destination_nodes,
                       Args&&... args) {
        return ordered_send_or_query<tag>(destination_nodes, std::forward<Args>(args)...);
    }

    /**
     * Sends a multicast to the entire subgroup that replicates this Replicated<T>,
     * invoking the RPC function identified by the FunctionTag template parameter.
     * The caller must keep the returned QueryResults object in scope in order to
     * receive replies.
     * @param args The arguments to the RPC function
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked.
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto ordered_query(Args&&... args) {
        // empty nodes means that the destination is the entire group
        return ordered_query<tag>({}, std::forward<Args>(args)...);
    }

    /**
     * Sends a peer-to-peer message over TCP to a single member of the subgroup
     * that replicates this Replicated<T>, invoking the RPC function identified
     * by the FunctionTag template parameter, but does not wait for a response.
     * This should only be used for RPC functions whose return type is void.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    void p2p_send(node_id_t dest_node, Args&&... args) {
        p2p_send_or_query<tag>(dest_node, std::forward<Args>(args)...);
    }

    /**
     * Sends a peer-to-peer message over TCP to a single member of the subgroup
     * that replicates this Replicated<T>, invoking the RPC function identified
     * by the FunctionTag template parameter. The caller must keep the returned
     * QueryResults object in scope in order to receive replies.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_query(node_id_t dest_node, Args&&... args) {
        return p2p_send_or_query<tag>(dest_node, std::forward<Args>(args)...);
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
    std::size_t object_size() const {
        return mutils::bytes_size(**user_object_ptr);
    }

    /**
     * Serializes and sends the state of the "wrapped" object (of type T) for
     * this Replicated<T> over the given socket. (This includes sending the
     * object's size before its data, so the receiver knows the size of buffer
     * to allocate).
     * @param receiver_socket
     */
    void send_object(tcp::socket& receiver_socket) const {
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
    void send_object_raw(tcp::socket& receiver_socket) const {
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
            receiver_socket.write(bytes, size); };
        mutils::post_object(bind_socket_write, **user_object_ptr);
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
        *user_object_ptr = std::move(mutils::from_bytes<T>(&group_rpc_manager->dsm, buffer));
        return mutils::bytes_size(**user_object_ptr);
    }
};
}
