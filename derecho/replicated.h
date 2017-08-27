/**
 * @file replicated.h
 *
 * @date Feb 3, 2017
 */

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#include "mutils-serialization/SerializationSupport.hpp"
#include "tcp/tcp.h"
#include "persistent/Persistent.hpp"

#include "derecho_internal.h"
#include "derecho_exception.h"
#include "remote_invocable.h"
#include "rpc_manager.h"
#include "rpc_utils.h"

using namespace ns_persistent;

namespace derecho {

template <typename T>
//using Factory = std::function<std::unique_ptr<T>(void)>;
using Factory = std::function<std::unique_ptr<T>(PersistentRegistry*)>;
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
    /** persistent registry for persistent<t>
     */
    PersistentRegistry persistent_registry;
    /** The user-provided state object with some RPC methods. Stored by
     * pointer-to-pointer because it must stay pinned at a specific location
     * in memory, and otherwise Replicated<T> would be unmoveable. */
#ifdef _PERFORMANCE_DEBUG
public:
#endif
    std::unique_ptr<std::unique_ptr<T>> user_object_ptr;
#ifdef _PERFORMANCE_DEBUG
private:
#endif
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that replicates this object. */
    subgroup_id_t subgroup_id;
    /** Reference to the RPCManager for the Group this Replicated is in */
    rpc::RPCManager& group_rpc_manager;
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
            while(!(buffer = group_rpc_manager.view_manager.get_sendbuffer_ptr(subgroup_id, wrapped_this->template get_size<tag>(std::forward<Args>(args)...), 0, true))) {
            };
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);

            std::size_t max_payload_size;
            int buffer_offset = group_rpc_manager.populate_nodelist_header(destination_nodes,
                                                                           buffer, max_payload_size);
            buffer += buffer_offset;
            // std::cout << "Replicated: doing ordered send/query for function tagged " << tag << " in subgroup " << subgroup_id << std::endl;
            auto send_return_struct = wrapped_this->template send<tag>(
                    [&buffer, &max_payload_size](size_t size) -> char* {
                        if(size <= max_payload_size) {
                            return buffer;
                        } else {
                            return nullptr;
                        }
                    },
                    std::forward<Args>(args)...);

            group_rpc_manager.finish_rpc_send(subgroup_id, destination_nodes, send_return_struct.pending);
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
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
            size_t size;
            auto max_payload_size = group_rpc_manager.view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
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
            group_rpc_manager.finish_p2p_send(dest_node, p2pSendBuffer.get(), size, return_pair.pending);
            return std::move(return_pair.results);
        } else {
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

public:
    /**
     * Constructs a Replicated<T> that enables sending and receiving RPC
     * function calls for an object of type T.
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
            : persistent_registry([&](){
                HLC hlc(this->compute_global_stability_frontier(),0);
                return hlc;
              }),
              user_object_ptr(std::make_unique<std::unique_ptr<T>>(client_object_factory(&this->persistent_registry))),
              node_id(nid),
              subgroup_id(subgroup_id),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(), subgroup_id, T::register_functions())),
              p2pSendBuffer(new char[group_rpc_manager.view_manager.derecho_params.max_payload_size]) {}

    /**
     * Constructs a Replicated<T> for an object without actually constructing an
     * instance of that object; the resulting Replicated will be in an invalid
     * state until receive_object() is called. This should be used when a new
     * subgroup member expects to receive the replicated object's state after
     * joining.
     * @param nid This node's node ID
     * @param subgroup_id The internally-generated subgroup ID for the subgroup
     * that participates in replicating this object
     * @param group_rpc_manager A reference to the RPCManager for the Group
     * that owns this Replicated<T>
     */
    Replicated(node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager)
            : persistent_registry([&](){
                HLC hlc(this->compute_global_stability_frontier(),0);
                return hlc;
              }),
              user_object_ptr(std::make_unique<std::unique_ptr<T>>(nullptr)),
              node_id(nid),
              subgroup_id(subgroup_id),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(), subgroup_id, T::register_functions())),
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
     * QueryResults object in scope in order to receive replies. It is up to the
     * caller to ensure that the node ID specified in the parameter is actually
     * a member of the subgroup that this Replicated<T> is bound to.
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
    char* get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns = 0, bool null_send = false) {
        return group_rpc_manager.view_manager.get_sendbuffer_ptr(subgroup_id,
                                                                 payload_size, pause_sending_turns, false, null_send);
    }

    uint64_t compute_global_stability_frontier() {
      return group_rpc_manager.view_manager.compute_global_stability_frontier(subgroup_id);
    }

    /**
     * Submits the contents of the send buffer to be multicast to the subgroup,
     * assuming it has been previously filled with a call to get_sendbuffer_ptr().
     */
    void raw_send() {
        group_rpc_manager.view_manager.send(subgroup_id);
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
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) { receiver_socket.write(bytes, size); };
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
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) { receiver_socket.write(bytes, size); };
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
        // *user_object_ptr = std::move(mutils::from_bytes<T>(&group_rpc_manager.dsm, buffer));
        mutils::RemoteDeserialization_v rdv{group_rpc_manager.rdv};
        rdv.insert(rdv.begin(),&persistent_registry);
        mutils::DeserializationManager dsm{rdv};
        *user_object_ptr = std::move(mutils::from_bytes<T>(&dsm,buffer));
        return mutils::bytes_size(**user_object_ptr);
    }

    /**
     * make a version for all the persistent<T> members.
     * @param ver - the version number to be made
     */
    virtual void make_version(const persistence_version_t & ver) noexcept(false) {
      persistent_registry.makeVersion(ver);
    };

    /**
     * persist the data to the latest version
     */
    virtual void persist(const persistence_version_t version) noexcept(false) {
      persistence_version_t persisted_ver;

      // persist variables
      do{
        persisted_ver = persistent_registry.persist();
        if(persisted_ver == -1) {
          // for replicated<T> without Persistent fields,
          // tell the persistent thread that we are done.
          persisted_ver = version;
         }
      } while(persisted_ver < version);

      // read lock the view
      std::shared_lock<std::shared_timed_mutex> read_lock(group_rpc_manager.view_manager.view_mutex);
      // update the persisted_num in SST

      View &Vc = *group_rpc_manager.view_manager.curr_view;
      Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id] = version;
      Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
        (char*)std::addressof(Vc.gmsSST->persisted_num[0][subgroup_id]) - Vc.gmsSST->getBaseAddress(),
        sizeof(long long int));

    };

    /**
     * trim the logs to a version, inclusively.
     * @param ver - the version number, before which, logs are going to be
     * trimmed
     */
    virtual void trim(const persistence_version_t & ver) noexcept(false) {
      persistent_registry.trim(ver);
    };

    /**
     * Register a persistent member
     * @param vf - the version function
     * @param pf - the persistent function
     * @param tf - the trim function
     */ 
    virtual void register_persistent_member(const char* object_name, const VersionFunc &vf, const PersistFunc &pf, const TrimFunc &tf) noexcept(false) {
      this->persistent_registry.registerPersist(object_name,vf,pf,tf);
    }
};

template <typename T>
class ExternalCaller {
private:
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that this ExternalCaller will contact. */
    subgroup_id_t subgroup_id;
    /** Reference to the RPCManager for the Group this ExternalCaller is in */
    rpc::RPCManager& group_rpc_manager;
    /** The actual implementation of ExternalCaller, which has lots of ugly template parameters */
    std::unique_ptr<rpc::RemoteInvokerFor<T>> wrapped_this;
    /** Buffer for replying to P2P messages, cached here so it doesn't need to be
     * created in every p2p_send call. */
    std::unique_ptr<char[]> p2pSendBuffer;

    //This is literally copied and pasted from Replicated<T>. I wish I could let them share code with inheritance,
    //but I'm afraid that will introduce unnecessary overheads.
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send_or_query(node_id_t dest_node, Args&&... args) {
        if(is_valid()) {
            assert(dest_node != node_id);
            //Ensure a view change isn't in progress
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
            size_t size;
            auto max_payload_size = group_rpc_manager.view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
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
            group_rpc_manager.finish_p2p_send(dest_node, p2pSendBuffer.get(), size, return_pair.pending);
            return std::move(return_pair.results);
        } else {
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

public:
    ExternalCaller(node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager)
            : node_id(nid),
              subgroup_id(subgroup_id),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invoker<T>(subgroup_id, T::register_functions())),
              p2pSendBuffer(new char[group_rpc_manager.view_manager.derecho_params.max_payload_size]) {}

    ExternalCaller(ExternalCaller&&) = default;
    ExternalCaller(const ExternalCaller&) = delete;

    bool is_valid() const { return true; }

    /**
     * Sends a peer-to-peer message over TCP to a single member of the subgroup
     * that this ExternalCaller targets, invoking the RPC function identified
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
     * that this ExternalCaller targets, invoking the RPC function identified
     * by the FunctionTag template parameter. The caller must keep the returned
     * QueryResults object in scope in order to receive replies. It is up to the
     * caller to ensure that the node ID specified in the parameter is actually
     * a member of the subgroup that this ExternalCaller is bound to.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_query(node_id_t dest_node, Args&&... args) {
        return p2p_send_or_query<tag>(dest_node, std::forward<Args>(args)...);
    }
};
}
