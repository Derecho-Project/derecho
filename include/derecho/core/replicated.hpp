/**
 * @file replicated.h
 *
 * @date Feb 3, 2017
 */

#pragma once

#include "derecho/persistent/Persistent.hpp"
#include "derecho/tcp/tcp.hpp"
#include "derecho_exception.hpp"
#include "detail/derecho_internal.hpp"
#include "detail/remote_invocable.hpp"
#include "detail/replicated_interface.hpp"
#include "detail/rpc_manager.hpp"
#include "detail/rpc_utils.hpp"

#include <functional>
#include <iostream>
#include <memory>
#include <tuple>
#include <type_traits>

namespace derecho {

class _Group;
class GroupReference;

/**
 * This is a marker interface for user-defined Replicated Objects (i.e. objects
 * that will be used with the Replicated<T> template) to indicate that at least
 * one of the object's fields is of type Persistent<T>. Users should inherit
 * from this class in order for Persistent<T> fields to work properly.
 */
class PersistsFields {};

/**
 * This is a marker interface for user-defined Replicated Objects (i.e. objects
 * that will be used with the Replicated<T> template) to indicate that the
 * object has Persistent<T> fields with signatures enabled. Users should inherit
 * from this class if their object constructs Persistent<T> fields with signatures
 * enabled; it is a subclass of PersistsFields, so it will also enable persistence.
 */
class SignedPersistentFields : public PersistsFields {};

/**
 * A template whose member field "value" will be true if type T inherits from
 * PersistsFields, and false otherwise. Just a convenient specialization of
 * std::is_base_of.
 */
template <typename T>
using has_persistent_fields = std::is_base_of<PersistsFields, T>;

/**
 * A template whose member field "value" will be true if type T inherits from
 * SignedPersistentFields.
 */
template <typename T>
using has_signed_fields = std::is_base_of<SignedPersistentFields, T>;

/**
 * An empty class to be used as the "replicated type" for a subgroup that
 * doesn't implement a Replicated Object. Subgroups of type RawObject will
 * have no replicated state or RPC functions and can only be used to send raw
 * (byte-buffer) multicasts.
 */
struct RawObject {
    static auto register_functions() { return std::tuple<>(); };
};

/**
 * An implementation of Factory<T> for RawObject, which is trivial because
 * RawObjects have no state.
 */
inline std::unique_ptr<RawObject> raw_object_factory(persistent::PersistentRegistry*, subgroup_id_t) {
    return std::make_unique<RawObject>();
}

template <typename T>
class Replicated : public ReplicatedObject, public persistent::ITemporalQueryFrontierProvider {
private:
    /** The PersistentRegistry for all Persistent<T> fields in this object */
    std::unique_ptr<persistent::PersistentRegistry> persistent_registry;
#if defined(_PERFORMANCE_DEBUG)
public:
#endif
    /**
     * The user-provided state object with some RPC methods. Stored by
     * pointer-to-pointer because it must stay pinned at a specific location
     * in memory, and otherwise Replicated<T> would be unmoveable.
     */
    std::unique_ptr<std::unique_ptr<T>> user_object_ptr;
#if defined(_PERFORMANCE_DEBUG)
private:
#endif
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that replicates this object. */
    const subgroup_id_t subgroup_id;
    const uint32_t subgroup_index;
    /**
     * The index, within the subgroup, of the shard that replicates this object.
     * This needs to be stored in order to detect if a node has moved to a different
     * shard within the same subgroup (and hence its Replicated state is obsolete).
     */
    const uint32_t shard_num;
    /**
     * The Signer to use for signing updates to this object, if signatures are
     * enabled. If signatures are disabled, this will be null.
     */
    std::unique_ptr<openssl::Signer> signer;
    /**
     * The size of a signature, which is a fixed run-time constant based on the
     * security parameter of the private key being used. This will be 0 if
     * signatures are disabled.
     */
    std::size_t signature_size;
    /** Reference to the RPCManager for the Group this Replicated is in */
    rpc::RPCManager& group_rpc_manager;
    /** The actual implementation of Replicated<T>, hiding its ugly template parameters. */
    std::unique_ptr<rpc::RemoteInvocableOf<T>> wrapped_this;
    /**
     * A type-erased pointer to the Group that contains this Replicated. Will
     * never be null since Group owns and outlives Replicated.
     */
    _Group* group;
    /** The current version number being processed by an ordered_send */
    persistent::version_t current_version = persistent::INVALID_VERSION;
    /** The timestamp associated with the current version number */
    uint64_t current_timestamp_us = 0;

public:
    /**
     * Constructs a Replicated<T> that enables sending and receiving RPC
     * function calls for an object of type T.
     * @param type_id A unique ID for type T within the Group that owns this
     * Replicated<T>
     * @param nid The ID of the node on which this Replicated<T> is running.
     * @param subgroup_id The internally-generated subgroup ID for the subgroup
     * that participates in replicating this object
     * @param subgroup_index The index of the subgroup that replicates this object
     * within (only) the set of subgroups that replicate type T; zero-indexed.
     * @param shard_num The zero-indexed shard number of the shard (within subgroup
     * subgroup_id) that participates in replicating this object
     * @param group_rpc_manager A reference to the RPCManager for the Group
     * that owns this Replicated<T>
     * @param client_object_factory A factory functor that can create instances
     * of T.
     */
    Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, uint32_t subgroup_index,
               uint32_t shard_num, rpc::RPCManager& group_rpc_manager,
               Factory<T> client_object_factory, _Group* group);

    /**
     * Constructs a Replicated<T> for an object without actually constructing an
     * instance of that object; the resulting Replicated will be in an invalid
     * state until receive_object() is called. This should be used when a new
     * subgroup member expects to receive the replicated object's state after
     * joining.
     * @param type_id A unique ID for type T within the Group that owns this
     * Replicated<T>
     * @param nid This node's node ID
     * @param subgroup_id The internally-generated subgroup ID for the subgroup
     * that participates in replicating this object
     * @param subgroup_index The index of the subgroup that replicates this object
     * within (only) the set of subgroups that replicate type T; zero-indexed.
     * @param shard_num The zero-indexed shard number of the shard (within subgroup
     * subgroup_id) that participates in replicating this object
     * @param group_rpc_manager A reference to the RPCManager for the Group
     * that owns this Replicated<T>
     */
    Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, uint32_t subgroup_index,
               uint32_t shard_num, rpc::RPCManager& group_rpc_manager, _Group* group);

    Replicated(Replicated&& rhs);
    Replicated(const Replicated&) = delete;
    virtual ~Replicated();

    /**
     * @return The value of has_persistent_fields<T> for this Replicated<T>'s
     * template parameter. This is true if any field of the user object T is
     * persistent.
     */
    virtual bool is_persistent() const {
        return has_persistent_fields<T>::value;
    }

    /**
     * @return The value of has_signed_fields<T> for this Replicated<T>'s
     * template parameter. This should be true if the user object T has
     * persistent fields that were initialized with signatures enabled.
     */
    virtual bool is_signed() const {
        return has_signed_fields<T>::value;
    }

    /**
     * @return True if this Replicated<T> actually contains a reference to a
     * replicated object, false if it is "empty" because this node is not a
     * member of the subgroup that replicates T.
     */
    bool is_valid() const {
        return *user_object_ptr && true;
    }

    /**
     * @return The subgroup_id of the subgroup containing this Replicated<T>
     * object.
     */
    subgroup_id_t get_subgroup_id() const {
        return subgroup_id;
    }

    /**
     * @return The shard of the Replicated<T>'s subgroup that the current node
     * belongs to, and that this Replicated<T> updates when it sends multicasts.
     */
    uint32_t get_shard_num() const {
        return shard_num;
    }

    /**
     * Sends a peer-to-peer message to a single member of the subgroup that
     * replicates this Replicated<T>, invoking the RPC function identified
     * by the FunctionTag template parameter.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(node_id_t dest_node, Args&&... args) const;

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
    auto ordered_send(Args&&... args);

    /**
     * Submits a call to send a "raw" (byte array) message in a multicast to
     * this object's subgroup; the message will be generated by invoking msg_generator
     * inside this function.
     */
    void send(unsigned long long int payload_size, const std::function<void(uint8_t* buf)>& msg_generator);

    /**
     * @return The serialized size of the object, of type T, that holds the
     * state of this Replicated<T>.
     */
    std::size_t object_size() const;

    /**
     * Serializes and sends the state of the "wrapped" object (of type T) for
     * this Replicated<T> over the given socket. (This includes sending the
     * object's size before its data, so the receiver knows the size of buffer
     * to allocate).
     * @param receiver_socket
     */
    void send_object(tcp::socket& receiver_socket) const;

    /**
     * Serializes and sends the state of the "wrapped" object (of type T) for
     * this Replicated<T> over the given socket *without* first sending its size.
     * Should only be used when sending a list of objects, preceded by their
     * total size, otherwise the recipient will have no way of knowing how large
     * a buffer to allocate for this object.
     * @param receiver_socket
     */
    void send_object_raw(tcp::socket& receiver_socket) const;

    /**
     * Updates the state of the "wrapped" object by replacing it with the object
     * serialized in a buffer. Returns the number of bytes read from the buffer,
     * in case the caller needs to know.
     * @param buffer A buffer containing a serialized T, which will replace this
     * Replicated<T>'s wrapped T
     * @return The number of bytes read from the buffer.
     */
    std::size_t receive_object(uint8_t* buffer);

    const uint64_t compute_global_stability_frontier();

    inline const HLC getFrontier() {
        // transform from ns to us:
        HLC hlc(this->compute_global_stability_frontier() / 1e3, 0);
        return hlc;
    }

    /**
     * Returns the minimum among the "latest version" numbers of all Persistent
     * fields of this object, i.e. the longest consistent cut of all the logs.
     * @return A version number
     */
    virtual persistent::version_t get_minimum_latest_persisted_version();

    /**
     * Returns the current global persistence frontier, aka, stable frontier that will survive whole system restart.
     * Please note this applies to persistent data ONLY. The data not in Persistent<> are not saved.
     */
    virtual persistent::version_t get_global_persistence_frontier();

    /**
     * Wait until the current global persistence frontier advanced beyond a version.
     * @param version   the version
     * @return false if the given version is beyond the latest atomic broadcast.
     */
    virtual bool wait_for_global_persistence_frontier(persistent::version_t version);

    /**
     * Returns the current global verified frontier, aka, stable frontier that will survive whole system restart.
     * Please note this applies to persistent data ONLY. The data not in Persistent<> are not saved.
     */
    virtual persistent::version_t get_global_verified_frontier();

    /**
     * make a version for all the persistent<T> members.
     * @param ver - the version number to be made
     */
    virtual void make_version(persistent::version_t ver, const HLC& hlc);

    /**
     * Persists the object's data up to at least the specified version; due to
     * batching, a later version may actually be persisted if it is available.
     * Returns the latest version actually persisted. If the signed log is
     * enabled, also returns the signature over the latest persisted version in
     * the provided buffer.
     * @param version The version to persist up to.
     * @param signature The byte array in which to put the signature, assumed to be
     * the correct length for this node's signing key.
     * @return The version actually persisted (and signed)
     */
    virtual persistent::version_t persist(persistent::version_t version,
                                          uint8_t* signature);

    /**
     * Retreives a copy of the signature in the persistent log for a specified
     * version of this object.
     * @param version The logged version to retrieve the signature for
     * @return The signature in the log for the requested version, or an empty
     * vector if signatures are disabled or the requested version doesn't exist
     */
    virtual std::vector<uint8_t> get_signature(persistent::version_t version);
    /**
     * Verifies the persistent log entry at the specified version against the
     * provided signature.
     * @param version The logged version to verify
     * @param verifier The Verifier to use, initialized with the public key that
     * should correspond to the signature
     * @param other_signature The signature to verify against, presumably from
     * some other node
     */
    virtual bool verify_log(persistent::version_t version,
                            openssl::Verifier& verifier,
                            const uint8_t* other_signature);

    /**
     * trim the logs to a version, inclusively.
     * @param earliest_version - the version number, before which, logs are
     * going to be trimmed
     */
    virtual void trim(persistent::version_t earliest_version);

    /**
     * Truncate the logs of all Persistent<T> members back to the version
     * specified. This deletes recently-used data, so it should only be called
     * during failure recovery when some versions must be rolled back.
     * @param latest_version The latest version number that should remain in the logs
     */
    virtual void truncate(persistent::version_t latest_version);

    /**
     * Post the next version to be assigned to an update. Called immediately
     * before invoking an ordered_send RPC function to update current_version.
     * @return version The new update's persistent version number
     * @return ts_us The new update's timestamp in microseconds
     */
    virtual void post_next_version(persistent::version_t version, uint64_t ts_us);

    /**
     * Get the current version, set by the most recent ordered_send update.
     * During the execution of an ordered_send RPC method, this represents the
     * version that will be assigned to that method's update.
     * Warning: It is NOT safe to call this function from a P2P RPC method,
     * since P2P method calls are handled in a separate thread from ordered_send
     * method calls, and there is no synchronization between these two threads on
     * the value of current_version.
     * @return an ordered pair (version number, timestamp)
     */
    virtual std::tuple<persistent::version_t, uint64_t> get_current_version();

    /**
     * Register a persistent member
     */
    virtual void register_persistent_member(const char* object_name,
                                            persistent::PersistentObject* member_pointer) {
        this->persistent_registry->registerPersistent(object_name, member_pointer);
    }
};

template <typename T>
class PeerCaller {
private:
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that this PeerCaller will contact. */
    subgroup_id_t subgroup_id;
    /** Reference to the RPCManager for the Group this PeerCaller is in */
    rpc::RPCManager& group_rpc_manager;
    /** The actual implementation of PeerCaller, which has lots of ugly template parameters */
    std::unique_ptr<rpc::RemoteInvokerFor<T>> wrapped_this;

public:
    PeerCaller(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager);

    PeerCaller(PeerCaller&&) = default;
    PeerCaller(const PeerCaller&) = delete;

    /**
     * Sends a peer-to-peer message to a single member of the subgroup that
     * this PeerCaller<T> connects to, invoking the RPC function identified
     * by the FunctionTag template parameter.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(node_id_t dest_node, Args&&... args);

    bool is_valid() const { return true; }
};

template <typename T>
class ExternalClientCallback {
private:
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that this ExternalClientCallback will contact. */
    subgroup_id_t subgroup_id;
    /** Reference to the RPCManager for the Group this ExternalClientCallback is in */
    rpc::RPCManager& group_rpc_manager;
    /** The actual implementation of ExternalClientCallback, which has lots of ugly template parameters */
    std::unique_ptr<rpc::RemoteInvokerFor<T>> wrapped_this;

public:
    ExternalClientCallback(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager);

    ExternalClientCallback(ExternalClientCallback&&) = default;
    ExternalClientCallback(const ExternalClientCallback&) = delete;

    /**
     * Sends a peer-to-peer message to a single member of the subgroup that
     * this ExternalClientCallback<T> connects to, invoking the RPC function identified
     * by the FunctionTag template parameter.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(node_id_t dest_node, Args&&... args);

    bool is_valid() const { return true; }
};

template <typename T>
class ShardIterator {
private:
    PeerCaller<T>& caller;
    const std::vector<node_id_t> shard_reps;

public:
    ShardIterator(PeerCaller<T>& caller, std::vector<node_id_t> shard_reps)
            : caller(caller),
              shard_reps(shard_reps) {}
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(Args&&... args);
};
}  // namespace derecho

#include "detail/replicated_impl.hpp"
