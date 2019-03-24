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

#include "derecho_exception.h"
#include "derecho_internal.h"
#include "remote_invocable.h"
#include "rpc_manager.h"
#include "rpc_utils.h"

#include "mutils-serialization/SerializationSupport.hpp"
#include "persistent/Persistent.hpp"
#include "tcp/tcp.h"

using namespace persistent;

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
 * A template whose member field "value" will be true if type T inherits from
 * PersistsFields, and false otherwise. Just a convenient specialization of
 * std::is_base_of.
 */
template <typename T>
using has_persistent_fields = std::is_base_of<PersistsFields, T>;

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
inline std::unique_ptr<RawObject> raw_object_factory(PersistentRegistry*) {
    return std::make_unique<RawObject>();
}

/**
 * Common interface for all types of Replicated<T>, specifying some methods for
 * state transfer and persistence. This allows non-templated Derecho components
 * like ViewManager to take these actions without knowing the full type of a subgroup.
 */
class ReplicatedObject {
public:
    virtual ~ReplicatedObject() = default;
    virtual bool is_valid() const = 0;
    virtual std::size_t object_size() const = 0;
    virtual void send_object(tcp::socket& receiver_socket) const = 0;
    virtual void send_object_raw(tcp::socket& receiver_socket) const = 0;
    virtual std::size_t receive_object(char* buffer) = 0;
    virtual bool is_persistent() const = 0;
    virtual void make_version(const persistent::version_t& ver, const HLC& hlc) noexcept(false) = 0;
    virtual const persistent::version_t get_minimum_latest_persisted_version() noexcept(false) = 0;
    virtual void persist(const persistent::version_t version) noexcept(false) = 0;
    virtual void truncate(const persistent::version_t& latest_version) = 0;
    virtual void post_next_version(const persistent::version_t& version) = 0;
};

template <typename T>
class Replicated : public ReplicatedObject, public ITemporalQueryFrontierProvider {
private:
    /** persistent registry for persistent<t>
     */
    std::unique_ptr<PersistentRegistry> persistent_registry_ptr;
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
public:
#endif
    /** The user-provided state object with some RPC methods. Stored by
     * pointer-to-pointer because it must stay pinned at a specific location
     * in memory, and otherwise Replicated<T> would be unmoveable. */
    std::unique_ptr<std::unique_ptr<T>> user_object_ptr;
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
private:
#endif
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that replicates this object. */
    const subgroup_id_t subgroup_id;
    const uint32_t subgroup_index;
    /** The index, within the subgroup, of the shard that replicates this object.
     * This needs to be stored in order to detect if a node has moved to a different
     * shard within the same subgroup (and hence its Replicated state is obsolete). */
    const uint32_t shard_num;
    /** Reference to the RPCManager for the Group this Replicated is in */
    rpc::RPCManager& group_rpc_manager;
    /** The actual implementation of Replicated<T>, hiding its ugly template parameters. */
    std::unique_ptr<rpc::RemoteInvocableOf<T>> wrapped_this;
    _Group* group;
    /** The version number being processed */
    persistent::version_t next_version = INVALID_VERSION;

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
    Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, uint32_t subgroup_index, uint32_t shard_num,
               rpc::RPCManager& group_rpc_manager, Factory<T> client_object_factory, _Group* group)
            : persistent_registry_ptr(std::make_unique<PersistentRegistry>(this, std::type_index(typeid(T)), subgroup_index, shard_num)),
              user_object_ptr(std::make_unique<std::unique_ptr<T>>(client_object_factory(persistent_registry_ptr.get()))),
              node_id(nid),
              subgroup_id(subgroup_id),
              subgroup_index(subgroup_index),
              shard_num(shard_num),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(), type_id, subgroup_id, T::register_functions())),
              group(group) {
        if constexpr(std::is_base_of_v<GroupReference, T>) {
            (**user_object_ptr).set_group_pointers(group, subgroup_index);
        }
    }

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
    Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, uint32_t subgroup_index, uint32_t shard_num,
               rpc::RPCManager& group_rpc_manager, _Group* group)
            : persistent_registry_ptr(std::make_unique<PersistentRegistry>(this, std::type_index(typeid(T)), subgroup_index, shard_num)),
              user_object_ptr(std::make_unique<std::unique_ptr<T>>(nullptr)),
              node_id(nid),
              subgroup_id(subgroup_id),
              subgroup_index(subgroup_index),
              shard_num(shard_num),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(), type_id, subgroup_id, T::register_functions())),
              group(group) {}

    // Replicated(Replicated&&) = default;
    Replicated(Replicated&& rhs) : persistent_registry_ptr(std::move(rhs.persistent_registry_ptr)),
                                   user_object_ptr(std::move(rhs.user_object_ptr)),
                                   node_id(rhs.node_id),
                                   subgroup_id(rhs.subgroup_id),
                                   subgroup_index(rhs.subgroup_index),
                                   shard_num(rhs.shard_num),
                                   group_rpc_manager(rhs.group_rpc_manager),
                                   wrapped_this(std::move(rhs.wrapped_this)),
                                   group(rhs.group) {
        persistent_registry_ptr->updateTemporalFrontierProvider(this);
    }
    Replicated(const Replicated&) = delete;
    virtual ~Replicated(){
        // hack to check if the object was merely moved
        if(wrapped_this) {
            group_rpc_manager.destroy_remote_invocable_class(subgroup_id);
        }
    };

    /**
     * @return The value of has_persistent_fields<T> for this Replicated<T>'s
     * template parameter. This is true if any field of the user object T is
     * persistent.
     */
    constexpr bool is_persistent() const {
        return has_persistent_fields<T>::value;
    }

    /**
     * @return True if this Replicated<T> actually contains a reference to a
     * replicated object, false if it is "empty" because this node is not a
     * member of the subgroup that replicates T.
     */
    bool is_valid() const {
        return *user_object_ptr && true;
    }

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
    auto p2p_send(node_id_t dest_node, Args&&... args) {
        if(is_valid()) {
            //Ensure a view change isn't in progress
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
            size_t size;
            auto max_payload_size = group_rpc_manager.view_manager.curr_view->multicast_group->max_msg_size - sizeof(header);
            auto return_pair = wrapped_this->template send<tag>(
                    [this, &dest_node, &max_payload_size, &size](size_t _size) -> char* {
                        size = _size;
                        if(size <= max_payload_size) {
                            return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node, sst::REQUEST_TYPE::P2P_SEND);
                        } else {
                            return nullptr;
                        }
                    },
                    std::forward<Args>(args)...);
            group_rpc_manager.finish_p2p_send(dest_node, return_pair.pending);
            return std::move(return_pair.results);
        } else {
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
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
    auto ordered_send(Args&&... args) {
        if(is_valid()) {
            size_t msg_size = wrapped_this->template get_size<tag>(std::forward<Args>(args)...);
            // // declare send_return_struct outside the lambda 'serializer'
            // // serializer will be called inside multicast_group.cpp, in send
            // // but we need the object after that, outside the lambda
            // // it's a pointer because one of its members is a reference
            // typename std::invoke_result<decltype (&rpc::RemoteInvocableOf<T>::template send<tag>)(rpc::RemoteInvocableOf<T>, std::function<char*(int)>&, Args...)>::type send_return_struct_ptr;

            using Ret = typename std::remove_pointer<decltype(wrapped_this->template getReturnType<tag>(std::forward<Args>(args)...))>::type;
            rpc::QueryResults<Ret>* results_ptr;
            rpc::PendingResults<Ret>* pending_ptr;

            auto serializer = [&](char* buffer) {
                std::size_t max_payload_size;
                int buffer_offset = group_rpc_manager.populate_nodelist_header({}, buffer, max_payload_size);
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
                results_ptr = new rpc::QueryResults<Ret>(std::move(send_return_struct.results));
                pending_ptr = &send_return_struct.pending;
            };

            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
            group_rpc_manager.view_manager.view_change_cv.wait(view_read_lock, [&]() {
                return group_rpc_manager.view_manager.curr_view
                        ->multicast_group->send(subgroup_id, msg_size, serializer, true);
            });
            group_rpc_manager.finish_rpc_send(*pending_ptr);
            return std::move(*results_ptr);
        } else {
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

    const uint64_t compute_global_stability_frontier() {
        return group_rpc_manager.view_manager.compute_global_stability_frontier(subgroup_id);
    }

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
    const persistent::version_t get_minimum_latest_persisted_version() noexcept(false) {
        return persistent_registry_ptr->getMinimumLatestPersistedVersion();
    }

    /**
     * Submits a call to send to be multicast to the subgroup,
     * with the message contents coming by invoking msg_generator inside the send function
     * It was named raw_send to contrast with cooked_send, but now renaming it to send
     * for a consistent API. There's no direct cooked send function anyway
     */
    void send(unsigned long long int payload_size, const std::function<void(char* buf)>& msg_generator) {
        group_rpc_manager.view_manager.send(subgroup_id, payload_size, msg_generator);
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
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
            receiver_socket.write(bytes, size);
        };
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
        rdv.insert(rdv.begin(), persistent_registry_ptr.get());
        mutils::DeserializationManager dsm{rdv};
        *user_object_ptr = std::move(mutils::from_bytes<T>(&dsm, buffer));
        if constexpr(std::is_base_of_v<GroupReference, T>) {
            (**user_object_ptr).set_group_pointers(group, subgroup_index);
        }
        return mutils::bytes_size(**user_object_ptr);
    }

    /**
     * make a version for all the persistent<T> members.
     * @param ver - the version number to be made
     */
    virtual void make_version(const persistent::version_t& ver, const HLC& hlc) noexcept(false) {
        persistent_registry_ptr->makeVersion(ver, hlc);
    };

    /**
     * persist the data to the latest version
     */
    virtual void persist(const persistent::version_t version) noexcept(false) {
        persistent::version_t persisted_ver;

        // persist variables
        do {
            persisted_ver = persistent_registry_ptr->persist();
            if(persisted_ver == -1) {
                // for replicated<T> without Persistent fields,
                // tell the persistent thread that we are done.
                persisted_ver = version;
            }
        } while(persisted_ver < version);
    };

    /**
     * trim the logs to a version, inclusively.
     * @param earliest_version - the version number, before which, logs are
     * going to be trimmed
     */
    virtual void trim(const persistent::version_t& earliest_version) noexcept(false) {
        persistent_registry_ptr->trim(earliest_version);
    };

    /**
     * Truncate the logs of all Persistent<T> members back to the version
     * specified. This deletes recently-used data, so it should only be called
     * during failure recovery when some versions must be rolled back.
     * @param latest_version The latest version number that should remain in the logs
     */
    virtual void truncate(const persistent::version_t& latest_version) {
        persistent_registry_ptr->truncate(latest_version);
    }

    /**
     * Post the next version to be handled.
     */
    virtual void post_next_version(const persistent::version_t& version) {
        next_version = version;
    }

    /**
     * Get the next version to be handled.
     */
    virtual persistent::version_t get_next_version() {
        return next_version;
    }

    /**
     * Register a persistent member
     * @param vf - the version function
     * @param pf - the persistent function
     * @param tf - the trim function
     */
    virtual void register_persistent_member(const char* object_name, const VersionFunc& vf, const PersistFunc& pf, const TrimFunc& tf, const LatestPersistedGetterFunc& gf, TruncateFunc tcf) noexcept(false) {
        this->persistent_registry_ptr->registerPersist(object_name, vf, pf, tf, gf, tcf);
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

public:
    ExternalCaller(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id, rpc::RPCManager& group_rpc_manager)
            : node_id(nid),
              subgroup_id(subgroup_id),
              group_rpc_manager(group_rpc_manager),
              wrapped_this(group_rpc_manager.make_remote_invoker<T>(type_id, subgroup_id, T::register_functions())) {}

    ExternalCaller(ExternalCaller&&) = default;
    ExternalCaller(const ExternalCaller&) = delete;

    //This is literally copied and pasted from Replicated<T>. I wish I could let them share code with inheritance,
    //but I'm afraid that will introduce unnecessary overheads.
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(node_id_t dest_node, Args&&... args) {
        if(is_valid()) {
            assert(dest_node != node_id);
            //Ensure a view change isn't in progress
            std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
            size_t size;
            const SubgroupSettings& settings = group_rpc_manager.view_manager.curr_view->multicast_group->get_subgroup_settings().at(subgroup_id); //TODO this code is copy pasted from above (like this method)
            auto max_payload_size = settings.profile.max_msg_size - sizeof(header);
            auto return_pair = wrapped_this->template send<tag>(
                    [this, &dest_node, &max_payload_size, &size](size_t _size) -> char* {
                        size = _size;
                        if(size <= max_payload_size) {
                            return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node, sst::REQUEST_TYPE::P2P_SEND);
                        } else {
                            return nullptr;
                        }
                    },
                    std::forward<Args>(args)...);
            group_rpc_manager.finish_p2p_send(dest_node, return_pair.pending);
            return std::move(return_pair.results);
        } else {
            throw derecho::empty_reference_exception{"Attempted to use an empty Replicated<T>"};
        }
    }

    bool is_valid() const { return true; }
};

template <typename T>
class ShardIterator {
private:
    ExternalCaller<T>& EC;
    const std::vector<node_id_t> shard_reps;

public:
    ShardIterator(ExternalCaller<T>& EC, std::vector<node_id_t> shard_reps)
            : EC(EC),
              shard_reps(shard_reps) {
    }
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(Args&&... args) {
        // shard_reps should have at least one member
        auto send_result = EC.template p2p_send<tag>(shard_reps.at(0), std::forward<Args>(args)...);
        std::vector<decltype(send_result)> send_result_vec;
        send_result_vec.emplace_back(std::move(send_result));
        for(uint i = 1; i < shard_reps.size(); ++i) {
            send_result_vec.emplace_back(EC.template p2p_send<tag>(shard_reps[i], std::forward<Args>(args)...));
        }
        return send_result_vec;
    }
};
}  // namespace derecho
