#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <exception>
#include <experimental/optional>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

#include "tcp/tcp.h"

#include "derecho_exception.h"
#include "derecho_internal.h"
#include "persistence_manager.h"
#include "raw_subgroup.h"
#include "replicated.h"
#include "rpc_manager.h"
#include "subgroup_info.h"
#include "view_manager.h"

#include "conf/conf.hpp"
#include "mutils-containers/KindMap.hpp"
#include "mutils-containers/TypeMap2.hpp"
#include "spdlog/spdlog.h"

namespace derecho {
//Type alias for a sparse-vector of Replicated, otherwise KindMap can't understand it's a template
template <typename T>
using replicated_index_map = std::map<uint32_t, Replicated<T>>;

class _Group {
private:
public:
    virtual ~_Group() = default;
    template <typename SubgroupType>
    auto& get_subgroup(uint32_t subgroup_num = 0);
};

template <typename ReplicatedType>
class GroupProjection : public virtual _Group {
protected:
    virtual void set_replicated_pointer(std::type_index, uint32_t, void**) = 0;

public:
    Replicated<ReplicatedType>& get_subgroup(uint32_t subgroup_num = 0);
};

template <>
class GroupProjection<RawObject> : public virtual _Group {
protected:
    virtual void set_replicated_pointer(std::type_index, uint32_t, void**) = 0;

public:
    RawSubgroup& get_subgroup(uint32_t subgroup_num = 0);
};

class GroupReference {
public:
    _Group* group;
    uint32_t subgroup_index;
    void set_group_pointers(_Group* group, uint32_t subgroup_index) {
        this->group = group;
        this->subgroup_index = subgroup_index;
    }
};

/**
 * The top-level object for creating a Derecho group. This implements the group
 * management service (GMS) features and contains a MulticastGroup instance that
 * manages the actual sending and tracking of messages within the group.
 * @tparam ReplicatedTypes The types of user-provided objects that will represent
 * state and RPC functions for subgroups of this group.
 */
template <typename... ReplicatedTypes>
class Group : public virtual _Group, public GroupProjection<RawObject>, public GroupProjection<ReplicatedTypes>... {
public:
    void set_replicated_pointer(std::type_index type, uint32_t subgroup_num, void** ret);

private:
    using pred_handle = sst::Predicates<DerechoSST>::pred_handle;

    //Same thing for a sparse-vector of ExternalCaller
    template <typename T>
    using external_caller_index_map = std::map<uint32_t, ExternalCaller<T>>;

#ifndef NOLOG
    std::shared_ptr<spdlog::logger> logger;
#endif

    const node_id_t my_id;
    /** Persist the objects. Once persisted, persistence_manager updates the SST
     * so that the persistent progress is known by group members. */
    PersistenceManager<ReplicatedTypes...> persistence_manager;
    /** Contains all state related to managing Views, including the
     * ManagedGroup and SST (since those change when the view changes). */
    ViewManager view_manager;
    /** Contains all state related to receiving and handling RPC function
     * calls for any Replicated objects implemented by this group. */
    rpc::RPCManager rpc_manager;
    /** Maps a type to the Factory for that type. */
    mutils::KindMap<Factory, ReplicatedTypes...> factories;
    /** Maps each type T to a map of (index -> Replicated<T>) for that type's
     * subgroup(s). If this node is not a member of a subgroup for a type, the
     * map will have no entry for that type and index. (Instead, external_callers
     * will have an entry for that type-index pair). If this node is a member
     * of a subgroup, the Replicated<T> will refer to the one shard that this
     * node belongs to. */
    mutils::KindMap<replicated_index_map, ReplicatedTypes...> replicated_objects;
    /** Maps subgroup index -> RawSubgroup for the subgroups of type RawObject.
     * If this node is not a member of RawObject subgroup i, the RawSubgroup at
     * index i will be invalid; otherwise, the RawObject will refer to the one
     * shard of that subgroup that this node belongs to. */
    std::vector<RawSubgroup> raw_subgroups;
    /** Maps each type T to a map of (index -> ExternalCaller<T>) for the
     * subgroup(s) of that type that this node is not a member of. The
     * ExternalCaller for subgroup i of type T can be used to contact any member
     * of any shard of that subgroup, so shards are not indexed. */
    mutils::KindMap<external_caller_index_map, ReplicatedTypes...> external_callers;
    /** Alternate view of the Replicated<T>s, indexed by subgroup ID. The entry at
     * index X is a reference to the Replicated<T> for this node's shard of
     * subgroup X, which may or may not be valid. The references are the abstract
     * base type ReplicatedObject because they are only used for send/receive_object.
     * Note that this is a std::map solely so that we can initialize it out-of-order;
     * its keys are continuous integers starting at 0 and it should be a std::vector. */
    std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>> objects_by_subgroup_id;

    /* get_subgroup is actually implemented in these two methods. This is an
     * ugly hack to allow us to specialize get_subgroup<RawObject> to behave differently than
     * get_subgroup<T>. The unnecessary unused parameter is for overload selection. */
    template <typename SubgroupType>
    Replicated<SubgroupType>& get_subgroup(SubgroupType*, uint32_t subgroup_index);
    RawSubgroup& get_subgroup(RawObject*, uint32_t subgroup_index);

    /** Type of a 2-dimensional vector used to store potential node IDs, or -1 */
    using vector_int64_2d = std::vector<std::vector<int64_t>>;

    /** Deserializes a vector of shard leader IDs sent over the given socket. */
    static std::unique_ptr<vector_int64_2d> receive_old_shard_leaders(tcp::socket& leader_socket);

#ifndef NOLOG
    /**
     * Constructs a spdlog::logger instance and registers it in spdlog's global
     * registry, so that dependent objects like ViewManager can retrieve a
     * pointer to the same logger without needing to pass it around.
     * @return A pointer to the logger that was created.
     */
    std::shared_ptr<spdlog::logger> create_logger() const;
#endif
    /**
     * Updates the state of the replicated objects that correspond to subgroups
     * identified in the provided map, by receiving serialized state from the
     * shard leader whose ID is paired with that subgroup ID.
     * @param subgroups_and_leaders Pairs of (subgroup ID, leader's node ID) for
     * subgroups that need to have their state initialized from the leader.
     */
    void receive_objects(const std::set<std::pair<subgroup_id_t, node_id_t>>& subgroups_and_leaders);

    /** Constructor helper that wires together the component objects of Group. */
    void set_up_components();

    /**
     * Constructor helper that constructs RawSubgroup objects for each subgroup
     * of type RawObject; called to initialize the raw_subgroups map.
     * @param curr_view A reference to the current view as reported by View_manager
     * @return A vector containing a RawSubgroup for each "raw" subgroup the user
     * requested, at the index corresponding to that subgroup's index.
     */
    std::vector<RawSubgroup> construct_raw_subgroups(const View& curr_view);

    /**
     * Base case for the construct_objects template. Note that the neat "varargs
     * trick" (defining construct_objects(...) as the base case) doesn't work
     * because varargs can't match const references, and will force a copy
     * constructor on View. So std::enable_if is the only way to match an empty
     * template pack.
     */
    template <typename... Empty>
    typename std::enable_if<0 == sizeof...(Empty), std::set<std::pair<subgroup_id_t, node_id_t>>>::type
    construct_objects(const View&, const std::unique_ptr<vector_int64_2d>&) {
        return std::set<std::pair<subgroup_id_t, node_id_t>>();
    }

    /**
     * Constructor helper that unpacks this Group's template parameter pack.
     * Constructs Replicated<T> wrappers for each object being replicated,
     * using the corresponding Factory<T> saved in Group::factories. If this
     * node is not a member of the subgroup for a type T, an "empty" Replicated<T>
     * will be constructed with no corresponding object. If this node is joining
     * an existing group and there was a previous leader for its shard of a
     * subgroup, an "empty" Replicated<T> will also be constructed for that
     * subgroup, since all object state will be received from the shard leader.
     *
     * @param curr_view A reference to the current view as reported by View_manager
     * @param old_shard_leaders A pointer to the array of old shard leaders for
     * each subgroup (indexed by subgroup ID), if one exists.
     * @return The set of subgroup IDs that are un-initialized because this node is
     * joining an existing group and needs to receive initial object state, paired
     * with the ID of the node that should be contacted to receive that state.
     */
    template <typename FirstType, typename... RestTypes>
    std::set<std::pair<subgroup_id_t, node_id_t>> construct_objects(
            const View& curr_view, const std::unique_ptr<vector_int64_2d>& old_shard_leaders);

    /**
     * Delegate constructor for joining an existing managed group, called after
     * the entry-point constructor constructs a socket that connects to the leader.
     * @param my_id The node ID of the node running this code
     * @param leader_connection A socket connected to the existing group's leader
     * @param callbacks
     * @param subgroup_info
     * @param _view_upcalls
     * @param factories
     */
    Group(const node_id_t my_id,
          tcp::socket leader_connection,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls,
          Factory<ReplicatedTypes>... factories);

public:
    /**
     * Constructor that starts a new managed Derecho group with this node as
     * the leader. The DerechoParams will be passed through to construct
     * the underlying DerechoGroup. If they specify a filename, the group will
     * run in persistent mode and log all messages to disk.
     *
     * @param my_id The node ID of the node executing this code
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions that define how membership in
     * each subgroup and shard will be determined in this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
     * @param _view_upcalls A list of functions to be called when the group
     * experiences a View-Change event (optional).
     * @param factories A variable number of Factory functions, one for each
     * template parameter of Group, providing a way to construct instances of
     * each Replicated Object
     */
    Group(const node_id_t my_id,
          const ip_addr my_ip,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          const DerechoParams& derecho_params,
          std::vector<view_upcall_t> _view_upcalls = {},
          Factory<ReplicatedTypes>... factories);

    /**
     * Constructor that joins an existing managed Derecho group. The parameters
     * normally set by DerechoParams will be initialized by copying them from
     * the existing group's leader.
     *
     * @param my_id The node ID of the node running this code
     * @param my_ip The IP address of the node running this code
     * @param leader_id The node ID of the existing group's leader
     * @param leader_ip The IP address of the existing group's leader
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions that define how membership in
     * each subgroup and shard will be determined in this group. Must be the
     * same as the SubgroupInfo that was used to configure the group's leader.
     * @param _view_upcalls A list of functions to be called when the group
     * experiences a View-Change event (optional).
     * @param factories A variable number of Factory functions, one for each
     * template parameter of Group, providing a way to construct instances of
     * each Replicated Object
     */
    Group(const node_id_t my_id,
          const ip_addr my_ip,
          const ip_addr leader_ip,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls = {},
          Factory<ReplicatedTypes>... factories);

    ~Group();

    /**
     * Gets the "handle" for the subgroup of the specified type and index (which
     * is either a Replicated<T> or a RawSubgroup) assuming this node is a
     * member of the desired subgroup. If the subgroup has type RawObject, the
     * return value will be a RawSubgroup; otherwise it will be a Replicated<T>.
     * The Replicated<T> will contain the replicated state of an object of type
     * T and be usable to send multicasts to this node's shard of the subgroup.
     *
     * @param subgroup_index The index of the subgroup within the set of
     * subgroups that replicate the same type of object. Defaults to 0, so
     * if there is only one subgroup of type T, it can be retrieved with
     * get_subgroup<T>();
     * @tparam SubgroupType The object type identifying the subgroup
     * @return A reference to either a Replicated<SubgroupType> or a RawSubgroup
     * for this subgroup
     * @throws subgroup_provisioning_exception If there are no subgroups because
     * the current View is inadequately provisioned
     * @throws invalid_subgroup_exception If this node is not a member of the
     * requested subgroup.
     */
    template <typename SubgroupType>
    auto& get_subgroup(uint32_t subgroup_index = 0);

    /**
     * Gets the "handle" for a subgroup of the specified type and index,
     * assuming this node is not a member of the subgroup. The returned
     * ExternalCaller can be used to make peer-to-peer RPC calls to a specific
     * member of the subgroup.
     *
     * @param subgroup_index The index of the subgroup within the set of
     * subgroups that replicate the same type of object.
     * @tparam SubgroupType The object type identifying the subgroup
     * @return A reference to the ExternalCaller for this subgroup
     * @throws invalid_subgroup_exception If this node is actually a member of
     * the requested subgroup, or if no such subgroup exists
     */
    template <typename SubgroupType>
    ExternalCaller<SubgroupType>& get_nonmember_subgroup(uint32_t subgroup_index = 0);

    template <typename SubgroupType>
    ShardIterator<SubgroupType> get_shard_iterator(uint32_t subgroup_index = 0);

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Creates and returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();

    /** Reports to the GMS that the given node has failed. */
    void report_failure(const node_id_t who);
    /** Waits until all members of the group have called this function. */
    void barrier_sync();
    void debug_print_status() const;

#ifndef NOLOG
    void log_event(const std::string& event_text) {
        logger->debug(event_text);
    }
#endif
};

} /* namespace derecho */

#include "group_impl.h"
