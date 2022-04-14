/**
 * @file group_impl.h
 * @date Apr 22, 2016
 */

#include "../group.hpp"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "derecho/utils/container_template_functions.hpp"
#include "derecho/utils/logger.hpp"
#include "derecho_internal.hpp"
#include "make_kind_map.hpp"

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace derecho {

template <typename SubgroupType>
auto& _Group::get_subgroup(uint32_t subgroup_num) {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_subgroup(subgroup_num);
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename SubgroupType>
auto& _Group::get_nonmember_subgroup(uint32_t subgroup_num) {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_nonmember_subgroup(subgroup_num);
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename SubgroupType>
ExternalClientCallback<SubgroupType>& _Group::get_client_callback(uint32_t subgroup_index) {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_client_callback(subgroup_index);
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename SubgroupType>
std::vector<std::vector<node_id_t>> _Group::get_subgroup_members(uint32_t subgroup_index) {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_subgroup_members(subgroup_index);
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename SubgroupType>
std::size_t _Group::get_number_of_shards(uint32_t subgroup_index) {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_number_of_shards(subgroup_index);
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename SubgroupType>
uint32_t _Group::get_num_subgroups() {
    if(auto gptr = dynamic_cast<GroupProjection<SubgroupType>*>(this)) {
        return gptr->get_num_subgroups();
    } else
        throw derecho_exception("Error: this top-level group contains no subgroups for the selected type.");
}

template <typename ReplicatedType>
Replicated<ReplicatedType>&
GroupProjection<ReplicatedType>::get_subgroup(uint32_t subgroup_num) {
    void* pointer_to_replicated{nullptr};
    set_replicated_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num,
                           &pointer_to_replicated);
    return *static_cast<Replicated<ReplicatedType>*>(pointer_to_replicated);
}

template <typename ReplicatedType>
PeerCaller<ReplicatedType>&
GroupProjection<ReplicatedType>::get_nonmember_subgroup(uint32_t subgroup_num) {
    void* pointer_to_peercaller{nullptr};
    set_peer_caller_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num,
                            &pointer_to_peercaller);
    return *static_cast<PeerCaller<ReplicatedType>*>(pointer_to_peercaller);
}

template <typename ReplicatedType>
ExternalClientCallback<ReplicatedType>&
GroupProjection<ReplicatedType>::get_client_callback(uint32_t subgroup_index) {
    void* pointer_to_client_callback{nullptr};
    set_external_client_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_index,
                                &pointer_to_client_callback);
    return *static_cast<ExternalClientCallback<ReplicatedType>*>(pointer_to_client_callback);
}

template <typename ReplicatedType>
std::vector<std::vector<node_id_t>>
GroupProjection<ReplicatedType>::get_subgroup_members(uint32_t subgroup_index) {
    return get_view_manager().get_subgroup_members(get_index_of_type(typeid(ReplicatedType)), subgroup_index);
}

template <typename ReplicatedType>
std::size_t
GroupProjection<ReplicatedType>::get_number_of_shards(uint32_t subgroup_index) {
    return get_view_manager().get_number_of_shards_in_subgroup(get_index_of_type(typeid(ReplicatedType)), subgroup_index);
}

template <typename ReplicatedType>
uint32_t GroupProjection<ReplicatedType>::get_num_subgroups() {
    return get_view_manager().get_num_subgroups(get_index_of_type(typeid(ReplicatedType)));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_replicated_pointer(std::type_index type,
                                                       uint32_t subgroup_index,
                                                       void** returned_replicated_ptr) {
    ((*returned_replicated_ptr = (type == std::type_index{typeid(ReplicatedTypes)}
                                          ? &get_subgroup<ReplicatedTypes>(subgroup_index)
                                          : *returned_replicated_ptr)),
     ...);
}

template <typename... ReplicatedTypes>
uint32_t Group<ReplicatedTypes...>::get_index_of_type(const std::type_info& ti) const {
    assert_always(((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)}) || ... || false));
    return (((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)}) ?  //
                     (index_of_type<ReplicatedTypes, ReplicatedTypes...>)
                                                                               : 0)
            + ... + 0);
    //return index_of_type<SubgroupType, ReplicatedTypes...>;
}

template <typename... ReplicatedTypes>
ViewManager& Group<ReplicatedTypes...>::get_view_manager() {
    return view_manager;
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_peer_caller_pointer(std::type_index type,
                                                        uint32_t subgroup_index,
                                                        void** returned_peercaller_ptr) {
    ((*returned_peercaller_ptr = (type == std::type_index{typeid(ReplicatedTypes)}
                                          ? &get_nonmember_subgroup<ReplicatedTypes>(subgroup_index)
                                          : *returned_peercaller_ptr)),
     ...);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_external_client_pointer(std::type_index type,
                                                            uint32_t subgroup_index,
                                                            void** returned_external_client_ptr) {
    ((*returned_external_client_ptr = (type == std::type_index{typeid(ReplicatedTypes)}
                                               ? &get_client_callback<ReplicatedTypes>(subgroup_index)
                                               : *returned_external_client_ptr)),
     ...);
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const UserMessageCallbacks& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 const std::vector<DeserializationContext*>& deserialization_context,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 Factory<ReplicatedTypes>... factories)
        : my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          user_deserialization_context(deserialization_context),
          persistence_manager(objects_by_subgroup_id,
                              std::disjunction_v<std::is_base_of<SignedPersistentFields, ReplicatedTypes>...>,
                              callbacks.local_persistence_callback),
          view_manager(subgroup_info,
                       {std::type_index(typeid(ReplicatedTypes))...},
                       std::disjunction_v<has_persistent_fields<ReplicatedTypes>...>,
                       objects_by_subgroup_id,
                       persistence_manager,
                       _view_upcalls),
          rpc_manager(view_manager, deserialization_context),
#if __GNUC__ < 9
          factories(make_kind_map(factories...)) {
#else
          factories(make_kind_map<Factory>(factories...)) {
#endif
    bool in_total_restart = view_manager.first_init();
    //State transfer must complete before an initial view can commit, and must retry if the view is aborted
    bool initial_view_confirmed = false;
    bool restart_leader_failed = false;
    while(!initial_view_confirmed) {
        if(restart_leader_failed) {
            //Retry connecting to the restart leader
            dbg_default_warn("Restart leader failed during 2PC! Trying again...");
            in_total_restart = view_manager.restart_to_initial_view();
        }
        //This might be the shard leaders from the previous view,
        //or the nodes with the longest logs in their shard if we're doing total restart,
        //or empty if this is the first View of a new group
        const vector_int64_2d& old_shard_leaders = view_manager.get_old_shard_leaders();
        //As a side effect, construct_objects filters old_shard_leaders to just the leaders
        //this node needs to receive object state from
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders_to_receive
                = construct_objects<ReplicatedTypes...>(view_manager.get_current_or_restart_view().get(),
                                                        old_shard_leaders, in_total_restart);
        if(in_total_restart) {
            view_manager.truncate_logs();
            view_manager.send_logs();
        }
        receive_objects(subgroups_and_leaders_to_receive);
        if(view_manager.is_starting_leader()) {
            if(in_total_restart) {
                bool leader_has_quorum = true;
                view_manager.leader_prepare_initial_view(initial_view_confirmed, leader_has_quorum);
                if(!leader_has_quorum) {
                    //If quorum was lost due to failures during the prepare message,
                    //stop here and wait for more nodes to rejoin before going back to state-transfer
                    view_manager.await_rejoining_nodes(my_id);
                }
            } else {
                initial_view_confirmed = true;
            }
        } else {
            //This will wait for a new view to be sent if the view was aborted
            //It must be called even in the non-restart case, since the initial view could still be aborted
            view_manager.check_view_committed(initial_view_confirmed, restart_leader_failed);
            if(restart_leader_failed && !(in_total_restart && getConfBoolean(CONF_DERECHO_ENABLE_BACKUP_RESTART_LEADERS))) {
                throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
            }
        }
    }
    if(view_manager.is_starting_leader()) {
        //In restart mode, once a prepare is successful, send a commit
        //(this function does nothing if we're not doing total restart)
        view_manager.leader_commit_initial_view();
    }
    //At this point the initial view is committed
    //Set up the multicast groups (including RDMA initialization) and register their callbacks
    MulticastGroupCallbacks internal_callbacks{
            //RPC message handler
            [this](subgroup_id_t subgroup, node_id_t sender, persistent::version_t version, uint64_t timestamp, uint8_t* buf, uint32_t size) {
                rpc_manager.rpc_message_handler(subgroup, sender, version, timestamp, buf, size);
            },
            //Post-next-version callback (set in ViewManager)
            nullptr,
            //Global persistence callback
            [this](subgroup_id_t subgroup, persistent::version_t version) {
                rpc_manager.notify_global_persistence_finished(subgroup, version);
            },
            //Verification callback
            [this](subgroup_id_t subgroup, persistent::version_t version) {
                rpc_manager.notify_verification_finished(subgroup, version);
            }};
    view_manager.initialize_multicast_groups(callbacks, internal_callbacks);
    rpc_manager.create_connections();
    //This function registers some new-view upcalls to view_manager, so it must come before finish_setup()
    set_up_components();
    view_manager.finish_setup();
    //Start all the predicates and listeners threads
    rpc_manager.start_listening();
    view_manager.start();
    persistence_manager.start();
}

/* A simpler constructor that uses "default" options for callbacks, upcalls, and deserialization context */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const SubgroupInfo& subgroup_info, Factory<ReplicatedTypes>... factories)
        : Group({}, subgroup_info, {}, {}, factories...) {}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::~Group() {
    // shutdown the persistence manager
    // TODO-discussion:
    // Will a node be able to come back once it leaves? if not, maybe we should
    // shut it down on leave().
    persistence_manager.shutdown(true);
}

template <typename... ReplicatedTypes>
template <typename FirstType, typename... RestTypes>
std::set<std::pair<subgroup_id_t, node_id_t>> Group<ReplicatedTypes...>::construct_objects(
        const View& curr_view,
        const vector_int64_2d& old_shard_leaders,
        bool in_restart) {
    std::set<std::pair<subgroup_id_t, uint32_t>> subgroups_to_receive;
    if(!curr_view.is_adequately_provisioned) {
        return subgroups_to_receive;
    }
    //The numeric type ID of this subgroup type is its position in the ordered list of subgroup types
    const subgroup_type_id_t subgroup_type_id = index_of_type<FirstType, ReplicatedTypes...>;
    const auto& subgroup_ids = curr_view.subgroup_ids_by_type_id.at(subgroup_type_id);
    for(uint32_t subgroup_index = 0; subgroup_index < subgroup_ids.size(); ++subgroup_index) {
        subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        // Find out if this node is in any shard of this subgroup
        bool in_subgroup = false;
        uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            const std::vector<node_id_t>& members = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num).members;
            //"If this node is in subview->members for this shard"
            if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                in_subgroup = true;
                // This node may have been re-assigned from a different shard, in which
                // case we should delete the old shard's object state
                auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
                if(old_object != replicated_objects.template get<FirstType>().end() && old_object->second.get_shard_num() != shard_num) {
                    dbg_default_debug("Deleting old Replicated Object state for type {}; I was reassigned from shard {} to shard {}",
                                      typeid(FirstType).name(), old_object->second.get_shard_num(), shard_num);
                    // also erase from objects_by_subgroup_id
                    objects_by_subgroup_id.erase(subgroup_id);
                    replicated_objects.template get<FirstType>().erase(old_object);
                }
                //Determine if there is existing state for this shard on another node
                bool has_previous_leader = old_shard_leaders.size() > subgroup_id
                                           && old_shard_leaders[subgroup_id].size() > shard_num
                                           && old_shard_leaders[subgroup_id][shard_num] > -1
                                           && old_shard_leaders[subgroup_id][shard_num] != my_id;
                //If we don't have a Replicated<T> for this (type, subgroup index), we just became a member of the shard
                if(replicated_objects.template get<FirstType>().count(subgroup_index) == 0) {
                    dbg_default_debug("Constructing a Replicated Object for type {}, subgroup {}, shard {}",
                                      typeid(FirstType).name(), subgroup_id, shard_num);
                    if(has_previous_leader) {
                        subgroups_to_receive.emplace(subgroup_id, old_shard_leaders[subgroup_id][shard_num]);
                    }
                    if(has_previous_leader && !has_persistent_fields<FirstType>::value) {
                        /* Construct an "empty" Replicated<T>, since all of T's state will
                         * be received from the leader and there are no logs to update */
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(subgroup_type_id, my_id,
                                                                      subgroup_id, subgroup_index,
                                                                      shard_num, rpc_manager, this));
                    } else {
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(subgroup_type_id, my_id,
                                                                      subgroup_id, subgroup_index, shard_num, rpc_manager,
                                                                      factories.template get<FirstType>(), this));
                    }
                    // Store a reference to the Replicated<T> just constructed
                    objects_by_subgroup_id.emplace(subgroup_id,
                                                   &replicated_objects.template get<FirstType>().at(subgroup_index));
                } else if(in_restart && has_previous_leader) {
                    //In restart mode, construct_objects may be called multiple times if the initial view
                    //is aborted, so we need to receive state for this shard even if we already constructed
                    //the Replicated<T> for it
                    subgroups_to_receive.emplace(subgroup_id, old_shard_leaders[subgroup_id][shard_num]);
                }
                break;  // This node can be in at most one shard, so stop here
            }
        }
        if(!in_subgroup) {
            // If we have a Replicated<T> for the subgroup, but we're no longer a member, delete it
            auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
            if(old_object != replicated_objects.template get<FirstType>().end()) {
                dbg_default_debug("Deleting old Replicated Object state (of type {}) for subgroup {} because this node is no longer a member",
                                  typeid(FirstType).name(), subgroup_index);
                objects_by_subgroup_id.erase(subgroup_id);
                replicated_objects.template get<FirstType>().erase(old_object);
            }
            // Create a PeerCaller for the subgroup if we don't already have one
            peer_callers.template get<FirstType>().emplace(
                    subgroup_index, PeerCaller<FirstType>(subgroup_type_id,
                                                          my_id, subgroup_id, rpc_manager));
        }
        // create the external client callback object if we don't have one
        external_client_callbacks.template get<FirstType>().emplace(
                subgroup_index, ExternalClientCallback<FirstType>(subgroup_type_id,
                                                                  my_id, subgroup_id, rpc_manager));
    }
    // add the client callback object
    // client_callback = std::make_unique<ExternalClientCallback<NotificationSupport>>(subgroup_type_id, my_id, rpc_manager);
    return functional_insert(subgroups_to_receive, construct_objects<RestTypes...>(curr_view, old_shard_leaders, in_restart));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_up_components() {
    //Give PersistenceManager this pointer to break the circular dependency
    persistence_manager.set_view_manager(view_manager);
    //Register RPCManager's notify_persistence_finished as a persistence callback
    persistence_manager.add_persistence_callback([this](subgroup_id_t subgroup_id, persistent::version_t version) {
        rpc_manager.notify_persistence_finished(subgroup_id, version);
    });
    //Connect ViewManager's external_join_handler to RPCManager
    view_manager.register_add_external_connection_upcall([this](uint32_t node_id) {
        rpc_manager.add_external_connection(node_id);
    });
    //Give RPCManager a standard "new view callback" on every View change
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    //ViewManager must call back to Group after a view change in order to call construct_objects,
    //since ViewManager doesn't know the template parameters
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view,
                                                           const vector_int64_2d& old_shard_leaders) {
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
                = construct_objects<ReplicatedTypes...>(view, old_shard_leaders, false);
        receive_objects(subgroups_and_leaders);
    });
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedTypes...>::get_subgroup(uint32_t subgroup_index) {
    static_assert(contains<SubgroupType, ReplicatedTypes...>::value, "get_subgroup was called with a template parameter that does not match any subgroup type");
    if(!view_manager.get_current_view().get().is_adequately_provisioned) {
        throw subgroup_provisioning_exception("View is inadequately provisioned because subgroup provisioning failed!");
    }
    try {
        return replicated_objects.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("Not a member of the requested subgroup.");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
PeerCaller<SubgroupType>& Group<ReplicatedTypes...>::get_nonmember_subgroup(uint32_t subgroup_index) {
    static_assert(contains<SubgroupType, ReplicatedTypes...>::value, "get_nonmember_subgroup was called with a template parameter that does not match any subgroup type");
    try {
        return peer_callers.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No PeerCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ExternalClientCallback<SubgroupType>& Group<ReplicatedTypes...>::get_client_callback(uint32_t subgroup_index) {
    try {
        return external_client_callbacks.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No ExternalClientCallback exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
uint32_t Group<ReplicatedTypes...>::get_num_subgroups() {
    static_assert(contains<SubgroupType, ReplicatedTypes...>::value, "get_num_subgroups was called with a template parameter that did not match any subgroup type");
    //No need to ask view_manager. This avoids locking the view_mutex.
    try {
        return replicated_objects.template get<SubgroupType>().size();
    } catch(std::out_of_range& ex) {
        //The SubgroupType must either be in replicated_objects or peer_callers
        return peer_callers.template get<SubgroupType>().size();
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ShardIterator<SubgroupType> Group<ReplicatedTypes...>::get_shard_iterator(uint32_t subgroup_index) {
    try {
        auto& caller = peer_callers.template get<SubgroupType>().at(subgroup_index);
        SharedLockedReference<View> curr_view = view_manager.get_current_view();
        auto subgroup_id = curr_view.get().subgroup_ids_by_type_id.at(index_of_type<SubgroupType, ReplicatedTypes...>).at(subgroup_index);
        const auto& shard_subviews = curr_view.get().subgroup_shard_views.at(subgroup_id);
        std::vector<node_id_t> shard_reps(shard_subviews.size());
        for(uint i = 0; i < shard_subviews.size(); ++i) {
            // for shard iteration to be possible, each shard must contain at least one member
            shard_reps[i] = shard_subviews[i].members.at(0);
        }
        return ShardIterator<SubgroupType>(caller, shard_reps);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No PeerCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::receive_objects(const std::set<std::pair<subgroup_id_t, node_id_t>>& subgroups_and_leaders) {
    //This will receive one object from each shard leader in ascending order of subgroup ID
    for(const auto& subgroup_and_leader : subgroups_and_leaders) {
        LockedReference<std::unique_lock<std::mutex>, tcp::socket> leader_socket
                = view_manager.get_transfer_socket(subgroup_and_leader.second);
        ReplicatedObject* subgroup_object = objects_by_subgroup_id.at(subgroup_and_leader.first);
        try {
            if(subgroup_object->is_persistent()) {
                persistent::version_t log_tail_length = subgroup_object->get_minimum_latest_persisted_version();
                dbg_default_debug("Sending log tail length of {} for subgroup {} to node {}.",
                                  log_tail_length, subgroup_and_leader.first, subgroup_and_leader.second);
                leader_socket.get().write(log_tail_length);
            }
            dbg_default_debug("Receiving Replicated Object state for subgroup {} from node {}",
                              subgroup_and_leader.first, subgroup_and_leader.second);
            std::size_t buffer_size;
            leader_socket.get().read(buffer_size);
            std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(buffer_size);
            leader_socket.get().read(buffer.get(), buffer_size);
            dbg_default_trace("Deserializing Replicated Object from buffer of size {}", buffer_size);
            subgroup_object->receive_object(buffer.get());
        } catch(tcp::socket_error& e) {
            //Convert socket exceptions to a more readable error message, since this will cause a crash
            throw derecho_exception("Fatal error: Node " + std::to_string(subgroup_and_leader.second) + " failed during state transfer!");
        }
    }

    dbg_default_debug("Done receiving all Replicated Objects from subgroup leaders {}", subgroups_and_leaders.empty() ? "(there were none to receive)" : "");
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::report_failure(const node_id_t who) {
    view_manager.report_failure(who);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::leave(bool group_shutdown) {
    if(group_shutdown) {
        view_manager.silence();
        view_manager.barrier_sync();
    }
    view_manager.leave();
}

template <typename... ReplicatedTypes>
std::vector<node_id_t> Group<ReplicatedTypes...>::get_members() {
    return view_manager.get_members();
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<std::vector<node_id_t>> Group<ReplicatedTypes...>::get_subgroup_members(uint32_t subgroup_index) {
    return GroupProjection<SubgroupType>::get_subgroup_members(subgroup_index);
}
template <typename... ReplicatedTypes>
template <typename SubgroupType>
int32_t Group<ReplicatedTypes...>::get_my_shard(uint32_t subgroup_index) {
    return view_manager.get_my_shard(index_of_type<SubgroupType, ReplicatedTypes...>, subgroup_index);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<uint32_t> Group<ReplicatedTypes...>::get_my_subgroup_indexes() {
    return view_manager.get_my_subgroup_indexes(index_of_type<SubgroupType, ReplicatedTypes...>);
}

template <typename... ReplicatedTypes>
int32_t Group<ReplicatedTypes...>::get_my_rank() {
    return view_manager.get_my_rank();
}

template <typename... ReplicatedTypes>
node_id_t Group<ReplicatedTypes...>::get_my_id() {
    return my_id;
}

template <typename... ReplicatedTypes>
node_id_t Group<ReplicatedTypes...>::get_rpc_caller_id() {
    return rpc::RPCManager::get_rpc_caller_id();
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::barrier_sync() {
    view_manager.barrier_sync();
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::debug_print_status() const {
    view_manager.debug_print_status();
}

} /* namespace derecho */
