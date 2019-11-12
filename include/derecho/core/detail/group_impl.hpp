/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 */

#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "../group.hpp"
#include "container_template_functions.hpp"
#include "derecho_internal.hpp"
#include "make_kind_map.hpp"
#include <derecho/utils/logger.hpp>

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

template <typename ReplicatedType>
Replicated<ReplicatedType>&
GroupProjection<ReplicatedType>::get_subgroup(uint32_t subgroup_num) {
    void* ret{nullptr};
    set_replicated_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num,
                           &ret);
    return *((Replicated<ReplicatedType>*)ret);
}

template <typename ReplicatedType>
ExternalCaller<ReplicatedType>&
GroupProjection<ReplicatedType>::get_nonmember_subgroup(uint32_t subgroup_num) {
    void* ret{nullptr};
    set_external_caller_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num,
                                &ret);
    return *((ExternalCaller<ReplicatedType>*)ret);
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

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_replicated_pointer(std::type_index type,
                                                       uint32_t subgroup_num,
                                                       void** ret) {
    ((*ret = (type == std::type_index{typeid(ReplicatedTypes)}
                      ? &get_subgroup<ReplicatedTypes>(subgroup_num)
                      : *ret)),
     ...);
}

template <typename... ReplicatedTypes>
uint32_t Group<ReplicatedTypes...>::get_index_of_type(const std::type_info& ti) {
    assert_always((std::type_index{ti} == std::type_index{typeid(ReplicatedTypes)} || ... || false));
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
void Group<ReplicatedTypes...>::set_external_caller_pointer(std::type_index type,
                                                            uint32_t subgroup_num,
                                                            void** ret) {
    ((*ret = (type == std::type_index{typeid(ReplicatedTypes)}
                      ? &get_nonmember_subgroup<ReplicatedTypes>(subgroup_num)
                      : *ret)),
     ...);
}

/* There is only one constructor */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 IDeserializationContext* deserialization_context,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 Factory<ReplicatedTypes>... factories)
        : my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          is_starting_leader((getConfString(CONF_DERECHO_LOCAL_IP) == getConfString(CONF_DERECHO_LEADER_IP))
                             && (getConfUInt16(CONF_DERECHO_GMS_PORT) == getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT))),
          leader_connection([&]() -> std::optional<tcp::socket> {
              if(!is_starting_leader) {
                  return tcp::socket{getConfString(CONF_DERECHO_LEADER_IP), getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT)};
              }
              return std::nullopt;
          }()),
          user_deserialization_context(deserialization_context),
          persistence_manager(objects_by_subgroup_id, callbacks.local_persistence_callback),
          //Initially empty, all connections are added in the new view callback
          tcp_sockets(std::make_shared<tcp::tcp_connections>(my_id, std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_RPC_PORT)}}})),
          view_manager([&]() {
              if(is_starting_leader) {
                  return ViewManager(subgroup_info,
                                     {std::type_index(typeid(ReplicatedTypes))...},
                                     std::disjunction_v<has_persistent_fields<ReplicatedTypes>...>,
                                     tcp_sockets, objects_by_subgroup_id,
                                     persistence_manager.get_callbacks(),
                                     _view_upcalls);
              } else {
                  return ViewManager(leader_connection.value(),
                                     subgroup_info,
                                     {std::type_index(typeid(ReplicatedTypes))...},
                                     std::disjunction_v<has_persistent_fields<ReplicatedTypes>...>,
                                     tcp_sockets, objects_by_subgroup_id,
                                     persistence_manager.get_callbacks(),
                                     _view_upcalls);
              }
          }()),
          rpc_manager(view_manager, deserialization_context),
          factories(make_kind_map(factories...)) {
    //State transfer must complete before an initial view can commit, and must retry if the view is aborted
    bool initial_view_confirmed = false;
    while(!initial_view_confirmed) {
        //This might be the shard leaders from the previous view,
        //or the nodes with the longest logs in their shard if we're doing total restart,
        //or empty if this is the first View of a new group
        const vector_int64_2d& old_shard_leaders = view_manager.get_old_shard_leaders();
        //As a side effect, construct_objects filters old_shard_leaders to just the leaders
        //this node needs to receive object state from
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders_to_receive
                = construct_objects<ReplicatedTypes...>(view_manager.get_current_view_const().get(),
                                                        old_shard_leaders);
        //These functions are no-ops if we're not doing total restart
        view_manager.truncate_logs();
        view_manager.send_logs();
        receive_objects(subgroups_and_leaders_to_receive);
        if(is_starting_leader) {
            bool leader_has_quorum = true;
            initial_view_confirmed = view_manager.leader_prepare_initial_view(leader_has_quorum);
            if(!leader_has_quorum) {
                //If quorum was lost due to failures during the prepare message,
                //stop here and wait for more nodes to rejoin before going back to state-transfer
                view_manager.await_rejoining_nodes(my_id);
            }
        } else {
            //This will wait for a new view to be sent if the view was aborted
            initial_view_confirmed = view_manager.check_view_committed(leader_connection.value());
        }
    }
    if(is_starting_leader) {
        //In restart mode, once a prepare is successful, send a commit
        //(this function does nothing if we're not doing total restart)
        view_manager.leader_commit_initial_view();
    }
    //Once the initial view is committed, we can make RDMA connections
    view_manager.initialize_multicast_groups(callbacks);
    rpc_manager.create_connections();
    //This function registers some new-view upcalls to view_manager, so it must come before finish_setup()
    set_up_components();
    view_manager.finish_setup();
    //Start all the predicates and listeners threads
    rpc_manager.start_listening();
    view_manager.start();
    persistence_manager.start();
}

//nope there's two now
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const SubgroupInfo& subgroup_info, Factory<ReplicatedTypes>... factories)
        : Group({}, subgroup_info, nullptr, {}, factories...) {}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::~Group() {
    // shutdown the persistence manager
    // TODO-discussion:
    // Will a node be able to come back once it leaves? if not, maybe we should
    // shut it down on leave().
    persistence_manager.shutdown(true);
    tcp_sockets->destroy();
}

template <typename... ReplicatedTypes>
template <typename FirstType, typename... RestTypes>
std::set<std::pair<subgroup_id_t, node_id_t>> Group<ReplicatedTypes...>::construct_objects(
        const View& curr_view,
        const vector_int64_2d& old_shard_leaders) {
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
                //If we don't have a Replicated<T> for this (type, subgroup index), we just became a member of the shard
                if(replicated_objects.template get<FirstType>().count(subgroup_index) == 0) {
                    //Determine if there is existing state for this shard that will need to be received
                    bool has_previous_leader = old_shard_leaders.size() > subgroup_id
                                               && old_shard_leaders[subgroup_id].size() > shard_num
                                               && old_shard_leaders[subgroup_id][shard_num] > -1
                                               && old_shard_leaders[subgroup_id][shard_num] != my_id;
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
                                                   replicated_objects.template get<FirstType>().at(subgroup_index));
                    break;  // This node can be in at most one shard, so stop here
                }
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
            // Create an ExternalCaller for the subgroup if we don't already have one
            external_callers.template get<FirstType>().emplace(
                    subgroup_index, ExternalCaller<FirstType>(subgroup_type_id,
                                                              my_id, subgroup_id, rpc_manager));
        }
    }
    return functional_insert(subgroups_to_receive, construct_objects<RestTypes...>(curr_view, old_shard_leaders));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_up_components() {
    //Give PersistenceManager this pointer to break the circular dependency
    persistence_manager.set_view_manager(view_manager);
    //Now that MulticastGroup is constructed, tell it about RPCManager's message handler
    SharedLockedReference<View> curr_view = view_manager.get_current_view();
    curr_view.get().multicast_group->register_rpc_callback([this](subgroup_id_t subgroup, node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(subgroup, sender, buf, size);
    });
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    //ViewManager must call back to Group after a view change in order to call construct_objects,
    //since ViewManager doesn't know the template parameters
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view,
                                                           const vector_int64_2d& old_shard_leaders) {
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
                = construct_objects<ReplicatedTypes...>(view, old_shard_leaders);
        receive_objects(subgroups_and_leaders);
    });
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedTypes...>::get_subgroup(uint32_t subgroup_index) {
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
ExternalCaller<SubgroupType>& Group<ReplicatedTypes...>::get_nonmember_subgroup(uint32_t subgroup_index) {
    try {
        return external_callers.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No ExternalCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ShardIterator<SubgroupType> Group<ReplicatedTypes...>::get_shard_iterator(uint32_t subgroup_index) {
    try {
        auto& EC = external_callers.template get<SubgroupType>().at(subgroup_index);
        View& curr_view = view_manager.get_current_view().get();
        auto subgroup_id = curr_view.subgroup_ids_by_type_id.at(index_of_type<SubgroupType, ReplicatedTypes...>)
                                   .at(subgroup_index);
        const auto& shard_subviews = curr_view.subgroup_shard_views.at(subgroup_id);
        std::vector<node_id_t> shard_reps(shard_subviews.size());
        for(uint i = 0; i < shard_subviews.size(); ++i) {
            // for shard iteration to be possible, each shard must contain at least one member
            shard_reps[i] = shard_subviews[i].members.at(0);
        }
        return ShardIterator<SubgroupType>(EC, shard_reps);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No ExternalCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::receive_objects(const std::set<std::pair<subgroup_id_t, node_id_t>>& subgroups_and_leaders) {
    //This will receive one object from each shard leader in ascending order of subgroup ID
    for(const auto& subgroup_and_leader : subgroups_and_leaders) {
        LockedReference<std::unique_lock<std::mutex>, tcp::socket> leader_socket
                = tcp_sockets->get_socket(subgroup_and_leader.second);
        ReplicatedObject& subgroup_object = objects_by_subgroup_id.at(subgroup_and_leader.first);
        if(subgroup_object.is_persistent()) {
            int64_t log_tail_length = subgroup_object.get_minimum_latest_persisted_version();
            dbg_default_debug("Sending log tail length of {} for subgroup {} to node {}.",
                              log_tail_length, subgroup_and_leader.first, subgroup_and_leader.second);
            leader_socket.get().write(log_tail_length);
        }
        dbg_default_debug("Receiving Replicated Object state for subgroup {} from node {}",
                          subgroup_and_leader.first, subgroup_and_leader.second);
        std::size_t buffer_size;
        bool success = leader_socket.get().read(buffer_size);
        assert_always(success);
        char* buffer = new char[buffer_size];
        success = leader_socket.get().read(buffer, buffer_size);
        assert_always(success);
        subgroup_object.receive_object(buffer);
        delete[] buffer;
    }
    dbg_default_debug("Done receiving all Replicated Objects from subgroup leaders");
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
int32_t Group<ReplicatedTypes...>::get_my_rank() {
    return view_manager.get_my_rank();
}

template <typename... ReplicatedTypes>
node_id_t Group<ReplicatedTypes...>::get_my_id() {
    return my_id;
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
