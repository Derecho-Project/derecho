/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 */

#include <mutils-serialization/SerializationSupport.hpp>
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/async.h"
#include "spdlog/sinks/rotating_file_sink.h"

#include "derecho_internal.h"
#include "group.h"
#include "container_template_functions.h"

namespace derecho {

template <typename SubgroupType>
auto& _Group::get_subgroup(uint32_t subgroup_num) {
    return (dynamic_cast<GroupProjection<SubgroupType>*>(this))->get_subgroup(subgroup_num);
}

template <typename ReplicatedType>
Replicated<ReplicatedType>& GroupProjection<ReplicatedType>::get_subgroup(uint32_t subgroup_num) {
    void* ret{nullptr};
    set_replicated_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num, &ret);
    return *((Replicated<ReplicatedType>*)ret);
}

RawSubgroup& GroupProjection<RawObject>::get_subgroup(uint32_t subgroup_num) {
    void* ret{nullptr};
    set_replicated_pointer(std::type_index{typeid(RawObject)}, subgroup_num, &ret);
    return *((RawSubgroup*)ret);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_replicated_pointer(std::type_index type, uint32_t subgroup_num, void** ret) {
    if(type == std::type_index{typeid(RawObject)}) {
        *ret = &get_subgroup<RawObject>(subgroup_num);
    } else {
        ((*ret = (type == std::type_index{typeid(ReplicatedTypes)}
                          ? &get_subgroup<ReplicatedTypes>(subgroup_num)
                          : *ret)),
         ...);
    }
}

/* Leader constructor */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(
        const node_id_t my_id,
        const ip_addr my_ip,
        const CallbackSet& callbacks,
        const SubgroupInfo& subgroup_info,
        const DerechoParams& derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        Factory<ReplicatedTypes>... factories)
        : whenlog(logger(create_logger()),)
          my_id(my_id),
          persistence_manager(callbacks.local_persistence_callback),
          //Initially empty, all connections are added in the new view callback
          tcp_sockets(std::make_shared<tcp::tcp_connections>(my_id, std::map<node_id_t, ip_addr>(),
                      getConfInt32(CONF_DERECHO_RPC_PORT))),
          //Yes, objects_by_subgroup_id is empty at this point, but view_manager stores a reference
          view_manager(my_id, my_ip, callbacks, subgroup_info, derecho_params,
                       tcp_sockets, objects_by_subgroup_id,
                       persistence_manager.get_callbacks(),
                       _view_upcalls, getConfInt32(CONF_DERECHO_GMS_PORT)),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(view_manager.get_current_view().get())) {
    set_up_components();
    view_manager.finish_setup();
    //In this case there will be no subgroups to receive objects for
    construct_objects<ReplicatedTypes...>(view_manager.get_current_view().get(), std::unique_ptr<vector_int64_2d>());
    //If in total restart mode, this will make ViewManager send out logs for shards in which this node is the shard leader
    view_manager.send_logs_if_total_restart(nullptr);
    rpc_manager.start_listening();
    view_manager.start();
    persistence_manager.start();
}

/* Non-leader constructor, phase 1 */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const node_id_t my_id,
                                 const ip_addr my_ip,
                                 const ip_addr leader_ip,
                                 const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 Factory<ReplicatedTypes>... factories)
        : Group(my_id, tcp::socket{leader_ip, getConfInt32(CONF_DERECHO_GMS_PORT)},
                callbacks, subgroup_info, _view_upcalls,
		factories...) {}

/* Non-leader constructor, phase 2 */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const node_id_t my_id,
                                 tcp::socket leader_connection,
                                 const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 Factory<ReplicatedTypes>... factories)
        : whenlog(logger(create_logger()),)
          my_id(my_id),
          persistence_manager(callbacks.local_persistence_callback),
          tcp_sockets(std::make_shared<tcp::tcp_connections>(my_id, std::map<node_id_t, ip_addr>(),
                      getConfInt32(CONF_DERECHO_RPC_PORT))),
          view_manager(my_id, leader_connection, callbacks, subgroup_info,
                       tcp_sockets, objects_by_subgroup_id,
                       persistence_manager.get_callbacks(),
                       _view_upcalls, getConfInt32(CONF_DERECHO_GMS_PORT)),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(view_manager.get_current_view().get())) {
    std::unique_ptr<vector_int64_2d> old_shard_leaders = receive_old_shard_leaders(leader_connection);
    set_up_components();
    view_manager.finish_setup();
    std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
            = construct_objects<ReplicatedTypes...>(view_manager.get_current_view().get(), old_shard_leaders);
    view_manager.send_logs_if_total_restart(old_shard_leaders);
    receive_objects(subgroups_and_leaders);
    rpc_manager.start_listening();
    view_manager.start();
    persistence_manager.start();
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::~Group() {
    // shutdown the persistence manager
    // TODO-discussion:
    // Will a nodebe able to come back once it leaves? if not, maybe we should
    // shut it down on leave().
    persistence_manager.shutdown(true);
    tcp_sockets->destroy();
}

template <typename... ReplicatedTypes>
template <typename FirstType, typename... RestTypes>
std::set<std::pair<subgroup_id_t, node_id_t>> Group<ReplicatedTypes...>::construct_objects(
        const View& curr_view,
        const std::unique_ptr<vector_int64_2d>& old_shard_leaders) {
    std::set<std::pair<subgroup_id_t, uint32_t>> subgroups_to_receive;
    if(!curr_view.is_adequately_provisioned) {
        return subgroups_to_receive;
    }
    const auto& subgroup_ids = curr_view.subgroup_ids_by_type.at(std::type_index(typeid(FirstType)));
    for(uint32_t subgroup_index = 0; subgroup_index < subgroup_ids.size(); ++subgroup_index) {
        subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        //Find out if this node is in any shard of this subgroup
        bool in_subgroup = false;
        uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            const std::vector<node_id_t>& members = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num).members;
            //"If this node is in subview->members for this shard"
            if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                in_subgroup = true;
                //This node may have been re-assigned from a different shard, in which case we should delete the old shard's object state
                auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
                if(old_object != replicated_objects.template get<FirstType>().end()
                   && old_object->second.get_shard_num() != shard_num) {
                    whenlog(logger->debug("Deleting old Replicated Object state for type {}; I was reassigned from shard {} to shard {}", typeid(FirstType).name(), old_object->second.get_shard_num(), shard_num));
                    replicated_objects.template get<FirstType>().erase(old_object);
                }
                //If we don't have a Replicated<T> for this (type, subgroup index), we just became a member of the shard
                if(replicated_objects.template get<FirstType>().count(subgroup_index) == 0) {
                    //Determine if there is existing state for this shard that will need to be received
                    bool has_previous_leader = old_shard_leaders && old_shard_leaders->size() > subgroup_id
                                               && (*old_shard_leaders)[subgroup_id].size() > shard_num
                                               && (*old_shard_leaders)[subgroup_id][shard_num] > -1
                                               && (*old_shard_leaders)[subgroup_id][shard_num] != my_id;
                    if(has_previous_leader) {
                        subgroups_to_receive.emplace(subgroup_id, (*old_shard_leaders)[subgroup_id][shard_num]);
                    }
                    if(has_previous_leader && !has_persistent_fields<FirstType>::value) {
                        /* Construct an "empty" Replicated<T>, since all of T's state will be received
                         * from the leader and there are no logs to update */
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(my_id, subgroup_id, subgroup_index, shard_num, rpc_manager, this));
                    } else {
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(my_id, subgroup_id, subgroup_index, shard_num, rpc_manager,
                                                                      factories.template get<FirstType>(), this));
                    }
                    //Store a reference to the Replicated<T> just constructed
                    objects_by_subgroup_id.emplace(subgroup_id, replicated_objects.template get<FirstType>().at(subgroup_index));
                    break;  //This node can be in at most one shard, so stop here
                }
            }
        }
        if(!in_subgroup) {
            //If we have a Replicated<T> for the subgroup, but we're no longer a member, delete it
            auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
            if(old_object != replicated_objects.template get<FirstType>().end()) {
                whenlog(logger->debug("Deleting old Replicated Object state (of type {}) for subgroup {} because this node is no longer a member", typeid(FirstType).name(), subgroup_index));
                replicated_objects.template get<FirstType>().erase(old_object);
                objects_by_subgroup_id.erase(subgroup_id);
            }
            //Create an ExternalCaller for the subgroup if we don't already have one
            external_callers.template get<FirstType>().emplace(subgroup_index,
                                                               ExternalCaller<FirstType>(my_id, subgroup_id, rpc_manager));
        }
    }
    return functional_insert(subgroups_to_receive, construct_objects<RestTypes...>(curr_view, old_shard_leaders));
}

template <typename... ReplicatedTypes>
std::vector<RawSubgroup> Group<ReplicatedTypes...>::construct_raw_subgroups(const View& curr_view) {
    std::vector<RawSubgroup> raw_subgroup_vector;
    std::type_index raw_object_type(typeid(RawObject));
    auto ids_entry = curr_view.subgroup_ids_by_type.find(raw_object_type);
    if(ids_entry != curr_view.subgroup_ids_by_type.end()) {
        for(uint32_t index = 0; index < ids_entry->second.size(); ++index) {
            subgroup_id_t subgroup_id = ids_entry->second.at(index);
            uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
            bool in_subgroup = false;
            for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
                const std::vector<node_id_t>& members = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num).members;
                //"If this node is in subview->members for this shard"
                if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                    in_subgroup = true;
                    raw_subgroup_vector.emplace_back(RawSubgroup(my_id, subgroup_id, view_manager));
                    break;
                }
            }
            if(!in_subgroup) {
                //Put an empty RawObject in the vector, so there's something at this index
                raw_subgroup_vector.emplace_back(RawSubgroup(my_id, view_manager));
            }
        }
    }
    return raw_subgroup_vector;
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_up_components() {
    //Give PersistenceManager some pointers
    persistence_manager.set_objects(std::addressof(replicated_objects));
    persistence_manager.set_view_manager(std::addressof(view_manager));
    //Now that MulticastGroup is constructed, tell it about RPCManager's message handler
    SharedLockedReference<View> curr_view = view_manager.get_current_view();
    curr_view.get().multicast_group->register_rpc_callback([this](subgroup_id_t subgroup, node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(subgroup, sender, buf, size);
    });
    //Now that ViewManager is constructed, register some new-view upcalls for system functionality
    view_manager.add_view_upcall([this](const View& new_view) {
        update_tcp_connections_callback(new_view);
    });
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    //ViewManager must call back to Group after a view change in order to call construct_objects,
    //since ViewManager doesn't know the template parameters
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view,
                                                           const vector_int64_2d& old_shard_leaders) {
        //ugh, we have to copy the vector to get it as a pointer
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
                = construct_objects<ReplicatedTypes...>(view, std::make_unique<vector_int64_2d>(old_shard_leaders));
        receive_objects(subgroups_and_leaders);
        raw_subgroups = construct_raw_subgroups(view);
    });
}

#ifndef NOLOG
template <typename... ReplicatedTypes>
std::shared_ptr<spdlog::logger> Group<ReplicatedTypes...>::create_logger() const {
    spdlog::init_thread_pool(1048576, 1);
    std::vector<spdlog::sink_ptr> log_sinks;
    log_sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>("derecho_debug_log", 1024 * 1024 * 5, 3));
    // Uncomment this to get debugging output printed to the terminal
    log_sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
    std::shared_ptr<spdlog::logger> log = std::make_shared<spdlog::async_logger>("derecho_debug_log",
        log_sinks.begin(), log_sinks.end(), spdlog::thread_pool(), spdlog::async_overflow_policy::block);
    spdlog::register_logger(log);
    log->set_pattern("[%H:%M:%S.%f] [Thread %t] [%^%l%$] %v");
    log->set_level(
        whendebug(spdlog::level::debug)
        whenrelease(spdlog::level::off)
        );
    //    log->set_level(spdlog::level::off);
    auto start_ms = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch());
    log->debug("Program start time (microseconds): {}", start_ms.count());
    return log;
}
#endif

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::update_tcp_connections_callback(const View& new_view) {
    if(std::find(new_view.joined.begin(), new_view.joined.end(), my_id) != new_view.joined.end()) {
        //If this node is in the joined list, we need to set up a connection to everyone
        for(int i = 0; i < new_view.num_members; ++i) {
            if(new_view.members[i] != my_id) {
                tcp_sockets->add_node(new_view.members[i], new_view.member_ips[i]);
                whendebug(logger->debug("Established a TCP connection to node {}", new_view.members[i]);)
            }
        }
    } else {
        //This node is already a member, so we already have connections to the previous view's members
        for(const node_id_t& joiner_id : new_view.joined) {
            tcp_sockets->add_node(joiner_id,
                                  new_view.member_ips[new_view.rank_of(joiner_id)]);
            whenlog(logger->debug("Established a TCP connection to node {}", joiner_id);)
        }
        for(const node_id_t& removed_id : new_view.departed) {
            whenlog(logger->debug("Removing TCP connection for failed node {}", removed_id);)
            tcp_sockets->delete_node(removed_id);
        }
    }
}

template <typename... ReplicatedTypes>
std::unique_ptr<std::vector<std::vector<int64_t>>> Group<ReplicatedTypes...>::receive_old_shard_leaders(
        tcp::socket& leader_socket) {
    std::size_t buffer_size;
    leader_socket.read(buffer_size);
    if(buffer_size == 0) {
        return std::make_unique<vector_int64_2d>();
    }
    char buffer[buffer_size];
    leader_socket.read(buffer, buffer_size);
    return mutils::from_bytes<std::vector<std::vector<int64_t>>>(nullptr, buffer);
}

template <typename... ReplicatedTypes>
RawSubgroup& Group<ReplicatedTypes...>::get_subgroup(RawObject*, uint32_t subgroup_index) {
    return raw_subgroups.at(subgroup_index);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedTypes...>::get_subgroup(SubgroupType*, uint32_t subgroup_index) {
    return replicated_objects.template get<SubgroupType>().at(subgroup_index);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
auto& Group<ReplicatedTypes...>::get_subgroup(uint32_t subgroup_index) {
    if(!view_manager.get_current_view().get().is_adequately_provisioned) {
        throw subgroup_provisioning_exception("View is inadequately provisioned because subgroup provisioning failed!");
    }
    SubgroupType* overload_selector = nullptr;
    try {
        return get_subgroup(overload_selector, subgroup_index);
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
        auto subgroup_id = curr_view.subgroup_ids_by_type.at(typeid(SubgroupType)).at(subgroup_index);
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
            whenlog(logger->debug("Sending log tail length of {} for subgroup {} to node {}.", log_tail_length, subgroup_and_leader.first, subgroup_and_leader.second));
            leader_socket.get().write(log_tail_length);
        }
        whenlog(logger->debug("Receiving Replicated Object state for subgroup {} from node {}", subgroup_and_leader.first, subgroup_and_leader.second));
        std::size_t buffer_size;
        bool success = leader_socket.get().read(buffer_size);
        assert_always(success);
        char buffer[buffer_size];
        success = leader_socket.get().read(buffer, buffer_size);
        assert_always(success);
        subgroup_object.receive_object(buffer);
    }
    whenlog(logger->debug("Done receiving all Replicated Objects from subgroup leaders"));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::report_failure(const node_id_t who) {
    view_manager.report_failure(who);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::leave() {
    view_manager.leave();
}

template <typename... ReplicatedTypes>
std::vector<node_id_t> Group<ReplicatedTypes...>::get_members() {
    return view_manager.get_members();
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
