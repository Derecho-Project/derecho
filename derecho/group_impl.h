/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 */

#include <mutils-serialization/SerializationSupport.hpp>

#include "derecho_internal.h"
#include "group.h"

namespace derecho {

template <typename MapType>
void kind_map_builder(MapType&){};

/**
 * Actual implementation of make_kind_map; needs to be a separate function
 * because the only way to build a KindMap is with a void mutator function.
 * @param map A mutable reference to the KindMap being constructed
 * @param curr_factory The first factory in the parameter pack of factories
 * @param rest_factories The rest of the parameter pack
 */
template <typename MapType, typename FirstType, typename... RestTypes>
void kind_map_builder(MapType& map, Factory<FirstType> curr_factory,
                      Factory<RestTypes>... rest_factories) {
    map.template get<FirstType>() = std::move(curr_factory);
    kind_map_builder<MapType, RestTypes...>(map, rest_factories...);
}

/**
 * Constructs a KindMap<Factory, Types...> from a list of factories of those
 * types. Could probably be made even more generic, to construct a KindMap of
 * any template given a list of objects that match that template, but that would
 * involve writing a template template parameter, which is too much black magic
 * for me to understand.
 * @param factories One instance of Factory<T> for each T in the type list
 * @return A KindMap of factories, mapping each type to a Factory for that type.
 */
template <typename... Types>
mutils::KindMap<Factory, Types...> make_kind_map(Factory<Types>... factories) {
    mutils::KindMap<Factory, Types...> factories_map;
    kind_map_builder<decltype(factories_map), Types...>(factories_map, factories...);
    return factories_map;
}

/**
 * Inserts set b into set a and returns the modified a. Hack to get around the
 * fact that set::insert doesn't return *this.
 * @param a The set to modify and return
 * @param b The set that should be inserted into a.
 * @return Set a.
 */
template <typename T>
std::set<T> functional_insert(std::set<T>& a, const std::set<T>& b) {
    a.insert(b.begin(), b.end());
    return a;
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(
        const node_id_t my_id,
        const ip_addr my_ip,
        const CallbackSet& callbacks,
        const SubgroupInfo& subgroup_info,
        const DerechoParams& derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedTypes>... factories)
        : logger(create_logger()),
          my_id(my_id),
          persistence_manager(callbacks.local_persistence_callback),
          view_manager(my_id, my_ip, callbacks, subgroup_info, derecho_params, 
            persistence_manager.get_callbacks(),
            _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(view_manager.get_current_view().get())){
    //In this case there will be no subgroups to receive objects for
    construct_objects<ReplicatedTypes...>(view_manager.get_current_view().get(), std::unique_ptr<vector_int64_2d>());
    set_up_components();
    persistence_manager.set_objects (std::addressof(replicated_objects));
    view_manager.start();
    persistence_manager.start();
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const node_id_t my_id,
                                 const ip_addr my_ip,
                                 const ip_addr leader_ip,
                                 const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 const int gms_port,
                                 Factory<ReplicatedTypes>... factories)
        : Group(my_id, tcp::socket{leader_ip, gms_port},
                callbacks, subgroup_info, _view_upcalls,
                gms_port, factories...) {}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const node_id_t my_id,
                                 tcp::socket leader_connection,
                                 const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 const int gms_port,
                                 Factory<ReplicatedTypes>... factories)
        : logger(create_logger()),
          my_id(my_id),
          persistence_manager(callbacks.local_persistence_callback),
          view_manager(my_id, leader_connection, callbacks, subgroup_info,
            persistence_manager.get_callbacks(),
            _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(view_manager.get_current_view().get())) {
    std::unique_ptr<vector_int64_2d> old_shard_leaders = receive_old_shard_leaders(leader_connection);
    set_up_components();
    persistence_manager.set_objects(std::addressof(replicated_objects));
    view_manager.start();
    std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
            = construct_objects<ReplicatedTypes...>(view_manager.get_current_view().get(), old_shard_leaders);
    receive_objects(subgroups_and_leaders);
    persistence_manager.start();
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const std::string& recovery_filename,
                                 const node_id_t my_id,
                                 const ip_addr my_ip,
                                 const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::experimental::optional<DerechoParams> _derecho_params,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 const int gms_port,
                                 Factory<ReplicatedTypes>... factories)
        : logger(create_logger()),
          my_id(my_id),
          view_manager(recovery_filename, my_id, my_ip, callbacks, subgroup_info, _derecho_params, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(view_manager.get_current_view().get())) {
    //TODO: This is the recover-from-saved-file constructor; I don't know how it will work
    construct_objects<ReplicatedTypes...>(view_manager.get_current_view().get(), std::unique_ptr<vector_int64_2d>());
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::~Group() {
    // shutdown the persistence manager
    // TODO-discussion:
    // Will a nodebe able to come back once it leaves? if not, maybe we should
    // shut it down on leave().
    persistence_manager.shutdown(true);
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
    assert(replicated_objects.template get<FirstType>().empty());
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
                if(old_shard_leaders && old_shard_leaders->size() > subgroup_id
                   && (*old_shard_leaders)[subgroup_id].size() > shard_num
                   && (*old_shard_leaders)[subgroup_id][shard_num] > -1
                   && (*old_shard_leaders)[subgroup_id][shard_num] != my_id) {
                    //Construct an empty Replicated because we'll receive object state from an old leader (who is not me)
                    replicated_objects.template get<FirstType>().emplace(
                            subgroup_index, Replicated<FirstType>(my_id, subgroup_id, rpc_manager));
                    subgroups_to_receive.emplace(subgroup_id, (*old_shard_leaders)[subgroup_id][shard_num]);
                } else {
                    replicated_objects.template get<FirstType>().emplace(
                            subgroup_index, Replicated<FirstType>(my_id, subgroup_id, rpc_manager,
                                                                  factories.template get<FirstType>()));
                }
                //Store a reference to the Replicated<T> just constructed
                objects_by_subgroup_id.emplace(subgroup_id, replicated_objects.template get<FirstType>().at(subgroup_index));
                break;  //This node can be in at most one shard, so stop here
            }
        }
        if(!in_subgroup) {
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
    SharedLockedReference<View> curr_view = view_manager.get_current_view();
    curr_view.get().multicast_group->register_rpc_callback([this](subgroup_id_t subgroup, node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(subgroup, sender, buf, size);
    });
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    view_manager.register_send_object_upcall([this](subgroup_id_t subgroup_id, node_id_t new_node_id) {
        objects_by_subgroup_id.at(subgroup_id).get().send_object(rpc_manager.get_socket(new_node_id).get());
    });
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view,
                                                           const vector_int64_2d& old_shard_leaders) {
        //ugh, we have to copy the vector to get it as a pointer
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
                = construct_objects<ReplicatedTypes...>(view, std::make_unique<vector_int64_2d>(old_shard_leaders));
        receive_objects(subgroups_and_leaders);
        raw_subgroups = construct_raw_subgroups(view);
    });
}

template <typename... ReplicatedTypes>
std::shared_ptr<spdlog::logger> Group<ReplicatedTypes...>::create_logger() const {
    spdlog::set_async_mode(1048576);
    std::vector<spdlog::sink_ptr> log_sinks;
    log_sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>("derecho_debug_log", 1024 * 1024 * 5, 3));
    // Uncomment this to get debugging output printed to the terminal
    // log_sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_mt>());
    std::shared_ptr<spdlog::logger> log = spdlog::create("debug_log", log_sinks.begin(), log_sinks.end());
    log->set_pattern("[%H:%M:%S.%f] [%l] %v");
    //log->set_level(spdlog::level::debug);
    log->set_level(spdlog::level::off);
    auto start_ms = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch());
    log->debug("Program start time (microseconds): {}", start_ms.count());
    return log;
}

template <typename... ReplicatedTypes>
std::unique_ptr<std::vector<std::vector<int64_t>>> Group<ReplicatedTypes...>::receive_old_shard_leaders(
        tcp::socket& leader_socket) {
    std::size_t buffer_size;
    leader_socket.read((char*)&buffer_size, sizeof(buffer_size));
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
void Group<ReplicatedTypes...>::receive_objects(const std::set<std::pair<subgroup_id_t, node_id_t>>& subgroups_and_leaders) {
    //This will receive one object from each shard leader in ascending order of subgroup ID
    for(const auto& subgroup_and_leader : subgroups_and_leaders) {
        LockedReference<std::unique_lock<std::mutex>, tcp::socket> leader_socket
                = rpc_manager.get_socket(subgroup_and_leader.second);
        std::size_t buffer_size;
        bool success = leader_socket.get().read((char*)&buffer_size, sizeof(buffer_size));
        assert(success);
        char buffer[buffer_size];
        objects_by_subgroup_id.at(subgroup_and_leader.first).get().receive_object(buffer);
    }
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
