/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 * @author Edward
 */

#include <mutils-serialization/SerializationSupport.hpp>

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
    map.template get<FirstType>() = curr_factory;
    kind_map_builder<MapType, RestTypes...>(map, rest_factories...);
}

/**
 * Constructs a KindMap<Factory, Types...> from a list of factories of those
 * types. Could probably be made even more generic, to construct a KindMap of
 * any template given a list of objects that match that template, but that would
 * involve writing a template template parameter.
 * @param factories One instance of Factory<T> for each T in the type list
 * @return A KindMap of factories, mapping each type to a Factory for that type.
 */
template <typename... Types>
mutils::KindMap<Factory, Types...> make_kind_map(Factory<Types>... factories) {
    mutils::KindMap<Factory, Types...> factories_map;
    kind_map_builder<decltype(factories_map), Types...>(factories_map, factories...);
    return factories_map;
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(
    const ip_addr my_ip,
    const CallbackSet& callbacks,
    const SubgroupInfo& subgroup_info,
    const DerechoParams& derecho_params,
    std::vector<view_upcall_t> _view_upcalls,
    const int gms_port,
    Factory<ReplicatedObjects>... factories)
        : view_manager(my_ip, callbacks, subgroup_info, derecho_params, _view_upcalls, gms_port),
          rpc_manager(0, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(0, view_manager.get_current_view())) {
    //    ^ In this constructor, this is the first node to start, so my ID will be 0
    construct_objects<ReplicatedObjects...>(0, view_manager.get_current_view());
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const node_id_t my_id,
                                   const ip_addr my_ip,
                                   const ip_addr leader_ip,
                                   const CallbackSet& callbacks,
                                   const SubgroupInfo& subgroup_info,
                                   std::vector<view_upcall_t> _view_upcalls,
                                   const int gms_port,
                                   Factory<ReplicatedObjects>... factories)
        : Group(my_id, tcp::socket{leader_ip, gms_port},
                callbacks, subgroup_info, _view_upcalls,
                gms_port, factories...) {}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const node_id_t my_id,
                                   tcp::socket leader_connection,
                                   const CallbackSet& callbacks,
                                   const SubgroupInfo& subgroup_info,
                                   std::vector<view_upcall_t> _view_upcalls,
                                   const int gms_port,
                                   Factory<ReplicatedObjects>... factories)
        : view_manager(my_id, leader_connection, callbacks, subgroup_info, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(my_id, view_manager.get_current_view())) {
    const View& dont_copy_this = view_manager.get_current_view();
    construct_objects<ReplicatedObjects...>(my_id, dont_copy_this);
    receive_objects(leader_connection);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const std::string& recovery_filename,
                                   const node_id_t my_id,
                                   const ip_addr my_ip,
                                   const CallbackSet& callbacks,
                                   const SubgroupInfo& subgroup_info,
                                   std::experimental::optional<DerechoParams> _derecho_params,
                                   std::vector<view_upcall_t> _view_upcalls,
                                   const int gms_port,
                                   Factory<ReplicatedObjects>... factories)
        : view_manager(recovery_filename, my_id, my_ip, callbacks, subgroup_info, _derecho_params, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          factories(make_kind_map(factories...)),
          raw_subgroups(construct_raw_subgroups(my_id, view_manager.get_current_view())) {
    //TODO: This is the recover-from-saved-file constructor; I don't know how it will work
    construct_objects<ReplicatedObjects...>(my_id, view_manager.get_current_view());
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::~Group() {
}

template <typename... ReplicatedObjects>
template <typename FirstType, typename... RestTypes>
void Group<ReplicatedObjects...>::construct_objects(node_id_t my_id, const View& curr_view) {
    if(!curr_view.is_adequately_provisioned) {
        std::cout << "construct_objects: View is not adequately provisioned! Returning early." << std::endl;
        return;
    }
    const auto& subgroup_ids = curr_view.subgroup_ids_by_type.at(std::type_index(typeid(FirstType)));
    for(uint32_t subgroup_index = 0; subgroup_index < subgroup_ids.size(); ++subgroup_index) {
        subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        //Find out if this node is in any shard of this subgroup
        bool in_subgroup = false;
        uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            const std::vector<node_id_t>& members = curr_view.subgroup_shard_views
                                                        .at(subgroup_id)
                                                        .at(shard_num)
                                                        ->members;
            //"If this node is in subview->members for this shard"
            if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                in_subgroup = true;
                auto insert_status = replicated_objects.template get<FirstType>().emplace(
                    subgroup_index, Replicated<FirstType>(my_id, subgroup_id, rpc_manager,
                                                          factories.template get<FirstType>()));
                if(insert_status.second == false) {
                    std::cout << "When constructing subgroup " << subgroup_id << " a Replicated already existed;"
                              << " its is_valid() status is " << std::boolalpha << insert_status.first->second.is_valid() << std::endl;
                }
                break;  //This node can be in at most one shard
            }
        }
        if(!in_subgroup) {
            //Put an empty Replicated in the map
            replicated_objects.template get<FirstType>().emplace(subgroup_index,
                                                                 Replicated<FirstType>(my_id, rpc_manager));
        }
    }
    construct_objects<RestTypes...>(my_id, curr_view);
}

template <typename... ReplicatedObjects>
std::map<uint32_t, RawSubgroup> Group<ReplicatedObjects...>::construct_raw_subgroups(
    node_id_t my_id, const View& curr_view) {
    std::map<uint32_t, RawSubgroup> raw_subgroup_map;
    std::type_index raw_object_type(typeid(RawObject));
    auto ids_entry = curr_view.subgroup_ids_by_type.find(raw_object_type);
    if(ids_entry != curr_view.subgroup_ids_by_type.end()) {
        for(uint32_t index = 0; index < ids_entry->second.size(); ++index) {
            subgroup_id_t subgroup_id = ids_entry->second.at(index);
            uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
            for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
                const std::vector<node_id_t>& members = curr_view.subgroup_shard_views
                                                            .at(subgroup_id)
                                                            .at(shard_num)
                                                            ->members;
                //"If this node is in subview->members for this shard"
                if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                    raw_subgroup_map.emplace(index,
                                             RawSubgroup(my_id, subgroup_id, view_manager));
                    break;
                }
            }
        }
    }
    return raw_subgroup_map;
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::set_up_components() {
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    view_manager.get_current_view().multicast_group->register_rpc_callback(
        [this](node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(sender, buf, size);
        });
    view_manager.register_send_objects_upcall([this](tcp::socket& joiner_socket) {
        send_objects(joiner_socket);
    });
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view) {
        construct_objects<ReplicatedObjects...>(my_id, view);
    });
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::rebuild_objects(node_id_t my_id, const View& next_view) {
    construct_objects<ReplicatedObjects...>(my_id, next_view);
}

template <typename... ReplicatedObjects>
RawSubgroup& Group<ReplicatedObjects...>::get_subgroup(RawObject*, uint32_t subgroup_index) {
    return raw_subgroups.at(subgroup_index);
}

template <typename... ReplicatedObjects>
template <typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedObjects...>::get_subgroup(SubgroupType*, uint32_t subgroup_index) {
    return replicated_objects.template get<SubgroupType>().at(subgroup_index);
}

template <typename... ReplicatedObjects>
template <typename SubgroupType>
auto& Group<ReplicatedObjects...>::get_subgroup(uint32_t subgroup_index) {
    if(!view_manager.get_current_view().is_adequately_provisioned) {
        throw subgroup_provisioning_exception("View is inadequately provisioned because subgroup provisioning failed!");
    }
    SubgroupType* overload_selector = nullptr;
    return get_subgroup(overload_selector, subgroup_index);
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::send_objects(tcp::socket& receiver_socket) {
    std::size_t total_size = 0;
    replicated_objects.for_each([&](const auto&, const auto& objects_map) {
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                total_size += index_object_pair.second.object_size();
            }
        }
    });
    mutils::post_object([&receiver_socket](const char* bytes, std::size_t size) {
        receiver_socket.write(bytes, size); },
                        total_size);
    replicated_objects.for_each([&](const auto&, const auto& objects_map) {
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                index_object_pair.second.send_object_raw(receiver_socket);
            }
        }
    });
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::receive_objects(tcp::socket& sender_socket) {
    std::size_t total_size;
    bool success = sender_socket.read((char*)&total_size, sizeof(size_t));
    assert(success);
    //If there are no objects to receive, don't try to receive any
    if(total_size == 0)
        return;
    char* buf = new char[total_size];
    success = sender_socket.read(buf, total_size);
    assert(success);
    size_t offset = 0;
    replicated_objects.for_each([&](const auto&, auto& objects_map) {
        for(auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                std::size_t bytes_read = index_object_pair.second.receive_object(buf + offset);
                offset += bytes_read;
            }
        }
    });
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::report_failure(const node_id_t who) {
    view_manager.report_failure(who);
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::leave() {
    view_manager.leave();
}

template <typename... ReplicatedObjects>
std::vector<node_id_t> Group<ReplicatedObjects...>::get_members() {
    return view_manager.get_members();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::barrier_sync() {
    view_manager.barrier_sync();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::debug_print_status() const {
    view_manager.debug_print_status();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::print_log(std::ostream& output_dest) const {
    view_manager.print_log(output_dest);
}

} /* namespace derecho */
