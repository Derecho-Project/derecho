/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 * @author Edward
 */


#include <mutils-serialization/SerializationSupport.hpp>

#include "group.h"

namespace derecho {

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(
        const ip_addr my_ip,
        CallbackSet callbacks,
        const DerechoParams& derecho_params,
        const SubgroupInfo& subgroup_info,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedObjects>... factories)
        : subgroup_info(subgroup_info),
          view_manager(my_ip, callbacks, derecho_params, _view_upcalls, gms_port),
          rpc_manager(0, view_manager) {
    //    ^ In this constructor, this is the first node to start, so my ID will be 0
    construct_objects(0, view_manager.get_current_view(), subgroup_info, factories...);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const node_id_t my_id,
        const ip_addr my_ip,
        const node_id_t leader_id,
        const ip_addr leader_ip,
        CallbackSet callbacks,
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
          CallbackSet callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls,
          const int gms_port,
          Factory<ReplicatedObjects>... factories)
          : view_manager(my_id, leader_connection, callbacks, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager) {
    construct_objects(my_id, view_manager.get_current_view(), factories...);
    receive_objects(leader_connection);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const std::string& recovery_filename,
        const node_id_t my_id,
        const ip_addr my_ip,
        CallbackSet callbacks,
        std::experimental::optional<SubgroupInfo> _subgroup_info,
        std::experimental::optional<DerechoParams> _derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedObjects>... factories)
        : subgroup_info(_subgroup_info.value()),
          view_manager(recovery_filename, my_id, my_ip, callbacks, _derecho_params, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager) {
    //TODO: This is the recover-from-saved-file constructor; I don't know how it will work
    set_up_components();

}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::~Group() {

}


template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::set_up_components() {
    view_manager.add_view_upcall([this](std::vector<node_id_t> new_members,
            std::vector<node_id_t> old_members) {
        rpc_manager.new_view_callback(new_members, old_members);
    });
    view_manager.get_current_view().multicast_group->register_rpc_callback(
            [this](node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(sender, buf, size);
    });
    view_manager.register_send_objects_upcall([this](tcp::socket& joiner_socket){
        send_objects(joiner_socket);
    });
}


template <typename... ReplicatedObjects>
template<typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedObjects...>::get_subgroup(uint32_t subgroup_index) {
    return replicated_objects.template get<SubgroupType>().at(subgroup_index);
}

template<typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::send_objects(tcp::socket& reciever_socket) {
    std::size_t total_size = 0;
    replicated_objects.for_each([&](const auto&, const auto& objects_map){
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair->second.is_valid()) {
                total_size += index_object_pair->second.object_size();
            }
        }
    });
    replicated_objects.for_each([&](const auto&, const auto& objects_map){
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair->second.is_valid()) {
                index_object_pair->second.send_object_raw(reciever_socket);
            }
        }
    });
}

template<typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::receive_objects(tcp::socket& sender_socket) {
    std::size_t total_size;
    bool success = sender_socket.read((char*)&total_size, sizeof(size_t));
    assert(success);
    char* buf = new char[total_size];
    success = sender_socket.read(buf, total_size);
    assert(success);
    size_t offset = 0;
    replicated_objects.for_each([&](const auto&, const auto& objects_map){
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair->second) {
                std::size_t bytes_read = index_object_pair->second.receive_object(buf + offset);
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
char* Group<ReplicatedObjects...>::get_sendbuffer_ptr(unsigned long long int payload_size,
        int pause_sending_turns, bool cooked_send) {
    return view_manager.get_sendbuffer_ptr(payload_size, pause_sending_turns, cooked_send);
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::send() {
    view_manager.send();
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
