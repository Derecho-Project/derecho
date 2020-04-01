#pragma once

#include <functional>
#include <mutex>
#include <utility>

#include "../replicated.hpp"

#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace derecho {

template <typename T>
Replicated<T>::Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                          uint32_t subgroup_index, uint32_t shard_num,
                          rpc::RPCManager& group_rpc_manager, Factory<T> client_object_factory,
                          _Group* group)
        : persistent_registry_ptr(std::make_unique<persistent::PersistentRegistry>(
                  this, std::type_index(typeid(T)), subgroup_index, shard_num)),
          user_object_ptr(std::make_unique<std::unique_ptr<T>>(
                  client_object_factory(persistent_registry_ptr.get(),subgroup_id))),
          node_id(nid),
          subgroup_id(subgroup_id),
          subgroup_index(subgroup_index),
          shard_num(shard_num),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(),
                                                                     type_id, subgroup_id,
                                                                     T::register_functions())),
          group(group) {
    if constexpr(std::is_base_of_v<GroupReference, T>) {
        (**user_object_ptr).set_group_pointers(group, subgroup_index);
    }
}

template <typename T>
Replicated<T>::Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                          uint32_t subgroup_index, uint32_t shard_num,
                          rpc::RPCManager& group_rpc_manager, _Group* group)
        : persistent_registry_ptr(std::make_unique<persistent::PersistentRegistry>(
                  this, std::type_index(typeid(T)), subgroup_index, shard_num)),
          user_object_ptr(std::make_unique<std::unique_ptr<T>>(nullptr)),
          node_id(nid),
          subgroup_id(subgroup_id),
          subgroup_index(subgroup_index),
          shard_num(shard_num),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(),
                                                                     type_id, subgroup_id,
                                                                     T::register_functions())),
          group(group) {}

template <typename T>
Replicated<T>::Replicated(Replicated&& rhs) : persistent_registry_ptr(std::move(rhs.persistent_registry_ptr)),
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

template <typename T>
Replicated<T>::~Replicated() {
    // hack to check if the object was merely moved
    if(wrapped_this) {
        group_rpc_manager.destroy_remote_invocable_class(subgroup_id);
    }
}

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto Replicated<T>::p2p_send(node_id_t dest_node, Args&&... args) {
    if(is_valid()) {
        if(group_rpc_manager.view_manager.get_current_view().get().rank_of(dest_node) == -1) {
            throw invalid_node_exception("Cannot send a p2p request to node "
                    + std::to_string(dest_node) + ": it is not a member of the Group.");
        }
        auto return_pair = wrapped_this->template send<tag>(
                [this, &dest_node](size_t size) -> char* {
                    const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
                    if(size <= max_payload_size) {
                        return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                           sst::REQUEST_TYPE::P2P_REQUEST);
                    } else {
                        throw derecho_exception("The size of serialized args exceeds the maximum message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.finish_p2p_send(dest_node, subgroup_id, return_pair.pending);
        return std::move(return_pair.results);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto Replicated<T>::ordered_send(Args&&... args) {
    if(is_valid()) {
        size_t payload_size_for_multicast_send = wrapped_this->template get_size_for_ordered_send<tag>(std::forward<Args>(args)...);

        using Ret = typename std::remove_pointer<decltype(wrapped_this->template getReturnType<tag>(
                std::forward<Args>(args)...))>::type;
        rpc::QueryResults<Ret>* results_ptr;
        rpc::PendingResults<Ret>* pending_ptr;
        auto serializer = [&](char* buffer) {
            //By the time this lambda runs, the current thread will be holding a read lock on view_mutex
            const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
            auto send_return_struct = wrapped_this->template send<tag>(
                    [&buffer, &max_payload_size](size_t size) -> char* {
                        if(size <= max_payload_size) {
                            return buffer;
                        } else {
                            throw derecho_exception("The size of serialized args exceeds the maximum message size.");
                        }
                    },
                    std::forward<Args>(args)...);
            results_ptr = new rpc::QueryResults<Ret>(std::move(send_return_struct.results));
            pending_ptr = &send_return_struct.pending;
        };

        std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
        group_rpc_manager.view_manager.view_change_cv.wait(view_read_lock, [&]() {
            return group_rpc_manager.view_manager.curr_view
                    ->multicast_group->send(subgroup_id, payload_size_for_multicast_send, serializer, true);
        });
        group_rpc_manager.finish_rpc_send(subgroup_id, *pending_ptr);
        return std::move(*results_ptr);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
void Replicated<T>::send(unsigned long long int payload_size,
                         const std::function<void(char* buf)>& msg_generator) {
    group_rpc_manager.view_manager.send(subgroup_id, payload_size, msg_generator);
}

template <typename T>
std::size_t Replicated<T>::object_size() const {
    return mutils::bytes_size(**user_object_ptr);
}

template <typename T>
void Replicated<T>::send_object(tcp::socket& receiver_socket) const {
    auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
        receiver_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write, object_size());
    send_object_raw(receiver_socket);
}

template <typename T>
void Replicated<T>::send_object_raw(tcp::socket& receiver_socket) const {
    auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {
        receiver_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write, **user_object_ptr);
}

template <typename T>
std::size_t Replicated<T>::receive_object(char* buffer) {
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

template <typename T>
void Replicated<T>::persist(const persistent::version_t version) noexcept(false) {
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

template <typename T>
const persistent::version_t Replicated<T>::get_minimum_latest_persisted_version() noexcept(false) {
    return persistent_registry_ptr->getMinimumLatestPersistedVersion();
}

template <typename T>
const uint64_t Replicated<T>::compute_global_stability_frontier() {
    return group_rpc_manager.view_manager.compute_global_stability_frontier(subgroup_id);
}

template <typename T>
ExternalCaller<T>::ExternalCaller(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                                  rpc::RPCManager& group_rpc_manager)
        : node_id(nid),
          subgroup_id(subgroup_id),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(rpc::make_remote_invoker<T>(nid, type_id, subgroup_id,
                                                                T::register_functions(), *group_rpc_manager.receivers)) {}

//This is literally copied and pasted from Replicated<T>. I wish I could let them share code with inheritance,
//but I'm afraid that will introduce unnecessary overheads.
template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto ExternalCaller<T>::p2p_send(node_id_t dest_node, Args&&... args) {
    if(is_valid()) {
        assert(dest_node != node_id);
        if(group_rpc_manager.view_manager.get_current_view().get().rank_of(dest_node) == -1) {
            throw invalid_node_exception("Cannot send a p2p request to node "
                    + std::to_string(dest_node) + ": it is not a member of the Group.");
        }
        auto return_pair = wrapped_this->template send<tag>(
                [this, &dest_node](size_t size) -> char* {
                    const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
                    if(size <= max_payload_size) {
                        return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                           sst::REQUEST_TYPE::P2P_REQUEST);
                    } else {
                        throw derecho_exception("The size of serialized args exceeds the maximum message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.finish_p2p_send(dest_node, subgroup_id, return_pair.pending);
        return std::move(return_pair.results);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto ShardIterator<T>::p2p_send(Args&&... args) {
    // shard_reps should have at least one member
    auto send_result = EC.template p2p_send<tag>(shard_reps.at(0), std::forward<Args>(args)...);
    std::vector<decltype(send_result)> send_result_vec;
    send_result_vec.emplace_back(std::move(send_result));
    for(uint i = 1; i < shard_reps.size(); ++i) {
        send_result_vec.emplace_back(EC.template p2p_send<tag>(shard_reps[i], std::forward<Args>(args)...));
    }
    return send_result_vec;
}

}  // namespace derecho
