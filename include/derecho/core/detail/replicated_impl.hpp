#pragma once

#include <functional>
#include <mutex>
#include <utility>

#include "../replicated.hpp"
#include "view_manager.hpp"

#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace derecho {

template <typename T>
Replicated<T>::Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                          uint32_t subgroup_index, uint32_t shard_num,
                          rpc::RPCManager& group_rpc_manager, Factory<T> client_object_factory,
                          _Group* group)
        : persistent_registry(std::make_unique<persistent::PersistentRegistry>(
                this, std::type_index(typeid(T)), subgroup_index, shard_num)),
          user_object_ptr(std::make_unique<std::unique_ptr<T>>(
                  client_object_factory(persistent_registry.get(), subgroup_id))),
          node_id(nid),
          subgroup_id(subgroup_id),
          subgroup_index(subgroup_index),
          shard_num(shard_num),
          signer(nullptr),
          signature_size(0),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(),
                                                                     type_id, subgroup_id,
                                                                     T::register_functions())),
          group(group) {
    if constexpr(std::is_base_of_v<GroupReference, T>) {
        (**user_object_ptr).set_group_pointers(group, subgroup_index);
    }
    if constexpr(std::is_base_of_v<SignedPersistentFields, T>) {
        //Attempt to load the private key and create a Signer
        //This will crash with a file_error if the private key doesn't actually exist
        signer = std::make_unique<openssl::Signer>(openssl::EnvelopeKey::from_pem_private(getConfString(CONF_PERS_PRIVATE_KEY_FILE)),
                                                   openssl::DigestAlgorithm::SHA256);
        signature_size = signer->get_max_signature_size();
    }
}

template <typename T>
Replicated<T>::Replicated(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                          uint32_t subgroup_index, uint32_t shard_num,
                          rpc::RPCManager& group_rpc_manager, _Group* group)
        : persistent_registry(std::make_unique<persistent::PersistentRegistry>(
                this, std::type_index(typeid(T)), subgroup_index, shard_num)),
          user_object_ptr(std::make_unique<std::unique_ptr<T>>(nullptr)),
          node_id(nid),
          subgroup_id(subgroup_id),
          subgroup_index(subgroup_index),
          shard_num(shard_num),
          signer(nullptr),
          signature_size(0),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(group_rpc_manager.make_remote_invocable_class(user_object_ptr.get(),
                                                                     type_id, subgroup_id,
                                                                     T::register_functions())),
          group(group) {
    if constexpr(std::is_base_of_v<SignedPersistentFields, T>) {
        //Attempt to load the private key and create a Signer
        //This will crash with a file_error if the private key doesn't actually exist
        signer = std::make_unique<openssl::Signer>(openssl::EnvelopeKey::from_pem_private(getConfString(CONF_PERS_PRIVATE_KEY_FILE)),
                                                   openssl::DigestAlgorithm::SHA256);
        signature_size = signer->get_max_signature_size();
    }
}

template <typename T>
Replicated<T>::Replicated(Replicated&& rhs) : persistent_registry(std::move(rhs.persistent_registry)),
                                              user_object_ptr(std::move(rhs.user_object_ptr)),
                                              node_id(rhs.node_id),
                                              subgroup_id(rhs.subgroup_id),
                                              subgroup_index(rhs.subgroup_index),
                                              shard_num(rhs.shard_num),
                                              signer(std::move(rhs.signer)),
                                              signature_size(rhs.signature_size),
                                              group_rpc_manager(rhs.group_rpc_manager),
                                              wrapped_this(std::move(rhs.wrapped_this)),
                                              group(rhs.group) {
    persistent_registry->updateTemporalFrontierProvider(this);
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
auto Replicated<T>::p2p_send(node_id_t dest_node, Args&&... args) const {
    if(is_valid()) {
        if(group_rpc_manager.view_manager.get_current_view().get().rank_of(dest_node) == -1) {
            throw invalid_node_exception("Cannot send a p2p request to node "
                                         + std::to_string(dest_node) + ": it is not a member of the Group.");
        }
        //Convert the user's desired tag into an "internal" function tag for a P2P function
        auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
                //Invoke the sending function with a buffer-allocator that uses the P2P request buffers
                [this, &dest_node](size_t size) -> char* {
                    const std::size_t max_p2p_request_payload_size = getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
                    if(size <= max_p2p_request_payload_size) {
                        return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                           sst::REQUEST_TYPE::P2P_REQUEST);
                    } else {
                        throw buffer_overflow_exception("The size of a P2P message exceeds the maximum P2P message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.finish_p2p_send(dest_node, subgroup_id, return_pair.pending);
        return std::move(*return_pair.results);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto Replicated<T>::ordered_send(Args&&... args) {
    if(is_valid()) {
        size_t payload_size_for_multicast_send = wrapped_this->template get_size_for_ordered_send<rpc::to_internal_tag<false>(tag)>(std::forward<Args>(args)...);

        using Ret = typename std::remove_pointer<decltype(wrapped_this->template getReturnType<rpc::to_internal_tag<false>(tag)>(
                std::forward<Args>(args)...))>::type;
        //These pointers help "return" the PendingResults/QueryResults out of the lambda
        std::unique_ptr<rpc::QueryResults<Ret>> results_ptr;
        std::weak_ptr<rpc::PendingResults<Ret>> pending_ptr;
        auto serializer = [&](char* buffer) {
            //By the time this lambda runs, the current thread will be holding a read lock on view_mutex
            const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
            auto send_return_struct = wrapped_this->template send<rpc::to_internal_tag<false>(tag)>(
                    //Invoke the sending function with a buffer-allocator that uses the buffer supplied as an argument to the serializer
                    [&buffer, &max_payload_size](size_t size) -> char* {
                        if(size <= max_payload_size) {
                            return buffer;
                        } else {
                            throw buffer_overflow_exception("The size of an ordered_send message exceeds the maximum message size.");
                        }
                    },
                    std::forward<Args>(args)...);
            results_ptr = std::move(send_return_struct.results);
            pending_ptr = send_return_struct.pending;
        };

        std::shared_lock<std::shared_timed_mutex> view_read_lock(group_rpc_manager.view_manager.view_mutex);
        group_rpc_manager.view_manager.view_change_cv.wait(view_read_lock, [&]() {
            return group_rpc_manager.view_manager.curr_view
                    ->multicast_group->send(subgroup_id, payload_size_for_multicast_send, serializer, true);
        });
        group_rpc_manager.finish_rpc_send(subgroup_id, pending_ptr);
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
    dbg_default_debug("send_object sending object size {} to {}", object_size(), receiver_socket.get_remote_ip());
    mutils::post_object(bind_socket_write, object_size());
    dbg_default_debug("send_object starting send to {}", receiver_socket.get_remote_ip());
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
    rdv.insert(rdv.begin(), persistent_registry.get());
    mutils::DeserializationManager dsm{rdv};
    *user_object_ptr = std::move(mutils::from_bytes<T>(&dsm, buffer));
    if constexpr(std::is_base_of_v<GroupReference, T>) {
        (**user_object_ptr).set_group_pointers(group, subgroup_index);
    }
    return mutils::bytes_size(**user_object_ptr);
}

template <typename T>
void Replicated<T>::make_version(persistent::version_t ver, const HLC& hlc) {
    persistent_registry->makeVersion(ver, hlc);
}

template <typename T>
persistent::version_t Replicated<T>::persist(persistent::version_t version, unsigned char* signature) {
    if constexpr(!std::is_base_of_v<PersistsFields, T>) {
        // for replicated<T> without Persistent fields,
        // tell the persistent thread that we are done.
        return version;
    }
    persistent::version_t next_persisted_ver;
    // Ask PersistentRegistry to persist all the Persistent fields
    do {
        next_persisted_ver = persistent_registry->getMinimumLatestVersion();
        if constexpr(std::is_base_of_v<SignedPersistentFields, T>) {
            persistent_registry->sign(next_persisted_ver, *signer, signature);
        }
        persistent_registry->persist(next_persisted_ver);
    } while(next_persisted_ver < version);
    return next_persisted_ver;
};

template <typename T>
bool Replicated<T>::verify_log(persistent::version_t version, openssl::Verifier& verifier,
                               const unsigned char* other_signature) {
    //If there are no signatures in this object's log, it can't be verified
    if constexpr(std::is_base_of_v<SignedPersistentFields, T>) {
        return persistent_registry->verify(version, verifier, other_signature);
    } else {
        return false;
    }
}

template <typename T>
std::vector<unsigned char> Replicated<T>::get_signature(persistent::version_t version) {
    std::vector<unsigned char> signature(signature_size);
    if(persistent_registry->getSignature(version, signature.data())) {
        return signature;
    } else {
        return {};
    }
}

template <typename T>
void Replicated<T>::trim(persistent::version_t earliest_version) {
    persistent_registry->trim(earliest_version);
}

template <typename T>
void Replicated<T>::truncate(persistent::version_t latest_version) {
    persistent_registry->truncate(latest_version);
}

template <typename T>
persistent::version_t Replicated<T>::get_minimum_latest_persisted_version() {
    return persistent_registry->getMinimumLatestPersistedVersion();
}

template <typename T>
void Replicated<T>::post_next_version(persistent::version_t version, uint64_t ts_us) {
    current_version = version;
    current_timestamp_us = ts_us;
}

template <typename T>
std::tuple<persistent::version_t, uint64_t> Replicated<T>::get_current_version() {
    return std::tie(current_version, current_timestamp_us);
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
        auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
                [this, &dest_node](size_t size) -> char* {
                    const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
                    if(size <= max_payload_size) {
                        return (char*)group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                           sst::REQUEST_TYPE::P2P_REQUEST);
                    } else {
                        throw buffer_overflow_exception("The size of a P2P message exceeds the maximum P2P message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.finish_p2p_send(dest_node, subgroup_id, return_pair.pending);
        return std::move(*return_pair.results);
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
