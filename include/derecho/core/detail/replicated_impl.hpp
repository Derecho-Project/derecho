#pragma once

#include "../replicated.hpp"
#include "view_manager.hpp"

#include "derecho/mutils-serialization/SerializationSupport.hpp"

#include <functional>
#include <mutex>
#include <utility>

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
          group(group),
          current_version(persistent::INVALID_VERSION),
          current_hlc(0, 0) {
    if constexpr(std::is_base_of_v<GroupReference, T>) {
        (**user_object_ptr).set_group_pointers(group, subgroup_index);
    }
    if constexpr(has_signed_fields_v<T>) {
        // Attempt to load the private key and create a Signer
        // This will crash with a file_error if the private key doesn't actually exist
        signer = std::make_unique<openssl::Signer>(openssl::EnvelopeKey::from_pem_private(getConfString(Conf::PERS_PRIVATE_KEY_FILE)),
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
    if constexpr(has_signed_fields_v<T>) {
        // Attempt to load the private key and create a Signer
        // This will crash with a file_error if the private key doesn't actually exist
        signer = std::make_unique<openssl::Signer>(openssl::EnvelopeKey::from_pem_private(getConfString(Conf::PERS_PRIVATE_KEY_FILE)),
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
        uint64_t message_seq_num;
        // Convert the user's desired tag into an "internal" function tag for a P2P function
        auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
                // Invoke the sending function with a buffer-allocator that uses the P2P request buffers
                [this, &dest_node, &message_seq_num](std::size_t size) -> uint8_t* {
                    const std::size_t max_p2p_request_payload_size = getConfUInt64(Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
                    if(size <= max_p2p_request_payload_size) {
                        auto buffer_handle = group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                                  sst::MESSAGE_TYPE::P2P_REQUEST);
                        // Record the sequence number for this message buffer
                        message_seq_num = buffer_handle.seq_num;
                        return buffer_handle.buf_ptr;
                    } else {
                        throw buffer_overflow_exception("The size of a P2P message exceeds the maximum P2P message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.send_p2p_message(dest_node, subgroup_id, message_seq_num, return_pair.pending);
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
        // These pointers help "return" the PendingResults/QueryResults out of the lambda
        std::unique_ptr<rpc::QueryResults<Ret>> results_ptr;
        std::weak_ptr<rpc::PendingResults<Ret>> pending_ptr;
        auto serializer = [&](uint8_t* buffer) {
            // By the time this lambda runs, the current thread will be holding a read lock on view_mutex
            const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
            auto send_return_struct = wrapped_this->template send<rpc::to_internal_tag<false>(tag)>(
                    // Invoke the sending function with a buffer-allocator that uses the buffer supplied as an argument to the serializer
                    [&buffer, &max_payload_size](size_t size) -> uint8_t* {
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
        group_rpc_manager.register_rpc_results(subgroup_id, pending_ptr);
        return std::move(*results_ptr);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
void Replicated<T>::send(unsigned long long int payload_size,
                         const std::function<void(uint8_t* buf)>& msg_generator) {
    group_rpc_manager.view_manager.send(subgroup_id, payload_size, msg_generator);
}

template <typename T>
std::size_t Replicated<T>::object_size() const {
    return mutils::bytes_size(**user_object_ptr);
}

template <typename T>
void Replicated<T>::send_object(tcp::socket& receiver_socket) const {
    auto bind_socket_write = [&receiver_socket](const uint8_t* bytes, std::size_t size) {
        receiver_socket.write(bytes, size);
    };
    dbg_default_trace("send_object sending object size {} to {}", object_size(), receiver_socket.get_remote_ip());
    mutils::post_object(bind_socket_write, object_size());
    dbg_default_trace("send_object starting send to {}", receiver_socket.get_remote_ip());
    send_object_raw(receiver_socket);
}

template <typename T>
void Replicated<T>::send_object_raw(tcp::socket& receiver_socket) const {
    auto bind_socket_write = [&receiver_socket](const uint8_t* bytes, std::size_t size) {
        receiver_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write, **user_object_ptr);
}

template <typename T>
std::size_t Replicated<T>::receive_object(uint8_t* buffer) {
    // Add this object's persistent registry to the list of deserialization contexts
    mutils::RemoteDeserialization_v rdv{group_rpc_manager.deserialization_contexts};
    rdv.insert(rdv.begin(), persistent_registry.get());
    mutils::DeserializationManager dsm{rdv};
    *user_object_ptr = std::move(mutils::from_bytes<T>(&dsm, buffer));
    if constexpr(std::is_base_of_v<GroupReference, T>) {
        (**user_object_ptr).set_group_pointers(group, subgroup_index);
    }
    return mutils::bytes_size(**user_object_ptr);
}

template <typename T>
void Replicated<T>::new_view_callback(const View& new_view) {
    if constexpr(view_callback_enabled_v<T>) {
        (**user_object_ptr).new_view_callback(new_view);
    }
}

template <typename T>
void Replicated<T>::make_version(persistent::version_t ver, const HLC& hlc) {
    persistent_registry->makeVersion(ver, hlc);
}

template <typename T>
persistent::version_t Replicated<T>::sign(uint8_t* signature_buffer) {
    if constexpr(!has_signed_fields_v<T>) {
        return persistent::INVALID_VERSION;
    }
    // Get the current version according to PersistentRegistry, and try to sign up through that version
    persistent::version_t version_to_sign = persistent_registry->getCurrentVersion();
    // Return the version actually signed, which might be earlier or later than the argument
    return persistent_registry->sign(version_to_sign, *signer, signature_buffer);
}

template <typename T>
persistent::version_t Replicated<T>::persist(persistent::version_t version) {
    if constexpr(!has_persistent_fields_v<T>) {
        // for replicated<T> without Persistent fields,
        // tell the persistent thread that we are done.
        return version;
    }

    return persistent_registry->persist(version);
};

template <typename T>
bool Replicated<T>::verify_log(persistent::version_t version, openssl::Verifier& verifier,
                               const uint8_t* other_signature) {
    // If there are no signatures in this object's log, it can't be verified
    if constexpr(has_signed_fields_v<T>) {
        return persistent_registry->verify(version, verifier, other_signature);
    } else {
        return false;
    }
}

template <typename T>
std::vector<uint8_t> Replicated<T>::get_signature(persistent::version_t version) {
    std::vector<uint8_t> signature(signature_size);
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
    if(current_hlc > HLC{ts_us, 0}) {
        current_hlc.m_logic++;
    } else {
        current_hlc = {ts_us, 0};
    }
}

template <typename T>
std::tuple<persistent::version_t, HLC> Replicated<T>::get_current_version() {
    return std::tie(current_version, current_hlc);
}

template <typename T>
const T& Replicated<T>::get_ref() const {
    return **user_object_ptr;
}

template <typename T>
void Replicated<T>::oob_remote_write(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_dest_addr, uint64_t rkey, size_t size) {
    group_rpc_manager.oob_remote_write(remote_node, iov, iovcnt, remote_dest_addr, rkey, size);
}

template <typename T>
void Replicated<T>::oob_remote_read(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_src_addr, uint64_t rkey, size_t size) {
    group_rpc_manager.oob_remote_read(remote_node, iov, iovcnt, remote_src_addr, rkey, size);
}

template <typename T>
void Replicated<T>::oob_send(const node_id_t& remote_node, const struct iovec* iov, int iovcnt) {
    group_rpc_manager.oob_send(remote_node, iov, iovcnt);
}

template <typename T>
void Replicated<T>::oob_recv(const node_id_t& remote_node, const struct iovec* iov, int iovcnt) {
    group_rpc_manager.oob_recv(remote_node, iov, iovcnt);
}

template <typename T>
void Replicated<T>::wait_for_oob_op(const node_id_t& remote_node, uint32_t op, uint64_t timeout_us) {
    group_rpc_manager.wait_for_oob_op(remote_node, op, timeout_us);
}

template <typename T>
const uint64_t Replicated<T>::compute_global_stability_frontier() {
    return group_rpc_manager.view_manager.compute_global_stability_frontier(subgroup_id);
}

template <typename T>
persistent::version_t Replicated<T>::get_global_persistence_frontier() {
    return group_rpc_manager.view_manager.get_global_persistence_frontier(subgroup_id);
}

template <typename T>
bool Replicated<T>::wait_for_global_persistence_frontier(persistent::version_t version) {
    return group_rpc_manager.view_manager.wait_for_global_persistence_frontier(subgroup_id, version);
}

template <typename T>
persistent::version_t Replicated<T>::get_global_verified_frontier() {
    return group_rpc_manager.view_manager.get_global_verified_frontier(subgroup_id);
}

template <typename T>
PeerCaller<T>::PeerCaller(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                          rpc::RPCManager& group_rpc_manager)
        : node_id(nid),
          subgroup_id(subgroup_id),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(rpc::make_remote_invoker<T>(nid, type_id, subgroup_id,
                                                   T::register_functions(), *group_rpc_manager.receivers)) {}

// This is literally copied and pasted from Replicated<T>. I wish I could let them share code with inheritance,
// but I'm afraid that will introduce unnecessary overheads.
template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto PeerCaller<T>::p2p_send(node_id_t dest_node, Args&&... args) {
    if(is_valid()) {
        assert(dest_node != node_id);
        if(group_rpc_manager.view_manager.get_current_view().get().rank_of(dest_node) == -1) {
            throw invalid_node_exception("Cannot send a p2p request to node "
                                         + std::to_string(dest_node) + ": it is not a member of the Group.");
        }
        uint64_t message_seq_num;
        auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
                [this, &dest_node, &message_seq_num](size_t size) -> uint8_t* {
                    const std::size_t max_payload_size = group_rpc_manager.view_manager.get_max_payload_sizes().at(subgroup_id);
                    if(size <= max_payload_size) {
                        auto buffer_handle = group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                                  sst::MESSAGE_TYPE::P2P_REQUEST);
                        // Record the sequence number for this message buffer
                        message_seq_num = buffer_handle.seq_num;
                        return buffer_handle.buf_ptr;
                    } else {
                        throw buffer_overflow_exception("The size of a P2P message exceeds the maximum P2P message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.send_p2p_message(dest_node, subgroup_id, message_seq_num, return_pair.pending);
        return std::move(*return_pair.results);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
ExternalClientCallback<T>::ExternalClientCallback(uint32_t type_id, node_id_t nid, subgroup_id_t subgroup_id,
                                                  rpc::RPCManager& group_rpc_manager)
        : node_id(nid),
          subgroup_id(subgroup_id),
          group_rpc_manager(group_rpc_manager),
          wrapped_this(rpc::make_remote_invoker<T>(nid, type_id, subgroup_id,
                                                   T::register_functions(), *group_rpc_manager.receivers)) {}

template <typename T>
bool ExternalClientCallback<T>::has_external_client(node_id_t client_id) const {
    return group_rpc_manager.connections->contains_node(client_id);
}

// This is literally copied and pasted from PeerCaller<T>, except that this does not check if the receiver
//  is in the subgroup.

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto ExternalClientCallback<T>::p2p_send(node_id_t dest_node, Args&&... args) {
    if(is_valid()) {
        assert(dest_node != node_id);
        uint64_t message_seq_num;
        auto return_pair = wrapped_this->template send<rpc::to_internal_tag<true>(tag)>(
                [this, &dest_node, &message_seq_num](size_t size) -> uint8_t* {
                    const std::size_t max_payload_size = getConfUInt64(Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
                    if(size <= max_payload_size) {
                        auto buffer_handle = group_rpc_manager.get_sendbuffer_ptr(dest_node,
                                                                                  sst::MESSAGE_TYPE::P2P_REQUEST);
                        // Record the sequence number for this message buffer
                        message_seq_num = buffer_handle.seq_num;
                        return buffer_handle.buf_ptr;
                    } else {
                        throw buffer_overflow_exception("The size of a P2P message exceeds the maximum P2P message size.");
                    }
                },
                std::forward<Args>(args)...);
        group_rpc_manager.send_p2p_message(dest_node, subgroup_id, message_seq_num, return_pair.pending);
        return std::move(*return_pair.results);
    } else {
        throw empty_reference_exception{"Attempted to use an empty Replicated<T>"};
    }
}

template <typename T>
template <rpc::FunctionTag tag, typename... Args>
auto ShardIterator<T>::p2p_send(Args&&... args) {
    // shard_reps should have at least one member
    auto send_result = caller.template p2p_send<tag>(shard_reps.at(0), std::forward<Args>(args)...);
    std::vector<decltype(send_result)> send_result_vec;
    send_result_vec.emplace_back(std::move(send_result));
    for(uint i = 1; i < shard_reps.size(); ++i) {
        send_result_vec.emplace_back(caller.template p2p_send<tag>(shard_reps[i], std::forward<Args>(args)...));
    }
    return send_result_vec;
}

}  // namespace derecho
