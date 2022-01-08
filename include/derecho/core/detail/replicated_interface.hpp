#pragma once

#include "derecho/openssl/signature.hpp"
#include "derecho/tcp/tcp.hpp"
#include "derecho_internal.hpp"

namespace derecho {

/**
 * Common interface for all types of Replicated<T>, specifying some methods for
 * state transfer and persistence. This allows non-templated Derecho components
 * like ViewManager to take these actions without knowing the full type of a subgroup.
 */
class ReplicatedObject {
public:
    virtual ~ReplicatedObject() = default;
    virtual bool is_valid() const = 0;
    virtual std::size_t object_size() const = 0;
    virtual void send_object(tcp::socket& receiver_socket) const = 0;
    virtual void send_object_raw(tcp::socket& receiver_socket) const = 0;
    virtual std::size_t receive_object(char* buffer) = 0;
    virtual bool is_persistent() const = 0;
    virtual bool is_signed() const = 0;
    virtual void make_version(persistent::version_t ver, const HLC& hlc) = 0;
    virtual persistent::version_t get_minimum_latest_persisted_version() = 0;
    virtual persistent::version_t persist(persistent::version_t version, unsigned char* signature) = 0;
    virtual std::vector<unsigned char> get_signature(persistent::version_t version) = 0;
    virtual bool verify_log(persistent::version_t version, openssl::Verifier& verifier,
                            const unsigned char* signature) = 0;
    virtual void truncate(persistent::version_t latest_version) = 0;
    virtual void post_next_version(persistent::version_t version, uint64_t msg_ts) = 0;
};

}  // namespace derecho
