#pragma once

#include "derecho_internal.hpp"
#include <derecho/tcp/tcp.hpp>

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
    virtual void make_version(const persistent::version_t& ver, const HLC& hlc) noexcept(false) = 0;
    virtual const persistent::version_t get_minimum_latest_persisted_version() noexcept(false) = 0;
    virtual void persist(const persistent::version_t version) noexcept(false) = 0;
    virtual void truncate(const persistent::version_t& latest_version) = 0;
    virtual void post_next_version(const persistent::version_t& version, const uint64_t& msg_ts) = 0;
};

}  // namespace derecho
