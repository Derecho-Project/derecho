#pragma once

#include "derecho/mutils-serialization/SerializationMacros.hpp"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "register_rpc_functions.hpp"

#include <functional>
#include <memory>
#include <optional>

namespace derecho {

struct NotificationMessage : mutils::ByteRepresentable {
    /**
     * A number identifying the type of notification message, which can be
     * defined and interpreted by the notification-supporting class in any
     * way it wants.
     */
    uint64_t message_type;
    /** The size of the message in bytes. */
    std::size_t size;
    /**
     * A pointer to the byte buffer containing the body of the message.
     * The NotificationMessage object may or may not own this buffer, which
     * will be indicated by owns_bytes. Specifically, if this object was
     * constructed as wrapper around a temporary buffer in a receive handler,
     * it probably doesn't own the buffer.
    */
    uint8_t* body;
    /**
     * True if this object owns the buffer pointed to by body, false if it
     * does not own the buffer and should not delete it.
     */
    bool owns_body;

    /**
     * Creates a new NotificationMessage. If the provided buffer is nullptr, but
     * size is > 0, allocates a new buffer of that size to be the body.
     */
    NotificationMessage(uint64_t type, uint8_t* const buffer, std::size_t size, bool owns_buffer);

    /**
     * Creates a new NotificationMessage by copying the body from a const buffer.
     * If the provided buffer is nullptr, but size is > 0, allocates a new buffer
     * of that size to be the body.
     */
    NotificationMessage(uint64_t type, const uint8_t* const buffer, std::size_t size);

    /**
     * Creates a new, empty NotificationMessage and allocates a buffer of the
     * requested size for the body.
     */
    NotificationMessage(uint64_t type, std::size_t size);

    /** Copy constructor; copies the message body from other */
    NotificationMessage(const NotificationMessage& other);

    /**
     * Move constructor; takes ownership of the message body from other.
     * Note that if other.owns_body is false, the newly constructed object
     * will not own the body either.
     */
    NotificationMessage(NotificationMessage&& other);

    /** Copy assignment operator; copies the message body from other */
    NotificationMessage& operator=(const NotificationMessage& other);
    /** Move assignment operator; takes ownership of the message body from other */
    NotificationMessage& operator=(NotificationMessage&& other);

    virtual ~NotificationMessage();

    // serialization/deserialization support
    std::size_t to_bytes(uint8_t* buffer) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& allocator) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<NotificationMessage> from_bytes(mutils::DeserializationManager*, const uint8_t* const buffer);

    static mutils::context_ptr<NotificationMessage> from_bytes_noalloc(
            mutils::DeserializationManager* ctx,
            const uint8_t* const buffer);

    static mutils::context_ptr<const NotificationMessage> from_bytes_noalloc_const(
            mutils::DeserializationManager* ctx,
            const uint8_t* const buffer);
};

using notification_handler_t = std::function<void(const NotificationMessage&)>;

struct NotificationSupport {
public:
    std::optional<notification_handler_t> handler;

    virtual void notify(const NotificationMessage& msg) const {
        dbg_default_trace("notification message of type {} received.", msg.message_type);
        if (handler) {
            (*handler)(msg);
        }
    }

    void set_notification_handler(const notification_handler_t& func) {
        handler = func;
    }

    void remove_notification_handler() {
        handler.reset();
    }
};

}  // namespace derecho
