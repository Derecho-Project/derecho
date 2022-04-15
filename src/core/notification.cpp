#include "derecho/core/notification.hpp"

#include <cstring>
#include <memory>

namespace derecho {

NotificationMessage::NotificationMessage(
        uint64_t type, uint8_t* const buffer, std::size_t size, bool owns_buffer)
        : message_type(type), size(size), body(buffer), owns_body(owns_buffer) {
    if(size > 0 && owns_buffer) {
        body = new uint8_t[size];
        if(buffer != nullptr) {
            memcpy(body, buffer, size);
        } else {
            memset(body, 0, size);
        }
    }
    // In case of illegal argument combination: non-null buffer, size = 0, owns = false
    if(size == 0) {
        body = nullptr;
    }
}

NotificationMessage::NotificationMessage(
        uint64_t type, const uint8_t* const buffer, std::size_t size)
        : message_type(type), size(size), body(new uint8_t[size]), owns_body(true) {
    if(size > 0) {
        if(buffer != nullptr) {
            memcpy(body, buffer, size);
        } else {
            memset(body, 0, size);
        }
    }
}

NotificationMessage::NotificationMessage(uint64_t type, std::size_t size)
        : NotificationMessage(type, nullptr, size) {}

NotificationMessage::NotificationMessage(const NotificationMessage& other)
        : message_type(other.message_type), size(other.size), body(new uint8_t[size]), owns_body(true) {
    if(size > 0) {
        if(other.body != nullptr) {
            memcpy(body, other.body, size);
        } else {
            memset(body, 0, size);
        }
    }
}

NotificationMessage::NotificationMessage(NotificationMessage&& other)
        : message_type(other.message_type),
          size(other.size),
          body(other.body),
          owns_body(other.owns_body) {
    //This ensures other won't delete the body buffer on destruction
    other.body = nullptr;
    other.size = 0;
}

NotificationMessage::~NotificationMessage() {
    if(body != nullptr && owns_body) {
        delete[] body;
    }
}

NotificationMessage& NotificationMessage::operator=(const NotificationMessage& other) {
    if(body != nullptr && owns_body) {
        delete[] body;
    }
    message_type = other.message_type;
    size = other.size;
    owns_body = true;
    if(size > 0) {
        body = new uint8_t[size];
        memcpy(body, other.body, size);
    } else {
        body = nullptr;
    }
    return *this;
}
NotificationMessage& NotificationMessage::operator=(NotificationMessage&& other) {
    if(body != nullptr && owns_body) {
        delete[] body;
    }
    message_type = other.message_type;
    size = other.size;
    owns_body = other.owns_body;
    body = other.body;
    other.body = nullptr;
    other.size = 0;
    return *this;
}

std::size_t NotificationMessage::bytes_size() const {
    return sizeof(message_type) + sizeof(size) + size;
}

std::size_t NotificationMessage::to_bytes(uint8_t* buffer) const {
    std::size_t offset = 0;
    memcpy(buffer + offset, &message_type, sizeof(message_type));
    offset += sizeof(message_type);
    memcpy(buffer + offset, &size, sizeof(size));
    offset += sizeof(size);
    if(size > 0) {
        memcpy(buffer + offset, body, size);
    }
    return offset + size;
}

void NotificationMessage::post_object(const std::function<void(uint8_t const* const, std::size_t)>& post_func) const {
    post_func(reinterpret_cast<const uint8_t*>(&message_type), sizeof(message_type));
    post_func(reinterpret_cast<const uint8_t*>(&size), sizeof(size));
    post_func(body, size);
}

std::unique_ptr<NotificationMessage> NotificationMessage::from_bytes(
        mutils::DeserializationManager*, const uint8_t* const buffer) {
    std::size_t offset = 0;
    uint64_t message_type;
    memcpy(&message_type, buffer + offset, sizeof(message_type));
    offset += sizeof(message_type);
    std::size_t size;
    memcpy(&size, buffer + offset, sizeof(size));
    offset += sizeof(size);
    return std::make_unique<NotificationMessage>(message_type, buffer + offset, size);
}

mutils::context_ptr<NotificationMessage> NotificationMessage::from_bytes_noalloc(
        mutils::DeserializationManager*, const uint8_t* const buffer) {
    std::size_t offset = 0;
    uint64_t message_type;
    memcpy(&message_type, buffer + offset, sizeof(message_type));
    offset += sizeof(message_type);
    std::size_t size;
    memcpy(&size, buffer + offset, sizeof(size));
    offset += sizeof(size);
    //This is dangerous, because we store the const buffer pointer in the non-const body pointer,
    //but from_bytes_noalloc *should* only be used to make a read-only temporary
    return mutils::context_ptr<NotificationMessage>{
            new NotificationMessage(message_type,
                                    const_cast<uint8_t*>(buffer + offset),
                                    size,
                                    false)};
}

mutils::context_ptr<const NotificationMessage> NotificationMessage::from_bytes_noalloc_const(
        mutils::DeserializationManager*, const uint8_t* const buffer) {
    std::size_t offset = 0;
    uint64_t message_type;
    memcpy(&message_type, buffer + offset, sizeof(message_type));
    offset += sizeof(message_type);
    std::size_t size;
    memcpy(&size, buffer + offset, sizeof(size));
    offset += sizeof(size);
    //We shouldn't need const_cast to create a const NotificationMessage, but we do
    return mutils::context_ptr<const NotificationMessage>{
            new NotificationMessage(message_type,
                                    const_cast<uint8_t*>(buffer + offset),
                                    size,
                                    false)};
}

}  // namespace derecho
