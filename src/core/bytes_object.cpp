#include "derecho/core/bytes_object.hpp"

#include <cstring>

namespace derecho {

Bytes::Bytes(const uint8_t* buffer, std::size_t size)
        : length(size), is_temporary(false) {
    bytes = nullptr;
    if(size > 0) {
        bytes = new uint8_t[length];
        memcpy(bytes, buffer, length);
    }
}

Bytes::Bytes(std::size_t size)
        : length(size), is_temporary(false) {
    bytes = nullptr;
    if(length > 0) {
        bytes = new uint8_t[length];
    }
}

//from_bytes_noalloc constructor: wraps a byte array without copying it
Bytes::Bytes(uint8_t* buffer, std::size_t size, bool is_temporary)
        : bytes(buffer),
          length(size),
          is_temporary(true) {}

Bytes::Bytes()
        : bytes(nullptr),
          length(0),
          is_temporary(false) {
}

Bytes::Bytes(const Bytes& other)
        : length(other.length), is_temporary(false) {
    if(length > 0) {
        bytes = new uint8_t[length];
        memcpy(bytes, other.bytes, length);
    } else {
        bytes = nullptr;
    }
}

Bytes::~Bytes() {
    if(bytes != nullptr && !is_temporary) {
        delete[] bytes;
    }
}

Bytes& Bytes::operator=(Bytes&& other) {
    uint8_t* swp_bytes = other.bytes;
    std::size_t swp_size = other.length;
    other.bytes = bytes;
    other.length = length;
    bytes = swp_bytes;
    length = swp_size;
    return *this;
}

Bytes& Bytes::operator=(const Bytes& other) {
    if(bytes != nullptr && !is_temporary) {
        delete[] bytes;
    }
    length = other.length;
    if(length > 0) {
        bytes = new uint8_t[length];
        memcpy(bytes, other.bytes, length);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Bytes::size() const {
    return length;
}

std::size_t Bytes::to_bytes(uint8_t* buffer) const {
    ((std::size_t*)(buffer))[0] = length;
    if(length > 0) {
        memcpy(buffer + sizeof(length), bytes, length);
    }
    return length + sizeof(length);
}

std::size_t Bytes::bytes_size() const {
    return length + sizeof(length);
}

void Bytes::post_object(const std::function<void(uint8_t const* const, std::size_t)>& post_func) const {
    post_func((uint8_t*)&length, sizeof(length));
    post_func(bytes, length);
}

void Bytes::ensure_registered(mutils::DeserializationManager&) {}

std::unique_ptr<Bytes> Bytes::from_bytes(mutils::DeserializationManager*, const uint8_t* const buffer) {
    return std::make_unique<Bytes>(buffer + sizeof(std::size_t),
                                   ((std::size_t*)(buffer))[0]);
}

mutils::context_ptr<Bytes> Bytes::from_bytes_noalloc(mutils::DeserializationManager*, const uint8_t* const buffer) {
    //This is dangerous, but from_bytes_noalloc *should* only be used to make a read-only temporary
    return mutils::context_ptr<Bytes>{new Bytes(const_cast<uint8_t*>(buffer + sizeof(std::size_t)),
                                                ((std::size_t*)(buffer))[0],
                                                true)};
}

mutils::context_ptr<const Bytes> Bytes::from_bytes_noalloc_const(mutils::DeserializationManager*, const uint8_t* const buffer) {
    //We shouldn't need to const_cast the byte buffer because we're constructing a const Bytes, but we do.
    return mutils::context_ptr<const Bytes>{new Bytes(const_cast<uint8_t*>(buffer + sizeof(std::size_t)),
                                                      ((std::size_t*)(buffer))[0],
                                                      true)};
}

uint8_t* Bytes::get() const {
    return bytes;
}
}  // namespace derecho
