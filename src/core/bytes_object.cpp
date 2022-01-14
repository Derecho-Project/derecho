#include "derecho/core/bytes_object.hpp"

#include <cstring>

namespace derecho {

Bytes::Bytes(const char* buffer, std::size_t size)
        : size(size), is_temporary(false) {
    bytes = nullptr;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, buffer, size);
    }
}

//from_bytes_noalloc constructor: wraps a byte array without copying it
Bytes::Bytes(char* buffer, std::size_t size, bool is_temporary)
        : bytes(buffer),
          size(size),
          is_temporary(true) {}

Bytes::Bytes()
        : bytes(nullptr),
          size(0),
          is_temporary(false) {
}

Bytes::Bytes(const Bytes& other)
    : size(other.size), is_temporary(false) {
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, other.bytes, size);
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
    char* swp_bytes = other.bytes;
    std::size_t swp_size = other.size;
    other.bytes = bytes;
    other.size = size;
    bytes = swp_bytes;
    size = swp_size;
    return *this;
}

Bytes& Bytes::operator=(const Bytes& other) {
    if(bytes != nullptr && !is_temporary) {
        delete[] bytes;
    }
    size = other.size;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, other.bytes, size);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Bytes::to_bytes(char* buffer) const {
    ((std::size_t*)(buffer))[0] = size;
    if(size > 0) {
        memcpy(buffer + sizeof(size), bytes, size);
    }
    return size + sizeof(size);
}

std::size_t Bytes::bytes_size() const {
    return size + sizeof(size);
}

void Bytes::post_object(const std::function<void(char const* const, std::size_t)>& post_func) const {
    post_func((char*)&size, sizeof(size));
    post_func(bytes, size);
}

void Bytes::ensure_registered(mutils::DeserializationManager&) {}

std::unique_ptr<Bytes> Bytes::from_bytes(mutils::DeserializationManager*, const char* const buffer) {
    return std::make_unique<Bytes>(buffer + sizeof(std::size_t),
                                   ((std::size_t*)(buffer))[0]);
}

mutils::context_ptr<Bytes> Bytes::from_bytes_noalloc(mutils::DeserializationManager*, const char* const buffer) {
    //This is dangerous, but from_bytes_noalloc *should* only be used to make a read-only temporary
    return mutils::context_ptr<Bytes>{new Bytes(const_cast<char*>(buffer + sizeof(std::size_t)),
                                                ((std::size_t*)(buffer))[0],
                                                true)};
}

mutils::context_ptr<const Bytes> Bytes::from_bytes_noalloc_const(mutils::DeserializationManager*, const char* const buffer) {
    //We shouldn't need to const_cast the byte buffer because we're constructing a const Bytes, but we do.
    return mutils::context_ptr<const Bytes>{new Bytes(const_cast<char*>(buffer + sizeof(std::size_t)),
                                                      ((std::size_t*)(buffer))[0],
                                                      true)};
}

char* Bytes::get() const {
    return bytes;
}
}  // namespace test
