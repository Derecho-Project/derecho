#include <cstring>

#include "bytes_object.hpp"

namespace test {

Bytes::Bytes(const char* buffer, std::size_t size)
        : size(size) {
    bytes = nullptr;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, buffer, size);
    }
}

Bytes::Bytes() {
    bytes = nullptr;
    size = 0;
}

Bytes::~Bytes() {
    if(bytes != nullptr) {
        delete bytes;
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
    if(bytes != nullptr) {
        delete bytes;
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
    return std::make_unique<Bytes>(buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0]);
}

mutils::context_ptr<Bytes> Bytes::from_bytes_noalloc(mutils::DeserializationManager*, const char* const buffer) {
    return mutils::context_ptr<Bytes>{new Bytes(buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0])};
}

mutils::context_ptr<const Bytes> Bytes::from_bytes_noalloc_const(mutils::DeserializationManager*, const char* const buffer) {
    return mutils::context_ptr<const Bytes>{new Bytes(buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0])};
}
}  // namespace test