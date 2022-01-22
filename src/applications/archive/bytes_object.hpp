#pragma once

#include <cstring>
#include <derecho/core/replicated.hpp>

namespace derecho {
//A ByteRepresentable object representing a byte array that is used in several experiments.

//This class is modified from Matt's implementation
struct Bytes : public mutils::ByteRepresentable, public derecho::PersistsFields {
    uint8_t* bytes;
    std::size_t size;

    Bytes(const uint8_t* b, decltype(size) s)
            : size(s) {
        bytes = nullptr;
        if(s > 0) {
            bytes = new uint8_t[s];
            memcpy(bytes, b, s);
        }
    }
    Bytes() {
        bytes = nullptr;
        size = 0;
    }
    virtual ~Bytes() {
        if(bytes != nullptr) {
            delete bytes;
        }
    }

    Bytes& operator=(Bytes&& other) {
        uint8_t* swp_bytes = other.bytes;
        std::size_t swp_size = other.size;
        other.bytes = bytes;
        other.size = size;
        bytes = swp_bytes;
        size = swp_size;
        return *this;
    }

    Bytes& operator=(const Bytes& other) {
        if(bytes != nullptr) {
            delete bytes;
        }
        size = other.size;
        if(size > 0) {
            bytes = new uint8_t[size];
            memcpy(bytes, other.bytes, size);
        } else {
            bytes = nullptr;
        }
        return *this;
    }

    std::size_t to_bytes(uint8_t* v) const {
        ((std::size_t*)(v))[0] = size;
        if(size > 0) {
            memcpy(v + sizeof(size), bytes, size);
        }
        return size + sizeof(size);
    }

    std::size_t bytes_size() const {
        return size + sizeof(size);
    }

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
        f((uint8_t*)&size, sizeof(size));
        f(bytes, size);
    }

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager*, const uint8_t* const v) {
        return std::make_unique<Bytes>(v + sizeof(std::size_t), ((std::size_t*)(v))[0]);
    }

    static mutils::context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager*, const uint8_t* const v) {
        return mutils::context_ptr<Bytes>{new Bytes(v + sizeof(std::size_t), ((std::size_t*)(v))[0])};
    }

    static mutils::context_ptr<const Bytes> from_bytes_noalloc_const(mutils::DeserializationManager*, const uint8_t* const v) {
        return mutils::context_ptr<const Bytes>{new Bytes(v + sizeof(std::size_t), ((std::size_t*)(v))[0])};
    }
};

}  // namespace derecho
