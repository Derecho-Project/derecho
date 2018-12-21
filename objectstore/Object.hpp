#ifndef OBJECT_HPP
#define OBJECT_HPP
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string.h>
#include <string>
#include <time.h>
#include <vector>

#include "conf/conf.hpp"
#include "derecho/derecho.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <optional>
#include <persistent/Persistent.hpp>

using std::cout;
using std::endl;
using namespace persistent;
using namespace std::chrono_literals;

namespace objectstore {

class Blob : public mutils::ByteRepresentable {
public:
    char* bytes;
    std::size_t size;

    // constructor - copy to own the data
    Blob(const char* const b, const decltype(size) s) : bytes(nullptr),
                                                        size(0) {
        if(s > 0) {
            bytes = new char[s];
            memcpy(bytes, b, s);
            size = s;
        }
    }

    // copy constructor - copy to own the data
    Blob(const Blob& other) : bytes(nullptr),
                              size(0) {
        if(other.size > 0) {
            bytes = new char[other.size];
            memcpy(bytes, other.bytes, other.size);
            size = other.size;
        }
    }

    // move constructor - accept the memory from another object
    Blob(Blob&& other) : bytes(other.bytes), size(other.size) {
        other.bytes = nullptr;
        other.size = 0;
    }

    // default constructor - no data at all
    Blob() : bytes(nullptr), size(0) {}

    // destructor
    virtual ~Blob() {
        if(bytes) delete bytes;
    }

    // move evaluator:
    Blob& operator=(Blob&& other) {
        char* swp_bytes = other.bytes;
        std::size_t swp_size = other.size;
        other.bytes = bytes;
        other.size = size;
        bytes = swp_bytes;
        size = swp_size;
        return *this;
    }

    // copy evaluator:
    Blob& operator=(const Blob& other) {
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

    std::size_t to_bytes(char* v) const {
        ((std::size_t*)(v))[0] = size;
        if(size > 0) {
            memcpy(v + sizeof(size), bytes, size);
        }
        return size + sizeof(size);
    }

    std::size_t bytes_size() const {
        return size + sizeof(size);
    }

    void post_object(const std::function<void(char const* const, std::size_t)>& f) const {
        f((char*)&size, sizeof(size));
        f(bytes, size);
    }

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const char* const v) {
        return std::make_unique<Blob>(v + sizeof(std::size_t), ((std::size_t*)(v))[0]);
    }

    // from_bytes_noalloc() implementation borrowed from mutils-serialization.
    // TODO: check with Matthew: if this will cause memory leak?
    mutils::context_ptr<Blob> from_bytes_noalloc(mutils::DeserializationManager* ctx, const char* const v, mutils::context_ptr<Blob> = mutils::context_ptr<Blob>{}) {
        return mutils::context_ptr<Blob>{from_bytes(ctx, v).release()};
    }
};

using OID = uint64_t;
#define INV_OID (0xffffffffffffffffLLU)

class Object : public mutils::ByteRepresentable {
public:
    OID oid;    // object_id
    Blob blob;  // the object

    bool operator==(const Object& other) {
        return this->oid == other.oid;
    }

    bool is_valid() const {
        return (oid == INV_OID);
    }

    // constructor 0 : copy constructor
    Object(const OID& _oid, const Blob& _blob) : oid(_oid),
                                                 blob(_blob) {}
    // constructor 1 : copy consotructor
    Object(const uint64_t _oid, const char* const _b, const std::size_t _s) : oid(_oid),
                                                                              blob(_b, _s) {}
    // constructor 2 : move constructor
    Object(Object&& other) : oid(other.oid),
                             blob(std::move(other.blob)) {}
    // constructor 3 : copy constructor
    Object(const Object& other) : oid(other.oid),
                                  blob(other.blob) {}
    // constructor 4 : default invalid constructor
    Object() : oid(INV_OID) {}

    DEFAULT_SERIALIZATION_SUPPORT(Object, oid, blob);
};

std::ostream& operator << (std::ostream &out, const Blob &b) {
    out << "[size:" << b.size << ", data:" << std::hex;
    if (b.size > 0) {
        uint32_t i = 0;
        for (i = 0;i<8 && i<b.size; i++) {
            out << " " << b.bytes[i];
        }
        if (i < b.size) {
            out << "...";
        }
    }
    out << std::dec << "]";
    return out;
}

std::ostream& operator << (std::ostream &out, const Object &o) {
    out << "Object{id:" << o.oid << ", data:" << o.blob << "}";
    return out;
}

}  // namespace objectstore
#endif  //OBJECT_HPP
