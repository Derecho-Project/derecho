#pragma once
#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace sospdemo {
/**
 * A serializable wrapper class for Binary Large OBject (BLOB)
 * It does not take ownership of the data.
 */
class BlobWrapper : public mutils::ByteRepresentable {
public:
     const char* bytes;
     const std::size_t size;

     // constructor
     BlobWrapper(const char* const b, const decltype(size) s);
 
     // default constructor - no data at all
     BlobWrapper();
 
     // serialization/deserialization supports
     std::size_t to_bytes(char* v) const;
 
     std::size_t bytes_size() const;
 
     void post_object(const std::function<void(char const* const, std::size_t)>& f) const;
 
     void ensure_registered(mutils::DeserializationManager&) {}
 
     static std::unique_ptr<BlobWrapper> from_bytes(mutils::DeserializationManager*, const char* const v);
 
     static mutils::context_ptr<BlobWrapper> from_bytes_noalloc(
         mutils::DeserializationManager* ctx,
         const char* const v,
         mutils::context_ptr<BlobWrapper> = mutils::context_ptr<BlobWrapper>{});
 
     static mutils::context_ptr<const BlobWrapper> from_bytes_noalloc_const(
         mutils::DeserializationManager* ctx,
         const char* const v,
         mutils::context_ptr<const BlobWrapper> = mutils::context_ptr<const BlobWrapper>{});
};

/**
 * Serialiazble Binary Large Object (BLOB)
 * It owns the data.
 */
class Blob : public mutils::ByteRepresentable {
public:
    char* bytes;
    std::size_t size;

    // constructors
    Blob(const char* const b, const decltype(size) s); 
    Blob(const Blob& other);
    Blob(Blob&& other);
    Blob();

    // destructor
    virtual ~Blob();

    // move evaluator:
    Blob& operator=(Blob&& other);

    // copy evaluator:
    Blob& operator=(const Blob& other);

    // serialization/deserialization supports
    std::size_t to_bytes(char* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(char const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const char* const v); 

    static mutils::context_ptr<Blob> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const char* const v,  
        mutils::context_ptr<Blob> = mutils::context_ptr<Blob>{});
};

}
