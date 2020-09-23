#pragma once

#include <functional>

#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace test {

/**
 * A very thin wrapper around a byte array that implements the ByteRepresentable
 * interface for serialization. Although a true byte array should use the type
 * unsigned char, not signed char, this class uses a signed char array to match
 * the type signature of the "byte buffer" provided by the mutils serialization
 * methods (which all used signed char*, not unsigned char*).
 */
class Bytes : public mutils::ByteRepresentable {
    char* bytes;
    std::size_t size;

public:
    Bytes(const char* buffer, std::size_t size);
    Bytes();
    virtual ~Bytes();

    Bytes& operator=(Bytes&& other);

    Bytes& operator=(const Bytes& other);

    std::size_t to_bytes(char* buffer) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(char const* const, std::size_t)>& post_func) const;

    void ensure_registered(mutils::DeserializationManager&);

    static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager* m, const char* const buffer);

    static mutils::context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager* m, const char* const buffer);

    static mutils::context_ptr<const Bytes> from_bytes_noalloc_const(mutils::DeserializationManager* m, const char* const buffer);
};

}  // namespace test
