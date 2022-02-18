#pragma once

#include <functional>

#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace test {

/**
 * A very thin wrapper around a byte array that implements the ByteRepresentable
 * interface for serialization.
 */
class Bytes : public mutils::ByteRepresentable {
    uint8_t* bytes;
    std::size_t size;
    //Indicates that this instance was created inside from_bytes_noalloc and doesn't own the bytes
    bool is_temporary;
    //Private constructor only used by from_bytes_noalloc.
    //The third parameter is just to make it distinct from the public one; it always sets is_temporary to true
    Bytes(uint8_t* buffer, std::size_t size, bool is_temporary);

public:
    uint8_t* get() const;
    /**
     * Constructs a Bytes object by copying the contents of a byte array into the
     * internal buffer.
     * @param buffer A pointer to the byte array
     * @param size The size of the byte array
     */
    Bytes(const uint8_t* buffer, std::size_t size);
    /**
     * Constructs an empty byte array
     */
    Bytes();
    /**
     * Copy constructor. Copies the other Bytes object's internal buffer to
     * initialize this one.
     */
    Bytes(const Bytes& other);
    /**
     * Non-empty destructor: frees the internal byte buffer
     */
    virtual ~Bytes();

    Bytes& operator=(Bytes&& other);

    Bytes& operator=(const Bytes& other);

    /* ---- ByteRepresentable serialization interface ---- */

    std::size_t to_bytes(uint8_t* buffer) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& post_func) const;

    void ensure_registered(mutils::DeserializationManager&);

    static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager* m, const uint8_t* const buffer);

    static mutils::context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager* m, const uint8_t* const buffer);

    static mutils::context_ptr<const Bytes> from_bytes_noalloc_const(mutils::DeserializationManager* m, const uint8_t* const buffer);
};

}  // namespace test
