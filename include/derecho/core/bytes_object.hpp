#pragma once

#include "derecho/mutils-serialization/SerializationSupport.hpp"

#include <cstring>
#include <memory>

namespace derecho {

/**
 * A very thin wrapper around a byte array that implements the ByteRepresentable
 * interface for serialization.
 */
class Bytes : public mutils::ByteRepresentable {
    //A pointer to a byte array
    uint8_t* bytes;
    //The length of the array. Can't be named size because that would conflict with the method.
    std::size_t length;
    //Indicates that this instance was created inside from_bytes_noalloc and doesn't own the bytes
    bool is_temporary;
    //Private constructor only used by from_bytes_noalloc.
    //The third parameter is just to make it distinct from the public one; it always sets is_temporary to true
    Bytes(uint8_t* buffer, std::size_t size, bool is_temporary);

public:
    /**
     * Provides access to the wrapped byte array, without allowing
     * the array pointer itself to be reassigned.
     * @return a pointer to the beginning of this object's byte array.
     */
    uint8_t* get() const;
    /**
     * @returns the size of the wrapped byte array
     */
    std::size_t size() const;
    /**
     * Constructs a byte array by copying the contents of an existing array into the
     * internal buffer.
     * @param buffer A pointer to the byte array
     * @param size The size of the byte array
     */
    Bytes(const uint8_t* buffer, std::size_t size);
    /**
     * Constructs an empty byte array of the specified size. This will create a
     * new array for the internal buffer, but not copy anything into it.
     * @param size The size of the array
     */
    explicit Bytes(std::size_t size);
    /**
     * Constructs an empty byte array of size 0.
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

}
