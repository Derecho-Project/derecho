#pragma once

#include "derecho/mutils-serialization/SerializationSupport.hpp"

#include <cstring>
#include <memory>

namespace derecho {

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
    //Indicates that this instance was created inside from_bytes_noalloc and doesn't own the bytes
    bool is_temporary;
    //Private constructor only used by from_bytes_noalloc.
    //The third parameter is just to make it distinct from the public one; it always sets is_temporary to true
    Bytes(char* buffer, std::size_t size, bool is_temporary);

public:
    /**
     * Provides access to the wrapped byte array, without allowing
     * the array pointer itself to be reassigned.
     * @return a pointer to the beginning of this object's byte array.
     */
    char* get() const;
    /**
     * Constructs a byte array by copying the contents of a char array into the
     * internal buffer.
     * @param buffer A pointer to the char array
     * @param size The size of the char array
     */
    Bytes(const char* buffer, std::size_t size);
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

    std::size_t to_bytes(char* buffer) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(char const* const, std::size_t)>& post_func) const;

    void ensure_registered(mutils::DeserializationManager&);

    static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager* m, const char* const buffer);

    static mutils::context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager* m, const char* const buffer);

    static mutils::context_ptr<const Bytes> from_bytes_noalloc_const(mutils::DeserializationManager* m, const char* const buffer);
};

}
