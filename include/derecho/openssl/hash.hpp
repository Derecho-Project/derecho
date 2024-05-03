/**
 * @file hash.hpp
 *
 * Classes and functions that wrap the "EVP_Digest" features of OpenSSL
 */
#pragma once

#include "derecho/config.h"
#include "openssl_exception.hpp"
#include "pointers.hpp"

#include <openssl/evp.h>
#include <vector>

namespace openssl {
/**
 * Enumerates the types of digest (hash) algorithms that can be used in OpenSSL
 */
enum class DigestAlgorithm {
    MD5,
    SHA1,
    SHA256,
    SHA384,
    SHA512,
    SHA3_224,
    SHA3_256,
    SHA3_384,
    SHA3_512
};

/**
 * Returns an EVP_MD pointer that indicates a specific digest type to use as a
 * parameter for OpenSSL functions. The pointer is owned by the OpenSSL library
 * and should NOT be deleted.
 */
const EVP_MD* get_digest_type_ptr(DigestAlgorithm digest_type);

/**
 * A class that wraps the EVP_Digest* functions for computing message digests
 * (cryptographic hashes). Each function will throw exceptions if the underlying
 * library calls return errors, rather than returning an error code.
 */
class Hasher {
    const DigestAlgorithm digest_type;
    std::unique_ptr<EVP_MD_CTX, DeleterFor<EVP_MD_CTX>> digest_context;

public:
    /**
     * Constructs a Hasher that will use the specified digest algorithm to hash
     * input bytes of data.
     */
    Hasher(DigestAlgorithm digest_type);
    /**
     * @return the size in bytes of each digest (hash) that this Hasher will
     * compute. This is a constant determined by the algorithm type.
     */
    int get_hash_size();
    /**
     * Initializes the Hasher to start hashing a new sequence of bytes. Must
     * be called before add_bytes or finalize.
     */
    void init();
    /**
     * Hashes a byte buffer into the ongoing digest (hash) being computed by
     * this Hasher.
     * @param buffer A pointer to a byte array
     * @param buffer_size The length of the byte array
     */
    void add_bytes(const void* buffer, std::size_t buffer_size);
    /**
     * Retrieves the hash of all the bytes that have been added with add_bytes
     * (since the last call to init) and returns it in the provided byte buffer,
     * which must be the correct length (i.e. the size returned by
     * get_hash_size).
     * @param hash_buffer A pointer to a byte array in which the hash will be
     * written by this function
     */
    void finalize(uint8_t* hash_buffer);
    /**
     * Retrieves the hash of all the bytes that have been added with add_bytes
     * (since the last call to init) and returns it in a new vector.
     * @return An array of bytes containing the hash.
     */
    std::vector<uint8_t> finalize();
    /**
     * A convenience method that hashes a single byte buffer and places the hash
     * in the output buffer, which is assumed to be the correct size. (i.e. the
     * size returned by get_hash_size).
     * @param buffer The byte buffer to hash
     * @param buffer_size The length of the byte buffer
     * @param hash_buffer The byte buffer in which the hash will be placed; must
     * be the correct size for this hash function.
     */
    void hash_bytes(const void* buffer, std::size_t buffer_size, uint8_t* hash_buffer);
};

}  // namespace openssl
