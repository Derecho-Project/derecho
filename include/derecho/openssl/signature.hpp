/**
 * @file signature.hpp
 *
 * Classes and functions that wrap the "EVP_DigestSign" and "EVP_DigestVerify"
 * features of OpenSSL.
 */

#pragma once
#include "hash.hpp"
#include "openssl_exception.hpp"
#include "pointers.hpp"
#include <openssl/evp.h>
#include <vector>

namespace openssl {

/**
 * A class that wraps the EVP_PKEY object used to represent public and private
 * keys in OpenSSL EVP_* functions.
 */
class EnvelopeKey {
    std::unique_ptr<EVP_PKEY, DeleterFor<EVP_PKEY>> key;

public:
    /**
     * Constructs a new EnvelopeKey that wraps an EVP_PKEY pointer returned by
     * some OpenSSL function.
     */
    EnvelopeKey(EVP_PKEY* pkey) : key(pkey){};
    /**
     * Copy constructor: "Copies" the wrapped EVP_PKEY by incrementing its
     * internal reference count and creating a new unique_ptr to it. After a
     * call to EVP_PKEY_up_ref, the first destructor-triggered call to
     * EVP_PKEY_free will actually decrement the reference count rather than
     * free the object.
     */
    EnvelopeKey(const EnvelopeKey& other) : key(other.key.get()) {
        EVP_PKEY_up_ref(key.get());
    }
    /**
     * Move constructor: Takes ownership of the wrapped EVP_PKEY from the other
     * EnvelopeKey, leaving it in an "empty" state.
     */
    EnvelopeKey(EnvelopeKey&& other) : key(std::move(other.key)) {}

    /**
     * Implicit converter to EVP_PKEY* so that this class can be used in OpenSSL
     * functions that expect an EVP_PKEY*; simply gets the wrapped raw pointer.
     */
    operator EVP_PKEY*() {
        return key.get();
    }
    /**
     * Implicit bool conversion that matches std::unique_ptr's, so this object
     * can be tested for emptiness in the same way.
     * @return false if the wrapped EVP_PKEY pointer is empty, true if it is not.
     */
    operator bool() {
        return key.operator bool();
    }
    /**
     * Copy assignment operator. Just like the copy constructor, this actually
     * updates the internal reference count of the wrapped EVP_PKEY and then
     * copies the raw pointer.
     */
    EnvelopeKey& operator=(const EnvelopeKey& other);
    /**
     * Move assignment operator.
     */
    EnvelopeKey& operator=(EnvelopeKey&& other);
    /**
     * @return the "maximum output size" (in bytes) reported by this key. For
     * RSA private keys, this is the exact size of every signature and can be
     * used as the size of signature buffers.
     */
    int get_max_size();
    /**
     * Factory function that constructs an EnvelopeKey by loading a public key
     * from a PEM file on disk.
     * @param pem_file_name The name (or path) of the PEM file to read from
     */
    static EnvelopeKey from_pem_public(const std::string& pem_file_name);
    /**
     * Factory function that constructs an EnvelopeKey by loading a public key
     * from a PEM file stored in a byte buffer in memory.
     * @param byte_buffer An array of bytes containing a public key in PEM
     * format
     * @param buffer_size The size of the byte array
     */
    static EnvelopeKey from_pem_public(const void* byte_buffer, std::size_t buffer_size);
    /**
     * Factory function that constructs an EnvelopeKey by loading a private key
     * from a PEM file on disk.
     * @param pem_file_name The name (or path) of the PEM file to read from
     */
    static EnvelopeKey from_pem_private(const std::string& pem_file_name);
    /**
     * Factory function that constructs an EnvelopeKey by loading a private key
     * from a PEM file stored in a byte buffer in memory.
     * @param byte_buffer An array of bytes containing a public key in PEM
     * format
     * @param buffer_size The size of the byte array
     */
    static EnvelopeKey from_pem_private(const void* byte_buffer, std::size_t buffer_size);
};

/**
 * A class that wraps the EVP_DigestSign* functions for signing a byte array
 * given a private key. Each function will throw exceptions if the underlying
 * library calls return errors, rather than returning an error code.
 */
class Signer {
    EnvelopeKey private_key;
    const DigestAlgorithm digest_type;
    std::unique_ptr<EVP_MD_CTX, DeleterFor<EVP_MD_CTX>> digest_context;

public:
    /**
     * Constructs a Signer that will use the given private key to sign messages,
     * using the specified digest algorithm to digest (hash) its input.
     */
    Signer(const EnvelopeKey& private_key, DigestAlgorithm digest_type);
    /**
     * @return the "maximum signature size" (in bytes) reported by the private
     * key associated with this Signer. For RSA private keys, this is the exact
     * size of every signature and can be used as the size of signature buffers.
     */
    int get_max_signature_size();
    /**
     * Initializes the Signer to start signing a new message. Must be called
     * before add_bytes or finalize.
     */
    void init();
    /**
     * Adds a byte buffer to the message (sequence of bytes) that this Signer
     * will hash and sign.
     * @param buffer A pointer to a byte array
     * @param buffer_size The length of the byte array
     */
    void add_bytes(const void* buffer, const std::size_t buffer_size);
    /**
     * Signs all of the bytes that have been added with add_bytes (since the
     * last call to init) and places the signature in the provided buffer, which
     * must be the correct length. Only use this if the caller knows the length
     * of the signature and has already allocated a buffer of the right size.
     * @param signature_buffer A pointer to a byte array in which the signature
     * will be written by this function.
     */
    void finalize(unsigned char* signature_buffer);
    /**
     * Signs all of the bytes that have been added with add_bytes (since the
     * last call to init) and returns a byte array containing the signature.
     * @return An array of bytes containing the signature.
     */
    std::vector<unsigned char> finalize();
    /**
     * A convenience method that signs a single byte buffer in one shot and
     * places the signature in a byte buffer assumed to be the correct size.
     * This will internally re-initialize and then finalize the Signer.
     * @param buffer The byte buffer to sign
     * @param buffer_size The length of the byte buffer
     * @param signature_buffer The byte buffer in which the signature will be
     * placed; must be the correct size for this signature.
     */
    void sign_bytes(const void* buffer, std::size_t buffer_size, unsigned char* signature_buffer);
};

class Verifier {
    EnvelopeKey public_key;
    const DigestAlgorithm digest_type;
    std::unique_ptr<EVP_MD_CTX, DeleterFor<EVP_MD_CTX>> digest_context;

public:
    Verifier(const EnvelopeKey& public_key, DigestAlgorithm digest_type);
    /**
     * Initializes the Verifier to start verifying a new message. Must be
     * called before add_bytes or finalize.
     */
    void init();
    /**
     * Adds a byte buffer to the message (sequence of bytes) that this Verifier
     * will hash and verify.
     * @param buffer A pointer to a byte array
     * @param buffer_size The length of the byte array
     */
    void add_bytes(const void* buffer, std::size_t buffer_size);
    /**
     * Signs all of the bytes that have been added with add_bytes (since the
     * last call to init) and compares them to the provided signature. Returns
     * true if the verification succeeds, false if it fails.
     * @param signature_buffer The byte buffer containing the signature to
     * compare against.
     * @param signature_length The length of the signature buffer
     * @return True if verification succeeds, false if it fails.
     */
    bool finalize(const unsigned char* signature_buffer, std::size_t signature_length);
    /**
     * Signs all of the bytes that have been added with add_bytes (since the
     * last call to init) and compares them to the provided signature. Returns
     * true if the verification succeeds, false if it fails. This version of
     * finalize accepts a modern C++ byte array instead of a plain-C buffer.
     * @param signature A byte array containing the signature to compare
     * against.
     */
    bool finalize(const std::vector<unsigned char>& signature);
    /**
     * A convenience method that verifies a single byte buffer in one shot,
     * given a signature buffer to compare against.
     * @param buffer The byte buffer to verify
     * @param buffer_size The length of the buffer
     * @param signature The signature to compare against
     * @param signature_size The length of the signature in bytes
     * @return True if verification succeeds, false if it fails.
     */
    bool verify_bytes(const void* buffer, std::size_t buffer_size, const unsigned char* signature, std::size_t signature_size);
};

}  // namespace openssl
