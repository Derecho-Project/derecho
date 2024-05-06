#include <derecho/openssl/hash.hpp>

#include <derecho/openssl/openssl_exception.hpp>

#include <openssl/evp.h>

namespace openssl {

const EVP_MD* get_digest_type_ptr(DigestAlgorithm digest_type) {
    switch(digest_type) {
        case DigestAlgorithm::MD5:
            return EVP_md5();
        case DigestAlgorithm::SHA1:
            return EVP_sha1();
        case DigestAlgorithm::SHA256:
            return EVP_sha256();
        case DigestAlgorithm::SHA384:
            return EVP_sha384();
        case DigestAlgorithm::SHA512:
            return EVP_sha512();
        case DigestAlgorithm::SHA3_224:
            return EVP_sha3_224();
        case DigestAlgorithm::SHA3_256:
            return EVP_sha3_256();
        case DigestAlgorithm::SHA3_384:
            return EVP_sha3_384();
        case DigestAlgorithm::SHA3_512:
            return EVP_sha3_512();
        default:
            return EVP_sha256();
    }
}

Hasher::Hasher(DigestAlgorithm digest_type)
        : digest_type(digest_type),
          digest_context(EVP_MD_CTX_new()) {}

int Hasher::get_hash_size() {
    return EVP_MD_CTX_size(digest_context.get());
}

void Hasher::init() {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestInit_ex(digest_context.get(), get_digest_type_ptr(digest_type), NULL) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestInit_ex");
    }
}

void Hasher::add_bytes(const void* buffer, std::size_t buffer_size) {
    if(EVP_DigestUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestUpdate");
    }
}

void Hasher::finalize(uint8_t* hash_buffer) {
    if(EVP_DigestFinal_ex(digest_context.get(), hash_buffer, NULL) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestFinal_ex");
    }
}

std::vector<uint8_t> Hasher::finalize() {
    std::vector<uint8_t> hash_buffer(get_hash_size());
    if(EVP_DigestFinal_ex(digest_context.get(), hash_buffer.data(), NULL) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestFinal_ex");
    }
    return hash_buffer;
}

void Hasher::hash_bytes(const void* buffer, std::size_t buffer_size, uint8_t* hash_buffer) {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestInit_ex(digest_context.get(), get_digest_type_ptr(digest_type), NULL) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestInit_ex");
    }
    if(EVP_DigestUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestUpdate");
    }
    if(EVP_DigestFinal_ex(digest_context.get(), hash_buffer, NULL) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestFinal_ex");
    }
}

}  // namespace openssl
