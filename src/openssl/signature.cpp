#include <cstdio>
#include <derecho/openssl/signature.hpp>
#include <openssl/err.h>
#include <openssl/pem.h>

namespace openssl {

EnvelopeKey& EnvelopeKey::operator=(const EnvelopeKey& other) {
    if(&other == this) {
        return *this;
    }
    key.reset(other.key.get());
    EVP_PKEY_up_ref(key.get());
    return *this;
}

EnvelopeKey& EnvelopeKey::operator=(EnvelopeKey&& other) {
    key = std::move(other.key);
    return *this;
}

int EnvelopeKey::get_max_size() {
    return EVP_PKEY_size(key.get());
}

EnvelopeKey load_private_key(const std::string& pem_file_name) {
    FILE* pem_file = fopen(pem_file_name.c_str(), "r");
    if(pem_file == NULL) {
        switch(errno) {
            case EACCES:
            case EPERM:
                throw permission_denied(errno, pem_file_name);
            case ENOENT:
                throw file_not_found(errno, pem_file_name);
            default:
                throw file_error(errno, pem_file_name);
        }
    }
    EnvelopeKey private_key(PEM_read_PrivateKey(pem_file, NULL, NULL, NULL));
    fclose(pem_file);
    if(!private_key) {
        throw openssl_error(ERR_get_error(), "Load private key");
    }
    return std::move(private_key);
}

EnvelopeKey load_public_key(const std::string& pem_file_name) {
    FILE* pem_file = fopen(pem_file_name.c_str(), "r");
    if(pem_file == NULL) {
        switch(errno) {
            case EACCES:
            case EPERM:
                throw permission_denied(errno, pem_file_name);
            case ENOENT:
                throw file_not_found(errno, pem_file_name);
            default:
                throw file_error(errno, pem_file_name);
        }
    }
    EnvelopeKey public_key(PEM_read_PUBKEY(pem_file, NULL, NULL, NULL));
    fclose(pem_file);
    if(!public_key) {
        throw openssl_error(ERR_get_error(), "Load public key");
    }
    return std::move(public_key);
}

Signer::Signer(const EnvelopeKey& _private_key, DigestAlgorithm digest_type)
        : private_key(_private_key),
          digest_type(digest_type),
          digest_context(EVP_MD_CTX_new()) {}

int Signer::get_max_signature_size() {
    return private_key.get_max_size();
}

void Signer::init() {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestSignInit(digest_context.get(), NULL, get_digest_type_ptr(digest_type), NULL, private_key) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignInit");
    }
}

void Signer::add_bytes(const void* buffer, std::size_t buffer_size) {
    if(EVP_DigestSignUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignUpdate");
    }
}

void Signer::finalize(unsigned char* signature_buffer) {
    //We assume the caller has allocated a signature buffer of the correct length,
    //but we have to pass a valid siglen to EVP_DigestSignFinal anyway
    size_t siglen;
    if(EVP_DigestSignFinal(digest_context.get(), NULL, &siglen) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
    if(EVP_DigestSignFinal(digest_context.get(), signature_buffer, &siglen) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
}

std::vector<unsigned char> Signer::finalize() {
    size_t signature_len = 0;
    if(EVP_DigestSignFinal(digest_context.get(), NULL, &signature_len) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
    std::vector<unsigned char> signature(signature_len);
    //Technically, this function call may change signature_len again, but with RSA it never does
    if(EVP_DigestSignFinal(digest_context.get(), signature.data(), &signature_len) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
    return signature;
}

void Signer::sign_bytes(const void* buffer, std::size_t buffer_size, unsigned char* signature_buffer) {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestSignInit(digest_context.get(), NULL, get_digest_type_ptr(digest_type), NULL, private_key) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignInit");
    }
    if(EVP_DigestSignUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignUpdate");
    }
    //We assume the caller has allocated a signature buffer of the correct length,
    //but we have to pass a valid siglen to EVP_DigestSignFinal anyway
    size_t siglen;
    if(EVP_DigestSignFinal(digest_context.get(), NULL, &siglen) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
    if(EVP_DigestSignFinal(digest_context.get(), signature_buffer, &siglen) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestSignFinal");
    }
}

Verifier::Verifier(const EnvelopeKey& _public_key, DigestAlgorithm digest_type)
        : public_key(_public_key),
          digest_type(digest_type),
          digest_context(EVP_MD_CTX_new()) {}

void Verifier::init() {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestVerifyInit(digest_context.get(), NULL, get_digest_type_ptr(digest_type), NULL, public_key) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyInit");
    }
}
void Verifier::add_bytes(const void* buffer, std::size_t buffer_size) {
    if(EVP_DigestVerifyUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyUpdate");
    }
}
bool Verifier::finalize(const unsigned char* signature_buffer, std::size_t signature_length) {
    //EVP_DigestVerifyFinal returns 1 on success, 0 on signature mismatch, and "another value" on a more serious error
    int status = EVP_DigestVerifyFinal(digest_context.get(), signature_buffer, signature_length);
    if(status == 1) {
        return true;
    } else if(status == 0) {
        return false;
    } else {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyFinal");
    }
}
bool Verifier::finalize(const std::vector<unsigned char>& signature) {
    return finalize(signature.data(), signature.size());
}
bool Verifier::verify_bytes(const void* buffer, std::size_t buffer_size, const unsigned char* signature, std::size_t signature_size) {
    if(EVP_MD_CTX_reset(digest_context.get()) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_MD_CTX_reset");
    }
    if(EVP_DigestVerifyInit(digest_context.get(), NULL, get_digest_type_ptr(digest_type), NULL, public_key) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyInit");
    }
    if(EVP_DigestVerifyUpdate(digest_context.get(), buffer, buffer_size) != 1) {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyUpdate");
    }
    int status = EVP_DigestVerifyFinal(digest_context.get(), signature, signature_size);
    if(status == 1) {
        return true;
    } else if(status == 0) {
        return false;
    } else {
        throw openssl_error(ERR_get_error(), "EVP_DigestVerifyFinal");
    }
}

}  // namespace openssl
