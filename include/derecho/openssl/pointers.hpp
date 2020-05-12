/**
 * @file pointers.hpp
 *
 * Some pointer utilities for the OpenSSL wrapper library.
 */

#pragma once

#include <memory>
#include <openssl/evp.h>

namespace openssl {

template <typename OpenSSLType>
struct DeleterFor;

template <>
struct DeleterFor<EVP_MD_CTX> {
    void operator()(EVP_MD_CTX* p) { EVP_MD_CTX_free(p); }
};

template <>
struct DeleterFor<EVP_PKEY> {
    void operator()(EVP_PKEY* p) { EVP_PKEY_free(p); }
};

}  // namespace openssl