/**
 * @file signature_chain_test.cpp
 *
 * This test creates a signed log with a sequence of non-consecutive versions,
 * simulating the way Replicated<T> would create such a log from a series of
 * ordered_send updates. It then verifies that all of the created signatures
 * are valid with their corresponding public key.
 */
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/openssl/signature.hpp>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#include <spdlog/fmt/bin_to_hex.h>

#include <iostream>
#include <iomanip>
#include <chrono>
#include <cstring>

class DummyReplicated {};

int main(int argc, char** argv) {
    using namespace persistent;
    PersistentRegistry registry(nullptr, std::type_index(typeid(DummyReplicated)), 0, 0);
    Persistent<std::string> pers_field_1(std::make_unique<std::string>, "PersistentString", &registry, true);
    Persistent<int> pers_field_2(std::make_unique<int>, "PersistentInteger", &registry, true);

    openssl::EnvelopeKey private_key = openssl::EnvelopeKey::from_pem_private(derecho::getConfString(derecho::Conf::PERS_PRIVATE_KEY_FILE));
    openssl::Signer signer(private_key,openssl::DigestAlgorithm::SHA256);
    openssl::Verifier verifier(private_key, openssl::DigestAlgorithm::SHA256);

    std::vector<std::string> update_strings = {"abcd", "efgh", "ijkl", "mnop", "qrst", "uvwx", "yz01", "qwerty", "yuiop", "asdf", "ghjkl"};
    std::vector<version_t> versions = {0, 2, 4, 6, 8, 10, 12, 14, 15, 16, 20};

    for(std::size_t i = 0; i < update_strings.size(); ++i) {
        //Update the objects with some new data
        *pers_field_1 = update_strings[i];
        *pers_field_2 = i;
        //Make a new version
        version_t new_ver = versions[i];
        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        registry.makeVersion(new_ver, HLC{timestamp,0});
        //Simulate Replicated<T>::persist
        std::vector<unsigned char> signature(signer.get_max_signature_size());
        version_t next_persisted_ver = registry.getMinimumLatestVersion();
        registry.sign(next_persisted_ver, signer, signature.data());
        registry.persist(next_persisted_ver);
        assert(next_persisted_ver == new_ver);
        dbg_default_info("Signature on version {}: {}", next_persisted_ver, spdlog::to_hex(signature));
    }

    for(version_t cur_version : versions) {
        //Simulate a verification request
        std::vector<unsigned char> signature(signer.get_max_signature_size());
        bool got_signature = registry.getSignature(cur_version, signature.data());
        if(!got_signature) {
            dbg_default_error("Failed to retrieve signature for version {}!", cur_version);
        } else {
            dbg_default_info("Retrieved signature for version {}: {}", cur_version, spdlog::to_hex(signature));
        }
        bool success = registry.verify(cur_version, verifier, signature.data());
        if(success) {
            dbg_default_info("Signature on version {} verified successfully.", cur_version);
        } else {
            dbg_default_warn("Signature on version {} failed to verify. Error {}", cur_version, openssl::get_error_string(ERR_get_error(), ""));
        }
    }
}
