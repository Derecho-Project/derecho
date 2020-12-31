#include <cstdio>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/openssl/hash.hpp>
#include <derecho/openssl/signature.hpp>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>

class StringObject : public mutils::ByteRepresentable {
    std::string log;

public:
    void append(const std::string& words) {
        log += words;
    }
    void clear() {
        log.clear();
    }
    std::string print() const {
        return log;
    }

    StringObject(const std::string& s = "") : log(s) {}

    DEFAULT_SERIALIZATION_SUPPORT(StringObject, log);
    REGISTER_RPC_FUNCTIONS(StringObject, ORDERED_TARGETS(append, clear), P2P_TARGETS(print));
};

int main(int argc, char** argv) {
    std::vector<StringObject> test_objects;
    test_objects.push_back(StringObject("Hello!"));
    test_objects.push_back(StringObject("This is a longer object with more data in it. abcdefghijklmnopqrstuvwxyz1234567890"));
    test_objects.push_back(StringObject("This is a much larger object with a lot of data in it. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. In nisl nisi scelerisque eu ultrices vitae auctor eu. Ultricies lacus sed turpis tincidunt id. Ultricies mi quis hendrerit dolor magna eget est lorem ipsum. Pellentesque habitant morbi tristique senectus et netus et malesuada. Id interdum velit laoreet id donec. Sagittis id consectetur purus ut. Donec pretium vulputate sapien nec sagittis aliquam. Lacus sed viverra tellus in hac habitasse. Libero volutpat sed cras ornare arcu dui vivamus arcu felis. Eget nulla facilisi etiam dignissim diam quis. Dictum non consectetur a erat nam at lectus. In dictum non consectetur a erat nam at lectus. Cras tincidunt lobortis feugiat vivamus at. Vitae aliquet nec ullamcorper sit amet risus nullam eget felis. Sed risus ultricies tristique nulla aliquet enim tortor. Nulla posuere sollicitudin aliquam ultrices sagittis orci. Tempus imperdiet nulla malesuada pellentesque elit. Velit sed ullamcorper morbi tincidunt ornare massa eget."));

    openssl::EnvelopeKey my_private_key = openssl::EnvelopeKey::from_pem_private("server_private_key.pem");
    openssl::EnvelopeKey my_public_key = openssl::EnvelopeKey::from_pem_public("server_public_key.pem");

    openssl::Signer my_signer(my_private_key, openssl::DigestAlgorithm::SHA256);
    openssl::Verifier my_verifier(my_public_key, openssl::DigestAlgorithm::SHA256);

    const int signature_size = my_signer.get_max_signature_size();

    std::vector<std::vector<unsigned char>> signatures;
    for(std::size_t i = 0; i < test_objects.size(); ++i) {
        std::cout << "For test object " << i << ":" << std::endl;
        std::size_t serialized_obj_size = mutils::bytes_size(test_objects[i]);
        std::cout << "Serialized object size is " << serialized_obj_size << std::endl;
        std::unique_ptr<char[]> serialized_obj = std::make_unique<char[]>(serialized_obj_size);
        mutils::to_bytes(test_objects[i], serialized_obj.get());

        my_signer.init();
        my_signer.add_bytes(serialized_obj.get(), serialized_obj_size);
        std::vector<unsigned char> signature1 = my_signer.finalize();

        unsigned char signature2[signature_size];
        my_signer.sign_bytes(serialized_obj.get(), serialized_obj_size, signature2);

        std::cout << "Signature is " << signature_size << " bytes. Multi-step signature: ";
        std::cout << std::hex;
        for(int i = 0; i < signature_size; ++i) {
            std::cout << std::setw(2) << std::setfill('0') << (int)signature1[i];
        }
        std::cout << std::endl;
        std::cout << "One-step signature: ";
        for(int i = 0; i < signature_size; ++i) {
            std::cout << std::setw(2) << std::setfill('0') << (int)signature2[i];
        }
        std::cout << std::dec << std::endl;

        assert(memcmp(signature1.data(), signature2, signature_size) == 0);
        signatures.emplace_back(signature1);
    }
    for(std::size_t i = 0; i < test_objects.size(); ++i) {
        std::size_t serialized_obj_size = mutils::bytes_size(test_objects[i]);
        std::unique_ptr<char[]> serialized_obj = std::make_unique<char[]>(serialized_obj_size);
        mutils::to_bytes(test_objects[i], serialized_obj.get());

        my_verifier.init();
        my_verifier.add_bytes(serialized_obj.get(), serialized_obj_size);
        if(my_verifier.finalize(signatures[i])) {
            std::cout << "Signature verified successfully" << std::endl;
        } else {
            std::cout << "ERROR: Signature failed to verify." << std::endl;
        }

        if(my_verifier.verify_bytes(serialized_obj.get(), serialized_obj_size, signatures[i].data(), signatures[i].size())) {
            std::cout << "One-step signature verified successfully" << std::endl;
        } else {
            std::cout << "ERROR: one-step signature failed to verify" << std::endl;
        }

        if(i < test_objects.size() - 1) {
            my_verifier.init();
            my_verifier.add_bytes(serialized_obj.get(), serialized_obj_size);
            if(my_verifier.finalize(signatures[i + 1])) {
                std::cout << "PROBLEM: Object " << i << " verified against a signature on object " << i + 1 << std::endl;
            } else {
                std::cout << "Mismatched signatures correctly failed to verify" << std::endl;
            }
        }
    }

    openssl::Hasher my_hasher(openssl::DigestAlgorithm::SHA256);
    const char* subgroup_type_name = std::type_index(typeid(StringObject)).name();
    unsigned char subgroup_type_name_digest[my_hasher.get_hash_size()];
    my_hasher.init();
    my_hasher.add_bytes(subgroup_type_name, strlen(subgroup_type_name));
    my_hasher.finalize(subgroup_type_name_digest);
    char prefix[32 * 2 + 32];
    for(uint32_t i = 0; i < 32; i++) {
        sprintf(prefix + 2 * i, "%02x", subgroup_type_name_digest[i]);
    }
    std::cout << "Hashed type name: " << prefix << std::endl;

    unsigned char subgroup_name_digest_2[my_hasher.get_hash_size()];
    my_hasher.hash_bytes(subgroup_type_name, strlen(subgroup_type_name), subgroup_name_digest_2);
    char prefix2[32 * 2 + 32];
    for(uint32_t i = 0; i < 32; i++) {
        sprintf(prefix2 + 2 * i, "%02x", subgroup_name_digest_2[i]);
    }
    std::cout << "Hashed type name: " << prefix2 << std::endl;
}
