#include "derecho/persistent/Persistent.hpp"

#include "derecho/openssl/hash.hpp"
#include "derecho/openssl/signature.hpp"

#include <functional>
#include <string>
#include <typeindex>

namespace persistent {

thread_local int64_t PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;

PersistentRegistry::PersistentRegistry(
        ITemporalQueryFrontierProvider* tqfp,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) : m_subgroupPrefix(generate_prefix(subgroup_type, subgroup_index, shard_num)),
                              m_temporalQueryFrontierProvider(tqfp),
                              m_lastSignedVersion(INVALID_VERSION) {
}

PersistentRegistry::~PersistentRegistry() {
    this->m_registry.clear();
};

void PersistentRegistry::makeVersion(version_t ver, const HLC& mhlc) {
    for(auto& entry : m_registry) {
        entry.second->version(ver, mhlc);
    }
};

version_t PersistentRegistry::getMinimumLatestVersion() {
    version_t min = -1;
    for(auto itr = m_registry.begin();
        itr != m_registry.end(); ++itr) {
        version_t ver = itr->second->getLatestVersion();
        if(itr == m_registry.begin()) {
            min = ver;
        } else if(min > ver) {
            min = ver;
        }
    }
    return min;
}

version_t PersistentRegistry::getMinimumVersionAfter(version_t version) {
    version_t min = -1;
    for(auto registry_itr = m_registry.begin(); registry_itr != m_registry.end(); ++registry_itr) {
        version_t field_next_ver = registry_itr->second->getNextVersionOf(version);
        if(registry_itr == m_registry.begin()
           || (field_next_ver != INVALID_VERSION && field_next_ver < min)) {
            min = field_next_ver;
        }
    }
    return min;
}

void PersistentRegistry::initializeLastSignature(version_t version,
                                                 const uint8_t* signature, std::size_t signature_size) {
    if(signature_size != m_lastSignature.size()) {
        m_lastSignature.resize(signature_size);
        // On the first call to initialize_signature with version == INVALID_VERSION,
        // this will initialize last_signature to all zeroes, which is our "genesis signature"
    }
    if(signature_size > 0 && version != INVALID_VERSION
       && (m_lastSignedVersion == INVALID_VERSION || m_lastSignedVersion < version)) {
        memcpy(m_lastSignature.data(), signature, signature_size);
        m_lastSignedVersion = version;
    }
}

void PersistentRegistry::sign(version_t latest_version, openssl::Signer& signer, uint8_t* signature_buffer) {
    version_t cur_nonempty_version = getMinimumVersionAfter(m_lastSignedVersion);
    dbg_default_debug("PersistentRegistry: sign() called with lastSignedVersion = {}, latest_version = {}. First version to sign = {}", m_lastSignedVersion, latest_version, cur_nonempty_version);
    while(cur_nonempty_version != INVALID_VERSION && cur_nonempty_version <= latest_version) {
        dbg_default_trace("PersistentRegistry: Attempting to sign version {} out of {}", cur_nonempty_version, latest_version);
        signer.init();
        std::size_t bytes_signed = 0;
        for(auto& field : m_registry) {
            bytes_signed += field.second->updateSignature(cur_nonempty_version, signer);
        }
        if(bytes_signed == 0) {
            // That version did not exist in any field, so there was nothing to sign. This should not happen with getMinimumVersionAfter().
            dbg_default_warn("Logic error in PersistentRegistry: Version {} was returned by getMinimumVersionAfter(), but it did not exist in any field", cur_nonempty_version);
            cur_nonempty_version = getMinimumVersionAfter(cur_nonempty_version);
            continue;
        }
        signer.add_bytes(m_lastSignature.data(), m_lastSignature.size());
        signer.finalize(signature_buffer);
        // After computing a signature over all fields of the object, go back and
        // tell each field to add that signature to its log.
        dbg_default_debug("PersistentRegistry: Adding signature to log in version {}, setting its previous signed version to {}", cur_nonempty_version, m_lastSignedVersion);
        for(auto& field : m_registry) {
            field.second->addSignature(cur_nonempty_version, signature_buffer, m_lastSignedVersion);
        }
        memcpy(m_lastSignature.data(), signature_buffer, m_lastSignature.size());
        m_lastSignedVersion = cur_nonempty_version;
        // Advance the current version to the next non-empty version, or INVALID_VERSION if it is already at the latest version
        cur_nonempty_version = getMinimumVersionAfter(cur_nonempty_version);
    }
}

bool PersistentRegistry::getSignature(version_t version, uint8_t* signature_buffer) {
    version_t previous_signed_version;
    for(auto& field : m_registry) {
        if(field.second->getSignature(version, signature_buffer, previous_signed_version)) {
            return true;
        }
    }
    return false;
}

bool PersistentRegistry::verify(version_t version, openssl::Verifier& verifier, const uint8_t* signature) {
    // For objects with no persistent fields, verification should always "succeed"
    if(m_registry.empty()) {
        return true;
    }
    dbg_default_debug("PersistentRegistry: Verifying signature on version {}", version);
    verifier.init();
    for(auto& field : m_registry) {
        field.second->updateVerifier(version, verifier);
    }
    const std::size_t signature_size = verifier.get_max_signature_size();
    version_t prev_signed_version;
    // Find a field that has a signature for this version; not all entries will
    // On that field, get the previous signed version, and retrieve that signature
    uint8_t current_sig[signature_size];
    uint8_t previous_sig[signature_size];
    for(auto& field : m_registry) {
        if(field.second->getSignature(version, current_sig, prev_signed_version)) {
            if(prev_signed_version == INVALID_VERSION) {
                // Special case if version is the very first version,
                // whose previous signature is the "genesis signature"
                memset(previous_sig, 0, signature_size);
            } else {
                version_t dummy;
                field.second->getSignature(prev_signed_version, previous_sig, dummy);
            }
            break;
        }
    }
    verifier.add_bytes(previous_sig, signature_size);
    return verifier.finalize(signature, signature_size);
}

void PersistentRegistry::persist(version_t latest_version) {
    for(auto& entry : m_registry) {
        entry.second->persist(latest_version);
    }
};

void PersistentRegistry::trim(version_t earliest_version) {
    for(auto& entry : m_registry) {
        entry.second->trim(earliest_version);
    }
};

int64_t PersistentRegistry::getMinimumLatestPersistedVersion() {
    int64_t min = -1;
    for(auto itr = m_registry.begin();
        itr != m_registry.end(); ++itr) {
        int64_t ver = itr->second->getLastPersistedVersion();
        if(itr == m_registry.begin()) {
            min = ver;
        } else if(min > ver) {
            min = ver;
        }
    }
    return min;
}

void PersistentRegistry::setEarliestVersionToSerialize(version_t ver) noexcept(true) {
    PersistentRegistry::earliest_version_to_serialize = ver;
}

void PersistentRegistry::resetEarliestVersionToSerialize() noexcept(true) {
    PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;
}

int64_t PersistentRegistry::getEarliestVersionToSerialize() noexcept(true) {
    return PersistentRegistry::earliest_version_to_serialize;
}

void PersistentRegistry::truncate(version_t last_version) {
    for(auto& entry : m_registry) {
        entry.second->truncate(last_version);
    }
}

void PersistentRegistry::registerPersistent(const std::string& obj_name,
                                            PersistentObject* persistent_object) {
    std::size_t key = std::hash<std::string>{}(obj_name);
    auto res = this->m_registry.insert(std::pair<std::size_t, PersistentObject*>(key, persistent_object));
    if(res.second == false) {
        // override the previous value:
        this->m_registry.erase(res.first);
        this->m_registry.insert(std::pair<std::size_t, PersistentObject*>(key, persistent_object));
    }
};

void PersistentRegistry::unregisterPersistent(const std::string& obj_name) {
    // The upcoming regsiterPersist() call will override this automatically.
    // this->_registry.erase(std::hash<std::string>{}(obj_name));
}

void PersistentRegistry::updateTemporalFrontierProvider(ITemporalQueryFrontierProvider* tqfp) {
    this->m_temporalQueryFrontierProvider = tqfp;
}

const char* PersistentRegistry::getSubgroupPrefix() {
    return this->m_subgroupPrefix.c_str();
}

std::string PersistentRegistry::generate_prefix(
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) {
    const char* subgroup_type_name = subgroup_type.name();

    // SHA256 subgroup_type_name to avoid a long file name
    uint8_t subgroup_type_name_digest[32];
    openssl::Hasher sha256(openssl::DigestAlgorithm::SHA256);
    try {
        sha256.hash_bytes(subgroup_type_name, strlen(subgroup_type_name), subgroup_type_name_digest);
    } catch(openssl::openssl_error& ex) {
        dbg_default_error("{}:{} Unable to compute SHA256 of subgroup type name. OpenSSL error: {}", __FILE__, __func__, ex.what());
        throw PERSIST_EXP_SHA256_HASH(errno);
    }

    // char prefix[strlen(subgroup_type_name) * 2 + 32];
    char prefix[32 * 2 + 32];
    uint32_t i = 0;
    for(i = 0; i < 32; i++) {
        sprintf(prefix + 2 * i, "%02x", subgroup_type_name_digest[i]);
    }
    sprintf(prefix + 2 * i, "-%u-%u", subgroup_index, shard_num);
    return std::string(prefix);
}

bool PersistentRegistry::match_prefix(
        const std::string str,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) noexcept(true) {
    std::string prefix = generate_prefix(subgroup_type, subgroup_index, shard_num);
    try {
        if(prefix == str.substr(0, prefix.length()))
            return true;
    } catch(const std::out_of_range&) {
        // str is shorter than prefix, just return false.
    }
    return false;
}

}  // namespace persistent
