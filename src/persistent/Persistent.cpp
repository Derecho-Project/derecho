#include <derecho/persistent/Persistent.hpp>

#include <derecho/persistent/detail/logger.hpp>
#include <derecho/openssl/hash.hpp>
#include <derecho/openssl/signature.hpp>
#include <derecho/utils/timestamp_logger.hpp>

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
                              m_logger(PersistLogger::get()),
                              m_temporalQueryFrontierProvider(tqfp),
                              m_lastSignedVersion(INVALID_VERSION) {
}

PersistentRegistry::~PersistentRegistry() {
    this->m_registry.clear();
};

void PersistentRegistry::makeVersion(version_t ver, const HLC& mhlc) {
    TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_MAKE_VERSION_BEGIN, 0, ver);
    for(auto& entry : m_registry) {
        entry.second->version(ver, mhlc);
    }
    TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_MAKE_VERSION_END, 0, ver);
};

version_t PersistentRegistry::getCurrentVersion() const {
    version_t min = -1;
    for(auto itr = m_registry.begin();
        itr != m_registry.end(); ++itr) {
        version_t ver = itr->second->getCurrentVersion();
        if(itr == m_registry.begin()) {
            min = ver;
        } else if(min > ver) {
            min = ver;
        }
    }
    dbg_trace(m_logger, "PersistentRegistry: getCurrentVersion() returning {}", min);
    return min;
}

version_t PersistentRegistry::getMinimumVersionAfter(version_t version) const {
    version_t min = -1;
    for(auto registry_itr = m_registry.begin(); registry_itr != m_registry.end(); ++registry_itr) {
        version_t field_next_ver = registry_itr->second->getNextVersionOf(version);
        if(registry_itr == m_registry.begin()
           || (field_next_ver != INVALID_VERSION && field_next_ver < min)) {
            min = field_next_ver;
        }
    }
    dbg_trace(m_logger, "PersistentRegistry: getMinimumVersionAfter({}) returning {}", version, min);
    return min;
}

version_t PersistentRegistry::getNextSignedVersion(version_t version) const {
    version_t min = -1;
    bool min_initialized = false;
    for(auto registry_itr = m_registry.begin(); registry_itr != m_registry.end(); ++registry_itr) {
        // Skip non-signed fields
        if(registry_itr->second->getSignatureSize() == 0) {
            continue;
        }
        version_t field_next_ver = registry_itr->second->getNextVersionOf(version);
        if(field_next_ver != INVALID_VERSION && (!min_initialized || field_next_ver < min)) {
            min = field_next_ver;
            min_initialized = true;
        }
    }
    dbg_trace(m_logger, "PersistentRegistry: getNextSignedVersion({}) returning {}", version, min);
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

version_t PersistentRegistry::sign(openssl::Signer& signer, uint8_t* signature_buffer) {
    version_t current_version = getCurrentVersion();
    version_t cur_signable_version = getNextSignedVersion(m_lastSignedVersion);
    dbg_debug(m_logger, "PersistentRegistry: sign() called with lastSignedVersion = {}, current_version = {}. First version to sign = {}", m_lastSignedVersion, current_version, cur_signable_version);
    // If there is nothing to do because the latest non-empty version has already been signed,
    // ensure the latest signature still gets returned in the output buffer
    if(cur_signable_version == INVALID_VERSION || cur_signable_version > current_version) {
        memcpy(signature_buffer, m_lastSignature.data(), m_lastSignature.size());
    }
    while(cur_signable_version != INVALID_VERSION && cur_signable_version <= current_version) {
        TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_SIGN_BEGIN, 0, cur_signable_version);
        dbg_trace(m_logger, "PersistentRegistry: Attempting to sign version {} out of {}", cur_signable_version, current_version);
        signer.init();
        std::size_t bytes_signed = 0;
        for(auto& field : m_registry) {
            dbg_trace(m_logger, "PersistentRegistry: Signing persistent field at {}", fmt::ptr(field.second));
            bytes_signed += field.second->updateSignature(cur_signable_version, signer);
        }
        if(bytes_signed == 0) {
            // That version did not exist in any field, so there was nothing to sign. This should not happen with getNextSignedVersion().
            dbg_warn(m_logger, "Logic error in PersistentRegistry: Version {} was returned by getNextSignedVersion(), but no field signed any data for it", cur_signable_version);
            cur_signable_version = getNextSignedVersion(cur_signable_version);
            continue;
        }
        signer.add_bytes(m_lastSignature.data(), m_lastSignature.size());
        signer.finalize(signature_buffer);
        // After computing a signature over all fields of the object, go back and
        // tell each field to add that signature to its log.
        dbg_debug(m_logger, "PersistentRegistry: Adding signature to log in version {}, setting its previous signed version to {}", cur_signable_version, m_lastSignedVersion);
        for(auto& field : m_registry) {
            field.second->addSignature(cur_signable_version, signature_buffer, m_lastSignedVersion);
        }
        memcpy(m_lastSignature.data(), signature_buffer, m_lastSignature.size());
        TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_SIGN_END, 0, cur_signable_version);
        m_lastSignedVersion = cur_signable_version;
        // Advance the current version to the next non-empty version, or INVALID_VERSION if it is already at the latest version
        cur_signable_version = getNextSignedVersion(cur_signable_version);
    }
    return m_lastSignedVersion;
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
    dbg_debug(m_logger, "PersistentRegistry: Verifying signature on version {}", version);
    verifier.init();
    for(auto& field : m_registry) {
        // Only adds bytes to the verifier for fields that have signatures enabled
        field.second->updateVerifier(version, verifier);
    }
    const std::size_t signature_size = verifier.get_max_signature_size();
    version_t prev_signed_version;
    // Find a field that has a signature for this version; not all entries will
    // On that field, get the previous signed version, and retrieve that signature
    uint8_t current_sig[signature_size];
    uint8_t previous_sig[signature_size];
    bool signature_found = false;
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
            signature_found = true;
            break;
        }
    }
    if(!signature_found) {
        dbg_warn(m_logger, "PersistentRegistry: Version {} had no fields with a signature! Unable to verify!", version);
    }
    verifier.add_bytes(previous_sig, signature_size);
    return verifier.finalize(signature, signature_size);
}

version_t PersistentRegistry::persist(std::optional<version_t> latest_version) {
    TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_PERSIST_BEGIN, 0, latest_version ? *latest_version : 0);
    version_t min = INVALID_VERSION;
    for(auto& entry : m_registry) {
        version_t ver = entry.second->persist(latest_version);
        if (min == INVALID_VERSION ||
            min > ver) {
            min = ver;
        }
    }
    TIMESTAMP_LOG(derecho::TimestampLogger::REGISTRY_PERSIST_END, 0, latest_version ? *latest_version : 0);
    return min;
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
    dbg_trace(m_logger, "PersistentRegistry: getMinimumLatestPersistedVersion() returning {}", min);
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
        dbg_error(PersistLogger::get(), "{}:{} Unable to compute SHA256 of subgroup type name. OpenSSL error: {}", __FILE__, __func__, ex.what());
        throw;
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
