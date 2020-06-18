#include <derecho/openssl/hash.hpp>
#include <derecho/persistent/Persistent.hpp>

namespace persistent {

thread_local int64_t PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;

PersistentRegistry::PersistentRegistry(
        ITemporalQueryFrontierProvider* tqfp,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) : _subgroup_prefix(generate_prefix(subgroup_type, subgroup_index, shard_num)),
                              _temporal_query_frontier_provider(tqfp),
                              last_signed_version(INVALID_VERSION) {
}

PersistentRegistry::~PersistentRegistry() {
    this->_registry.clear();
};

void PersistentRegistry::makeVersion(version_t ver, const HLC& mhlc) {
    for(auto& entry : _registry) {
        entry.second.version(ver, mhlc);
    }
};

version_t PersistentRegistry::getMinimumLatestVersion() {
    version_t min = -1;
    for(auto itr = _registry.begin();
        itr != _registry.end(); ++itr) {
        version_t ver = itr->second.get_latest_version();
        if(itr == _registry.begin()) {
            min = ver;
        } else if(min > ver) {
            min = ver;
        }
    }
    return min;
}

void PersistentRegistry::initializeLastSignature(version_t version,
                                                 const unsigned char* signature, std::size_t signature_size) {
    if(signature_size != last_signature.size()) {
        last_signature.resize(signature_size);
        //On the first call to initialize_signature with version == INVALID_VERSION,
        //this will initialize last_signature to all zeroes, which is our "genesis signature"
    }
    if(signature_size > 0 && version != INVALID_VERSION
       && (last_signed_version == INVALID_VERSION || last_signed_version < version)) {
        memcpy(last_signature.data(), signature, signature_size);
        last_signed_version = version;
    }
}

void PersistentRegistry::sign(version_t latest_version, openssl::Signer& signer, unsigned char* signature_buffer) {
    //Find the last version successfully persisted to disk so we know where to start
    version_t last_signed_version = getMinimumLatestPersistedVersion();
    for(version_t version = last_signed_version; version <= latest_version; ++version) {
        signer.init();
        std::size_t bytes_signed = 0;
        for(auto& entry : _registry) {
            bytes_signed += entry.second.update_signature(version, signer);
        }
        if(bytes_signed > 0) {
            //If this version's log entry is non-empty, add the previous version's signature to the signed data
            signer.add_bytes(last_signature.data(), last_signature.size());
        }
        signer.finalize(signature_buffer);
        //After computing a signature over all fields of the object, go back and
        //tell each field to add that signature to its log
        for(auto& entry : _registry) {
            entry.second.add_signature(version, signature_buffer, last_signed_version);
        }
        memcpy(last_signature.data(), signature_buffer, last_signature.size());
        last_signed_version = version;
    }
}

bool PersistentRegistry::get_signature(version_t version, unsigned char* signature_buffer) {
    for(auto& entry : _registry) {
        if(entry.second.get_signature(version, signature_buffer) != INVALID_VERSION) {
            return true;
        }
    }
    return false;
}

bool PersistentRegistry::verify(version_t version, openssl::Verifier& verifier, const unsigned char* signature) {
    verifier.init();
    for(auto& field : _registry) {
        field.second.update_verifier(version, verifier);
    }
    const std::size_t signature_size = verifier.get_max_signature_size();
    version_t prev_signed_version;
    //Find a field that has a signature for this version; not all entries will
    //On that field, get the previous signed version, and retrieve that signature
    unsigned char current_sig[signature_size];
    unsigned char previous_sig[signature_size];
    for(auto& field : _registry) {
        if((prev_signed_version = field.second.get_signature(version, current_sig)) != INVALID_VERSION) {
            field.second.get_signature(prev_signed_version, previous_sig);
        }
    }
    verifier.add_bytes(previous_sig, signature_size);
    return verifier.finalize(signature, signature_size);
}

void PersistentRegistry::persist(version_t latest_version) {
    for(auto& entry : _registry) {
        entry.second.persist(latest_version);
    }
};

void PersistentRegistry::trim(version_t earliest_version) {
    for(auto& entry : _registry) {
        entry.second.trim(earliest_version);
    }
};

int64_t PersistentRegistry::getMinimumLatestPersistedVersion() {
    int64_t min = -1;
    for(auto itr = _registry.begin();
        itr != _registry.end(); ++itr) {
        int64_t ver = itr->second.get_latest_persisted();
        if(itr == _registry.begin()) {
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
    for(auto& entry : _registry) {
        entry.second.truncate(last_version);
    }
}

void PersistentRegistry::registerPersist(const char* obj_name,
                                         const PersistentObjectFunctions& interface_functions) {
    std::size_t key = std::hash<std::string>{}(obj_name);
    auto res = this->_registry.insert(std::pair<std::size_t, PersistentObjectFunctions>(key, interface_functions));
    if(res.second == false) {
        //override the previous value:
        this->_registry.erase(res.first);
        this->_registry.insert(std::pair<std::size_t, PersistentObjectFunctions>(key, interface_functions));
    }
};

void PersistentRegistry::unregisterPersist(const char* obj_name) {
    // The upcoming regsiterPersist() call will override this automatically.
    // this->_registry.erase(std::hash<std::string>{}(obj_name));
}

void PersistentRegistry::updateTemporalFrontierProvider(ITemporalQueryFrontierProvider* tqfp) {
    this->_temporal_query_frontier_provider = tqfp;
}

const char* PersistentRegistry::get_subgroup_prefix() {
    return this->_subgroup_prefix.c_str();
}

std::string PersistentRegistry::generate_prefix(
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) {
    const char* subgroup_type_name = subgroup_type.name();

    // SHA256 subgroup_type_name to avoid a long file name
    unsigned char subgroup_type_name_digest[32];
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
