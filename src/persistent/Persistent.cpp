#include <derecho/openssl/hash.hpp>
#include <derecho/persistent/Persistent.hpp>

namespace persistent {

thread_local int64_t PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;

PersistentRegistry::PersistentRegistry(
        ITemporalQueryFrontierProvider* tqfp,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) : _subgroup_prefix(generate_prefix(subgroup_type, subgroup_index, shard_num)),
                              _temporal_query_frontier_provider(tqfp) {
}

PersistentRegistry::~PersistentRegistry() {
    this->_registry.clear();
};

void PersistentRegistry::makeVersion(const int64_t& ver, const HLC& mhlc) noexcept(false) {
    for(auto& entry : _registry) {
        entry.second.version(ver, mhlc);
    }
};

void PersistentRegistry::sign(openssl::Signer& signer, unsigned char* signature_buffer) {
    signer.init();
    for(auto itr = _registry.begin();
        itr != _registry.end(); ++itr) {
        itr->second.sign(signer);
    }
    signer.finalize(signature_buffer);
}

const int64_t PersistentRegistry::persist(const unsigned char* signature, std::size_t signature_size) noexcept(false) {
    int64_t min = -1;
    for(auto itr = _registry.begin();
        itr != _registry.end(); ++itr) {
        int64_t ver = itr->second.persist(signature, signature_size);
        if(itr == _registry.begin()) {
            min = ver;
        } else if(min > ver) {
            min = ver;
        }
    }
    return min;
};

void PersistentRegistry::trim(const int64_t& earliest_version) noexcept(false) {
    for(auto& entry : _registry) {
        entry.second.trim(earliest_version);
    }
};

const int64_t PersistentRegistry::getMinimumLatestPersistedVersion() noexcept(false) {
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

void PersistentRegistry::setEarliestVersionToSerialize(const int64_t& ver) noexcept(true) {
    PersistentRegistry::earliest_version_to_serialize = ver;
}

void PersistentRegistry::resetEarliestVersionToSerialize() noexcept(true) {
    PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;
}

int64_t PersistentRegistry::getEarliestVersionToSerialize() noexcept(true) {
    return PersistentRegistry::earliest_version_to_serialize;
}

void PersistentRegistry::truncate(const int64_t& last_version) {
    for(auto& entry : _registry) {
        entry.second.truncate(last_version);
    }
}

void PersistentRegistry::registerPersist(const char* obj_name,
                                         const PersistentObjectFunctions& interface_functions) noexcept(false) {
    std::size_t key = std::hash<std::string>{}(obj_name);
    auto res = this->_registry.insert(std::pair<std::size_t, PersistentObjectFunctions>(key, interface_functions));
    if(res.second == false) {
        //override the previous value:
        this->_registry.erase(res.first);
        this->_registry.insert(std::pair<std::size_t, PersistentObjectFunctions>(key, interface_functions));
    }
};

void PersistentRegistry::unregisterPersist(const char* obj_name) noexcept(false) {
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
        uint32_t shard_num) noexcept(false) {
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
