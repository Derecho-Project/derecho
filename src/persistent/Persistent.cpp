#include <derecho/persistent/Persistent.hpp>
#include <openssl/sha.h>

namespace persistent {

    thread_local int64_t PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;

#define VERSION_FUNC_IDX (0)
#define PERSIST_FUNC_IDX (1)
#define TRIM_FUNC_IDX (2)
#define GET_ML_PERSISTED_VER (3)
#define TRUNCATE_FUNC_IDX (4)

    PersistentRegistry::PersistentRegistry(
        ITemporalQueryFrontierProvider* tqfp,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) : 
        _subgroup_prefix(generate_prefix(subgroup_type, subgroup_index, shard_num)),
        _temporal_query_frontier_provider(tqfp) {
    }

    PersistentRegistry::~PersistentRegistry() {
        this->_registry.clear();
    };

    void PersistentRegistry::makeVersion(const int64_t& ver, const HLC& mhlc) noexcept(false) {
        callFunc<VERSION_FUNC_IDX>(ver, mhlc);
    };

    const int64_t PersistentRegistry::persist() noexcept(false) {
        return callFuncMin<PERSIST_FUNC_IDX, int64_t>();
    };

    void PersistentRegistry::trim(const int64_t& earliest_version) noexcept(false) {
        callFunc<TRIM_FUNC_IDX>(earliest_version);
    };

    const int64_t PersistentRegistry::getMinimumLatestPersistedVersion() noexcept(false) {
        return callFuncMin<GET_ML_PERSISTED_VER, int64_t>();
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
        callFunc<TRUNCATE_FUNC_IDX>(last_version);
    }

    void PersistentRegistry::registerPersist(const char* obj_name,
                         const VersionFunc& vf,
                         const PersistFunc& pf,
                         const TrimFunc& tf,
                         const LatestPersistedGetterFunc& lpgf,
                         const TruncateFunc& tcf) noexcept(false) {
        //this->_registry.push_back(std::make_tuple(vf,pf,tf));
        auto tuple_val = std::make_tuple(vf, pf, tf, lpgf, tcf);
        std::size_t key = std::hash<std::string>{}(obj_name);
        auto res = this->_registry.insert(std::pair<std::size_t, std::tuple<VersionFunc, PersistFunc, TrimFunc, LatestPersistedGetterFunc, TruncateFunc>>(key, tuple_val));
        if(res.second == false) {
            //override the previous value:
            this->_registry.erase(res.first);
            this->_registry.insert(std::pair<std::size_t, std::tuple<VersionFunc, PersistFunc, TrimFunc, LatestPersistedGetterFunc, TruncateFunc>>(key, tuple_val));
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

    std::string PersistentRegistry::generate_prefix (
        const std::type_index& subgroup_type,
        uint32_t subgroup_index,
        uint32_t shard_num) noexcept(false){
        const char* subgroup_type_name = subgroup_type.name();

        // SHA256 subgroup_type_name to avoid a long file name
        unsigned char subgroup_type_name_digest[32];
        SHA256_CTX ctxt;
        if (!SHA256_Init(&ctxt)) {
            dbg_default_error("{}:{} Unable to initialize SHA256 context. errno = {}", __FILE__, __func__, errno);
            throw PERSIST_EXP_SHA256_HASH(errno);
        }
        if (!SHA256_Update(&ctxt,subgroup_type_name,strlen(subgroup_type_name))) {
            dbg_default_error("{}:{} Unable to update SHA256 context. string = {}, length = {}, errno = {}",
                __FILE__, __func__, subgroup_type_name, strlen(subgroup_type_name), errno);
            throw PERSIST_EXP_SHA256_HASH(errno);
        }
        if (!SHA256_Final(subgroup_type_name_digest,&ctxt)) {
            dbg_default_error("{}:{} Unable to final SHA256 context. errno = {}", __FILE__, __func__, errno);
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

}
