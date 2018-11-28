#include "Persistent.hpp"

namespace persistent {
DEFINE_PERSISTENT_REGISTRY_STATIC_MEMBERS;

const version_t getMinimumLatestPersistedVersion(const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num) {
    // All persistent log implementation MUST implement getMinimumLatestPersistedVersion()
    // All of them need to be checked here
    // NOTE: we assume that an application will only use ONE type of PERSISTED LOG (ST_FILE or ST_NVM, ...). Otherwise,
    // if some persistentlog returns INVALID_VERSION, it is ambiguous for the following two case:
    // 1) the subgroup/shard has some persistent<T> member storing data in corresponding persisted log but the log is empty.
    // 2) the subgroup/shard has no persistent<T> member storing data in corresponding persisted log.
    // In case we get a valid version from log stored in other storage type, we should return INVALID_VERSION for 1)
    // but return the valid version for 2).
    uint64_t mlpv = INVALID_VERSION;
    mlpv = FilePersistLog::getMinimumLatestPersistedVersion(PersistentRegistry::generate_prefix(subgroup_type, subgroup_index, shard_num));
    return mlpv;
}
}
