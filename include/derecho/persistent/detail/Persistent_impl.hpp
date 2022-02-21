#ifndef PERSISTENT_IMPL_HPP
#define PERSISTENT_IMPL_HPP

#include <derecho/openssl/hash.hpp>

#include <utility>

namespace persistent {

template <typename int_type>
version_t combine_int32s(const int_type high_bits, const int_type low_bits) {
    return static_cast<version_t>((static_cast<uint64_t>(high_bits) << 32) | (0xffffffffll & low_bits));
}

template <typename int_type>
std::pair<int_type, int_type> unpack_version(const version_t packed_int) {
    return std::make_pair(static_cast<int_type>(packed_int >> 32), static_cast<int_type>(0xffffffffll & packed_int));
}

//===========================================
// _NameMaker
//===========================================
template <typename ObjectType, StorageType storageType>
_NameMaker<ObjectType, storageType>::_NameMaker() : m_sObjectTypeName(typeid(ObjectType).name()) {
    this->m_iCounter = 0;
    if(pthread_spin_init(&this->m_oLck, PTHREAD_PROCESS_SHARED) != 0) {
        throw PERSIST_EXP_SPIN_INIT(errno);
    }
}

template <typename ObjectType, StorageType storageType>
_NameMaker<ObjectType, storageType>::~_NameMaker() noexcept(true) {
    pthread_spin_destroy(&this->m_oLck);
}

template <typename ObjectType, StorageType storageType>
std::unique_ptr<std::string> _NameMaker<ObjectType, storageType>::make(const char* prefix) {
    int cnt;
    if(pthread_spin_lock(&this->m_oLck) != 0) {
        throw PERSIST_EXP_SPIN_LOCK(errno);
    }
    cnt = this->m_iCounter++;
    if(pthread_spin_unlock(&this->m_oLck) != 0) {
        throw PERSIST_EXP_SPIN_UNLOCK(errno);
    }
    std::unique_ptr<std::string> ret = std::make_unique<std::string>();
    //char * buf = (char *)malloc((strlen(this->m_sObjectTypeName)+13)/8*8);
    // SHA256 object type name to avoid file name length overflow.
    uint8_t digest[32];
    openssl::Hasher sha256(openssl::DigestAlgorithm::SHA256);
    try {
        sha256.hash_bytes(this->m_sObjectTypeName, strlen(this->m_sObjectTypeName), digest);
    } catch(openssl::openssl_error& ex) {
        dbg_default_error("{}:{} Unable to compute SHA256 of typename. string = {}, length = {}, OpenSSL error: {}",
                          __FILE__, __func__, this->m_sObjectTypeName, strlen(this->m_sObjectTypeName), ex.what());
        throw PERSIST_EXP_SHA256_HASH(errno);
    }

    // char prefix[strlen(this->m_sObjectTypeName) * 2 + 32];
    char buf[256];
    sprintf(buf, "%s-%d-", (prefix) ? prefix : "none", storageType);
    // fill the object type name SHA256
    char* tbuf = buf + strlen(buf);
    for(uint32_t i = 0; i < 32; i++) {
        sprintf(tbuf + 2 * i, "%02x", digest[i]);
    }
    sprintf(tbuf + 64, "-%d", cnt);
    // return std::make_shared<const char *>((const char*)buf);
    *ret = buf;
    return ret;
}

//===========================================
// Persistent
//===========================================
template <typename ObjectType,
          StorageType storageType>
inline void Persistent<ObjectType, storageType>::initialize_log(const char* object_name, bool enable_signatures) {
    // STEP 1: initialize log
    this->m_pLog = nullptr;
    switch(storageType) {
        // file system
        case ST_FILE:
            this->m_pLog = std::make_unique<FilePersistLog>(object_name, enable_signatures);
            if(this->m_pLog == nullptr) {
                throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
            }
            break;
        // volatile
        case ST_MEM: {
            // const std::string tmpfsPath = "/dev/shm/volatile_t";
            this->m_pLog = std::make_unique<FilePersistLog>(object_name, getPersRamdiskPath(), enable_signatures);
            if(this->m_pLog == nullptr) {
                throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
            }
            break;
        }
        //default
        default:
            throw PERSIST_EXP_STORAGE_TYPE_UNKNOWN(storageType);
    }
}

template <typename ObjectType,
          StorageType storageType>
inline void Persistent<ObjectType, storageType>::initialize_object_from_log(const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
                                                                            mutils::DeserializationManager* dm) {
    if(this->getNumOfVersions() > 0) {
        // load the object from log.
        this->m_pWrappedObject = this->getByIndex(this->getLatestIndex(), dm);
    } else {  // create a new one;
        this->m_pWrappedObject = object_factory();
    }
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(
        const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
        const char* object_name,
        PersistentRegistry* persistent_registry,
        bool enable_signatures,
        mutils::DeserializationManager dm)
        : m_pRegistry(persistent_registry) {
    // Initialize log
    initialize_log((object_name == nullptr)
                           ? (*Persistent::getNameMaker().make(persistent_registry ? persistent_registry->getSubgroupPrefix() : nullptr)).c_str()
                           : object_name,
                   enable_signatures);
    // Initialize object
    initialize_object_from_log(object_factory, &dm);
    if(persistent_registry) {
        // Register with PersistentRegistry
        persistent_registry->registerPersistent(this->m_pLog->m_sName, this);
        // Set up PersistentRegistry's last signed version
        version_t latest_version = getLatestVersion();
        std::unique_ptr<uint8_t[]> latest_signature;
        bool has_signature = enable_signatures;
        if(latest_version != INVALID_VERSION) {
            version_t prev_ver;  //Unused out-parameter
            latest_signature = std::make_unique<uint8_t[]>(this->m_pLog->signature_size);
            has_signature = getSignature(latest_version, latest_signature.get(), prev_ver);
        }
        // Don't do anything if signatures are disabled or getSignature() failed
        if(has_signature) {
            persistent_registry->initializeLastSignature(latest_version, latest_signature.get(), this->m_pLog->signature_size);
        }
    }
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(Persistent&& other) {
    this->m_pWrappedObject = std::move(other.m_pWrappedObject);
    this->m_pLog = std::move(other.m_pLog);
    this->m_pRegistry = other.m_pRegistry;
    if(this->m_pRegistry != nullptr) {
        // this will override the previous registry entry
        this->m_pRegistry->registerPersistent(this->m_pLog->m_sName, this);
    }
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(
        const char* object_name,
        std::unique_ptr<ObjectType>& wrapped_obj_ptr,
        bool enable_signatures,
        const uint8_t* log_tail,
        PersistentRegistry* persistent_registry,
        mutils::DeserializationManager)
        : m_pRegistry(persistent_registry) {
    // Initialize log
    initialize_log(object_name, enable_signatures);
    // patch it
    if(log_tail != nullptr) {
        this->m_pLog->applyLogTail(log_tail);
    }
    // Initialize Wrapped Object
    assert(wrapped_obj_ptr != nullptr);
    this->m_pWrappedObject = std::move(wrapped_obj_ptr);
    // Register with PersistentRegistry
    if(this->m_pRegistry) {
        this->m_pRegistry->registerPersistent(this->m_pLog->m_sName, this);
    }
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(
        PersistentRegistry* persistent_registry,
        bool enable_signatures)
        : Persistent(std::make_unique<ObjectType>,
                     nullptr,
                     persistent_registry,
                     enable_signatures,
                     {{}}) {}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::~Persistent() noexcept(true) {
    // destroy the in-memory log:
    // We don't need this anymore. m_pLog is managed by smart pointer
    // automatically.
    // if(this->m_pLog != NULL){
    //   delete this->m_pLog;
    // }
    // unregister the version creator and persist callback,
    // if the Persistent<T> is added to the pool dynamically.
    if(m_pRegistry && m_pLog) {
        m_pRegistry->unregisterPersistent(m_pLog->m_sName);
    }
};

template <typename ObjectType,
          StorageType storageType>
ObjectType& Persistent<ObjectType, storageType>::operator*() {
    return *this->m_pWrappedObject;
}

template <typename ObjectType,
          StorageType storageType>
const ObjectType& Persistent<ObjectType, storageType>::operator*() const {
    return *this->m_pWrappedObject;
}

template <typename ObjectType,
          StorageType storageType>
ObjectType* Persistent<ObjectType, storageType>::operator->() {
    return this->m_pWrappedObject.get();
}

template <typename ObjectType,
          StorageType storageType>
const ObjectType* Persistent<ObjectType, storageType>::operator->() const {
    return this->m_pWrappedObject.get();
}

template <typename ObjectType,
          StorageType storageType>
const ObjectType& Persistent<ObjectType, storageType>::getConstRef() const {
    return *this->m_pWrappedObject;
}

template <typename ObjectType,
          StorageType storageType>
const std::string& Persistent<ObjectType, storageType>::getObjectName() const {
    return this->m_pLog->m_sName;
}

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::getByIndex(
        int64_t idx,
        const Func& fun,
        mutils::DeserializationManager* dm) const {
    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        return fun(*this->getByIndex(idx, dm));
    } else {
        // return mutils::deserialize_and_run<ObjectType>(dm, (uint8_t*)this->m_pLog->getEntryByIndex(idx), fun);
        return mutils::deserialize_and_run(dm, (uint8_t*)this->m_pLog->getEntryByIndex(idx), fun);
    }
}

template <typename ObjectType,
          StorageType storageType>
template <typename DeltaType, typename Func>
std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::result_of_t<Func(const DeltaType&)>>
Persistent<ObjectType, storageType>::getDeltaByIndex(int64_t idx, const Func& fun, mutils::DeserializationManager* dm) const {
    return mutils::deserialize_and_run(dm, (uint8_t*)this->m_pLog->getEntryByIndex(idx), fun);
}

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::getByIndex(
        int64_t idx,
        mutils::DeserializationManager* dm) const {
    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        // ObjectType* ot = new ObjectType{};
        std::unique_ptr<ObjectType> p = ObjectType::create(dm);
        // TODO: accelerate this by checkpointing
        for(int64_t i = this->m_pLog->getEarliestIndex(); i <= idx; i++) {
            const uint8_t* entry_data = (const uint8_t*)this->m_pLog->getEntryByIndex(i);
            p->applyDelta(entry_data);
        }

        return p;
    } else {
        return mutils::from_bytes<ObjectType>(dm, (const uint8_t*)this->m_pLog->getEntryByIndex(idx));
    }
}

template <typename ObjectType,
          StorageType storageType>
template <typename DeltaType>
std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::unique_ptr<DeltaType>> Persistent<ObjectType, storageType>::getDeltaByIndex(
        int64_t idx,
        mutils::DeserializationManager* dm) const {
    return mutils::from_bytes<DeltaType>(dm, (uint8_t const*)this->m_pLog->getEntryByIndex(idx));
}

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::get(
        version_t ver,
        const Func& fun,
        mutils::DeserializationManager* dm) const {
    uint8_t* pdat = (uint8_t*)this->m_pLog->getEntry(ver);
    if(pdat == nullptr) {
        throw PERSIST_EXP_INV_VERSION;
    }
    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        // "So far, the IDeltaSupport does not work with zero-copy 'Persistent::get()'. Emulate with the copy version."
        return f(*this->get(ver, dm));
    } else {
        return mutils::deserialize_and_run(dm, pdat, fun);
    }
}

template <typename ObjectType,
          StorageType storageType>
template <typename DeltaType, typename Func>
std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::result_of_t<Func(const DeltaType&)>>
Persistent<ObjectType, storageType>::getDelta(const version_t ver,
                                              bool exact,
                                              const Func& fun, mutils::DeserializationManager* dm) const {
    uint8_t * pdat = (uint8_t*)this->m_pLog->getEntry(ver, exact);
    if(pdat == nullptr) {
        throw PERSIST_EXP_INV_VERSION;
    }
    return mutils::deserialize_and_run(dm, pdat, fun);
}

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::get(
        version_t ver,
        mutils::DeserializationManager* dm) const {
    int64_t idx = this->m_pLog->getVersionIndex(ver);
    if(idx == INVALID_INDEX) {
        throw PERSIST_EXP_INV_VERSION;
    }

    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        return getByIndex(idx, dm);
    } else {
        return mutils::from_bytes<ObjectType>(dm, (const uint8_t*)this->m_pLog->getEntryByIndex(idx));
    }
}

template <typename ObjectType,
          StorageType storageType>
template <typename DeltaType>
std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::unique_ptr<DeltaType>>
Persistent<ObjectType, storageType>::getDelta(
        const version_t ver,
        bool exact,
        mutils::DeserializationManager* dm) const {
    int64_t idx = this->m_pLog->getVersionIndex(ver, exact);
    if(idx == INVALID_INDEX) {
        throw PERSIST_EXP_INV_VERSION;
    }

    return mutils::from_bytes<DeltaType>(dm, (const uint8_t*)this->m_pLog->getEntryByIndex(idx));
}

template <typename ObjectType,
          StorageType storageType>
template <typename DeltaType, typename DummyObjectType>
std::enable_if_t<std::is_base_of<IDeltaSupport<DummyObjectType>, DummyObjectType>::value, bool>
Persistent<ObjectType, storageType>::getDeltaSignature(const version_t ver,
                                                       const std::function<bool(const DeltaType&)>& search_predicate,
                                                       uint8_t* signature, version_t& prev_ver,
                                                       mutils::DeserializationManager* dm) const {
    int64_t version_index = m_pLog->getVersionIndex(ver, true);
    dbg_default_trace("getDeltaSignature: Converted version {} to index {}", ver, version_index);
    if(version_index == INVALID_INDEX) {
        return false;
    }
    const uint8_t* delta_data = reinterpret_cast<const uint8_t*>(m_pLog->getEntryByIndex(version_index));
    if(mutils::deserialize_and_run(dm, delta_data, search_predicate)) {
        dbg_default_trace("getDeltaSignature: Search predicate was true, getting signature from index {}", version_index);
        return m_pLog->getSignatureByIndex(version_index, signature, prev_ver);
    } else {
        return false;
    }
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::trim(const HLC& key) {
    dbg_default_trace("trim.");
    this->m_pLog->trim(key);
    dbg_default_trace("trim...done");
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::trim(version_t ver) {
    dbg_default_trace("trim.");
    this->m_pLog->trim(ver);
    dbg_default_trace("trim...done");
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::truncate(const version_t ver) {
    dbg_default_trace("truncate.");
    this->m_pLog->truncate(ver);
    dbg_default_trace("truncate...done");
}

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::get(
        const HLC& hlc,
        const Func& fun,
        mutils::DeserializationManager* dm) const {
    // global stability frontier test
    if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
        throw PERSIST_EXP_BEYOND_GSF;
    }

    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        int64_t idx = this->m_pLog->getHLCIndex(hlc);
        if(idx == INVALID_INDEX) {
            throw PERSIST_EXP_INV_HLC;
        }
        return getByIndex(idx, fun, dm);
    } else {
        uint8_t* pdat = (uint8_t*)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }
        return mutils::deserialize_and_run(dm, pdat, fun);
    }
};

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::get(
        const HLC& hlc,
        mutils::DeserializationManager* dm) const {
    // global stability frontier test
    if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
        throw PERSIST_EXP_BEYOND_GSF;
    }
    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        int64_t idx = this->m_pLog->getHLCIndex(hlc);
        if(idx == INVALID_INDEX) {
            throw PERSIST_EXP_INV_HLC;
        }
        return getByIndex(idx, dm);
    } else {
        uint8_t const* pdat = (uint8_t const*)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }
        return mutils::from_bytes<ObjectType>(dm, pdat);
    }
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getNumOfVersions() const {
    return this->m_pLog->getLength();
};

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getEarliestIndex() const {
    return this->m_pLog->getEarliestIndex();
}

template <typename ObjectType,
          StorageType storageType>
version_t Persistent<ObjectType, storageType>::getEarliestVersion() const {
    return this->m_pLog->getEarliestVersion();
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getLatestIndex() const {
    return this->m_pLog->getLatestIndex();
}

template <typename ObjectType,
          StorageType storageType>
version_t Persistent<ObjectType, storageType>::getLatestVersion() const {
    return this->m_pLog->getLatestVersion();
}

template <typename ObjectType,
          StorageType storageType>
version_t Persistent<ObjectType, storageType>::getLastPersistedVersion() const {
    return this->m_pLog->getLastPersistedVersion();
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getIndexAtTime(const HLC& hlc) const {
    return this->m_pLog->getHLCIndex(hlc);
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::set(ObjectType& v, version_t ver, const HLC& mhlc) {
    dbg_default_trace("append to log with ver({}),hlc({},{})", ver, mhlc.m_rtc_us, mhlc.m_logic);
    if constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
        v.finalizeCurrentDelta([&](uint8_t const* const buf, size_t len) {
            // will not create a log for versions without data change.
            if (len > 0) {
                this->m_pLog->append((const void* const)buf, len, ver, mhlc);
            }
        });
    } else {
        // ObjectType does not support Delta, logging the whole current state.
        auto size = mutils::bytes_size(v);
        uint8_t* buf = new uint8_t[size];
        bzero(buf, size);
        mutils::to_bytes(v, buf);
        this->m_pLog->append((void*)buf, size, ver, mhlc);
        delete[] buf;
    }
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::version(version_t ver, const HLC& mhlc) {
    dbg_default_trace("In Persistent<T>: make version (ver={}, hlc={}us.{})", ver, mhlc.m_rtc_us, mhlc.m_logic);
    this->set(*this->m_pWrappedObject, ver, mhlc);
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::set(ObjectType& v, version_t ver) {
    HLC mhlc;  // generate a default timestamp for it.
#if defined(_PERFORMANCE_DEBUG)
    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);
#endif
    this->set(v, ver, mhlc);

#if defined(_PERFORMANCE_DEBUG)
    clock_gettime(CLOCK_REALTIME, &t2);
    cnt_in_set++;
    ns_in_set += ((t2.tv_sec - t1.tv_sec) * 1000000000ul + t2.tv_nsec - t1.tv_nsec);
#endif
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::version(const version_t ver) {
    dbg_default_trace("In Persistent<T>: make version {}.", ver);
    this->set(*this->m_pWrappedObject, ver);
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::updateSignature(version_t ver, openssl::Signer& signer) {
    if(this->m_pLog->signature_size == 0) {
        return 0;
    }
    std::size_t bytes_added = 0;
    this->m_pLog->processEntryAtVersion(ver, [&signer, &bytes_added](const void* data, std::size_t size) {
        if(size > 0) {
            signer.add_bytes(data, size);
        }
        bytes_added = size;
    });
    return bytes_added;
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::addSignature(version_t ver, const uint8_t* signature, version_t prev_signed_ver) {
    this->m_pLog->addSignature(ver, signature, prev_signed_ver);
}

template <typename ObjectType,
          StorageType storageType>
bool Persistent<ObjectType, storageType>::getSignature(version_t ver, uint8_t* signature, version_t& prev_signed_ver) const {
    return this->m_pLog->getSignature(ver, signature, prev_signed_ver);
}

template <typename ObjectType,
          StorageType storageType>
bool Persistent<ObjectType, storageType>::getSignatureByIndex(int64_t index, uint8_t* signature, version_t& prev_signed_ver) const {
    return this->m_pLog->getSignatureByIndex(index, signature, prev_signed_ver);
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::getSignatureSize() const {
    return this->m_pLog->signature_size;
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::updateVerifier(version_t ver, openssl::Verifier& verifier) {
    if(this->m_pLog->signature_size == 0) {
        return;
    }
    this->m_pLog->processEntryAtVersion(ver, [&verifier](const void* data, std::size_t size) {
        if(size > 0) {
            verifier.add_bytes(data, size);
        }
    });
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::persist(version_t ver) {
#if defined(_PERFORMANCE_DEBUG)
    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);
    this->m_pLog->persist(ver);
    clock_gettime(CLOCK_REALTIME, &t2);
    cnt_in_persist++;
    ns_in_persist += ((t2.tv_sec - t1.tv_sec) * 1000000000ul + t2.tv_nsec - t1.tv_nsec);
#else
    this->m_pLog->persist(ver);
#endif  //_PERFORMANCE_DEBUG
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::to_bytes(uint8_t* ret) const {
    std::size_t sz = 0;
    // object name
    dbg_default_trace("{0}[{1}] object_name starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += mutils::to_bytes(this->m_pLog->m_sName, ret + sz);
    // wrapped object
    dbg_default_trace("{0}[{1}] wrapped_object starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += mutils::to_bytes(*this->m_pWrappedObject, ret + sz);
    // flag to indicate whether the log has signatures
    dbg_default_trace("{0}[{1}] signatures_enabled starts at {2}", this->m_pLog->m_sName, __func__, sz);
    const bool signatures_enabled = this->m_pLog->signature_size > 0;
    sz += mutils::to_bytes(signatures_enabled, ret + sz);
    // and the log
    dbg_default_trace("{0}[{1}] log starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += this->m_pLog->to_bytes(ret + sz, PersistentRegistry::getEarliestVersionToSerialize());
    return sz;
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::bytes_size() const {
    // object name, wrapped object, signature flag, and log
    return mutils::bytes_size(this->m_pLog->m_sName)
           + mutils::bytes_size(*this->m_pWrappedObject)
           + sizeof(bool)
           + this->m_pLog->bytes_size(PersistentRegistry::getEarliestVersionToSerialize());
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f)
        const {
    // object name
    mutils::post_object(f, this->m_pLog->m_sName);
    // wrapped object
    mutils::post_object(f, *this->m_pWrappedObject);
    // flag to indicate whether the log has signatures
    mutils::post_object(f, (this->m_pLog->signature_size > 0));
    // and the log
    this->m_pLog->post_object(f, PersistentRegistry::getEarliestVersionToSerialize());
}

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<Persistent<ObjectType, storageType>> Persistent<ObjectType, storageType>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* v) {
    size_t ofst = 0;
    dbg_default_trace("{0} object_name is loaded at {1}", __func__, ofst);
    auto obj_name = mutils::from_bytes<std::string>(dsm, v);
    ofst += mutils::bytes_size(*obj_name);

    dbg_default_trace("{0} wrapped_obj is loaded at {1}", __func__, ofst);
    auto wrapped_obj = mutils::from_bytes<ObjectType>(dsm, v + ofst);
    ofst += mutils::bytes_size(*wrapped_obj);

    dbg_default_trace("{0} signatures_enabled is loaded at {1}", __func__, ofst);
    bool signatures_enabled = *mutils::from_bytes_noalloc<bool>(dsm, v + ofst);
    ofst += mutils::bytes_size(signatures_enabled);
    dbg_default_trace("{0} log is loaded at {1}", __func__, ofst);
    PersistentRegistry* pr = nullptr;
    if(dsm != nullptr) {
        pr = &dsm->mgr<PersistentRegistry>();
    }
    dbg_default_trace("{0}[{1}] create object from serialized bytes.", obj_name->c_str(), __func__);
    return std::make_unique<Persistent>(obj_name->data(), wrapped_obj, signatures_enabled, v + ofst, pr);
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::applyLogTail(mutils::DeserializationManager* dsm, uint8_t const* v) {
    this->m_pLog->applyLogTail(v);
}

#if defined(_PERFORMANCE_DEBUG)
template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::print_performance_stat() {
    std::cout << "Performance Statistic of Persistent<"
              << typeid(ObjectType).name() << ">,var_name="
              << this->m_pLog->m_sName << ":" << std::endl;
    std::cout << "\tpersist:\t" << ns_in_persist << " ns/"
              << cnt_in_persist << " ops" << std::endl;
    ;
    std::cout << "\tset:\t" << ns_in_set << " ns/"
              << cnt_in_set << " ops" << std::endl;
    ;
}
#endif  //_PERFORMANCE_DEBUG

template <typename ObjectType, StorageType storageType>
_NameMaker<ObjectType, storageType>& Persistent<ObjectType, storageType>::getNameMaker(const std::string& prefix) {
    static std::map<std::string, _NameMaker<ObjectType, storageType>> name_makers;
    // make sure prefix does exist.
    auto search = name_makers.find(prefix);
    if(search == name_makers.end()) {
        name_makers.emplace(std::make_pair(std::string(prefix), _NameMaker<ObjectType, storageType>()));
    }

    return name_makers[prefix];
}

template <typename ObjectType, StorageType storageType>
void saveObject(ObjectType& obj, const char* object_name) {
    switch(storageType) {
        // file system
        case ST_FILE: {
            saveNoLogObjectInFile(obj, object_name);
            break;
        }
        // volatile
        case ST_MEM: {
            saveNoLogObjectInMem(obj, object_name);
            break;
        }
        default:
            throw PERSIST_EXP_STORAGE_TYPE_UNKNOWN(storageType);
    }
}

template <typename ObjectType, StorageType storageType>
std::unique_ptr<ObjectType> loadObject(const char* object_name) {
    switch(storageType) {
        // file system
        case ST_FILE:
            return loadNoLogObjectFromFile<ObjectType>(object_name);
        // volatile
        case ST_MEM:
            return loadNoLogObjectFromMem<ObjectType>(object_name);
        default:
            throw PERSIST_EXP_STORAGE_TYPE_UNKNOWN(storageType);
    }
}

template <StorageType storageType>
const typename std::enable_if<(storageType == ST_FILE || storageType == ST_MEM), version_t>::type getMinimumLatestPersistedVersion(const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num) {
    // All persistent log implementation MUST implement getMinimumLatestPersistedVersion()
    // All of them need to be checked here
    // NOTE: we assume that an application will only use ONE type of PERSISTED LOG (ST_FILE or ST_NVM, ...). Otherwise,
    // if some persistentlog returns INVALID_VERSION, it is ambiguous for the following two case:
    // 1) the subgroup/shard has some persistent<T> member storing data in corresponding persisted log but the log is empty.
    // 2) the subgroup/shard has no persistent<T> member storing data in corresponding persisted log.
    // In case we get a valid version from log stored in other storage type, we should return INVALID_VERSION for 1)
    // but return the valid version for 2).
    version_t mlpv = INVALID_VERSION;
    mlpv = FilePersistLog::getMinimumLatestPersistedVersion(PersistentRegistry::generate_prefix(subgroup_type, subgroup_index, shard_num));
    return mlpv;
}
}  // namespace persistent
#endif  //PERSISTENT_IMPL_HPP
