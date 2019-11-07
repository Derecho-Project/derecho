#ifndef PERSISTENT_IMPL_HPP
#define PERSISTENT_IMPL_HPP

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
// PersistentRegistry
//===========================================

template <int funcIdx, typename... Args>
void PersistentRegistry::callFunc(Args... args) {
    for(auto itr = this->_registry.begin();
        itr != this->_registry.end(); ++itr) {
        std::get<funcIdx>(itr->second)(args...);
    }
};

template <int funcIdx, typename ReturnType, typename... Args>
ReturnType PersistentRegistry::callFuncMin(Args... args) {
    ReturnType min_ret = -1;  // -1 means invalid value.
    for(auto itr = this->_registry.begin();
        itr != this->_registry.end(); ++itr) {
        ReturnType ret = std::get<funcIdx>(itr->second)(args...);
        if(itr == this->_registry.begin()) {
            min_ret = ret;
        } else if(min_ret > ret) {
            min_ret = ret;
        }
    }
    return min_ret;
}

//===========================================
// _NameMaker
//===========================================
template <typename ObjectType, StorageType storageType>
_NameMaker<ObjectType, storageType>::_NameMaker() noexcept(false) : m_sObjectTypeName(typeid(ObjectType).name()) {
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
std::unique_ptr<std::string> _NameMaker<ObjectType, storageType>::make(const char* prefix) noexcept(false) {
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
    char buf[256];
    sprintf(buf, "%s-%d-%s-%d", (prefix) ? prefix : "none", storageType, this->m_sObjectTypeName, cnt);
    // return std::make_shared<const char *>((const char*)buf);
    *ret = buf;
    return ret;
}

//===========================================
// Persistent
//===========================================
template <typename ObjectType,
          StorageType storageType>
inline void Persistent<ObjectType, storageType>::initialize_log(const char* object_name) noexcept(false) {
    // STEP 1: initialize log
    this->m_pLog = nullptr;
    switch(storageType) {
        // file system
        case ST_FILE:
            this->m_pLog = std::make_unique<FilePersistLog>(object_name);
            if(this->m_pLog == nullptr) {
                throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
            }
            break;
        // volatile
        case ST_MEM: {
            // const std::string tmpfsPath = "/dev/shm/volatile_t";
            this->m_pLog = std::make_unique<FilePersistLog>(object_name, getPersRamdiskPath());
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
inline void Persistent<ObjectType, storageType>::register_callbacks() noexcept(false) {
    if(this->m_pRegistry != nullptr) {
        this->m_pRegistry->registerPersist(
                this->m_pLog->m_sName.c_str(),
                std::bind(&Persistent<ObjectType, storageType>::version, this, std::placeholders::_1),
                std::bind(&Persistent<ObjectType, storageType>::persist, this),
                std::bind(&Persistent<ObjectType, storageType>::trim<const int64_t>, this, std::placeholders::_1),  //trim by version:(const int64_t)
                std::bind(&Persistent<ObjectType, storageType>::getLatestVersion, this),                            //get the latest persisted versions
                std::bind(&Persistent<ObjectType, storageType>::truncate, this, std::placeholders::_1)              // truncate persistent versions.
                );
    }
}

template <typename ObjectType,
          StorageType storageType>
inline void Persistent<ObjectType, storageType>::unregister_callbacks() noexcept(false) {
    if(this->m_pRegistry != nullptr && this->m_pLog != nullptr) {
        this->m_pRegistry->unregisterPersist(this->m_pLog->m_sName.c_str());
    }
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(
        const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
        const char* object_name,
        PersistentRegistry* persistent_registry,
        mutils::DeserializationManager dm) noexcept(false)
        : m_pRegistry(persistent_registry) {
    // Initialize log
    initialize_log((object_name == nullptr) ? (*Persistent::getNameMaker().make(persistent_registry ? persistent_registry->get_subgroup_prefix() : nullptr)).c_str() : object_name);
    // Initialize object
    initialize_object_from_log(object_factory, &dm);
    // Register Callbacks
    register_callbacks();
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(Persistent&& other) noexcept(false) {
    this->m_pWrappedObject = std::move(other.m_pWrappedObject);
    this->m_pLog = std::move(other.m_pLog);
    this->m_pRegistry = other.m_pRegistry;
    register_callbacks();  // this callback will override the previous registry entry.
}

template <typename ObjectType,
          StorageType storageType>
Persistent<ObjectType, storageType>::Persistent(
        const char* object_name,
        std::unique_ptr<ObjectType>& wrapped_obj_ptr,
        const char* log_tail,
        PersistentRegistry* persistent_registry,
        mutils::DeserializationManager) noexcept(false)
        : m_pRegistry(persistent_registry) {
    // Initialize log
    initialize_log(object_name);
    // patch it
    if(log_tail != nullptr) {
        this->m_pLog->applyLogTail(log_tail);
    }
    // Initialize Wrapped Object
    assert(wrapped_obj_ptr != nullptr);
    this->m_pWrappedObject = std::move(wrapped_obj_ptr);
    // Register callbacks
    register_callbacks();
}

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
    unregister_callbacks();
};

template <typename ObjectType,
          StorageType storageType>
ObjectType& Persistent<ObjectType, storageType>::operator*() {
    return *this->m_pWrappedObject;
}

template <typename ObjectType,
          StorageType storageType>
ObjectType* Persistent<ObjectType, storageType>::operator->() {
    return this->m_pWrappedObject.get();
}

template <typename ObjectType,
          StorageType storageType>
const ObjectType& Persistent<ObjectType, storageType>::getConstRef() const {
    return *this->m_pWrappedObject;
}

template <typename ObjectType,
          StorageType storageType>
const std::string& Persistent<ObjectType, storageType>::getObjectName() {
    return this->m_pLog->m_sName;
}

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::get(
        const Func& fun,
        mutils::DeserializationManager* dm) noexcept(false) {
    return this->getByIndex(-1L, fun, dm);
}

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::get(
        mutils::DeserializationManager* dm) noexcept(false) {
    return this->getByIndex(-1L, dm);
}

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::getByIndex(
        int64_t idx,
        const Func& fun,
        mutils::DeserializationManager* dm) noexcept(false) {
    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            return f(*this->getByIndex(idx, dm));
        }
    else {
        return mutils::deserialize_and_run<ObjectType>(dm, (char*)this->m_pLog->getEntryByIndex(idx), fun);
    }
};

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::getByIndex(
        int64_t idx,
        mutils::DeserializationManager* dm) noexcept(false) {
    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            // ObjectType* ot = new ObjectType{};
            std::unique_ptr<ObjectType> p = ObjectType::create(dm);
            // TODO: accelerate this by checkpointing
            for(int64_t i = this->m_pLog->getEarliestIndex(); i <= idx; i++) {
                const char* entry_data = (const char*)this->m_pLog->getEntryByIndex(i);
                p->applyDelta(entry_data);
            }

            return p;
        }
    else {
        return mutils::from_bytes<ObjectType>(dm, (char const*)this->m_pLog->getEntryByIndex(idx));
    }
};

template <typename ObjectType,
          StorageType storageType>
template <typename Func>
auto Persistent<ObjectType, storageType>::get(
        const int64_t& ver,
        const Func& fun,
        mutils::DeserializationManager* dm) noexcept(false) {
    char* pdat = (char*)this->m_pLog->getEntry(ver);
    if(pdat == nullptr) {
        throw PERSIST_EXP_INV_VERSION;
    }
    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            // "So far, the IDeltaSupport does not work with zero-copy 'Persistent::get()'. Emulate with the copy version."
            return f(*this->get(ver, dm));
        }
    else {
        return mutils::deserialize_and_run<ObjectType>(dm, pdat, fun);
    }
};

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::get(
        const int64_t& ver,
        mutils::DeserializationManager* dm) noexcept(false) {
    int64_t idx = this->m_pLog->getVersionIndex(ver);
    if(idx == INVALID_INDEX) {
        throw PERSIST_EXP_INV_VERSION;
    }

    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            return getByIndex(idx, dm);
        }
    else {
        return mutils::from_bytes<ObjectType>(dm, (const char*)this->m_pLog->getEntryByIndex(idx));
    }
}

template <typename ObjectType,
          StorageType storageType>
template <typename TKey>
void Persistent<ObjectType, storageType>::trim(const TKey& k) noexcept(false) {
    dbg_default_trace("trim.");
    this->m_pLog->trim(k);
    dbg_default_trace("trim...done");
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::truncate(const int64_t& ver) {
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
        mutils::DeserializationManager* dm) noexcept(false) {
    // global stability frontier test
    if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
        throw PERSIST_EXP_BEYOND_GSF;
    }

    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            int64_t idx = this->m_pLog->getHLCIndex(hlc);
            if(idx == INVALID_INDEX) {
                throw PERSIST_EXP_INV_HLC;
            }
            return getByIndex(idx, fun, dm);
        }
    else {
        char* pdat = (char*)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }
        return mutils::deserialize_and_run<ObjectType>(dm, pdat, fun);
    }
};

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<ObjectType> Persistent<ObjectType, storageType>::get(
        const HLC& hlc,
        mutils::DeserializationManager* dm) noexcept(false) {
    // global stability frontier test
    if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
        throw PERSIST_EXP_BEYOND_GSF;
    }
    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            int64_t idx = this->m_pLog->getHLCIndex(hlc);
            if(idx == INVALID_INDEX) {
                throw PERSIST_EXP_INV_HLC;
            }
            return getByIndex(idx, dm);
        }
    else {
        char const* pdat = (char const*)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }
        return mutils::from_bytes<ObjectType>(dm, pdat);
    }
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getNumOfVersions() noexcept(false) {
    return this->m_pLog->getLength();
};

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getEarliestIndex() noexcept(false) {
    return this->m_pLog->getEarliestIndex();
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getEarliestVersion() noexcept(false) {
    return this->m_pLog->getEarliestVersion();
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getLatestIndex() noexcept(false) {
    return this->m_pLog->getLatestIndex();
}

template <typename ObjectType,
          StorageType storageType>
int64_t Persistent<ObjectType, storageType>::getLatestVersion() noexcept(false) {
    return this->m_pLog->getLatestVersion();
}

template <typename ObjectType,
          StorageType storageType>
const int64_t Persistent<ObjectType, storageType>::getLastPersisted() noexcept(false) {
    return this->m_pLog->getLastPersisted();
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::set(ObjectType& v, const version_t& ver, const HLC& mhlc) noexcept(false) {
    dbg_default_trace("append to log with ver({}),hlc({},{})", ver, mhlc.m_rtc_us, mhlc.m_logic);
    if
        constexpr(std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value) {
            v.finalizeCurrentDelta([&](char const* const buf, size_t len) {
                this->m_pLog->append((const void* const)buf, len, ver, mhlc);
            });
        }
    else {
        // ObjectType does not support Delta, logging the whole current state.
        auto size = mutils::bytes_size(v);
        char* buf = new char[size];
        bzero(buf, size);
        mutils::to_bytes(v, buf);
        this->m_pLog->append((void*)buf, size, ver, mhlc);
        delete[] buf;
    }
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::set(ObjectType& v, const version_t& ver) noexcept(false) {
    HLC mhlc;  // generate a default timestamp for it.
#if defined(_PERFORMANCE_DEBUG) || defined(DERECHO_DEBUG)
    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);
#endif
    this->set(v, ver, mhlc);

#if defined(_PERFORMANCE_DEBUG) || defined(DERECHO_DEBUG)
    clock_gettime(CLOCK_REALTIME, &t2);
    cnt_in_set++;
    ns_in_set += ((t2.tv_sec - t1.tv_sec) * 1000000000ul + t2.tv_nsec - t1.tv_nsec);
#endif
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::version(const version_t& ver) noexcept(false) {
    dbg_default_trace("In Persistent<T>: make version {}.", ver);
    this->set(*this->m_pWrappedObject, ver);
}

template <typename ObjectType,
          StorageType storageType>
const int64_t Persistent<ObjectType, storageType>::persist() noexcept(false) {
#if defined(_PERFORMANCE_DEBUG) || defined(DERECHO_DEBUG)
    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);
    const int64_t ret = this->m_pLog->persist();
    clock_gettime(CLOCK_REALTIME, &t2);
    cnt_in_persist++;
    ns_in_persist += ((t2.tv_sec - t1.tv_sec) * 1000000000ul + t2.tv_nsec - t1.tv_nsec);
    return ret;
#else
    return this->m_pLog->persist();
#endif  //_PERFORMANCE_DEBUG
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::to_bytes(char* ret) const {
    std::size_t sz = 0;
    // object name
    dbg_default_trace("{0}[{1}] object_name starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += mutils::to_bytes(this->m_pLog->m_sName, ret + sz);
    // wrapped object
    dbg_default_trace("{0}[{1}] wrapped_object starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += mutils::to_bytes(*this->m_pWrappedObject, ret + sz);
    // and the log
    dbg_default_trace("{0}[{1}] log starts at {2}", this->m_pLog->m_sName, __func__, sz);
    sz += this->m_pLog->to_bytes(ret + sz, PersistentRegistry::getEarliestVersionToSerialize());
    return sz;
}

template <typename ObjectType,
          StorageType storageType>
std::size_t Persistent<ObjectType, storageType>::bytes_size() const {
    return mutils::bytes_size(this->m_pLog->m_sName) + mutils::bytes_size(*this->m_pWrappedObject) + this->m_pLog->bytes_size(PersistentRegistry::getEarliestVersionToSerialize());
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::post_object(const std::function<void(char const* const, std::size_t)>& f)
        const {
    mutils::post_object(f, this->m_pLog->m_sName);
    mutils::post_object(f, *this->m_pWrappedObject);
    this->m_pLog->post_object(f, PersistentRegistry::getEarliestVersionToSerialize());
}

template <typename ObjectType,
          StorageType storageType>
std::unique_ptr<Persistent<ObjectType, storageType>> Persistent<ObjectType, storageType>::from_bytes(mutils::DeserializationManager* dsm, char const* v) {
    size_t ofst = 0;
    dbg_default_trace("{0} object_name is loaded at {1}", __func__, ofst);
    auto obj_name = mutils::from_bytes<std::string>(dsm, v);
    ofst += mutils::bytes_size(*obj_name);

    dbg_default_trace("{0} wrapped_obj is loaded at {1}", __func__, ofst);
    auto wrapped_obj = mutils::from_bytes<ObjectType>(dsm, v + ofst);
    ofst += mutils::bytes_size(*wrapped_obj);

    dbg_default_trace("{0} log is loaded at {1}", __func__, ofst);
    PersistentRegistry* pr = nullptr;
    if(dsm != nullptr) {
        pr = &dsm->mgr<PersistentRegistry>();
    }
    dbg_default_trace("{0}[{1}] create object from serialized bytes.", obj_name->c_str(), __func__);
    return std::make_unique<Persistent>(obj_name->data(), wrapped_obj, v + ofst, pr);
}

template <typename ObjectType,
          StorageType storageType>
void Persistent<ObjectType, storageType>::applyLogTail(mutils::DeserializationManager* dsm, char const* v) {
    this->m_pLog->applyLogTail(v);
}

#if defined(_PERFORMANCE_DEBUG) || defined(DERECHO_DEBUG)
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
_NameMaker<ObjectType, storageType>& Persistent<ObjectType, storageType>::getNameMaker(const std::string& prefix) noexcept(false) {
    static std::map<std::string, _NameMaker<ObjectType, storageType>> name_makers;
    // make sure prefix does exist.
    auto search = name_makers.find(prefix);
    if(search == name_makers.end()) {
        name_makers.emplace(std::make_pair(std::string(prefix), _NameMaker<ObjectType, storageType>()));
    }

    return name_makers[prefix];
}

template <typename ObjectType, StorageType storageType>
void saveObject(ObjectType& obj, const char* object_name) noexcept(false) {
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
std::unique_ptr<ObjectType> loadObject(const char* object_name) noexcept(false) {
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
}
#endif  //PERSISTENT_IMPL_HPP
