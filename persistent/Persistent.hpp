#pragma once
#ifndef PERSISTENT_HPP
#define PERSISTENT_HPP

#include "FilePersistLog.hpp"
#include "HLC.hpp"
#include "PersistentTypenames.hpp"
#include "PersistException.hpp"
#include "PersistLog.hpp"
#include "PersistNoLog.hpp"
#include "SerializationSupport.hpp"
#include <functional>
#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <sys/types.h>
#include <time.h>
#include <typeindex>

#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
#include "time.h"
#endif  //_PERFORMANCE_DEBUG

/**
 * Compilation Macros:
 * _PERFORMANCE_DEBUG
 */

namespace persistent {

// #define DEFINE_PERSIST_VAR(_t,_n) DEFINE_PERSIST_VAR(_t,_n,ST_FILE)
#define DEFINE_PERSIST_VAR(_t, _n, _s) \
    Persistent<_t, _s> _n(#_n)
#define DECLARE_PERSIST_VAR(_t, _n, _s) \
    extern DEFINE_PERSIST_VAR(_t, _n, _s)

/* deprecated
  // number of versions in the cache
  #define MAX_NUM_CACHED_VERSION        (1024)
  #define VERSION_HASH(v)               ( (int)((v) & (MAX_NUM_CACHED_VERSION-1)) )

  // invalid version:
  #define INVALID_VERSION (-1ll)

  // read from cache
  #define VERSION_IS_CACHED(v)  (this->m_aCache[VERSION_HASH(v)].ver == (v))
  #define GET_CACHED_OBJ_PTR(v) (this->m_aCache[VERSION_HASH(v)].obj)
  */

class ITemporalQueryFrontierProvider {
public:
    virtual const HLC getFrontier() = 0;
};


/**
   * Helper function for creating Persistent version numbers out of MulticastGroup
   * sequence numbers and View IDs. Packs two 32-bit integer types into an
   * unsigned 64-bit int; the template allows them to be signed or unsigned.
   * @param high_bits The integer that should become the high order bits of the
   * version number.
   * @param low_bits The integer that should become the low order bits of the
   * version number
   * @return The concatenation of the two integers as a 64-bit version number.
   */
template <typename int_type>
version_t combine_int32s(const int_type high_bits, const int_type low_bits) {
    return static_cast<version_t>((static_cast<uint64_t>(high_bits) << 32) | (0xffffffffll & low_bits));
}

/**
   * Helper function for unpacking a Persistent version number into two signed
   * or unsigned int32 values. The template parameter determines whether each
   * 32-bit half of the version number will be intepreted as a signed int or an
   * unsigned int.
   * @param packed_int The version number to unpack
   * @return A std::pair in which the first element is the high-order bits of
   * the version number, and the second element is the low-order bits of the
   * version number.
   */
template <typename int_type>
std::pair<int_type, int_type> unpack_version(const version_t packed_int) {
    return std::make_pair(static_cast<int_type>(packed_int >> 32), static_cast<int_type>(0xffffffffll & packed_int));
}

  /**
   * PersistentRegistry is a book for all the Persistent<T> or Volatile<T>
   * variables. Replicated<T> class should maintain such a registry to perform
   * the following operations:
   * - makeVersion(const int64_t & ver): create a version 
   * - persist(): persist the existing versions
   * - trim(const int64_t & ver): trim all versions earlier than ver
   */
class PersistentRegistry:public mutils::RemoteDeserializationContext{
public:
    // TODO: take the subgroup_type,shubgroup_index,shard_num
    PersistentRegistry(ITemporalQueryFrontierProvider * tqfp, const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num):
        _subgroup_prefix(generate_prefix(subgroup_type,subgroup_index,shard_num)),
        _temporal_query_frontier_provider(tqfp){
    };
    virtual ~PersistentRegistry() {
        dbg_warn("PersistentRegistry@{} has been deallocated!", (void *)this);
        this->_registry.clear();
    };
#define VERSION_FUNC_IDX (0)
#define PERSIST_FUNC_IDX (1)
#define TRIM_FUNC_IDX (2)
#define GET_ML_PERSISTED_VER (3)
#define TRUNCATE_FUNC_IDX (4)
    /** Make a new version capturing the current state of the object. */
    void makeVersion(const int64_t &ver, const HLC &mhlc) noexcept(false) {
        callFunc<VERSION_FUNC_IDX>(ver, mhlc);
    };

    /** (attempt to) Persist all existing versions
     * @return The newest version number that was actually persisted. */
    const int64_t persist() noexcept(false) {
        return callFuncMin<PERSIST_FUNC_IDX, int64_t>();
    };

    /** Trims the log of all versions earlier than the argument. */
    void trim(const int64_t & earliest_version) noexcept(false) {
        callFunc<TRIM_FUNC_IDX>(earliest_version);
    };

    /** Returns the minimum of the latest persisted versions among all Persistent fields. */
    const int64_t getMinimumLatestPersistedVersion() noexcept(false) {
        return callFuncMin<GET_ML_PERSISTED_VER, int64_t>();
    }

    /**
     * Set the earliest version for serialization, exclusive. This version will
     * be stored in a thread-local variable. When to_bytes() is next called on
     * Persistent<T>, it will serialize the logs starting after that version
     * (so the serialized logs exclude version ver).
     * @param ver The version after which to begin serializing logs
     */
    static void setEarliestVersionToSerialize(const int64_t &ver) noexcept(true) {
        PersistentRegistry::earliest_version_to_serialize = ver;
    }

    /** Reset the earliest version for serialization to an invalid "uninitialized" state */
    static void resetEarliestVersionToSerialize() noexcept(true) {
        PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;
    }

    /** Returns the earliest version for serialization. */
    static int64_t getEarliestVersionToSerialize() noexcept(true) {
        return PersistentRegistry::earliest_version_to_serialize;
    }

    /**
     * Truncates the log, deleting all versions newer than the provided argument.
     * Since this throws away recently-used data, it should only be used during
     * failure recovery when those versions must be rolled back.
     */
    void truncate(const int64_t& last_version) {
        callFunc<TRUNCATE_FUNC_IDX>(last_version);
    }

    // set the latest version for serialization
    // register a Persistent<T> along with its lambda
    void registerPersist(const char *obj_name,
                         const VersionFunc &vf,
                         const PersistFunc &pf,
                         const TrimFunc &tf,
                         const LatestPersistedGetterFunc &lpgf,
                         const TruncateFunc &tcf) noexcept(false) {
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
    // deregister
    void unregisterPersist(const char *obj_name) noexcept(false) {
        // The upcoming regsiterPersist() call will override this automatically.
        // this->_registry.erase(std::hash<std::string>{}(obj_name));
    }
    // get temporal query frontier
    inline const HLC getFrontier() {
        if(_temporal_query_frontier_provider != nullptr) {
#ifndef NDEBUG
            const HLC r = _temporal_query_frontier_provider->getFrontier();
            dbg_warn("temporal_query_frontier=HLC({},{})", r.m_rtc_us, r.m_logic);
            return r;
#else
            return _temporal_query_frontier_provider->getFrontier();
#endif  //NDEBUG
        } else {
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            return HLC((uint64_t)(t.tv_sec * 1e6 + t.tv_nsec / 1e3), (uint64_t)0);
        }
    }

    // update temporal query frontier
    // we didn't use a lock on this becuase we assume this is only updated
    // object construction. please use this when you are sure there is no
    // concurrent threads relying on it.
    void updateTemporalFrontierProvider(ITemporalQueryFrontierProvider *tqfp) {
        this->_temporal_query_frontier_provider = tqfp;
    }
    PersistentRegistry(PersistentRegistry &&) = default;
    PersistentRegistry(const PersistentRegistry &) = delete;
    // get prefix for subgroup, this will appear in the file name of Persistent<T>
    const char *get_subgroup_prefix() {
        return this->_subgroup_prefix.c_str();
    }
    /** prefix generator
     * prefix format: [hex of subgroup_type]-[subgroup_index]-[shard_num]
     * @param subgroup_type, the type information of a subgroup
     * @param subgroup_index, the index of a subgroup
     * @param shard_num, the shard number of a subgroup
     * @return a std::string representation of the prefix
     */
    static std::string generate_prefix(const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num) noexcept(true) {
        const char* subgroup_type_name = subgroup_type.name();
        char prefix[strlen(subgroup_type_name)*2+32];
        uint32_t i=0;
        for(i=0;i<strlen(subgroup_type.name());i++) {
            sprintf(prefix+2*i,"%02x",subgroup_type_name[i]);
        }
        sprintf(prefix+2*i,"-%u-%u",subgroup_index,shard_num);
        return std::string(prefix);
    }
    /** match prefix
     * @param str, a string begin with a prefix like [hex64 of subgroup_type]-[subgroup_index]-[shard_num]-
     * @param subgroup_type, the type information of a subgroup
     * @param subgroup_index, the index of a subgroup
     * @param shard_num, the shard number of a subgroup
     * @return true if the prefix match the subgroup type,index, and shard_num; otherwise, false.
     */
    static bool match_prefix(const std::string str,const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num) noexcept(true) {
        std::string prefix = generate_prefix(subgroup_type,subgroup_index,shard_num);
        try {
            if (prefix == str.substr(0,prefix.length()))
                return true;
        } catch (const std::out_of_range& ) {
            // str is shorter than prefix, just return false.
        }
        return false;
    }

protected:
    const std::string _subgroup_prefix;  // this appears in the first part of storage file for persistent<T>
    ITemporalQueryFrontierProvider *_temporal_query_frontier_provider;
    std::map<std::size_t, std::tuple<VersionFunc, PersistFunc, TrimFunc, LatestPersistedGetterFunc, TruncateFunc>> _registry;
    template <int funcIdx, typename... Args>
    void callFunc(Args... args) {
        for(auto itr = this->_registry.begin();
            itr != this->_registry.end(); ++itr) {
            std::get<funcIdx>(itr->second)(args...);
        }
    };
    template <int funcIdx, typename ReturnType, typename... Args>
    ReturnType callFuncMin(Args... args) {
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
    static thread_local int64_t earliest_version_to_serialize;
};
#define DEFINE_PERSISTENT_REGISTRY_STATIC_MEMBERS \
    thread_local int64_t PersistentRegistry::earliest_version_to_serialize = INVALID_VERSION;

// Persistent represents a variable backed up by persistent storage. The
// backend is PersistLog class. PersistLog handles only raw bytes and this
// class is repsonsible for converting it back and forth between raw bytes
// and ObjectType. But, the serialization/deserialization functionality is
// actually defined by ObjectType and provided by Persistent users.
// - ObjectType: user-defined type of the variable it is required to support
//   serialization and deserialization as follows:
//   // serialize
//   void * ObjectType::serialize(const ObjectType & obj, uint64_t *psize)
//   - obj: obj is the reference to the object to be serialized
//   - psize: psize is a uint64_t pointer to receive the size of the serialized
//     data.
//   - Return value is a pointer to a new malloced buffer with the serialized
//     //TODO: this may not be efficient for large object...open to be changed.
//   // deserialize
//   ObjectType * ObjectType::deserialize(const void *pdata)
//   - pdata: a buffer of the serialized data
//   - Return value is a pointer to a new created ObjectType deserialized from
//     'pdata' buffer.
// - StorageType: storage type is defined in PersistLog. The value could be
//   ST_FILE/ST_MEM/ST_3DXP ... I will start with ST_FILE and extend it to
//   other persistent Storage.
// TODO:comments
//TODO: Persistent<T> has to be serializable, extending from mutils::ByteRepresentable
template <typename ObjectType,
          StorageType storageType = ST_FILE>
class Persistent : public mutils::ByteRepresentable {
protected:
    /** initialize from local state.
       *  @param object_name Object name
       */
    inline void initialize_log(const char *object_name) noexcept(false) {
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
    /** initialize the object from log
       */
    inline void initialize_object_from_log() {
        if(this->getNumOfVersions() > 0) {
            // load the object from log.
            this->m_pWrappedObject = std::move(this->getByIndex(this->getLatestIndex()));
        } else {  // create a new one;
            this->m_pWrappedObject = std::make_unique<ObjectType>();
        }
    }
    /** register the callbacks.
       */
    inline void register_callbacks() noexcept(false) {
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
    inline void unregister_callbacks() noexcept(false) {
        if(this->m_pRegistry != nullptr && this->m_pLog != nullptr) {
            this->m_pRegistry->unregisterPersist(this->m_pLog->m_sName.c_str());
        }
    }

public:
    /** constructor 1 is for building a persistent<T> locally, load/create a
       * log and register itself to a persistent registry.
       * @param object_name This name is used for persistent data in file.
       * @param persistent_registry A normal pointer to the registry.
       */
    Persistent(
            const char * object_name = nullptr,
            PersistentRegistry * persistent_registry = nullptr) // TODO: get the subgroup_type,subgroup_id,shard_num to intialize Persistent<T>
            noexcept(false)
            : m_pRegistry(persistent_registry) {
        // Initialize log
        initialize_log((object_name == nullptr) ? (*Persistent::getNameMaker().make(persistent_registry ? persistent_registry->get_subgroup_prefix() : nullptr)).c_str() : object_name);
        // Initialize object
        initialize_object_from_log();
        // Register Callbacks
        register_callbacks();
    }

    /** constructor 2 is move constructor. It "steals" the resource from
       * another object.
       * @param other The other object.
       */
    Persistent(Persistent &&other) noexcept(false) {
        this->m_pWrappedObject = std::move(other.m_pWrappedObject);
        this->m_pLog = std::move(other.m_pLog);
        this->m_pRegistry = other.m_pRegistry;
        register_callbacks();  // this callback will override the previous registry entry.
    }

    /** constructor 3 is for deserialization. It builds a Persistent<T> from
       * the object name, a unique_ptr to the wrapped object, a unique_ptr to
       * the log.
       * @param object_name The name is used for persistent data in file.
       * @param wrapped_obj_ptr A unique pointer to the wrapped object.
       * @param log_ptr A unique pointer to the log.
       * @param persistent_registry A normal pointer to the registry.
       */
    /*
      Persistent(
        const char * object_name,
        std::unique_ptr<ObjectType> & wrapped_obj_ptr,
        std::unique_ptr<PersistLog> & log_ptr = nullptr,
        PersistentRegistry * persistent_registry = nullptr)
        noexcept(false)
        : m_pRegistry(persistent_registry) {
        // Initialize log
        if ( log_ptr == nullptr ) {
          initialize_log((object_name==nullptr)?
            (*Persistent::getNameMaker().make(persistent_registry?persistent_registry->get_subgroup_prefix():nullptr)).c_str() : object_name);
        } else {
          this->m_pLog = std::move(log_ptr);
        }
        // Initialize Warpped Object
        if ( wrapped_obj_ptr == nullptr ) {
          initialize_object_from_log();
        } else {
          this->m_pWrappedObject = std::move(wrapped_obj_ptr);
        }
        // Register callbacks
        register_callbacks();
      }
*/
    Persistent(
            const char *object_name,
            std::unique_ptr<ObjectType> &wrapped_obj_ptr,
            const char *log_tail = nullptr,
            PersistentRegistry *persistent_registry = nullptr) noexcept(false)
            : m_pRegistry(persistent_registry) {
        // Initialize log
        initialize_log(object_name);
        // patch it
        if(log_tail != nullptr) {
            this->m_pLog->applyLogTail(log_tail);
        }
        // Initialize Wrapped Object
        if(wrapped_obj_ptr == nullptr) {
            initialize_object_from_log();
        } else {
            this->m_pWrappedObject = std::move(wrapped_obj_ptr);
        }
        // Register callbacks
        register_callbacks();
    }

    /** constructor 4, the default copy constructor, is disabled
       */
    Persistent(const Persistent &) = delete;

    // destructor: release the resources
    virtual ~Persistent() noexcept(true) {
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

    /**
       * * operator to get the memory version
       * @return ObjectType&
       */
    ObjectType &operator*() {
        return *this->m_pWrappedObject;
    }

    /**
       * overload the '->' operator to access the wrapped object
       */
    ObjectType *operator->() {
        return this->m_pWrappedObject.get();
    }

    /**
       * get a const reference to the wrapped object
       * @return const ObjectType&
       */
    const ObjectType &getConstRef() const {
        return *this->m_pWrappedObject;
    }

    /*
       * get object name
       */
    const std::string &getObjectName() {
        return this->m_pLog->m_sName;
    }

    // get the latest Value of T. The user lambda will be fed with the latest object
    // zerocopy:this object will not live once it returns.
    // return value is decided by user lambda
    template <typename Func>
    auto get(
            const Func &fun,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        return this->getByIndex(-1L, fun, dm);
    };

    // get the latest Value of T, returns a unique pointer to the object
    std::unique_ptr<ObjectType> get(mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        return this->getByIndex(-1L, dm);
    };

    // get a version of Value T. the user lambda will be fed with the given object
    // zerocopy:this object will not live once it returns.
    // return value is decided by user lambda
    template <typename Func>
    auto getByIndex(
            int64_t idx,
            const Func &fun,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        return mutils::deserialize_and_run<ObjectType>(dm, (char *)this->m_pLog->getEntryByIndex(idx), fun);
    };

    // get a version of value T. returns a unique pointer to the object
    std::unique_ptr<ObjectType> getByIndex(
            int64_t idx,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        return mutils::from_bytes<ObjectType>(dm, (char const *)this->m_pLog->getEntryByIndex(idx));
    };

    // get a version of Value T, specified by version. the user lambda will be fed with
    // an object of T.
    // zerocopy: this object will not live once it returns.
    // return value is decided by the user lambda.
    template <typename Func>
    auto get(
            const int64_t &ver,
            const Func &fun,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        char *pdat = (char *)this->m_pLog->getEntry(ver);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_VERSION;
        }
        return mutils::deserialize_and_run<ObjectType>(dm, pdat, fun);
    };

    // get a version of value T. specified version.
    // return a deserialized copy for the variable.
    std::unique_ptr<ObjectType> get(
            const int64_t &ver,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        char const *pdat = (char const *)this->m_pLog->getEntry(ver);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_VERSION;
        }

        return mutils::from_bytes<ObjectType>(dm, pdat);
    }

    template <typename TKey>
    void trim(const TKey &k) noexcept(false) {
        dbg_trace("trim.");
        this->m_pLog->trim(k);
        dbg_trace("trim...done");
    }

    // truncate the log
    // @param ver: all versions strictly newer than 'ver' will be truncated.
    //
    void truncate(const int64_t &ver) {
        dbg_trace("truncate.");
        this->m_pLog->truncate(ver);
        dbg_trace("truncate...done");
    }

    // get a version of Value T, specified by HLC clock. the user lambda will be fed with
    // an object of T.
    // zerocopy: this object will not live once it returns.
    // return value is decided by the user lambda.
    template <typename Func>
    auto get(
            const HLC &hlc,
            const Func &fun,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        // global stability frontier test
        if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
            throw PERSIST_EXP_BEYOND_GSF;
        }
        char *pdat = (char *)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }
        return mutils::deserialize_and_run<ObjectType>(dm, pdat, fun);
    };

    // get a version of value T. specified by HLC clock.
    std::unique_ptr<ObjectType> get(
            const HLC &hlc,
            mutils::DeserializationManager *dm = nullptr) noexcept(false) {
        // global stability frontier test
        if(m_pRegistry != nullptr && m_pRegistry->getFrontier() <= hlc) {
            throw PERSIST_EXP_BEYOND_GSF;
        }
        char const *pdat = (char const *)this->m_pLog->getEntry(hlc);
        if(pdat == nullptr) {
            throw PERSIST_EXP_INV_HLC;
        }

        return mutils::from_bytes<ObjectType>(dm, pdat);
    }

    // syntax sugar: get a specified version of T without DSM
    /*
      std::unique_ptr<ObjectType> operator [](const int64_t idx)
        noexcept(false) {
        return this->getByIndex(idx);
      }
*/

    // syntax sugar: get a specified version of T without DSM
    std::unique_ptr<ObjectType> operator[](const int64_t ver) noexcept(false) {
        return this->get(ver);
    }

    // syntax sugar: get a specified version of T without DSM
    std::unique_ptr<ObjectType> operator[](const HLC &hlc) noexcept(false) {
        return this->get(hlc);
    }

    // get number of the versions
    virtual int64_t getNumOfVersions() noexcept(false) {
        return this->m_pLog->getLength();
    };

    virtual int64_t getEarliestIndex() noexcept(false) {
        return this->m_pLog->getEarliestIndex();
    }

    virtual int64_t getEarliestVersion() noexcept(false) {
        return this->m_pLog->getEarliestVersion();
    }

    virtual int64_t getLatestIndex() noexcept(false) {
        return this->m_pLog->getLatestIndex();
    }

    virtual int64_t getLatestVersion() noexcept(false) {
        return this->m_pLog->getLatestVersion();
    }

    virtual const int64_t getLastPersisted() noexcept(false) {
        return this->m_pLog->getLastPersisted();
    };

    // make a version with version and mhlc clock
    virtual void set(const ObjectType &v, const int64_t &ver, const HLC &mhlc) noexcept(false) {
        dbg_trace("append to log with ver({}),hlc({},{})", ver, mhlc.m_rtc_us, mhlc.m_logic);
        auto size = mutils::bytes_size(v);
        char *buf = new char[size];
        bzero(buf, size);
        mutils::to_bytes(v, buf);
        this->m_pLog->append((void *)buf, size, ver, mhlc);
        delete buf;
    };

    // make a version with version
    virtual void set(const ObjectType &v, const int64_t &ver) noexcept(false) {
        HLC mhlc;  // generate a default timestamp for it.
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
        struct timespec t1, t2;
        clock_gettime(CLOCK_REALTIME, &t1);
#endif
        this->set(v, ver, mhlc);

#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
        clock_gettime(CLOCK_REALTIME, &t2);
        cnt_in_set++;
        ns_in_set += ((t2.tv_sec - t1.tv_sec) * 1000000000ul + t2.tv_nsec - t1.tv_nsec);
#endif
    }

    // make a version
    virtual void version(const int64_t &ver) noexcept(false) {
        //TODO: compare if value has been changed?
        dbg_trace("In Persistent<T>: make version {}.", ver);
        this->set(*this->m_pWrappedObject, ver);
    }

    /** persist till version
       * @param ver version number
       * @return the given version to be persisted.
       */
    virtual const int64_t persist() noexcept(false) {
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
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

    // internal _NameMaker class
    class _NameMaker {
    public:
        // Constructor
        _NameMaker() noexcept(false) : m_sObjectTypeName(typeid(ObjectType).name()) {
            this->m_iCounter = 0;
            if(pthread_spin_init(&this->m_oLck, PTHREAD_PROCESS_SHARED) != 0) {
                throw PERSIST_EXP_SPIN_INIT(errno);
            }
        }

        // Destructor
        virtual ~_NameMaker() noexcept(true) {
            pthread_spin_destroy(&this->m_oLck);
        }

        // guess a name
        std::unique_ptr<std::string> make(const char *prefix) noexcept(false) {
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

    private:
        int m_iCounter;
        const char *m_sObjectTypeName;
        pthread_spinlock_t m_oLck;
    };

public:
    // wrapped objected
    std::unique_ptr<ObjectType> m_pWrappedObject;

protected:
    // PersistLog
    std::unique_ptr<PersistLog> m_pLog;
    // Persistence Registry
    PersistentRegistry *m_pRegistry;
    // get the static name maker.
    static _NameMaker &getNameMaker(const std::string & prefix = std::string(""));

    //serialization supports
public:
    ///////////////////////////////////////////////////////////////////////
    // Serialization and Deserialization of Persistent<T>
    // Serialization of the persistent<T> is packed in the following order
    // 1) the log name
    // 2) current state of the object
    // 3) number of log entries
    // 4) the log entries from the earliest to the latest
    // TODO.
    //Note: this rely on PersistentRegistry::earliest_version_to_serialize
    std::size_t to_bytes(char *ret) const {
        std::size_t sz = 0;
        // object name
        dbg_trace("{0}[{1}] object_name starts at {2}", this->m_pLog->m_sName, __func__, sz);
        sz += mutils::to_bytes(this->m_pLog->m_sName, ret + sz);
        // wrapped object
        dbg_trace("{0}[{1}] wrapped_object starts at {2}", this->m_pLog->m_sName, __func__, sz);
        sz += mutils::to_bytes(*this->m_pWrappedObject, ret + sz);
        // and the log
        dbg_trace("{0}[{1}] log starts at {2}", this->m_pLog->m_sName, __func__, sz);
        sz += this->m_pLog->to_bytes(ret + sz, PersistentRegistry::getEarliestVersionToSerialize());
        return sz;
    }
    std::size_t bytes_size() const {
        return mutils::bytes_size(this->m_pLog->m_sName) + mutils::bytes_size(*this->m_pWrappedObject) +
                this->m_pLog->bytes_size(PersistentRegistry::getEarliestVersionToSerialize());
    }
    void post_object(const std::function<void(char const *const, std::size_t)> &f)
            const {
        mutils::post_object(f, this->m_pLog->m_sName);
        mutils::post_object(f, *this->m_pWrappedObject);
        this->m_pLog->post_object(f, PersistentRegistry::getEarliestVersionToSerialize());
    }
    // NOTE: we do not set up the registry here. This will only happen in the
    // construction of Replicated<T>
    static std::unique_ptr<Persistent> from_bytes(mutils::DeserializationManager *dsm, char const *v) {
        size_t ofst = 0;
        dbg_trace("{0} object_name is loaded at {1}", __func__, ofst);
        auto obj_name = mutils::from_bytes<std::string>(dsm, v);
        ofst += mutils::bytes_size(*obj_name);

        dbg_trace("{0} wrapped_obj is loaded at {1}", __func__, ofst);
        auto wrapped_obj = mutils::from_bytes<ObjectType>(dsm, v + ofst);
        ofst += mutils::bytes_size(*wrapped_obj);

        dbg_trace("{0} log is loaded at {1}", __func__, ofst);
        PersistentRegistry *pr = nullptr;
        if(dsm != nullptr) {
            pr = &dsm->mgr<PersistentRegistry>();
        }
        dbg_trace("{0}[{1}] create object from serialized bytes.", obj_name->c_str(), __func__);
        return std::make_unique<Persistent>(obj_name->data(), wrapped_obj, v + ofst, pr);
    }
    // derived from ByteRepresentable
    virtual void ensure_registered(mutils::DeserializationManager &) {}
    // apply the serialized log tail to existing log
    // @dsm - deserialization manager
    // @v - bytes representation of the log tail)
    void applyLogTail(mutils::DeserializationManager *dsm, char const *v) {
        /*
        size_t ofst = 0;
        // STEP 1: get object_name
        auto obj_name = mutils::from_bytes<std::string>(dsm,v);
        ofst += mutils::bytes_size(*obj_name);
        if (obj_name->compare(this->m_pLog->m_sName)!=0) {
          dbg_warn("{0}: trying to merge local object {1} with tail log from {2}.", __func__, *obj_name, this->m_pLog->m_sName);
          throw PERSIST_EXP_INV_OBJNAME;
        }
        // Step 1: update the current state
        this->m_pWrappedObject = std::move(mutils::from_bytes<ObjectType>(dsm,v));
        ofst += mutils::bytes_size(*this->m_pWrappedObject);
       */
        // Step 2: apply log tail
        this->m_pLog->applyLogTail(v);
    }

#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
    uint64_t ns_in_persist = 0ul;
    uint64_t ns_in_set = 0ul;
    uint64_t cnt_in_persist = 0ul;
    uint64_t cnt_in_set = 0ul;
    virtual void print_performance_stat() {
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
};

// How many times the constructor was called.
template <typename ObjectType, StorageType storageType>
typename Persistent<ObjectType, storageType>::_NameMaker &
Persistent<ObjectType, storageType>::getNameMaker(const std::string & prefix) noexcept(false) {
    static std::map<std::string,Persistent<ObjectType,storageType>::_NameMaker> name_makers;
    // make sure prefix does exist.
    auto search = name_makers.find(prefix);
    if (search == name_makers.end()) {
        name_makers.emplace(std::make_pair(std::string(prefix),Persistent<ObjectType,storageType>::_NameMaker()));
    }

    return name_makers[prefix];
}

template <typename ObjectType>
class Volatile : public Persistent<ObjectType, ST_MEM> {
public:
    /** constructor 1 is for building a persistent<T> locally, load/create a
     * log and register itself to a persistent registry.
     * @param object_name This name is used for persistent data in file.
     * @param persistent_registry A normal pointer to the registry.
     */
    Volatile(
            const char *object_name = nullptr,
            PersistentRegistry *persistent_registry = nullptr) noexcept(false)
            : Persistent<ObjectType, ST_MEM>(object_name, persistent_registry) {}

    /** constructor 2 is move constructor. It "steals" the resource from
     * another object.
     * @param other The other object.
     */
    Volatile(Volatile &&other) noexcept(false)
            : Persistent<ObjectType, ST_MEM>(other) {}

    /** constructor 3 is for deserialization. It builds a Persistent<T> from
     * the object name, a unique_ptr to the wrapped object, a unique_ptr to
     * the log.
     * @param object_name The name is used for persistent data in file.
     * @param wrapped_obj_ptr A unique pointer to the wrapped object.
     * @param log_ptr A unique pointer to the log.
     * @param persistent_registry A normal pointer to the registry.
     */
    Volatile(
            const char *object_name,
            std::unique_ptr<ObjectType> &wrapped_obj_ptr,
            std::unique_ptr<PersistLog> &log_ptr = nullptr,
            PersistentRegistry *persistent_registry = nullptr) noexcept(false)
            : Persistent<ObjectType, ST_MEM>(object_name, wrapped_obj_ptr, log_ptr, persistent_registry) {}

    /** constructor 4, the default copy constructor, is disabled
     */
    Volatile(const Volatile &) = delete;

    // destructor:
    virtual ~Volatile() noexcept(true){
            // do nothing
    };
};

/* Utilities for manage a single "ByteRepresentable" persistent object. */
/**
   * saveObject() saves a serializable object
   * @param obj The object to be persisted.
   * @param object_name Optional object name. If not given, the object_name
   *        is <storage type>-<object type name>-nolog. NOTE: please provide
   *        an object name if you trying to persist two objects of the same
   *        type. NOTE: the object has to be ByteRepresentable.
   * @return 
   */
template <typename ObjectType, StorageType storageType = ST_FILE>
void saveObject(ObjectType &obj, const char *object_name = nullptr) noexcept(false) {
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
/**
    * loadObject() loads a serializable object from a persistent store
    * @return If there is no such object in the persistent store, just
    *         return a nullptr.
    */
template <typename ObjectType, StorageType storageType = ST_FILE>
std::unique_ptr<ObjectType> loadObject(const char *object_name = nullptr) noexcept(false) {
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

  /// get the minmum latest persisted version for a Replicated<T>
  /// identified by
  /// @param subgroup_type
  /// @param subgroup_index
  /// @param shard_num
  /// @return The minimum latest persisted version across the Replicated's Persistent<T> fields, as a version number
  const version_t getMinimumLatestPersistedVersion(const std::type_index &subgroup_type,uint32_t subgroup_index,uint32_t shard_num);

}

#endif  //PERSIST_VAR_H
