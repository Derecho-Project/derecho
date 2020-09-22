#pragma once
#ifndef PERSISTENT_HPP
#define PERSISTENT_HPP

#include "detail/FilePersistLog.hpp"
#include "HLC.hpp"
#include "PersistException.hpp"
#include "detail/PersistLog.hpp"
#include "PersistNoLog.hpp"
#include "PersistentTypenames.hpp"
#include <derecho/mutils-serialization/SerializationSupport.hpp>
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

#include <derecho/utils/logger.hpp>

#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
  #include <derecho/utils/time.h>
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
version_t combine_int32s(const int_type high_bits, const int_type low_bits);

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
std::pair<int_type, int_type> unpack_version(const version_t packed_int);

/**
   * PersistentRegistry is a book for all the Persistent<T> or Volatile<T>
   * variables. Replicated<T> class should maintain such a registry to perform
   * the following operations:
   * - makeVersion(const int64_t & ver): create a version
   * - persist(): persist the existing versions
   * - trim(const int64_t & ver): trim all versions earlier than ver
   */
class PersistentRegistry : public mutils::RemoteDeserializationContext {
public:
    /* Constructor */
    PersistentRegistry(
        ITemporalQueryFrontierProvider* tqfp,
        const std::type_index& subgroup_type,
        uint32_t subgroup_index, uint32_t shard_num);

    /* Destructor */
    virtual ~PersistentRegistry();

    /** Make a new version capturing the current state of the object. */
    void makeVersion(const int64_t& ver, const HLC& mhlc);

    /** (attempt to) Persist all existing versions
     * @return The newest version number that was actually persisted. */
    const int64_t persist();

    /** Trims the log of all versions earlier than the argument. */
    void trim(const int64_t& earliest_version);

    /** Returns the minimum of the latest persisted versions among all Persistent fields. */
    const int64_t getMinimumLatestPersistedVersion();

    /**
     * Set the earliest version for serialization, exclusive. This version will
     * be stored in a thread-local variable. When to_bytes() is next called on
     * Persistent<T>, it will serialize the logs starting after that version
     * (so the serialized logs exclude version ver).
     * @param ver The version after which to begin serializing logs
     */
    static void setEarliestVersionToSerialize(const int64_t& ver) noexcept(true);

    /** Reset the earliest version for serialization to an invalid "uninitialized" state */
    static void resetEarliestVersionToSerialize() noexcept(true);

    /** Returns the earliest version for serialization. */
    static int64_t getEarliestVersionToSerialize() noexcept(true);

    /**
     * Truncates the log, deleting all versions newer than the provided argument.
     * Since this throws away recently-used data, it should only be used during
     * failure recovery when those versions must be rolled back.
     */
    void truncate(const int64_t& last_version);

    /**
     * set the latest version for serialization
     * register a Persistent<T> along with its lambda
     */
    void registerPersist(const char* obj_name,
                         const VersionFunc& vf,
                         const PersistFunc& pf,
                         const TrimFunc& tf,
                         const LatestPersistedGetterFunc& lpgf,
                         const TruncateFunc& tcf);

    /**
     * deregister
     */
    void unregisterPersist(const char* obj_name);

    /**
     * get temporal query frontier
     */
    inline const HLC getFrontier() {
        if(_temporal_query_frontier_provider != nullptr) {
#ifndef NDEBUG
            const HLC r = _temporal_query_frontier_provider->getFrontier();
            dbg_default_warn("temporal_query_frontier=HLC({},{})", r.m_rtc_us, r.m_logic);
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

    /**
     * update temporal query frontier
     * we didn't use a lock on this becuase we assume this is only updated
     * object construction. please use this when you are sure there is no
     * concurrent threads relying on it.
     */
    void updateTemporalFrontierProvider(ITemporalQueryFrontierProvider* tqfp);

    /**
     * Enable move constructor
     */
    PersistentRegistry(PersistentRegistry&&) = default;

    /**
     * Disable copy constructor
     */
    PersistentRegistry(const PersistentRegistry&) = delete;

    /**
     * Get prefix for subgroup, this will appear in the file name of Persistent<T>
     */
    const char* get_subgroup_prefix();

    /** prefix generator
     * prefix format: [hex of subgroup_type]-[subgroup_index]-[shard_num]
     * @param subgroup_type, the type information of a subgroup
     * @param subgroup_index, the index of a subgroup
     * @param shard_num, the shard number of a subgroup
     * @return a std::string representation of the prefix
     */
    static std::string generate_prefix(const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num);

    /** match prefix
     * @param str, a string begin with a prefix like [hex64 of subgroup_type]-[subgroup_index]-[shard_num]-
     * @param subgroup_type, the type information of a subgroup
     * @param subgroup_index, the index of a subgroup
     * @param shard_num, the shard number of a subgroup
     * @return true if the prefix match the subgroup type,index, and shard_num; otherwise, false.
     */
    static bool match_prefix(const std::string str, const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num) noexcept(true);

protected:
    /**
     * this appears in the first part of storage file for persistent<T>
     */
    const std::string _subgroup_prefix;

    /**
     * Pointer to an entity providing TemporalQueryFrontier service.
     */
    ITemporalQueryFrontierProvider* _temporal_query_frontier_provider;

    /**
     * Callback registry.
     */
    std::map<std::size_t, std::tuple<VersionFunc, PersistFunc, TrimFunc, LatestPersistedGetterFunc, TruncateFunc>> _registry;

    /**
     * Helper function I
     */
    template <int funcIdx, typename... Args>
    void callFunc(Args... args);

    /**
     * Helper function II
     */
    template <int funcIdx, typename ReturnType, typename... Args>
    ReturnType callFuncMin(Args... args);

    /**
     * Set the earliest version to serialize for recovery.
     */
    static thread_local int64_t earliest_version_to_serialize;
};

// If the type T in persistent<T> is a big object and the operations are small
// updates, for example, an object store or a file system, then T would better
// implement the IDeltaSupport interface. This interface allows persistent<T>
// only store the delta in the log, avoiding huge duplicated data wasting
// storage space as well as I/O bandwidth.
//
// The idea is that T is responsible of keeping track of the updates in the form
// of a byte array - the DELTA, as long as the update should be persisted. Each
// time Persistent<T> trying to make a version, it collects the DELTA and write
// it to the log. On reloading data from persistent storage, the DELTAs in the
// log entries are applied in order. TODO: use checkpointing to accelerate it!
//
// There are three method included in this interface:
// - 'finalizeCurrentDelta'     This method is called when Persistent<T> trying to
//   make a version. Once done, the delta needs to be cleared.
// - 'applyDelta' This method is called on object construction from the disk
// - 'create' This static method is used to create an empty object from deserialization
//   manager.
using DeltaFinalizer = std::function<void(char const* const, std::size_t)>;

template <typename DeltaObjectType>
class IDeltaObjectFactory {
public:
    static std::unique_ptr<DeltaObjectType> create(mutils::DeserializationManager* dm) {
        return DeltaObjectType::create(dm);
    }
};

template <typename ObjectType>
class IDeltaSupport : public IDeltaObjectFactory<ObjectType> {
public:
    virtual void finalizeCurrentDelta(const DeltaFinalizer&) = 0;
    virtual void applyDelta(char const* const) = 0;
};


// _NameMaker is a tool makeing the name for the log corresponding to a
// given Persistent<ObjectType> object.
template <typename ObjectType, StorageType storageType>
class _NameMaker {
public:
    // Constructor
    _NameMaker();

    // Destructor
    virtual ~_NameMaker() noexcept(true);

    // guess a name
    std::unique_ptr<std::string> make(const char* prefix);

private:
    int m_iCounter;
    const char* m_sObjectTypeName;
    pthread_spinlock_t m_oLck;
};


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
    inline void initialize_log(const char* object_name);

    /** initialize the object from log
     */
    inline void initialize_object_from_log(const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
                                           mutils::DeserializationManager* dm);

    /** register the callbacks.
     */
    inline void register_callbacks();

    /** unregister the callbacks.
     */
    inline void unregister_callbacks();

public:
    /**
     * Persistent(std::unqieu_ptr<ObjectType>&,const char*,PersistentRegistry*,mutils::DeserializationManager)
     *
     * constructor 1 is for building a persistent<T> locally, load/create a
     * log and register itself to a persistent registry.
     *
     * @param object_factory        A factory to create an empty Object.
     * @param object_name           This name is used for persistent data in file.
     * @param persistent_registry   A normal pointer to the registry.
     * @param dm                    The deserialization manager for deserializing local log entries.
     */
    Persistent(
        const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
        const char* object_name = nullptr,
        PersistentRegistry* persistent_registry = nullptr,
        mutils::DeserializationManager dm = {{}});

    /**
     * Persistent(Persistent&&)
     *
     * constructor 2 is move constructor. It "steals" the resource from
     * another object.
     *
     * @param other The other object.
     */
    Persistent(Persistent&& other);

    /** 
     * Persistent(const char*,std::unqieu_ptr<ObjectType>&,const char*,
     *            PersistentRegistry*,mutils::DeserializationManager)
     *
     * constructor 3 is for deserialization. It builds a Persistent<T> from
     * the object name, a unique_ptr to the wrapped object, a unique_ptr to
     * the log.
     *
     * @param object_name       The name is used for persistent data in file.
     * @param wrapped_obj_ptr   A unique pointer to the wrapped object.
     * @param log_ptr           A unique pointer to the log.
     * @param dm                The deserialization manager for deserializing local log entries.
     */
    Persistent(
        const char* object_name,
        std::unique_ptr<ObjectType>& wrapped_obj_ptr,
        const char* log_tail = nullptr,
        PersistentRegistry* persistent_registry = nullptr,
        mutils::DeserializationManager dm = {{}});

    /**
     * Persistent(const Persistent&)
     *
     * constructor 4, the default copy constructor, is disabled
     */
    Persistent(const Persistent&) = delete;

    /**
     * ~Persistent()
     *
     * destructor: release the resources
     */
    virtual ~Persistent() noexcept(true);

    /**
     * *()
     *
     * * operator to get the memory version
     *
     * @return a reference to the current ObjectType object.
     */
    ObjectType& operator*();

    /**
     * -> ()
     *
     * overload the '->' operator to access the wrapped object
     *
     * @return a pointer to the current ObjectType object.
     */
    ObjectType* operator->();

    /**
     * getConstRef()
     *
     * get a const reference to the wrapped object
     *
     * @return a const reference to the current ObjectType object.
     */
    const ObjectType& getConstRef() const;

    /**
     * getObjectName()
     *
     * get object name
     *
     * @return a const reference to the object name.
     */
    const std::string& getObjectName();

    /**
     * getByIndex(int64_t,const Func&,mutils::DeserializationManager*)
     *
     * Get a version of Value T by log index. The user lambda will be fed with the given object of type
     * (const ObjectType&). Please note that due to zero copy design, this object may not be accessible anymore after
     * it returns.
     *
     * A note for ObjectType implementing IDeltaSupport<> interface: a history state will be reconstructed from the very
     * first log entry, making it extremely inefficient. @TODO: use cached checkpoint to accelerate it.
     *
     * @param idx   index
     * @param fun   the user function to process a const ObjectType& object
     * @param dm    the deserialization manager
     *
     * @return  Returns whatever fun returns.
     *
     * @throws PERSIST_EXP_INV_ENTRY_IDX(int64_t) if the idx is not found.
     */
     template <typename Func>
     auto getByIndex(
         int64_t idx,
         const Func& fun,
         mutils::DeserializationManager* dm = nullptr);

    /**
     * getByIndex(int64_t,mutils::DeserializationManager)
     *
     * Get a version of value T by log index. Returns a copy of the object.
     *
     * @TODO: see getByIndex(int64_t,const Func&,mutils::DeserializationManager*) for more on the performance.
     *
     * @param idx   index
     * @param dm    the deserialization manager
     *
     * @return Return a copy of the object held by a unique pointer. 
     *
     * @throws PERSIST_EXP_INV_ENTRY_IDX(int64_t), if the idx is not found.
     */
    std::unique_ptr<ObjectType> getByIndex(
            int64_t idx,
            mutils::DeserializationManager* dm = nullptr);

    /**
     * get(const version_t,const Func&,mutils::DeserializationManager*)
     *
     * Get a version of Value T by log version. The user lambda will be fed with the given object of type
     * (const ObjectType&). Please note that due to zero copy design, this object may not be accessible anymore after
     * it returns.
     *
     * @TODO: see getByIndex(int64_t,const Func&,mutils::DeserializationManager*) for more on the performance.
     *
     * @param ver   if 'ver', the specified version, matches a log entry, the state corresponding to that entry will be
     *              send to 'fun'; if 'ver' does not match a log entry, the latest state before 'ver' will be applied to
     *              'fun'; if the latest state before 'ver' is empty, it throws PERSIST_EXP_INV_VERSION.
     * @param fun   the user function to process a const ObjectType& object
     * @param dm    the deserialization manager
     *
     * @return Returns whatever fun returns.
     *
     * @throws PERSIST_EXP_INV_VERSION, when the state at 'ver' has no state.
     */
    template <typename Func>
    auto get(
        const version_t ver,
        const Func& fun,
        mutils::DeserializationManager* dm = nullptr);

    /**
     * get(const version_t,mutils::DeserializationManager*)
     *
     * Get a version of value T. specified version.
     *
     * @TODO: see getByIndex(int64_t,const Func&,mutils::DeserializationManager*) for more on the performance.
     *
     * @param ver   if 'ver', the specified version, matches a log entry, the state corresponding to that entry will be
     *              send to 'fun'; if 'ver' does not match a log entry, the latest state before 'ver' will be applied to
     *              'fun'; if the latest state before 'ver' is empty, it throws PERSIST_EXP_INV_VERSION.
     * @param dm    the deserialization manager
     *
     * @return a unique pointer to the deserialized copy of ObjectType.
     *
     * @throws PERSIST_EXP_INV_VERSION, when the state at 'ver' has no state.
     */
    std::unique_ptr<ObjectType> get(
        const version_t ver,
        mutils::DeserializationManager* dm = nullptr);

    /**
     * getDeltaByIndex(int64_t,const Func&,mutils::DeserializationManager*)
     *
     * Get a delta of ObjectType at a given log index. The user lambda will be fed with the given object of type
     * (const DeltaType&). Please note that due to zero copy design, this object may not be accessible anymore after
     * it returns.
     *
     * This function is enabled only if ObjectType implements IDeltaSupport<> interface.
     * 
     * @tparam DeltaType    User-specified DeltaType. DeltaType must be a pod type or implement mutils::ByteRepresentable.
     * @tparam Func         User-specified function type, which is usually deduced.
     *
     * @param idx   index
     * @param fun   the user function to process a const DeltaType& object
     * @param dm    the deserialization manager
     *
     * @return Returns whatever fun returns.
     *
     * @throws PERSIST_EXP_INV_INDEX, when the index 'idx' does not exists.
     */
    template <typename DeltaType, typename Func>
    std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::result_of_t<Func(const DeltaType&)>> 
    getDeltaByIndex(int64_t idx, 
                    const Func& fun, 
                    mutils::DeserializationManager* dm = nullptr);

    /**
     * getDeltaByIndex(int64_t,mutils::DeserializationManager*)
     *
     * Get a delta of ObjectType at a given log index. A copy of the delta will be returned.
     *
     * This function is enabled only if ObjectType implements IDeltaSupport<> interface.
     *
     * @tparam DeltaType    User-specified DeltaType. DeltaType must be a pod type or implement mutils::ByteRepresentable.
     *
     * @param idx   index
     * @param dm    the deserialization manager
     *
     * @return Returns a unique pointer to the copied DeltaType object.
     *
     * @throws PERSIST_EXP_INV_INDEX, when the index 'idx' does not exists.
     */
    template <typename DeltaType>
    std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::unique_ptr<DeltaType>> getDeltaByIndex(
            int64_t idx,
            mutils::DeserializationManager* dm = nullptr);

    /**
     * getDelta(const version_t,const Func&,mutils::DeserializationManager*)
     *
     * Get a delta of ObjectType at a given version. The user lambda will be fed with the given object of type
     * (const DeltaType&). Please note that due to zero copy design, this object may not be accessible anymore after
     * it returns.
     *
     * This function is enabled only if ObjectType implements IDeltaSupport<> interface.
     * 
     * @tparam DeltaType    User-specified DeltaType. DeltaType must be a pod type or implement mutils::ByteRepresentable.
     * @tparam Func         User-specified function type, which is usually deduced.
     *
     * @param ver   version
     * @param fun   the user function to process a const DeltaType& object
     * @param dm    the deserialization manager
     *
     * @return Returns whatever fun returns.
     *
     * @throws PERSIST_EXP_INV_VERSION, when version 'ver' is not found in the log.
     */
    template <typename DeltaType, typename Func>
    std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::result_of_t<Func(const DeltaType&)>>
    getDelta(const version_t ver,
             const Func& fun,
             mutils::DeserializationManager* dm = nullptr);

    /**
     * getDelta(const version_t,mutils::DeserializationManager*)
     *
     * Get a delta of ObjectType at a given version. A copy of the delta will be returned.
     *
     * This function is enabled only if ObjectType implements IDeltaSupport<> interface.
     * 
     * @tparam DeltaType    User-specified DeltaType. DeltaType must be a pod type or implement mutils::ByteRepresentable.
     *
     * @param ver   version
     * @param dm    the deserialization manager
     *
     * @return Returns a unique pointer to the copied DeltaType object.
     *
     * @throws PERSIST_EXP_INV_VERSION, when version 'ver' is not found in the log.
     */
    template <typename DeltaType>
    std::enable_if_t<std::is_base_of<IDeltaSupport<ObjectType>, ObjectType>::value, std::unique_ptr<DeltaType>>
    getDelta(const version_t ver,
             mutils::DeserializationManager* dm = nullptr);

    /**
     * trim(TKey& k)
     *
     * Trim by version or HLC clock.
     *
     * @tparam TKey TKey can be either version_t or HLC.
     *
     * @param k if k is of type version_t, all log entries inclusively before ver will be trimmed; if k is of type HLC,
     * all log entries inclusively before hlc will be trimmed.
     */
    template <typename TKey>
    void trim(const TKey& k);

    /**
     * truncate(const version_t)
     *
     * Truncate the log by version.
     *
     * @param ver: all versions strictly newer than 'ver' will be truncated.
     */
    void truncate(const version_t ver);

    /**
     * get(const HLC&,const Func&,mutils::DeserializationManager*)
     *
     * Get a version of ObjectType, specified by HLC clock. the user function will be fed with an object of type 'const
     * ObjectType&'. Due to the zero-copy design, this object might not be accessible after get() returns.
     *
     * @TODO: see getByIndex(int64_t,const Func&,mutils::DeserializationManager*) for more on the performance.
     *
     * @tparam Func         User-specified function type, which is usually deduced.
     *
     * @param hlc   the HLC timestamp
     * @param fun   the user function to process a const ObjectType& object
     * @param dm    the deserialization manager
     *
     * @return Returns whatever fun returns.
     *
     * @throws PERSIST_EXP_BEYOND_GSF if hlc is beyond the global stability frontier.
     */
    template <typename Func>
    auto get(
        const HLC& hlc,
        const Func& fun,
        mutils::DeserializationManager* dm = nullptr);

    /**
     * get(const HLC&,mutils::DeserializationManager*)
     *
     * Get a version of ObjectType, specified by HLC clock. A copy of ObjectType object will be returned.
     *
     * @TODO: see getByIndex(int64_t,const Func&,mutils::DeserializationManager*) for more on the performance.
     *
     * @param hlc   the HLC timestamp
     * @param dm    the deserialization manager
     *
     * @return a unique pointer to the copied ObjectType object.
     *
     * @throws PERSIST_EXP_BEYOND_GSF if hlc is beyond the global stability frontier.
     */
    std::unique_ptr<ObjectType> get(
            const HLC& hlc,
            mutils::DeserializationManager* dm = nullptr);

    /**
     * [](const version_t)
     *
     * syntax sugar: get a specified version of T without DSM
     *
     * @param ver   version
     *
     * @return a unique_pointer to the copied ObjectType object.
     */
    std::unique_ptr<ObjectType> operator[](const version_t ver) {
        return this->get(ver);
    }

    /**
     * [](const HLC& hlc)
     *
     * syntax sugar: get a specified version of T without DSM
     *
     * @param hlc   HLC timestamp
     *
     * @return a unique_pointer to the copied ObjectType object.
     */
    std::unique_ptr<ObjectType> operator[](const HLC& hlc) {
        return this->get(hlc);
    }

    /**
     * getNumOfVersions()
     *
     * Get the number of versions excluding trimmed/truncated ones.
     * 
     * @return the number of versions.
     */
    virtual int64_t getNumOfVersions();

    /**
     * getEarliestIndex()
     *
     * Get the earliest index excluding trimmed ones.
     *
     * @return the earliest index.
     */
    virtual int64_t getEarliestIndex();

    /**
     * getEarlisestVersion()
     *
     * Get the earliest  version excluding trimmed ones.
     *
     * @return the earliest version.
     */
    virtual version_t getEarliestVersion();

    /**
     * getLatestIndex()
     *
     * Get the latest index excluding truncated ones.
     *
     * @return the latest index.
     */
    virtual int64_t getLatestIndex();

    /**
     * getLatestVersion()
     *
     * Get the lastest version excluding truncated ones.
     *
     * @return the latest version.
     */
    virtual version_t getLatestVersion();

    /**
     * getLastPersistedVersion()
     *
     * Get the last persisted version.
     *
     * @return the last persisted version.
     */
    virtual version_t getLastPersistedVersion();

    /**
     * getIndexAtTime
     *
     * Get the latest index inclusively before time.
     */
    virtual int64_t getIndexAtTime(const HLC& hlc);

    /**
     * set(ObjectType&,const version_t,const HLC&)
     *
     * Make a version with a version number and mhlc clock
     *
     * @param v     the value to be set.
     * @param ver   the version of this value, if ver is inclusively lower than the latest version in the log, set()
     *              will throw an exception.
     * @param mhlc  the timestamp for this value, normally assigned by callbacks in PersistentRegistry.
     *
     * @throws  PERSIST_EXP_INV_VERSION when ver is inclusively lower than the latest version in the log.
     */
    virtual void set(ObjectType& v, const version_t ver, const HLC& mhlc);

    /**
     * set(ObjectType&,const version_t)
     *
     * Make a version with version 'ver' and use current clock time for this log entry.
     *
     * @param v     the value to be set.
     * @param ver   the version of this value, if ver is inclusively lower than the latest version in the log, set()
     *              will throw an exception.
     *
     * @throws  PERSIST_EXP_INV_VERSION when ver is inclusively lower than the latest version in the log.
     */
    virtual void set(ObjectType& v, const version_t ver);

    /**
     * version(const version_t)
     *
     * Make a version with a version number, using the current state as value.
     *
     * @param ver   the version of this value, if ver is inclusively lower than the latest version in the log, set()
     *              will throw an exception.
     *
     * @throws  PERSIST_EXP_INV_VERSION when ver is inclusively lower than the latest version in the log.
     */
    virtual void version(const version_t ver);

    /** 
     * persist()
     *
     * Persist as many log entries as possible.
     *
     * @return the given version has been persisted.
     */
    virtual const version_t persist();

    // wrapped objected
    std::unique_ptr<ObjectType> m_pWrappedObject;

protected:
    // PersistLog
    std::unique_ptr<PersistLog> m_pLog;
    // Persistence Registry
    PersistentRegistry* m_pRegistry;
    // get the static name maker.
    static _NameMaker<ObjectType,storageType>& getNameMaker(const std::string& prefix = std::string(""));

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
    std::size_t to_bytes(char* ret) const;
    std::size_t bytes_size() const;
    void post_object(const std::function<void(char const* const, std::size_t)>& f) const;
    // NOTE: we do not set up the registry here. This will only happen in the
    // construction of Replicated<T>
    static std::unique_ptr<Persistent> from_bytes(mutils::DeserializationManager* dsm, char const* v);
    // derived from ByteRepresentable
    virtual void ensure_registered(mutils::DeserializationManager&) {}
    // apply the serialized log tail to existing log
    // @dsm - deserialization manager
    // @v - bytes representation of the log tail)
    void applyLogTail(mutils::DeserializationManager* dsm, char const* v);

#if defined(_PERFORMANCE_DEBUG)
    uint64_t ns_in_persist = 0ul;
    uint64_t ns_in_set = 0ul;
    uint64_t cnt_in_persist = 0ul;
    uint64_t cnt_in_set = 0ul;
    virtual void print_performance_stat();
#endif  //_PERFORMANCE_DEBUG
};

template <typename ObjectType>
class Volatile : public Persistent<ObjectType, ST_MEM> {
public:
    /** constructor 1 is for building a persistent<T> locally, load/create a
     * log and register itself to a persistent registry.
     * @param object_factory factory for ObjectType
     * @param object_name This name is used for persistent data in file.
     * @param persistent_registry A normal pointer to the registry.
     * @param dm DeserializationManager for deserializing logged object.
     */
    Volatile(
            const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
            const char* object_name = nullptr,
            PersistentRegistry* persistent_registry = nullptr,
            mutils::DeserializationManager dm = {{}})
            : Persistent<ObjectType, ST_MEM>(object_factory, object_name, persistent_registry, std::move(dm)) {}

    /** constructor 2 is move constructor. It "steals" the resource from
     * another object.
     * @param other The other object.
     */
    Volatile(Volatile&& other)
            : Persistent<ObjectType, ST_MEM>(other) {}

    /** constructor 3 is for deserialization. It builds a Persistent<T> from
     * the object name, a unique_ptr to the wrapped object, and a pointer to
     * the log.
     * @param object_factory factory for ObjectType
     * @param object_name The name is used for persistent data in file.
     * @param wrapped_obj_ptr A unique pointer to the wrapped object.
     * @param log_ptr A pointer to the beginning of the log within the serialized buffer
     * @param persistent_registry A normal pointer to the registry.
     * @param dm DeserializationManager for deserializing logged object.
     */
    Volatile(
            const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
            const char* object_name,
            std::unique_ptr<ObjectType>& wrapped_obj_ptr,
            const char* log_tail,
            PersistentRegistry* persistent_registry = nullptr,
            mutils::DeserializationManager dm = {{}})
            : Persistent<ObjectType, ST_MEM>(object_factory, object_name, wrapped_obj_ptr, log_tail, persistent_registry, std::move(dm)) {}

    /** constructor 4, the default copy constructor, is disabled
     */
    Volatile(const Volatile&) = delete;

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
void saveObject(ObjectType& obj, const char* object_name = nullptr);

/**
 * loadObject() loads a serializable object from a persistent store
 * @return If there is no such object in the persistent store, just
 *         return a nullptr.
 */
template <typename ObjectType, StorageType storageType = ST_FILE>
std::unique_ptr<ObjectType> loadObject(const char* object_name = nullptr);

/// get the minmum latest persisted version for a Replicated<T>
/// identified by
/// @param subgroup_type
/// @param subgroup_index
/// @param shard_num
/// @return The minimum latest persisted version across the Replicated's Persistent<T> fields, as a version number
template <StorageType storageType = ST_FILE>
const typename std::enable_if<(storageType == ST_FILE || storageType == ST_MEM), version_t>::type getMinimumLatestPersistedVersion(const std::type_index& subgroup_type, uint32_t subgroup_index, uint32_t shard_num);

///
}  // namespace persistent

#include "detail/Persistent_impl.hpp"

#endif  //PERSISTENT_HPP
