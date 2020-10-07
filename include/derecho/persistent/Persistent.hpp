#pragma once
#ifndef PERSISTENT_HPP
#define PERSISTENT_HPP

#include "HLC.hpp"
#include "PersistException.hpp"
#include "PersistNoLog.hpp"
#include "PersistentInterface.hpp"
#include "detail/FilePersistLog.hpp"
#include "detail/PersistLog.hpp"
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

    /**
     * Initializes the PersistentRegistry's cache of the last signature in the
     * log, so that the first call to sign() can correctly continue the chain
     * of signatures. If this is called with a version of INVALID_VERSION, the
     * "last signature" will be initialized to an empty "genesis signature."
     * @param version The version that was signed by the last signature
     * @param signature The last signature in the log (in a byte buffer)
     * @param signature_size The size of the signature, in bytes
     */
    void initializeLastSignature(version_t version, const unsigned char* signature, std::size_t signature_size);

    /** Make a new version capturing the current state of the object. */
    void makeVersion(version_t ver, const HLC& mhlc);

    /**
     * Returns the minumum of the latest version across all Persistent fields.
     * This is effectively the "current version" of the object, since all the
     * Persistent fields should advance their version numbers at the same rate.
     */
    version_t getMinimumLatestVersion();

    /**
     * Adds signatures to the log up to the specified version, and returns the
     * signature for the latest version. The version specified should be the
     * result of calling getMinimumLatestVersion().
     * @param latest_version The version to add signatures up through
     * @param signer The Signer object to use for generating signatures,
     * initialized with the appropriate private key
     * @param signature_buffer A byte buffer in which the latest signature will
     * be placed after running this function
     */
    void sign(version_t latest_version, openssl::Signer& signer, unsigned char* signature_buffer);

    /**
     * Retrieves a signature from the log for a specific version of the object,
     * unless there is no version with that exact version number, in which case
     * the output buffer will be unchanged.
     * @param version The desired version
     * @param signature_buffer A byte buffer in which the signature will be placed
     * @return True if a signature was retrieved successfully, false if there
     * was no version matching the requested version number
     */
    bool getSignature(version_t version, unsigned char* signature_buffer);

    /**
     * Verifies the log up to the specified version against the specified
     * signature, using a Verifier that has been initialized with the
     * appropriate public key.
     * @param version The version to verify up to
     * @param verifier The Verifier object to use for digesting and verifying
     * the log, intialized with the public key corresponding to the signature
     * @param signature A signature over the log up to the specified version
     * @return True if the signature verifies, false if it doesn't
     */
    bool verify(version_t version, openssl::Verifier& verifier, const unsigned char* signature);

    /**
     * Persist versions up to a specified version, which should be the result of
     * calling getMinimumLatestVersion().
     * @param latest_version The version to persist up to.
     */
    void persist(version_t latest_version);

    /** Trims the log of all versions earlier than the argument. */
    void trim(version_t earliest_version);

    /** Returns the minimum of the latest persisted versions among all Persistent fields. */
    version_t getMinimumLatestPersistedVersion();

    /**
     * Set the earliest version for serialization, exclusive. This version will
     * be stored in a thread-local variable. When to_bytes() is next called on
     * Persistent<T>, it will serialize the logs starting after that version
     * (so the serialized logs exclude version ver).
     * @param ver The version after which to begin serializing logs
     */
    static void setEarliestVersionToSerialize(version_t ver) noexcept(true);

    /** Reset the earliest version for serialization to an invalid "uninitialized" state */
    static void resetEarliestVersionToSerialize() noexcept(true);

    /** Returns the earliest version for serialization. */
    static int64_t getEarliestVersionToSerialize() noexcept(true);

    /**
     * Truncates the log, deleting all versions newer than the provided argument.
     * Since this throws away recently-used data, it should only be used during
     * failure recovery when those versions must be rolled back.
     */
    void truncate(version_t last_version);

    /**
     * Add a Persistent<T> to the registry, identified by its name. Since
     * PersistentRegistry does not own the pointer to the Persistent<T>, the
     * caller must guarantee that the provided Persistent<T> outlives this
     * PersistentRegistry.
     * @param obj_name A string containing a name for the Persistent object
     * @param persistent_object A pointer to the Persistent<T> that this
     * PersistentRegistry should manage, using the type-erased base class
     * PersistentObject.
     */
    void registerPersistent(const std::string& obj_name,
                            PersistentObject* persistent_object);

    /**
     * Does nothing in the current implementation.
     */
    void unregisterPersistent(const std::string& obj_name);

    /**
     * get temporal query frontier
     */
    inline const HLC getFrontier() {
        if(m_temporalQueryFrontierProvider != nullptr) {
#ifndef NDEBUG
            const HLC r = m_temporalQueryFrontierProvider->getFrontier();
            dbg_default_warn("temporal_query_frontier=HLC({},{})", r.m_rtc_us, r.m_logic);
            return r;
#else
            return m_temporalQueryFrontierProvider->getFrontier();
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
    const char* getSubgroupPrefix();

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
    const std::string m_subgroupPrefix;

    /**
     * Pointer to an entity providing TemporalQueryFrontier service.
     */
    ITemporalQueryFrontierProvider* m_temporalQueryFrontierProvider;

    /**
     * Registry of pointers to Persistent objects, indexed by the hash of the
     * object's name
     */
    std::map<std::size_t, PersistentObject*> m_registry;

    /**
     * The last (most recent) signature to be added to a persistent log entry.
     * This is cached in memory since it is needed for the next call to sign()
     * in order to include the previous entry's signature in the next entry's
     * signed data.
     */
    std::vector<unsigned char> m_lastSignature;
    /**
     * The version number associated with the last signature to be added to a
     * persistent log entry.
     */
    version_t m_lastSignedVersion;
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
class Persistent : public PersistentObject, public mutils::ByteRepresentable {
protected:
    /** initialize from local state.
     *  @param object_name Object name
     */
    inline void initialize_log(const char* object_name, bool enable_signatures);

    /** initialize the object from log
     */
    inline void initialize_object_from_log(const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
                                           mutils::DeserializationManager* dm);

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
     * @param enable_signatures     True if each update to this Persistent<T> should be signed, false otherwise
     * @param dm                    The deserialization manager for deserializing local log entries.
     */
    Persistent(
            const std::function<std::unique_ptr<ObjectType>(void)>& object_factory,
            const char* object_name = nullptr,
            PersistentRegistry* persistent_registry = nullptr,
            bool enable_signatures = false,
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
     * @param object_name           The name is used for persistent data in file.
     * @param wrapped_obj_ptr       A unique pointer to the wrapped object.
     * @param enable_signatures     True if the received log has signatures in it, false if not
     * @param log_tail              A pointer to the beginning of the log within the serialized buffer
     * @param persistent_registry   A pointer to the persistent registry
     * @param dm                    The deserialization manager for deserializing local log entries.
     */
    Persistent(
            const char* object_name,
            std::unique_ptr<ObjectType>& wrapped_obj_ptr,
            bool enable_signatures,
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
            version_t ver,
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
     * Trim versions prior to the specified version.
     *
     * @param ver all log entries inclusively before this version will be trimmed
     */
    void trim(version_t ver);

    /**
     * Trim versions prior to the specified timestamp.
     * 
     * @param key all log entries inclusively before this HLC timestamp will be trimmed
     */
    void trim(const HLC& key);

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
    virtual int64_t getNumOfVersions() const;

    /**
     * getEarliestIndex()
     *
     * Get the earliest index excluding trimmed ones.
     *
     * @return the earliest index.
     */
    virtual int64_t getEarliestIndex() const;

    /**
     * getEarlisestVersion()
     *
     * Get the earliest  version excluding trimmed ones.
     *
     * @return the earliest version.
     */
    virtual version_t getEarliestVersion() const;

    /**
     * getLatestIndex()
     *
     * Get the latest index excluding truncated ones.
     *
     * @return the latest index.
     */
    virtual int64_t getLatestIndex() const;

    /**
     * getLatestVersion()
     *
     * Get the lastest version excluding truncated ones.
     *
     * @return the latest version.
     */
    virtual version_t getLatestVersion() const;

    /**
     * getLastPersistedVersion()
     *
     * Get the last persisted version.
     *
     * @return the last persisted version.
     */
    virtual version_t getLastPersistedVersion() const;

    /**
     * getIndexAtTime
     *
     * Get the latest index inclusively before time.
     */
    virtual int64_t getIndexAtTime(const HLC& hlc);

    /**
     * set(ObjectType&, version_t,const HLC&)
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
    virtual void set(ObjectType& v, version_t ver, const HLC& mhlc);

    /**
     * set(ObjectType&, version_t)
     *
     * Make a version with version 'ver' and use current clock time for this log entry.
     *
     * @param v     the value to be set.
     * @param ver   the version of this value, if ver is inclusively lower than the latest version in the log, set()
     *              will throw an exception.
     *
     * @throws  PERSIST_EXP_INV_VERSION when ver is inclusively lower than the latest version in the log.
     */
    virtual void set(ObjectType& v, version_t ver);

    /**
     * make a version with a version number and mhlc clock, using the current state.
     */
    virtual void version(version_t ver, const HLC& mhlc);

    /**
     * version(version_t)
     *
     * Make a version with a version number, using the current state as value.
     *
     * @param ver   the version of this value, if ver is inclusively lower than the latest version in the log, set()
     *              will throw an exception.
     *
     * @throws  PERSIST_EXP_INV_VERSION when ver is inclusively lower than the latest version in the log.
     */
    virtual void version(version_t ver);

    /** 
     * persist(version_t)
     *
     * Persist log entries up to the specified version. To avoid inefficiency, this 
     * should be the latest version.
     *
     * @param latest_version The version to persist up to
     */
    virtual void persist(version_t latest_version);

    /**
     * Update the provided Signer with the state of T at the specified version.
     * This should not finalize the Signer, since other Persistent fields in
     * the same Replicated object might need to update it too.
     * @param ver The version whose data to use in updating the Signer
     * @param signer A Signer object that has been initialized and is ready to
     * accept bytes for signing
     * @return the number of bytes added to the Signer, i.e. the size of the
     * log entry at the specified version
     */
    virtual std::size_t updateSignature(version_t ver, openssl::Signer& signer);

    /**
     * Add the provided signature to the specified version in the log. The length
     * of the signature buffer must be equal to the configured signature length for
     * this log. Also specifies the previous version whose signature has been included
     * in this signature, to make it easier to verify the chain of signatures.
     * @param ver the version to add the signature to; should be the same version
     * that was used previously in update_signature to create this signature.
     * @param signature A byte buffer containing the signature to add to the log
     * @param pervious_signed_version The previous version that this signature
     * depends on (i.e. whose signature was signed when creating this signature).
     */
    virtual void addSignature(version_t ver, const unsigned char* signature,
                              version_t previous_signed_version);

    /**
     * @return the size, in bytes, of each signature in this Persistent object's log.
     * Useful for allocating a correctly-sized buffer before calling get_signature.
     */
    virtual std::size_t getSignatureSize() const;

    /**
     * Retrieves the signature associated with the specified version and copies
     * it into the provided buffer, which must be of the correct length.
     * Note: It would be better to throw an exception to indicate that the version
     * is invalid, but Persistent doesn't have an exception hierarchy that can be
     * caught; it only throws integers, which can't be matched to catch blocks.
     * @param ver The version to get the signature for
     * @param signature A byte buffer into which the signature will be placed
     * @param prev_ver A variable which will be updated to equal the previous
     * version whose signature is included in this version's signature, or
     * INVALID_VERSION if there was no version in the log with the requested
     * version number
     * @return true if a signature was successfully retrieved, false if there was
     * no version in the log with the requested version number
     */
    virtual bool getSignature(version_t ver, unsigned char* signature, version_t& prev_ver);

    /**
     * Update the provided Verifier with the state of T at the specified version.
     * This is analogous to update_signature, only for verifying the log against
     * an existing signature.
     */
    virtual void updateVerifier(version_t ver, openssl::Verifier& verifier);

    // wrapped objected
    std::unique_ptr<ObjectType> m_pWrappedObject;

protected:
    // PersistLog
    std::unique_ptr<PersistLog> m_pLog;
    // Persistence Registry
    PersistentRegistry* m_pRegistry;
    // get the static name maker.
    static _NameMaker<ObjectType, storageType>& getNameMaker(const std::string& prefix = std::string(""));

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
            : Persistent<ObjectType, ST_MEM>(object_factory, object_name, persistent_registry, false, std::move(dm)) {}

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
            : Persistent<ObjectType, ST_MEM>(object_factory, object_name, wrapped_obj_ptr, false, log_tail, persistent_registry, std::move(dm)) {}

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
