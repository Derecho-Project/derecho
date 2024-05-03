#pragma once
#ifndef PERSIST_LOG_HPP
#define PERSIST_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error PersistLog.hpp only works with clang and gnu compilers
#endif

#include <derecho/config.h>
#include "../HLC.hpp"
#include "../PersistException.hpp"
#include "../PersistentInterface.hpp"
#include <functional>
#include <inttypes.h>
#include <map>
#include <set>
#include <stdio.h>
#include <string>

namespace persistent {

// Storage type:
enum StorageType {
    ST_FILE = 0,
    ST_MEM,
    ST_3DXP
};

struct persistent_unknown_storage_type : public persistent_exception {
    StorageType requested_type;
    persistent_unknown_storage_type(StorageType requested_type)
            : persistent_exception("Unknown storage type: " + std::to_string(requested_type)),
              requested_type(requested_type) {}
};

constexpr version_t INVALID_VERSION = -1L;
constexpr int64_t INVALID_INDEX = INT64_MAX;

// index entry for the hlc index
struct hlc_index_entry {
    HLC hlc;
    int64_t log_idx;
    //default constructor
    hlc_index_entry() : log_idx(-1) {
    }
    // constructor with hlc and index.
    hlc_index_entry(const HLC& _hlc, const int64_t& _log_idx) : hlc(_hlc), log_idx(_log_idx) {
    }
    // constructor with time stamp and index.
    hlc_index_entry(const uint64_t& r, const uint64_t& l, const int64_t& _log_idx) : hlc(r, l), log_idx(_log_idx) {
    }
    //copy constructor
    hlc_index_entry(const struct hlc_index_entry& _entry) : hlc(_entry.hlc), log_idx(_entry.log_idx) {
    }
};
// comparator for the hlc index
struct hlc_index_entry_comp {
    bool operator()(const struct hlc_index_entry& e1,
                    const struct hlc_index_entry& e2) const {
        return e1.hlc < e2.hlc;
    }
};

/**
 * Persistent log interface.
 * This class defines the interface that all persistent logs must implement, and
 * declares some common member variables such as the log's name.
 */
class PersistLog {
public:
    // LogName
    const std::string m_sName;
    /**
     * The size, in bytes, of a signature in the log. This is a constant based
     * on the configured private key. It is 0 if signatures are disabled.
     */
    const uint32_t signature_size;
    // HLCIndex
    std::set<hlc_index_entry, hlc_index_entry_comp> hidx;
#ifndef NDEBUG
    void dump_hidx();
#endif  //NDEBUG
    /**
     * Constructor.
     * Remark: A subclass's constructor should check the persistent storage to
     * see if a log with this name (using the parameter "name") is already
     * there. If it is, load it from disk. Otherwise, create the log.
     * @param name The name of the log.
     * @param enable_signatures True if this log should sign every entry, false
     * if there are no signatures.
     */
    PersistLog(const std::string& name, bool enable_signatures);
    virtual ~PersistLog() noexcept(true);
    /**
     * @fn void append(const void*, uint64_t, version_t, const HLC&)
     * @brief   append to persistent log.
     * Note that the entry appended can only become persistent till the persist()
     * is called on that entry.
     *
     * @param[in]   pdata   Serialized data to be append
     * @param[in]   size    Length of the data
     * @param[in]   ver     Version of the data, the implementation is responsible for
     *                      making sure it grows monotonically.
     * @param[in]   mhlc    The hlc clock of the data, the implementation is
     *                      responsible for making sure it grows monotonically.
     */
    virtual void append(const void* pdata,
                        uint64_t size, version_t ver,
                        const HLC& mhlc) = 0;
    /**
     * @fn  void append(const std::function<void(void*)>&,uint64_t,version_t,const HLC&)
     * @brief   append to persistent log in a zero-copy fashion.
     * Note that the entry appended can only become persistent till the persist() is
     * called on that entry.
     *
     * @param[in]   blob_generator  The lambda that generator the data.
     * @param[in]   size            The size of the data being generated.
     * @param[in]   ver             Version of the data, the implementation is responsible
     *                              for making sure it grows monotonically.
     * @param[in]   mhlc            The hlc clock of the data, the implementation is
     *                              responsible for making sure it grows monotonically.
     */
    virtual void append(const std::function<void(void*,uint64_t)>& blob_generator,
                        uint64_t size, version_t ver,
                        const HLC& mhlc) = 0;
    /**
     * Advance the version number without appending a log. This is useful
     * to create gap between versions.
     */
    virtual void advanceVersion(version_t ver) = 0;

    // Get the length of the log
    virtual int64_t getLength() = 0;

    // Get the Earliest Index
    virtual int64_t getEarliestIndex() = 0;

    // Get the Latest Index
    virtual int64_t getLatestIndex() = 0;

    /** Get the Index corresponding to a version equal or immediately eariler than ver
     *  @param ver - the version
     *  @param exact - exact match
     *  @return index of the corresponding log entry
     */
    virtual int64_t getVersionIndex(version_t ver, bool exact = false) = 0;

    // Get the Index corresponding to an HLC timestamp
    virtual int64_t getHLCIndex(const HLC& hlc) = 0;

    // Get the version corresponding to an HLC timestamp
    virtual version_t getHLCVersion(const HLC& hlc) = 0;

    // Get previous/next version
    virtual version_t getPreviousVersionOf(version_t ver) = 0;
    virtual version_t getNextVersionOf(version_t ver) = 0;

    // Get the Earlist version
    virtual version_t getEarliestVersion() = 0;

    /**
     * Get the latest version with a log entry
     */
    virtual version_t getLatestVersion() = 0;

    /**
     * Get the current in-memory version, even if it has no corresponding log
     * entry due to a call to advanceVersion().
     */
    virtual version_t getCurrentVersion() = 0;

    // return the last persisted version
    virtual version_t getLastPersistedVersion() = 0;

    // Get a version by entry number return both length and buffer
    virtual const void* getEntryByIndex(int64_t eno) = 0;

    // Get the latest version equal or earlier than ver.
    // @param ver - version requested
    // @param exact - ask for the exact version
    // @return the pointer to the data, nullptr if exact is true and no corresponding version are found.
    virtual const void* getEntry(version_t ver, bool exact = false) = 0;

    // Get the latest version - deprecated.
    // virtual const void* getEntry() = 0;
    // Get a version specified by hlc
    virtual const void* getEntry(const HLC& hlc) = 0;

    /**
     * process the entry at exactly version `ver`
     * if such a version does not exist, nothing will happen.
     * @param ver - the specified version
     * @param func - the function to run on the entry
     */
    virtual void processEntryAtVersion(version_t ver, const std::function<void(const void*, std::size_t)>& func) = 0;

    /**
     * Persist the log, either until the specified version or until the latest version
     * @param latest_version - Optional version number. If provided, persist()
     * will only persist the log up through this version. If std::nullopt,
     * persist() will persist the log up through the current version
     * @param preLocked - True if the calling function has already acquired both
     * the mutex lock and the read lock on the persistent log. Default is false,
     * which means this function will acquire the locks before accessing the log.
     * @return - the version till which has been persisted. Note that this will
     * be equal to either the requested version or the current version at the time
     * persist() was called.
     */
    virtual version_t persist(std::optional<version_t> latest_version,
                              bool preLocked = false) = 0;

    /**
     * Add a signature to a specific version; does nothing if signatures are disabled
     * @param ver - version
     * @param signature - signature
     * @param prev_signed_ver - THe previous version whose signature is
     * included in this signature
     */
    virtual void addSignature(version_t ver, const uint8_t* signature,
                              version_t prev_signed_ver) = 0;

    /**
     * Retrieve a signature from a specified version, assuming signatures are enabled.
     * @param ver - version
     * @param signature         A byte buffer into which the signature will be copied.
     * Must be at least signature_size bytes.
     * @param prev_signed_ver   A variable which will be updated to equal the previous
     * version whose signature is included in this version's signature, or
     * INVALID_VERSION if there was no version in the log with the requested
     * version number (in this case, no signature will be returned either)
     * @return true if a signature was successfully retrieved, false if there was
     * no version in the log with the requested version number or signatures are
     * disabled.
     */
    virtual bool getSignature(version_t ver, uint8_t* signature,
                              version_t& prev_signed_ver) = 0;

    /**
     * Retrieve a signature from a specific log index, assuming signatures are enabled.
     * This is more efficient than getSignature, but only works if you already know the
     * log index number, which is an implementation detail.
     * @param index - log index number
     * @param signature - A byte buffer into which the signature will be copied.
     * Must be at least signature_size bytes.
     * @param prev_ver A variable which will be updated to equal the previous
     * version whose signature is included in this version's signature, or
     * INVALID_VERSION if the index is invalid or signatures are disabled (in
     * this case, no signature will be returned either)
     * @return true if a signature was successfully retrieved, false if the index is
     * invalid or signatures are disabled.
     */
    virtual bool getSignatureByIndex(int64_t index, uint8_t* signature, version_t& prev_ver) = 0;
    /**
     * Trim the log till entry number eno, inclusively.
     * For exmaple, there is a log: [7,8,9,4,5,6]. After trim(3), it becomes [5,6]
     * @param eno -  the log number to be trimmed
     */
    virtual void trimByIndex(int64_t eno) = 0;

    /**
     * Trim the log till version, inclusively.
     * @param ver - all log entries before ver will be trimmed.
     */
    virtual void trim(version_t ver) = 0;

    /**
     * Trim the log till HLC clock, inclusively.
     * @param hlc - all log entries before hlc will be trimmed.
     */
    virtual void trim(const HLC& hlc) = 0;

    /**
     * Calculate the byte size required for serialization
     * @param ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t bytes_size(version_t ver) = 0;

    /**
     * Write the serialized log bytes to the given buffer
     * @param buf - the buffer to receive serialized bytes
     * @param ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t to_bytes(uint8_t* buf, version_t ver) = 0;

    /**
     * Post the serialized log bytes to a function
     * @param f - the function to handle the serialzied bytes
     * @param ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f,
                             version_t ver)
            = 0;

    /**
     * Check/Merge the LogTail to the existing log.
     * @param v - serialized log bytes to be apllied
     */
    virtual void applyLogTail(uint8_t const* v) = 0;

    /**
     * Truncate the log strictly newer than 'ver'.
     * @param ver - all log entry strictly after ver will be truncated.
     */
    virtual void truncate(version_t ver) = 0;
};
}  // namespace persistent

#endif  //PERSIST_LOG_HPP
