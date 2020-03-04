#ifndef SPDK_PERSIST_LOG_HPP
#define SPDK_PERSIST_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error SPDKPersistLog.hpp only works with clang and gnu compilers
#endif

#include "PersistLog.hpp"
#include "PersistThreads.hpp"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <pthread.h>
#include <queue>
#include <thread>
#include <unordered_map>
#include <shared_mutex>

namespace persistent {

namespace spdk {

/**
 * Global Metadata on NVMe
 * +-------------------+     
 * |  log meta data 0  |
 * +-------------------+
 * |  log meta data 1  |
 * +-------------------+
 * |  log meta data 2  |
 * +-------------------+
 * |         *         |
 * |         *         |
 * |         *         |
 * +-------------------+
 * |  log meta data N  | <-- N = SPDK_NUM_LOGS_SUPPORTED
 * +-------------------+
 * |                   |
 * 
 */

// TODO hardcoded sizes for now

/** maximum number of segments in one log = (128 K - 256),     \
* the space for 256 index (or 1KB) is reserved for the other \
* metadata*/
/** #define SPDK_LOG_METADATA_SIZE \
*     ((SPDK_LOG_ADDRESS_SPACE / SPDK_NUM_LOGS_SUPPORTED) * sizeof(uint32_t))
* SPDK_LOG_METADATA_SIZE = 512 KB*/

#define NUM_DATA_PLANE 1
#define NUM_CONTROL_PLANE 1

#define METADATA m_currLogMetadata.persist_metadata_info->fields

// global metadata include segment management information. It exists both in
// NVMe and memory.
// - GlobalMetaDataPers is the data struct in NVMe.
// typedef struct global_metadata_pers {
//     LogMetadata log_metadata_entries[SPDK_NUM_LOGS_SUPPORTED];
// } GlobalMetadataPers;
// SPDK_NUM_RESERVED_SEGMENTS are the number of segments reserved for the
// global metadata at the beginning of SPDK address space.
#define SPDK_NUM_RESERVED_SEGMENTS (((sizeof(GlobalMetadataPers) - 1) / SPDK_SEGMENT_SIZE) + 1)
// - GlobalMetaData is the data structure in memory, not necessary to be
//   aligned with segments. This data needs to be constructed from the NVMe
//   contents during initialization.
// typedef struct global_metadata {
//     std::unordered_map<std::string, uint32_t> log_name_to_id;
//     LogMetadata logs[SPDK_NUM_LOGS_SUPPORTED];
//     int64_t smallest_data_address;

// } GlobalMetadata;

// Persistent log interfaces
class SPDKPersistLog : public PersistLog {
protected:
    // log metadata
    LogMetadata m_currLogMetadata;
    /**Lock on head field */
    std::shared_mutex head_mutex;
    /**Lock on tail field */
    std::shared_mutex tail_mutex;

    int64_t lower_bound(const HLC& hlc);
    
    template <typename TKey, typename KeyGetter>
	int64_t binarySearch(const KeyGetter & keyGetter, const TKey & key) {
	    int64_t begin = METADATA.head;
	    int64_t end = METADATA.tail - 1;
	    if (end < begin) {
		return (int64_t)-1L;
	    }
	    int64_t pivot = 0;
	    while (begin <= end) {
		pivot = (begin + end)/2;
		const TKey p_key = keyGetter(PersistThreads::get()->read_entry(METADATA.id, pivot));
		if (p_key == key) {
		    break;
		} else if (p_key < key) {
		    if (pivot + 1 >= METADATA.tail) {
			break;
		    } else if (keyGetter(PersistThreads::get()->read_entry(METADATA.id, pivot + 1)) > key) {
			break;
		    } else {
			begin = pivot + 1;
		    }
		} else {
		    end = pivot - 1;
		    if (begin > end) {
			return (int64_t)-1L;
		    }
		}
	    }
	    return pivot;
	} 
     int64_t getMinimumIndexBeyondVersion(const int64_t& ver) noexcept(false);

public:
    // Constructor:
    // Remark: the constructor will check the persistent storage
    // to make sure if this named log(by "name" in the template
    // parameters) is already there. If it is, load it from disk.
    // Otherwise, create the log.
    SPDKPersistLog(const std::string& name) noexcept(false);
    // Destructor
    virtual ~SPDKPersistLog() noexcept(true);
    /** Persistent Append
     * @param pdata - serialized data to be append
     * @param size - length of the data
     * @param ver - version of the data, the implementation is responsible for
	*              making sure it grows monotonically.
     * @param mhlc - the hlc clock of the data, the implementation is 
     *               responsible for making sure it grows monotonically.
     * Note that the entry appended can only become persistent till the persist()
     * is called on that entry.
     */
    virtual void append(const void* pdata,
                        const uint64_t& size, const version_t& ver,
                        const HLC& mhlc) noexcept(false);

    /**
     * Advance the version number without appendding a log. This is useful
     * to create gap between versions.
     */
    // virtual void advanceVersion(const __int128 & ver) noexcept(false) = 0;
    virtual void advanceVersion(const version_t& ver) noexcept(false);

    // Get the length of the log
    virtual int64_t getLength() noexcept(false);

    // Get the Earliest Index
    virtual int64_t getEarliestIndex() noexcept(false);

    // Get the Latest Index
    virtual int64_t getLatestIndex() noexcept(false);

    // Get the Index corresponding to a version
    virtual int64_t getVersionIndex(const version_t& ver);

    virtual int64_t getHLCIndex(const HLC& hlc) noexcept(false);

    // Get the Earlist version
    virtual version_t getEarliestVersion() noexcept(false);

    // Get the Latest version
    virtual version_t getLatestVersion() noexcept(false);

    // return the last persisted value
    virtual const version_t getLastPersisted() noexcept(false);

    // Get a version by entry number return both length and buffer
    template <typename ProcessLogEntryFunc>
    auto getEntryByIndex(const int64_t& eidx, const ProcessLogEntryFunc& process_entry) noexcept(false) {
        std::shared_lock head_rlock(head_mutex);
        std::shared_lock tail_rlock(tail_mutex);
        return process_entry(static_cast<char*>(PersistThreads::get()->read_data(METADATA.id, eidx)));
    }

    virtual void* getLBA(const uint64_t& lba_index);

    // Get the latest version equal or earlier than ver.
    template <typename ProcessLogEntryFunc>
    auto getEntry(const version_t& ver, const ProcessLogEntryFunc& process_entry) noexcept(false) {
        std::shared_lock head_rlock(head_mutex);
        std::shared_lock tail_rlock(tail_mutex);
        int64_t idx = binarySearch<int64_t>(
            [&](const LogEntry*ple){
                return ple->fields.ver;
            },
            ver);

        if (idx == -1) {
            return process_entry(nullptr);
        } else {
            return process_entry(PersistThreads::get()->read_data(METADATA.id,idx));
        }
    }

    // Get the latest version - deprecated.
    // virtual const void* getEntry() noexcept(false) = 0;
    // Get a version specified by hlc
    template <typename ProcessLogEntryFunc>
    auto getEntry(const HLC& hlc, const ProcessLogEntryFunc& process_entry) noexcept(false) {
        std::shared_lock head_rlock(head_mutex);
        std::shared_lock tail_rlock(tail_mutex);
        int64_t idx = lower_bound(hlc);
        //TODO: check if lower_bound logic is right? what would happen if hlc is older than head entry?
        return process_entry(PersistThreads::get()->read_data(METADATA.id,idx));
    }

    LogEntry getLogEntry(const int64_t& idx);
    /**
     * Persist the log till specified version
     * @return - the version till which has been persisted.
     * Note that the return value could be higher than the the version asked
     * is lower than the log that has been actually persisted.
     */
    virtual const version_t persist(const bool preLocked = false) noexcept(false);

    /**
     * Trim the log till entry number eno, inclusively.
     * For exmaple, there is a log: [7,8,9,4,5,6]. After trim(3), it becomes [5,6]
     * @param eno -  the log number to be trimmed
     */
    virtual void trimByIndex(const int64_t& idx) noexcept(false);

    /**
     * Trim the log till version, inclusively.
     * @param ver - all log entry before ver will be trimmed.
     */
    virtual void trim(const version_t& ver) noexcept(false);

    /**
     * Trim the log till HLC clock, inclusively.
     * @param hlc - all log entry before hlc will be trimmed.
     */
    virtual void trim(const HLC& hlc) noexcept(false);

    /**
     * Calculate the byte size required for serialization
     * @PARAM ver - from which version the detal begins(tail log) 
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t bytes_size(const version_t& ver);

    /**
     * Write the serialized log bytes to the given buffer
     * @PARAM buf - the buffer to receive serialized bytes
     * @PARAM ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t to_bytes(char* buf, const version_t& ver);

    /**
     * Post the serialized log bytes to a function
     * @PARAM f - the function to handle the serialzied bytes
     * @PARAM ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual void post_object(const std::function<void(char const* const, std::size_t)>& f,
                             const version_t& ver);

    /**
     * Check/Merge the LogTail to the existing log.
     * @PARAM dsm - deserialization manager
     * @PARAM v - serialized log bytes to be apllied
     */
    virtual void applyLogTail(char const* v);

    /**
     * Truncate the log strictly newer than 'ver'.
     * @param ver - all log entry strict after ver will be truncated.
     */
    virtual void truncate(const version_t& ver) noexcept(false);

    virtual void zeroout();
};

}  // namespace spdk
}  // namespace persistent
#endif  //SPDK_PERSIST_LOG_HPP
