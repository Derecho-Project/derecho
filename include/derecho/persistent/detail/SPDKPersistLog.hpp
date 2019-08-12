#ifndef SPDK_PERSIST_LOG_HPP
#define SPDK_PERSIST_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error SPDKPersistLog.hpp only works with clang and gnu compilers
#endif

#include "PersistLog.hpp"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include <bitset>
#include <pthread.h>
#include <queue>
#include <thread>
#include <unordered_map>

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
#define SPDK_NUM_LOGS_SUPPORTED (1UL << 10)  // support 1024 logs
#define SPDK_SEGMENT_BIT 26
#define SPDK_SEGMENT_SIZE (1ULL << 26)  // segment size is 64 MB
#define SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH (1ULL << 11)
#define SPDK_DATA_ADDRESS_TABLE_LENGTH (3 * 1ULL << 12)
#define SPDK_LOG_ADDRESS_SPACE (1ULL << (SPDK_SEGMENT_BIT + 11))  // address space per log is 1TB
#define SPDK_NUM_SEGMENTS \
    ((SPDK_LOG_ADDRESS_SPACE / SPDK_NUM_LOGS_SUPPORTED) - 256)  // maximum number of segments in one log = (128 K - 256),     \
                                                                // the space for 256 index (or 1KB) is reserved for the other \
                                                                // metadata
// #define SPDK_LOG_METADATA_SIZE \
//     ((SPDK_LOG_ADDRESS_SPACE / SPDK_NUM_LOGS_SUPPORTED) * sizeof(uint32_t))
// SPDK_LOG_METADATA_SIZE = 512 KB
#define SPDK_LOG_METADATA_SIZE (1ULL << 15)

#define NUM_DATA_PLANE 1
#define NUM_CONTROL_PLANE 1

#define PAGE_SIZE (1ULL << 12)
#define PAGE_BIT 12

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

typedef union global_metadata {
    struct {
        PTLogMetadata log_metadata_entries[SPDK_NUM_LOGS_SUPPORTED];
    } fields;
    uint8_t bytes[SPDK_SEGMENT_SIZE];
} GlobalMetadata;

// log entry format
typedef union log_entry {
    struct {
        int64_t ver;     // version of the data
        uint64_t dlen;   // length of the data
        uint64_t ofst;   // offset of the data in the memory buffer
        uint64_t hlc_r;  // realtime component of hlc
        uint64_t hlc_l;  // logic component of hlc
    } fields;
    uint8_t bytes[64];
} LogEntry;

/**Per log metadata */
typedef struct log_metadata {
    /**Info part of metadata entry */
    PTLogMetadataInfo* persist_metadata_info;
    /**Log entries*/
    LogEntry* log_entry;
    /**Lock on head field */
    pthread_rwlock_t head_lock;
    /**Lock on tail field */
    pthread_rwlock_t tail_lock;
    // bool operator
    bool operator==(const struct log_metadata& other) {
        return (this->persist_metadata_info->fields.head == other.persist_metadata_info->fields.head)
               && (this->persist_metadata_info->fields.tail == other.persist_metadata_info->fields.tail)
               && (this->persist_metadata_info->fields.ver == other.persist_metadata_info->fields.ver);
    }
} LogMetadata;

/**Info part of log metadata entries stored in persist thread. */
typedef union persist_thread_log_metadata_info {
    struct {
        /**Name of the log */
        uint8_t name[256];
        /**Log index */
        uint32_t id;
        /**Head index */
        int64_t head;
        /**Tail index */
        int64_t tail;
        /**Latest version number */
        int64_t ver;
        /**Whether the metadata entry is occupied */
        bool inuse;
    } fields;
    uint8_t bytes[PAGE_SIZE];
} PTLogMetadataInfo;

/**Address transalation part of log metadata entries stored in persist thread */
typedef struct persist_thread_log_metadata_address {
    /**Log entry segment address translation table */
    uint16_t segment_log_entry_at_table[SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH];
    /**Data segment address translation table */
    uint16_t segment_data_at_table[SPDK_DATA_ADDRESS_TABLE_LENGTH];
} PTLogMetadataAddress;

/**Log metadata entry stored in persist thread */
typedef union persist_thread_log_metadata {
    struct {
        /**Address part of the entry */
        PTLogMetadataAddress log_metadata_address;
        /**Info part of the entry */
        PTLogMetadataInfo log_metadata_info;
    } fields;
    uint8_t bytes[SPDK_LOG_METADATA_SIZE];
} PTLogMetadata;

// SPDK info
typedef struct SpdkInfo {
    struct spdk_nvme_ctrlr* ctrlr;
    struct spdk_nvme_ns* ns;
    //struct spdk_nvme_qpair* qpair;
    uint32_t sector_bit;
    uint32_t sector_size;
};

// Data write request
typedef struct persist_data_request_t {
    void* buf;
    uint64_t request_id;
    uint16_t part_id;
    uint16_t part_num;
    std::atomic<int>* completed;
    uint64_t lba;
    uint32_t lba_count;
};

// Control write request
typedef struct persist_control_request_t {
    PTLogMetadataInfo buf;
    uint16_t part_num;
    uint64_t request_id;
    std::atomic<int>* completed;
    uint64_t lba;
    uint32_t lba_count;
    spdk_nvme_cmd_cb cb_fn;
    void* args;
};

// Persistent log interfaces
class SPDKPersistLog : public PersistLog {
private:
    class PersistThread {
    protected:
        /** SPDK general info */
        static SpdkInfo general_spdk_info;
        /** SPDK queue for data planes */
        static spdk_nvme_qpair* SpdkQpair_data[NUM_DATA_PLANE];
        /** SPDK queue for control planes */
        static spdk_nvme_qpair* SpdkQpair_control[NUM_CONTROL_PLANE];
        /** Threads corresponding to data planes */
        static std::thread data_plane[NUM_DATA_PLANE];
        /** Threads corresponding to control planes */
        static std::thread control_plane[NUM_CONTROL_PLANE];
        std::unordered_map<std::string, uint32_t> log_name_to_id;

        /** Data write request queue */
        static std::queue<persist_data_request_t> data_write_queue;
        //static std::queue<persist_data_request_t> in_progress_data_write_queue;
        /** Control write request queue */
        static std::queue<persist_control_request_t> control_write_queue;
        //static std::queue<persist_control_request_t> in_progress_control_write_queue;
        static std::mutex control_queue_mtx;
        //static std::mutex in_progress_control_queue_mtx;
        static std::mutex data_queue_mtx;
        //static std::mutex in_progress_data_queue_mtx;

        /** Segment usage table */
        static std::bitset<SPDK_NUM_SEGMENTS> segment_usage_table;
        /** Array of lot metadata entry */
        static GlobalMetadata global_metadata;
        /** Last completd request id */
        static uint64_t compeleted_request_id;
        /** Last assigned request id */
        static uint64_t assigned_request_id;
        static pthread_mutex_t segment_assignment_lock;
        static pthread_mutex_t metadata_entry_assignment_lock;
        static pthread_mutex_t metadata_load_lock;
        static sem_t new_data_request;
        static condition_variable data_request_completed;

        static std::map<uint32_t, LogEntry*> id_to_log;

        static bool initialized;
        static bool loaded;

        static bool probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                             struct spdk_nvme_ctrlr_opts* opts);
        static void attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                              struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts);
        static void data_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
        static void control_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
        static void release_segments(void* args, const struct spdk_nvme_cpl* completion);
        static void load_request_complete(void* args, const struct spdk_nvme_cpl* completion);

    public:
        /**
         * Constructor
         */
        PersistThread();
        /**
         * Destructor
         */
        virtual ~PersistThread();
        /**
         * Load metadata entry and log entries of a given log from persistent memory.
         * @param name - name of the log
         */
        void load(const std::string& name, LogMetadata* log_metadata);
        /**
         * Submit data_request and control_request. Data offset must be ailgned
         * with spdk sector size.
         * @param id - id of the log
         * @param data - data to be appended
         * @param data_offset - offset of the data w.r.t virtual data space
         * @param log - log entry to be appended
         * @param log_offset - offset of the log entry w.r.t virtual log entry space
         * @param metadata - updated metadata
         */
        void append(const uint32_t& id, char* data, const uint64_t& data_offset,
                    const uint64_t& data_size, void* log,
                    const uint64_t& log_offset, PTLogMetadataInfo metadata);

        void update_metadata(const uint32_t& id, PTLogMetadataInfo metadata, bool garbage_collection);
        void release(std::set<uint16_t> data_segments, std::set<uint16_t> log_segments);
    };

protected:
    // log metadata
    LogMetadata m_currLogMetadata;
    // Persistence Thread
    inline static PersistThread persist_thread;

    void head_rlock() noexcept(false);
    void head_wlock() noexcept(false);
    void tail_rlock() noexcept(false);
    void tail_wlock() noexcept(false);
    void head_unlock() noexcept(false);
    void tail_unlock() noexcept(false);

public:
    // Constructor:
    // Remark: the constructor will check the persistent storage
    // to make sure if this named log(by "name" in the template
    // parameters) is already there. If it is, load it from disk.
    // Otherwise, create the log.
    SPDKPersistLog(const std::string& name) noexcept(true) : PersistLog(name){};
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

    // Get the Earlist version
    virtual version_t getEarliestVersion() noexcept(false);

    // Get the Latest version
    virtual version_t getLatestVersion() noexcept(false);

    // return the last persisted value
    virtual const version_t getLastPersisted() noexcept(false);

    // Get a version by entry number return both length and buffer
    virtual const void* getEntryByIndex(const int64_t& eno) noexcept(false);

    // Get the latest version equal or earlier than ver.
    virtual const void* getEntry(const version_t& ver) noexcept(false);

    // Get the latest version - deprecated.
    // virtual const void* getEntry() noexcept(false) = 0;
    // Get a version specified by hlc
    virtual const void* getEntry(const HLC& hlc) noexcept(false);

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
};

}  // namespace spdk
}  // namespace persistent
#endif  //SPDK_PERSIST_LOG_HPP
