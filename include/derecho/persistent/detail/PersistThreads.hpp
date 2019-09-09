#include "PersistLog.hpp"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <thread>
#include <unordered_map>

#define NUM_DATA_PLANE 1
#define NUM_CONTROL_PLANE 1

#define SPDK_NUM_LOGS_SUPPORTED (1UL << 10)  // support 1024 logs
#define SPDK_SEGMENT_BIT 26
#define SPDK_SEGMENT_SIZE (1ULL << 26)  // segment size is 64 MB
#define SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH (1ULL << 11)
#define SPDK_DATA_ADDRESS_TABLE_LENGTH (3 * 1ULL << 12)
#define SPDK_LOG_METADATA_SIZE (1ULL << 15)
#define SPDK_LOG_ADDRESS_SPACE (1ULL << (SPDK_SEGMENT_BIT + 11))  // address space per log is 1TB
#define SPDK_NUM_SEGMENTS \
    ((SPDK_LOG_ADDRESS_SPACE / SPDK_NUM_LOGS_SUPPORTED) - 256)
#define LOG_AT_TABLE(idx) m_PersistThread->global_metadata.fields.log_metadata_entries[idx].fields.log_metadata_address.segment_log_entry_at_table
#define DATA_AT_TABLE(idx) m_PersistThread->global_metadata.fields.log_metadata_entries[idx].fields.log_metadata_address.segment_data_at_table

namespace persistent {

namespace spdk {
// SPDK info
struct SpdkInfo {
    struct spdk_nvme_ctrlr* ctrlr;
    struct spdk_nvme_ns* ns;
    //struct spdk_nvme_qpair* qpair;
    uint32_t sector_bit;
    uint32_t sector_size;
};

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

// Data write request
struct persist_data_request_t {
    void* buf;
    uint64_t request_id;
    uint16_t part_id;
    uint16_t part_num;
    std::atomic<int>* completed;
    uint64_t lba;
    uint32_t lba_count;
    spdk_nvme_cmd_cb cb_fn;
    void* args;
    bool is_write;
};

// Control write request
struct persist_control_request_t {
    PTLogMetadataInfo buf;
    uint16_t part_num;
    uint64_t request_id;
    std::atomic<int>* completed;
    uint64_t lba;
    uint32_t lba_count;
    spdk_nvme_cmd_cb cb_fn;
    void* args;
};

/**Per log metadata */
typedef struct log_metadata {
    /**Info part of metadata entry */
    PTLogMetadataInfo* persist_metadata_info;

    // bool operator
    bool operator==(const struct log_metadata& other) {
        return (this->persist_metadata_info->fields.head == other.persist_metadata_info->fields.head)
               && (this->persist_metadata_info->fields.tail == other.persist_metadata_info->fields.tail)
               && (this->persist_metadata_info->fields.ver == other.persist_metadata_info->fields.ver);
    }
} LogMetadata;

class PersistThreads {
protected:
    /** SPDK general info */
    SpdkInfo general_spdk_info;
    /** SPDK queue for data planes */
    spdk_nvme_qpair* SpdkQpair_data[NUM_DATA_PLANE];
    /** SPDK queue for control planes */
    spdk_nvme_qpair* SpdkQpair_control[NUM_CONTROL_PLANE];
    /** Threads corresponding to data planes */
    std::thread data_plane[NUM_DATA_PLANE];
    /** Threads corresponding to control planes */
    std::thread control_plane[NUM_CONTROL_PLANE];
    /** Map log name to log id */
    std::unordered_map<std::string, uint32_t> log_name_to_id;

    /** Data write request queue */
    std::queue<persist_data_request_t> data_write_queue;
    /** Control write request queue */
    std::queue<persist_control_request_t> control_write_queue;
    /** Control write queue mutex */
    std::mutex control_queue_mtx;
    /** Data write queue mutex */
    std::mutex data_queue_mtx;

    /** Segment usage table */
    std::bitset<SPDK_NUM_SEGMENTS> segment_usage_table;
    /** Array of lot metadata entry */
    GlobalMetadata global_metadata;
    /** Last completd request id */
    uint64_t compeleted_request_id;
    /** Last assigned request id */
    uint64_t assigned_request_id;
    /** Lock for changing segment usage table */
    pthread_mutex_t segment_assignment_lock;
    /** Lock for assigning new metadata entry */
    pthread_mutex_t metadata_entry_assignment_lock;
    /** Semapore of new data request */
    sem_t new_data_request;
    std::condition_variable data_request_completed;

    /** Singleton Design Patern */
    static PersistThreads* m_PersistThread;
    static bool initialized;
    static std::mutex initialization_lock;

    static bool probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                         struct spdk_nvme_ctrlr_opts* opts);
    static void attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                          struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts);
    /** Default callback for data write request*/
    static void data_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    /** Default callback for control write request*/
    static void control_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    /** Release segemnts held by log entries and data before head of the log*/
    static void release_segments(void* args, const struct spdk_nvme_cpl* completion);
    static void load_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    static void dummy_request_complete(void* args, const struct spdk_nvme_cpl* completion);

public:
    /**
         * Constructor
         */
    PersistThreads();
    /**
         * Destructor
         */
    virtual ~PersistThreads();
    /**
         * Load metadata entry and log entries of a given log from persistent memory.
         * @param name - name of the log
         * @param log_metadata - pointer to metadata held by the log
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

    LogEntry* read_entry(const uint32_t& id, const uint64_t& index);
    void* read_data(const uint32_t& id, const uint64_t& index);

    std::map<uint32_t, int64_t> id_to_last_version;
    /** Map log id to log entry space*/
    static std::map<uint32_t, LogEntry*> id_to_log;
    static bool loaded;
    static pthread_mutex_t metadata_load_lock;

    static PersistThreads* get();
};
}  // namespace spdk
}  // namespace persistent
