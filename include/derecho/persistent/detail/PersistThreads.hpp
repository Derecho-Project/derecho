#include "PersistLog.hpp"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <thread>
#include <unordered_map>
#include <iostream>

#define NUM_IO_THREAD 1
#define NUM_METADATA_THREAD 1

#define SPDK_NUM_LOGS_SUPPORTED (1ULL << 10)  // support 1024 logs
#define SPDK_SEGMENT_BIT 26
#define SPDK_SEGMENT_SIZE (1ULL << 26)  // segment size is 64 MB
#define SPDK_SEGMENT_ROUND_MASK ~((1ULL << 26) - 1)
#define SPDK_SEGMENT_MASK ((1ULL << 26) - 1)
#define SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH (1ULL << 12)
#define SPDK_DATA_ADDRESS_TABLE_LENGTH (6 * 1ULL << 12)
#define SPDK_LOG_METADATA_SIZE (1ULL << 15)
#define SPDK_LOG_ADDRESS_SPACE ((1ULL << (SPDK_SEGMENT_BIT + 11)) >> 6)  // address space per log is 1TB
#define SPDK_NUM_SEGMENTS \
    ((SPDK_LOG_ADDRESS_SPACE / SPDK_NUM_LOGS_SUPPORTED) - 256)
#define LOG_AT_TABLE(idx) (m_PersistThread->pt_global_metadata->fields.log_metadata_entries[idx].fields.log_metadata_address.segment_log_entry_at_table)
#define DATA_AT_TABLE(idx) (m_PersistThread->pt_global_metadata->fields.log_metadata_entries[idx].fields.log_metadata_address.segment_data_at_table)

#define LOG_BUFFER_SIZE 2048 //# of LogEntrys
#define DATA_BUFFER_SIZE (1ULL << 16) //# of sectors
#define READ_BUFFER_SIZE 256 //# of batches
#define READ_BATCH_BIT 15

namespace persistent {

namespace spdk {
// SPDK info
struct SpdkInfo {
    struct spdk_nvme_ctrlr* ctrlr;
    struct spdk_nvme_ns* ns;
    uint32_t sector_bit;
    uint32_t sector_size;
    uint32_t qpair_size;
    uint32_t qpair_requests;
    uint32_t sectors_per_max_io;
    uint64_t sector_round_mask;         //equivalent to >> sector_bit << sector_bit
    uint64_t sector_mask;               //equivalent to % sector_size
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
struct io_request_t {
    void* buf;
    uint64_t lba;
    uint32_t lba_count;
    spdk_nvme_cmd_cb cb_fn;
    void* args;
    int request_type;
};

// Control write request
struct persist_metadata_request_t {
    void* buf;
    uint64_t lba;
    uint32_t lba_count;
    spdk_nvme_cmd_cb cb_fn;
    void* args;
    int request_type;
};

struct atomic_sub_req {
    void* buf;
    uint32_t data_length;
    uint64_t virtaddress;
    int content_type;
};

struct data_write_cbfn_args {
    uint32_t id;            //Log id
    int64_t ver;            //Log version the request is attached to
    int64_t new_written_idx;
    uint64_t new_written_addr;
    void* buf;              //Pointer to write buffer
    std::atomic<int>* completed; //Number of completed sub request
    int num_sub_req;        //Number of sub requests
    int io_thread_id;       //ID of the io thread that submits the request
    uint32_t dlen;          //Length of data written/read
    int req_type;
};

struct metadata_write_cbfn_args {
    uint32_t id;
    int64_t ver;
    int io_thread_id;
    void* buf;
};

struct general_cbfn_args {
    std::atomic<bool>* completed;
    uint32_t dlen;
    int io_thread_id;
};

struct PreWriteMetadata {
    PTLogMetadataInfo metadata;
    std::mutex processing;
    std::atomic<int64_t> ver;
};

/**Per log metadata */
typedef struct log_metadata {
    /**Info part of metadata entry */
    PTLogMetadataInfo* persist_metadata_info;
    /**LogEntry write buffer*/
    LogEntry* log_write_buffer;
    /**Data write buffer*/
    uint8_t* data_write_buffer;
    /**The smallest index of log entries in buffer*/
    std::atomic<int64_t> in_memory_idx;
    /**The largest index of persisted log entry*/
    std::atomic<int64_t> last_written_idx;
    std::atomic<int64_t> last_submitted_idx;
    std::mutex log_write_buffer_lock;
    /**The highest ver that has been written for each PersistLog. */
    int64_t last_written_ver;
    /**The smallest address of data in buffer*/
    std::atomic<uint64_t> in_memory_addr;
    /**The largest address of data written*/
    std::atomic<uint64_t> last_written_addr;
    std::atomic<uint64_t> last_submitted_addr;
    std::mutex data_write_buffer_lock; 
    /**file des for data_write_buffer and read_buffer */
    int dw_fd;
    
    
    uint8_t* log_read_buffer;
    int log_rd_fd;
    uint8_t* data_read_buffer;
    int data_rd_fd;
    /**Map nvme_bid to length of the in-buffer part*/
    std::unordered_map<uint64_t, uint32_t> log_read_buf_index; 
    /**Map readbuf_bid to nvme_bid*/
    std::unordered_map<uint16_t, uint64_t> log_idx_to_batch;
    // -1=waiting for read req 0=cleared positive=being used
    std::atomic<int> log_idx_to_numref[READ_BUFFER_SIZE];
    std::mutex log_idx_to_mtx[READ_BUFFER_SIZE];
    std::condition_variable log_idx_to_cv[READ_BUFFER_SIZE];
    std::unordered_map<uint64_t, uint8_t> log_batch_to_numref;
    //-----------------DATA READ BUFFER----------------
    std::unordered_map<uint64_t, uint32_t> data_read_buf_index;
    std::unordered_map<uint16_t, uint64_t> data_idx_to_batch;
    std::atomic<int> data_idx_to_numref[READ_BUFFER_SIZE];
    std::mutex data_idx_to_mtx[READ_BUFFER_SIZE];
    std::condition_variable data_idx_to_cv[READ_BUFFER_SIZE];
    

    // bool operator
    bool operator==(const struct log_metadata& other) {
        return (this->persist_metadata_info->fields.head == other.persist_metadata_info->fields.head)
               && (this->persist_metadata_info->fields.tail == other.persist_metadata_info->fields.tail)
               && (this->persist_metadata_info->fields.ver == other.persist_metadata_info->fields.ver);
    }
} LogMetadata;


class PersistThreads {
protected:
    
	//----------------------------SPDK Related Info--------------------------------
    /** SPDK qpair for threads handling data and log entry io requests. */
    spdk_nvme_qpair* spdk_qpair[NUM_IO_THREAD];
    /** SPDK qpair for threads handling metadata write requests. */
    spdk_nvme_qpair* metadata_spdk_qpair;
   
    //-----------------------IO request handling threads---------------------------
    /** Threads handling io requests. */
    std::thread io_threads[NUM_IO_THREAD];
    /** Data and log entry io request queue. */
    std::queue<io_request_t> io_queue;
    /** Data write queue mutex */
    std::mutex io_queue_mtx;
    /** Tracker of remaining requests in each thread */
    std::atomic<uint32_t> uncompleted_io_req[NUM_IO_THREAD];
    std::atomic<uint32_t> uncompleted_io_sub_req[NUM_IO_THREAD];
    std::atomic<bool> asked_for_polling[NUM_IO_THREAD];
    /** Thread handling metadata write requests. */
    std::thread metadata_thread;
    /** Metadata write request queue. */
    std::queue<io_request_t> metadata_io_queue;
    /** Metadata io queue mutex. */
    std::mutex metadata_io_queue_mtx;
    /** Tracker of remaining requests in metadata io thread */
    std::atomic<uint32_t> uncompleted_metadata_req;
    std::atomic<bool> metadata_asked_for_polling;

    /** Condition Variables for new io request. */
    std::condition_variable new_io_request;
    std::condition_variable new_metadata_request;    
	
    //------------------------Metadata entries of each log-------------------------
    /** Array of all up-to-date metadata entries. */
    GlobalMetadata* pt_global_metadata;
    /** Array of all to-be-written metadata entries with highest ver w.r.t each PersitLog. */
    PreWriteMetadata to_write_metadata[SPDK_NUM_LOGS_SUPPORTED];
 
    //-------------------General Info on segment usage and logs--------------------
    /** Map log name to log id */
    std::unordered_map<std::string, uint32_t> log_name_to_id;
    /** Segment usage table */
    std::bitset<SPDK_NUM_SEGMENTS> segment_usage_table;
    /** Lock for changing segment usage table */
    pthread_mutex_t segment_assignment_lock;
    /** Lock for assigning new metadata entry */
    pthread_mutex_t metadata_entry_assignment_lock;
    
    //------------------------Destructor related fields---------------------------- 
    /** Whether destructor is called. */
    std::atomic<bool> destructed;
    /** Boolean of data all done */
    std::atomic<bool> io_request_all_done;
    
    //-------------------------Singleton Design Patern-----------------------------
    static PersistThreads* m_PersistThread;
    static std::atomic<bool> initialized;
    static std::mutex initialization_lock;
    int initialize_threads();
    
    //-------------------------Read Buffer-----------------------------------------

    //-------------------------SPDK call back functions----------------------------
    /** Spdk device probing callback function. */
    static bool probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                         struct spdk_nvme_ctrlr_opts* opts);
    /** Spdk device ataching callback function. */
    static void attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                          struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts);
    
    /** Data and log entry write request callback function. 
     * @param args - a pointer to data_write_cbfn_args.
     */
    static void data_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    
    /** Read request callback function.
     * @param args - a tuple to a pair of an atomic boolean pointer, dlen  and a io thread id
     */
    static void read_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    
    /** Metadata write request callback function. 
     * @param args - a tuple of log id, the version written and a io thread id. 
     */
    static void metadata_write_request_complete(void* args, const struct spdk_nvme_cpl* completion);
    
    /** Dummy callback function. Used if the completion does not matter. 
     * @param args - a pair of dlen and io thread id */
    static void dummy_request_complete(void* args, const struct spdk_nvme_cpl* completion); 

    static int metadata_io_thread_fn(void* arg);
    static int data_io_thread_fn(void* arg);

    int non_atomic_rw(char* buf, uint32_t data_length, uint64_t virtaddress, int blocking_mode, int content_type, bool is_write, const uint32_t id);
    int atomic_w(std::vector<atomic_sub_req> sub_requests, PTLogMetadataInfo metadata, const uint32_t id, int64_t new_written_idx, uint64_t new_written_addr);

public:
    class Guard {
	    public:
	    // protected:
            uint16_t batch_start;
	        uint16_t num_batch;
            uint32_t id;
	        bool from_write_buffer;
            bool is_logentry;

       // public:
            Guard():num_batch(0),
	        from_write_buffer(false){}
            Guard(uint16_t batch_start, uint16_t num_batch, uint32_t id, bool fwb, bool il):batch_start(batch_start),
                                                                                            num_batch(num_batch),
                                                                                            id(id),
                                                                                            from_write_buffer(fwb),
	                                                                                        is_logentry(il){}
            Guard(Guard&& rhs):batch_start(rhs.batch_start),
	                           num_batch(rhs.num_batch),
                               id(rhs.id),
                               from_write_buffer(rhs.from_write_buffer),
	                           is_logentry(rhs.is_logentry){
                rhs.num_batch = 0;
                rhs.from_write_buffer = false;		
            }
	    
            Guard(const Guard& rhs) = delete;

            void swap(Guard&& rhs) {
                uint16_t tmp_bs = this->batch_start;
                uint16_t tmp_nb = this->num_batch;
                uint32_t tmp_id = this->id;
                bool tmp_fwb = this->from_write_buffer;
                bool tmp_il = this->is_logentry;

                this->batch_start = rhs.batch_start;
                this->num_batch = rhs.num_batch;
                this->id = rhs.id;
                this->from_write_buffer = rhs.from_write_buffer;
                this->is_logentry = rhs.is_logentry;

                rhs.batch_start = tmp_bs;
                rhs.num_batch = tmp_nb;
                rhs.id = tmp_id;
                rhs.from_write_buffer = tmp_fwb;
                rhs.is_logentry = tmp_il;
            }

            void operator = (Guard&& rhs) {
                swap(std::move(rhs));
            }
            
            virtual ~Guard() {
                if (this->from_write_buffer) {
                    get()->metadata_entries[id].log_write_buffer_lock.unlock();
                    return;
                }
                
                if (this->num_batch > 0) {
                    for (uint16_t i = 0; i < (this->batch_start + this->num_batch - READ_BUFFER_SIZE - 1); i++) {
                        if (is_logentry) {
                            get()->metadata_entries[id].log_idx_to_mtx[i].lock();
                            (get()->metadata_entries[id].log_idx_to_numref[i])--;
                            if (get()->metadata_entries[id].log_idx_to_numref[i] == 0) {
                                get()->metadata_entries[id].log_idx_to_cv[i].notify_one();
                            }
                            get()->metadata_entries[id].log_idx_to_mtx[i].unlock();
                        } else {
                            get()->metadata_entries[id].data_idx_to_mtx[i].lock();
                            (get()->metadata_entries[id].data_idx_to_numref[i])--;
                            if (get()->metadata_entries[id].data_idx_to_numref[i] == 0) {
                                get()->metadata_entries[id].data_idx_to_cv[i].notify_one();
                            }
                            get()->metadata_entries[id].data_idx_to_mtx[i].unlock();
                        }
                    }

                    for (uint16_t i = batch_start; i < std::min(this->batch_start + this->num_batch, READ_BUFFER_SIZE); i++) {
                        if (is_logentry) {
                            get()->metadata_entries[id].log_idx_to_mtx[i].lock();
                            (get()->metadata_entries[id].log_idx_to_numref[i])--;
                            if (get()->metadata_entries[id].log_idx_to_numref[i] == 0) {
                                get()->metadata_entries[id].log_idx_to_cv[i].notify_one();
                            }
                            get()->metadata_entries[id].log_idx_to_mtx[i].unlock();
                        } else {
                            get()->metadata_entries[id].data_idx_to_mtx[i].lock();
                            (get()->metadata_entries[id].data_idx_to_numref[i])--;
                            if (get()->metadata_entries[id].data_idx_to_numref[i] == 0) {
                                get()->metadata_entries[id].data_idx_to_cv[i].notify_one();
                            }
                            get()->metadata_entries[id].data_idx_to_mtx[i].unlock();
                        }
                }
            }
	}
    };
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
    void append(const uint32_t& id, char* data, 
		    const uint64_t& data_size, const version_t& ver,
		    const HLC& mhlc);

    void update_metadata(const uint32_t& id, PTLogMetadataInfo metadata);
    const version_t persist(const uint32_t& id);
    const version_t getLastPersisted(const uint32_t& id);
    std::tuple<LogEntry*, Guard> read_entry(const uint32_t& id, const int64_t& index);
    std::tuple<void*, Guard> read_data(const uint32_t& id, const int64_t& index);
    void* read_lba(const uint64_t& lba_index);
   
    LogMetadata metadata_entries[SPDK_NUM_LOGS_SUPPORTED];
    /** Map log id to log entry space*/
    std::map<uint32_t, LogEntry*> id_to_log;
    static bool loaded;
    static pthread_mutex_t metadata_load_lock;

    static PersistThreads* get();
    /** SPDK general info */
    SpdkInfo general_spdk_info;
};
}  // namespace spdk
}  // namespace persistent
