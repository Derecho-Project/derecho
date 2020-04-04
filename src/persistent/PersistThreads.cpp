#include <cmath>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/persistent/detail/PersistThreads.hpp>
#include <iostream>


#define UNBLOCKING 1
#define BLOCKING 2

#define METADATA 0
#define LOGENTRY 1
#define DATA 2

#define READ 0
#define WRITE 1
#define POLL 2

#define LBA_PER_SEG_BIT (SPDK_SEGMENT_BIT - general_spdk_info.sector_bit)
#define LBA_PER_SEG (1 << (SPDK_SEGMENT_BIT - general_spdk_info.sector_bit))
const uint32_t MAX_LBA_COUNT = 130816;

namespace persistent {
namespace spdk {

PersistThreads* PersistThreads::m_PersistThread;
std::atomic<bool> PersistThreads::initialized;
bool PersistThreads::loaded;
pthread_mutex_t PersistThreads::metadata_load_lock;
std::mutex PersistThreads::initialization_lock;
//std::map<uint32_t, LogEntry*> PersistThreads::id_to_log;


bool PersistThreads::probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
        struct spdk_nvme_ctrlr_opts* opts) {
    std::printf("Attaching to %s\n", trid->traddr);
    std::cout.flush();
    return true;
}

struct my_spdk_nvme_ns {
        void          *ctrlr;
        uint32_t                        sector_size;

        /*
         * Size of data transferred as part of each block,
         * including metadata if FLBAS indicates the metadata is transferred
         * as part of the data buffer at the end of each LBA.
         */
        uint32_t                        extended_lba_size;

        uint32_t                        md_size;
        uint32_t                        pi_type;
        uint32_t                        sectors_per_max_io;
        uint32_t                        sectors_per_stripe;
        uint32_t                        id;
        uint16_t                        flags;

        /* Namespace Identification Descriptor List (CNS = 03h) */
        uint8_t                         id_desc_list[4096];
};


void PersistThreads::attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
		struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts) {
    struct spdk_nvme_ns* ns;
    std::printf("Attached to %s\n", trid->traddr);
    std::cout.flush();
    // Step 0: store the ctrlr
    m_PersistThread->general_spdk_info.ctrlr = ctrlr;

    // Step 1: register one of the namespaces
    int num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
    std::printf("Using %d namespaces.\n", num_ns);
    std::cout.flush();
    for(int nsid = 1; nsid <= num_ns; nsid++) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if(ns == NULL) {
            continue;
        }
        if(!spdk_nvme_ns_is_active(ns)) {
            continue;
        }

        std::printf("Namespace sector per max io: %d, sector per max stripe: %d", ((struct my_spdk_nvme_ns*)ns)->sectors_per_max_io,((struct my_spdk_nvme_ns*)ns)->sectors_per_stripe);

        std::printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
                    spdk_nvme_ns_get_size(ns) / 1000000000);
        std::cout.flush();
        m_PersistThread->general_spdk_info.ns = ns;
        m_PersistThread->general_spdk_info.sectors_per_max_io = ((my_spdk_nvme_ns*)ns)->sectors_per_max_io;
        m_PersistThread->general_spdk_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        m_PersistThread->general_spdk_info.sector_bit = std::log2(m_PersistThread->general_spdk_info.sector_size);
        m_PersistThread->general_spdk_info.sector_mask = (1 << m_PersistThread->general_spdk_info.sector_bit) - 1;
        m_PersistThread->general_spdk_info.sector_round_mask = ~m_PersistThread->general_spdk_info.sector_mask;
        break;
    }
    // if no available ns, throw an exception
    if(m_PersistThread->general_spdk_info.ns == NULL) {
        throw derecho::derecho_exception("No available namespace");
    }
}

void PersistThreads::data_write_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    // if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        data_write_cbfn_args* cbfn_args = (data_write_cbfn_args*) args;
        std::printf("failed req type is %d\n", cbfn_args->req_type);
        std::cout.flush();
        throw derecho::derecho_exception("data write request failed.");
    }
    data_write_cbfn_args* cbfn_args = (data_write_cbfn_args*) args;
    (*(cbfn_args->completed))++;

    if(cbfn_args->completed->load() == cbfn_args->num_sub_req) {
        //Update the last_written_addr and last_written_idx fields
        get()->metadata_entries[cbfn_args->id].log_write_buffer_lock.lock();
        get()->metadata_entries[cbfn_args->id].last_written_idx = cbfn_args->new_written_idx;
        get()->metadata_entries[cbfn_args->id].log_write_buffer_lock.unlock();
        
        get()->metadata_entries[cbfn_args->id].data_write_buffer_lock.lock();
        get()->metadata_entries[cbfn_args->id].last_written_addr = cbfn_args->new_written_addr;
        get()->metadata_entries[cbfn_args->id].data_write_buffer_lock.unlock();

        //Check whether the metadata to be written is of the corresponding ver
        if(cbfn_args->ver == get()->to_write_metadata[cbfn_args->id].ver.load()){
            
            get()->to_write_metadata[cbfn_args->id].processing.lock(); 
            //Submit the corresponding metadata write request
            void* buf = spdk_malloc(sizeof(PTLogMetadataInfo), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
            std::memcpy(buf, &(get()->to_write_metadata[cbfn_args->id].metadata), sizeof(PTLogMetadataInfo));
            get()->to_write_metadata[cbfn_args->id].processing.unlock();
            
            io_request_t metadata_request;
            metadata_request.lba = ((cbfn_args->id) * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataAddress)) >> get()->general_spdk_info.sector_bit;
            metadata_request.lba_count = sizeof(PTLogMetadataInfo) >> get()->general_spdk_info.sector_bit;
            metadata_request.buf = buf;
            metadata_request.cb_fn = metadata_write_request_complete;
            metadata_request.request_type = WRITE;
 
            metadata_write_cbfn_args* metadata_args = new metadata_write_cbfn_args();
            metadata_args->id = cbfn_args->id;
            metadata_args->ver = cbfn_args->ver;
            metadata_args->io_thread_id = cbfn_args->io_thread_id;
            metadata_args->buf = buf;
            metadata_request.args = metadata_args;            

            get()->metadata_io_queue_mtx.lock();
            get()->metadata_io_queue.push(metadata_request);
            get()->new_metadata_request.notify_one();
            get()->metadata_io_queue_mtx.unlock();
        }
    }
    
    if (cbfn_args->io_thread_id < 0) {
        get()->uncompleted_metadata_req -= 1;
    } else {  
        get()->uncompleted_io_req[cbfn_args->io_thread_id] -= 1;
    }
    // spdk_free(cbfn_args->buf);
    free(cbfn_args);
}

void PersistThreads::read_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("data read request failed.");
    }
    general_cbfn_args* cbfn_args = (general_cbfn_args*) args;
    *cbfn_args->completed = true;
    get()->uncompleted_io_req[cbfn_args->io_thread_id] -= 1;
}

void PersistThreads::dummy_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("dummy request failed.");
    }
    general_cbfn_args* cbfn_args = (general_cbfn_args*) args;
    get()->uncompleted_io_req[cbfn_args->io_thread_id] -= 1;
}

void PersistThreads::metadata_write_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    // if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("control request failed.");
    }
    metadata_write_cbfn_args* cbfn_args;
    cbfn_args = (metadata_write_cbfn_args*) args;
    
    get()->metadata_entries[cbfn_args->id].last_written_ver = cbfn_args->ver;
    get()->uncompleted_metadata_req -= 1;
    spdk_free(cbfn_args->buf);
    free(cbfn_args);
}

int PersistThreads::non_atomic_rw(char* buf, uint32_t data_length, uint64_t virtaddress, int blocking_mode, int content_type, bool is_write, uint32_t id){
    int num_segment = ((virtaddress + data_length - 1) >> SPDK_SEGMENT_BIT) - (virtaddress >> SPDK_SEGMENT_BIT) + 1;
    uint32_t ofst = 0;
    std::queue<std::atomic<bool>*> completed_queue;
    for (int i = 0; i < num_segment; i++) {
        uint32_t curr_ofst = 0;
        uint32_t size = std::min((uint32_t)SPDK_SEGMENT_SIZE, data_length - ofst);
        // Extract physical lba index based on content type using the corresponding address translation table
        uint64_t lba_index;
        switch(content_type){
            case METADATA:
                lba_index = (virtaddress + ofst) >> general_spdk_info.sector_bit;
                break;
            case LOGENTRY:
                lba_index = LOG_AT_TABLE(id)[((virtaddress + ofst) >> SPDK_SEGMENT_BIT) % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH] * LBA_PER_SEG + (((virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                break;
            case DATA:
                lba_index = DATA_AT_TABLE(id)[((virtaddress + ofst) >> SPDK_SEGMENT_BIT) % SPDK_DATA_ADDRESS_TABLE_LENGTH] * LBA_PER_SEG + (((virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                break;
            default:
                break;
        }

        // Submit data request for the current segment
        while (curr_ofst < size) {  
            io_request_t data_request;
            uint32_t curr_size = std::min(size - curr_ofst, general_spdk_info.sectors_per_max_io << general_spdk_info.sector_bit);
            data_request.buf = buf + ofst +  curr_ofst;
            data_request.lba = lba_index + (curr_ofst >> general_spdk_info.sector_bit);
            data_request.lba_count = ((curr_size - 1) >> general_spdk_info.sector_bit) + 1;
            // Use different callback function based on input mode
            switch (blocking_mode) {
                case UNBLOCKING:
                    {
                        data_request.cb_fn = dummy_request_complete;
                        data_request.request_type = (is_write? WRITE : READ);
                        general_cbfn_args* cbfn_args = new general_cbfn_args();
                        cbfn_args->dlen = curr_size;
                        data_request.args = (void*) cbfn_args;
                    }                
                    break;

                case BLOCKING:
                    {    
                        std::atomic<bool>* completed = new std::atomic<bool>();
                        data_request.cb_fn = read_request_complete;
                        general_cbfn_args* cbfn_args = new general_cbfn_args();
                        cbfn_args->completed = completed;
                        cbfn_args->dlen = curr_size;
                        data_request.args = (void*) cbfn_args;
                        completed_queue.push(completed);
                        data_request.request_type = (is_write? WRITE : READ);
		    } 
                    break;

                default:
                    break;
            }
            // Push the request to the data queue
            std::cout.flush();
            io_queue_mtx.lock();
            io_queue.push(data_request);
            new_io_request.notify_one();
            io_queue_mtx.unlock();
            curr_ofst += curr_size;
        }
        ofst += size;
    }

    // Wait for completion
    switch (blocking_mode) {
        case UNBLOCKING:
            return 0;

        case BLOCKING:
            while (completed_queue.size() > 0) {
                std::atomic<bool>* completed_ptr = completed_queue.front();
                completed_queue.pop();
                while (!*completed_ptr);
                free(completed_ptr);
            }
            return 0;

        default:
            return 0;
    }
}

int PersistThreads::atomic_w(std::vector<atomic_sub_req> sub_requests, PTLogMetadataInfo metadata, uint32_t id, int64_t new_written_idx, uint64_t new_written_addr){
    std::queue<bool*> completed_queue;
    std::queue<io_request_t> io_request_queue;
    std::queue<io_request_t> metadata_request_queue;
    std::atomic<int>* completed = new std::atomic<int>();
    
    //Issue write request for each sub request
    for (atomic_sub_req req : sub_requests) {
        int num_segment = ((req.virtaddress + req.data_length - 1) >> SPDK_SEGMENT_BIT) - (req.virtaddress >> SPDK_SEGMENT_BIT) + 1;
        uint32_t ofst = 0;
        for (int i = 0; i < num_segment; i++) {
            uint32_t curr_ofst = 0;
            uint32_t size = std::min((uint32_t)SPDK_SEGMENT_SIZE, req.data_length - ofst);
            // Extract physical lba index based on content type using the corresponding address translation table
            uint64_t lba_index;
            switch(req.content_type){
                case METADATA:
                    lba_index = (req.virtaddress + ofst) >> general_spdk_info.sector_bit;
                    break;
                case LOGENTRY:
                    lba_index = LOG_AT_TABLE(id)[((req.virtaddress + ofst) >> SPDK_SEGMENT_BIT) % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH] * LBA_PER_SEG + (((req.virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                    break;
                case DATA:
                    lba_index = DATA_AT_TABLE(id)[((req.virtaddress + ofst) >> SPDK_SEGMENT_BIT) % SPDK_DATA_ADDRESS_TABLE_LENGTH] * LBA_PER_SEG + (((req.virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                    break;
                default:
                    break;
            }

            // Submit data request for the current segment
            while (curr_ofst < size) {  
                io_request_t data_request;
                uint32_t curr_size = std::min(size - curr_ofst, general_spdk_info.sectors_per_max_io << general_spdk_info.sector_bit);
                data_request.buf = ((uint8_t*)req.buf) + ofst + curr_ofst; 
                data_request.lba = lba_index + (curr_ofst >> general_spdk_info.sector_bit);
                data_request.lba_count = ((curr_size - 1) >> general_spdk_info.sector_bit) + 1;
                data_request.cb_fn = data_write_request_complete;
                
                data_write_cbfn_args* cbfn_args = new data_write_cbfn_args();
                cbfn_args->id = id;
                cbfn_args->ver = metadata.fields.ver;
                cbfn_args->buf = data_request.buf;
                cbfn_args->completed = completed;
                cbfn_args->dlen = curr_size;
                cbfn_args->req_type = req.content_type;
                cbfn_args->new_written_idx = new_written_idx;
                cbfn_args->new_written_addr = new_written_addr;
                
                data_request.args = cbfn_args; 
                data_request.request_type = WRITE;
                
                if (req.content_type == METADATA){
                    metadata_request_queue.push(data_request);
                } else {                
                    io_request_queue.push(data_request);
                }
                curr_ofst += curr_size;
            }
            ofst += size;
        }
    }
    
    // Update to_write_metadata entry
    // to_write_metadata[id].processing.lock();
    // to_write_metadata[id].ver = metadata.fields.ver;
    // to_write_metadata[id].metadata = metadata;
    // to_write_metadata[id].processing.unlock();

    // Push all io requests to io_queue
    int num_sub_req = io_request_queue.size() + metadata_request_queue.size();
    while (!io_request_queue.empty()){
        io_request_t req = io_request_queue.front();
        io_request_queue.pop();
        ((data_write_cbfn_args*) req.args)->num_sub_req = num_sub_req;
        io_queue_mtx.lock();
        io_queue.push(req);
        new_io_request.notify_one();
        io_queue_mtx.unlock();
    }

    while (!metadata_request_queue.empty()) {
        io_request_t req = metadata_request_queue.front();
        metadata_request_queue.pop();
        ((data_write_cbfn_args*) req.args)->num_sub_req = num_sub_req;
        metadata_io_queue_mtx.lock();
        metadata_io_queue.push(req);
        new_metadata_request.notify_one();
        metadata_io_queue_mtx.unlock();
    }
    return 0;
}

uint64_t segment_address_log_location(const uint32_t& id, const uint16_t& virtual_log_segment) {
    return id * sizeof(persist_thread_log_metadata) + virtual_log_segment * sizeof(uint16_t);
}

uint64_t segment_address_data_location(const uint32_t& id, const uint16_t& virtual_data_segment) {
    return id * sizeof(persist_thread_log_metadata) + SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH * sizeof(uint16_t) + virtual_data_segment * sizeof(uint16_t);
}

PersistThreads::PersistThreads() {}

PersistThreads::~PersistThreads() {
	destructed = true;
    new_io_request.notify_all();
    new_metadata_request.notify_all();
    // Wait for all requests to be submitted
    while (!io_queue.empty()){}
	// Wait for all submitted requests to be completed
    for (int i = 0; i < NUM_IO_THREAD; i++) {
        while (uncompleted_io_req[i] > 0){}
    }
    // Wait for all metadata requests to be submitted
    while (!metadata_io_queue.empty()){}
    // Wait for all submitted requests to be completed
    while (uncompleted_io_req > 0){}
    // Free log entry spaces
    for (unsigned int i = 0; i < SPDK_NUM_LOGS_SUPPORTED; i++) {
        if (metadata_entries[i].log_write_buffer != nullptr) {
            close(metadata_entries[i].dw_fd);
            close(metadata_entries[i].log_rd_fd);
	    close(metadata_entries[i].data_rd_fd);
            munmap(metadata_entries[i].log_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1));
            munmap(metadata_entries[i].data_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1));
	    munmap(metadata_entries[i].data_write_buffer, DATA_BUFFER_SIZE << (general_spdk_info.sector_bit + 1));
            spdk_free((void*)metadata_entries[i].log_write_buffer);
        }
    }
    if (loaded) {
        spdk_free((void*)pt_global_metadata);
    }
}

PersistThreads* PersistThreads::get() {
    if(initialized) {
        return m_PersistThread;
    } else {
        // Step 0: Grab the initialization lock
        initialization_lock.lock();
        if(initialized) {
            initialization_lock.unlock();
            return m_PersistThread;
        }
        m_PersistThread = new PersistThreads();
        if(m_PersistThread->initialize_threads() != 0){ 
            std::printf("m_PersistThread initialization failed.\n");
            std::cout.flush();
            exit(1);
        }
        initialized = true;
        initialization_lock.unlock();
        return m_PersistThread;
    }
}

int PersistThreads::metadata_io_thread_fn(void* arg) {
    get()->uncompleted_metadata_req = 0;
    while(true) {
        get()->metadata_io_queue_mtx.lock();
        if (!get()->metadata_io_queue.empty()) {
            io_request_t request = get()->metadata_io_queue.front();
            get()->metadata_io_queue.pop();
            get()->metadata_io_queue_mtx.unlock();

            // Check whether space avaliable for the new req
            while (get()->uncompleted_metadata_req > get()->general_spdk_info.qpair_requests) {
                spdk_nvme_qpair_process_completions(get()->metadata_spdk_qpair, 0);
            }

            if (request.cb_fn == data_write_request_complete) {
                ((data_write_cbfn_args*)(request.args))->io_thread_id = -1;
            } else if (request.cb_fn == metadata_write_request_complete) {
            }

            int rc = spdk_nvme_ns_cmd_write(get()->general_spdk_info.ns, get()->metadata_spdk_qpair, request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
            if (rc != 0){
                std::fprintf(stderr, "Failed to initialize write io %d with lba_count %d.\n", rc, request.lba_count);
                std::cout.flush();
            }
            get()->uncompleted_metadata_req += 1;
        } else if (get()->uncompleted_metadata_req > 0) {
            get()->metadata_io_queue_mtx.unlock();
            spdk_nvme_qpair_process_completions(get()->metadata_spdk_qpair, 0);
        } else if (get()->destructed) {
            get()->metadata_io_queue_mtx.unlock();
            std::printf("metadata destructed.\n");
            std::cout.flush();
            bool no_uc_data_req = true;
            for (int i = 0; i < NUM_IO_THREAD; i++){
                if (get()->uncompleted_io_req[no_uc_data_req] > 0) {
                    no_uc_data_req = false;
                    break;
                } 
            }
            if (no_uc_data_req) {
                spdk_nvme_ctrlr_free_io_qpair(get()->metadata_spdk_qpair);
                return 0;
            }
        } else {
            get()->metadata_io_queue_mtx.unlock();
            std::unique_lock<std::mutex> lk(get()->metadata_io_queue_mtx);
            get()->new_metadata_request.wait(lk, [&]{return (get()->destructed || !get()->metadata_io_queue.empty());});
        }
    }
    return 0;
}

int PersistThreads::data_io_thread_fn(void* arg) {
    uint32_t thread_id = *(uint32_t*)arg;
    get()->uncompleted_io_req[thread_id] = 0;
    //TODO: Assuming that each sub req is of size 32KB
    get()->uncompleted_io_sub_req[thread_id] = 0;
    while(true) {
        get()->io_queue_mtx.lock();
        if (!get()->io_queue.empty()) {
            io_request_t request = get()->io_queue.front();
            get()->io_queue.pop();
            get()->io_queue_mtx.unlock();

            while (get()->uncompleted_io_req[thread_id] > get()->general_spdk_info.qpair_requests) {
                spdk_nvme_qpair_process_completions(get()->spdk_qpair[thread_id], 0);
            }

            // Submit the request
            if (request.cb_fn == data_write_request_complete) {
                ((data_write_cbfn_args*) request.args)->io_thread_id = thread_id; 
            } else {
                ((general_cbfn_args*) request.args)->io_thread_id = thread_id;
            }

            switch(request.request_type) {
                case READ:
                    {
			int rc = spdk_nvme_ns_cmd_read(get()->general_spdk_info.ns, get()->spdk_qpair[thread_id], request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
                        if (rc != 0){
                            std::fprintf(stderr, "Failed to initialize read io %d with lba_count %d.\n", rc, request.lba_count);
                            std::cout.flush();
                        }
                        get()->uncompleted_io_req[thread_id] += 1;
                    }
                    break;

                case WRITE:
                    {
                        int rc = spdk_nvme_ns_cmd_write(get()->general_spdk_info.ns, get()->spdk_qpair[thread_id], request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
                        if (rc != 0){
                            std::fprintf(stderr, "Failed to initialize write io %d with lba_count %d.\n", rc, request.lba_count);
                            std::cout.flush();
                        }
                        get()->uncompleted_io_req[thread_id] += 1;
                    }
                    break;

                default:
                    break;
            }
        } else if (get()->uncompleted_io_req[thread_id] > 0) {
            get()->io_queue_mtx.unlock();
            spdk_nvme_qpair_process_completions(get()->spdk_qpair[thread_id], 0);
        } else if (get()->destructed) {
            get()->io_queue_mtx.unlock();
            spdk_nvme_ctrlr_free_io_qpair(get()->spdk_qpair[thread_id]);
            return 0;
        } else {
	    get()->io_queue_mtx.unlock();
            std::unique_lock<std::mutex> lk(get()->io_queue_mtx);
            get()->new_io_request.wait(lk, [&]{return (get()->destructed || !get()->io_queue.empty());});
        }
    }
    return 0;
}

int PersistThreads::initialize_threads() {
    // TODO: using first available device and namespace. This should come from configuration file.
    // Step 0: initialize spdk nvme info
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.core_mask= "0xB";
    if(spdk_env_init(&opts) < 0) {
        // Failed to initialize spdk env, throw an exception
        throw derecho::derecho_exception("Failed to initialize spdk namespace.");
    }
    int res = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
    if(res != 0) {
        //initialization of nvme ctrlr failed
        throw derecho::derecho_exception("Failed to initialize nvme ctrlr.");
    }

    struct spdk_nvme_io_qpair_opts qpair_opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(general_spdk_info.ctrlr, &qpair_opts, sizeof(qpair_opts));
    general_spdk_info.qpair_size = qpair_opts.io_queue_size;
    general_spdk_info.qpair_requests = qpair_opts.io_queue_requests;
   
    // Step 1: get qpair for io threads
    for(int i = 0; i < NUM_IO_THREAD; i++) {
        spdk_qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
        if(spdk_qpair[i] == NULL) {
            //qpair initialization failed
            throw derecho::derecho_exception("Failed to initialize data qpair.");
        }
    }
    metadata_spdk_qpair = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
    if(metadata_spdk_qpair == NULL) {
        throw derecho::derecho_exception("Failed to initialize metadata qpair.");
    }

    // Step 2: initialize sem and locks
    if(pthread_mutex_init(&segment_assignment_lock, NULL) != 0) {
        // mutex init failed
        throw derecho::derecho_exception("Failed to initialize mutex.");
    }
    if(pthread_mutex_init(&metadata_entry_assignment_lock, NULL) != 0) {
        // mutex init failed
        throw derecho::derecho_exception("Failed to initialize mutex.");
    }

    // Step 3: initialize other fields
    segment_usage_table.reset();
    segment_usage_table[0].flip();
    destructed = false;
    
    // Step 4: initialize threads
    bool metathread_started = false;
    uint32_t i;
    // SPDK_ENV_FOREACH_CORE(i) {
    //	std::cout << "Starting thread " << i << std::endl;
    //	if (i == spdk_env_get_current_core()) {
    //        std::cout << "This is master core" << std::endl;
    //    } else if (!metathread_started) {    
    //        std::cout << "Starting metadata thread." << std::endl;
    //        spdk_env_thread_launch_pinned(i, metadata_io_thread_fn, NULL);
    //        std::cout << "End starting metadata thread." << std::endl;
    //    } else {
    //        int j = i - 2;
    //        spdk_env_thread_launch_pinned(i, data_io_thread_fn, &j);
    //    }
    //}
    i = spdk_env_get_next_core(i);
    spdk_env_thread_launch_pinned(1, metadata_io_thread_fn, NULL);

    i = 0;
    spdk_env_thread_launch_pinned(3, data_io_thread_fn, &i);

    spdk_unaffinitize_thread();
    return 0;
}

void PersistThreads::append(const uint32_t& id, char* pdata, 
                            const uint64_t& data_size, const version_t& ver,
                            const HLC& mhlc) {
    if (metadata_entries[id].last_written_idx >= 0) {
        if (metadata_entries[id].persist_metadata_info->fields.tail - metadata_entries[id].last_written_idx >= LOG_BUFFER_SIZE) {
            throw derecho::derecho_exception("Covering log entries that have not been persisted.");
        }
    }
   
    // Step 1: Update log entry info
    LogEntry* next_log_entry = metadata_entries[id].log_write_buffer + (metadata_entries[id].persist_metadata_info->fields.tail % LOG_BUFFER_SIZE);
    next_log_entry->fields.dlen = data_size;
    next_log_entry->fields.ver = ver;
    next_log_entry->fields.hlc_r = mhlc.m_rtc_us;
    next_log_entry->fields.hlc_l = mhlc.m_logic;
     
    if (metadata_entries[id].persist_metadata_info->fields.tail - metadata_entries[id].persist_metadata_info->fields.head == 0) {
        next_log_entry->fields.ofst = 0;
    } else {
        LogEntry* last_entry;
        Guard guard;
        std::tie(last_entry, guard) = read_entry(id, metadata_entries[id].persist_metadata_info->fields.tail - 1);
        next_log_entry->fields.ofst = last_entry->fields.ofst + last_entry->fields.dlen; 
    }
   
    uint64_t extra = 0; 
    if (metadata_entries[id].in_memory_idx < 0) {
        // Shift data to be sector aligned
        if (next_log_entry->fields.ofst % general_spdk_info.sector_size != 0) {
            next_log_entry->fields.ofst = ((next_log_entry->fields.ofst >> general_spdk_info.sector_bit) + 1) << general_spdk_info.sector_bit;
        }
        
        // Read in an lba
        uint64_t r_vaddr = metadata_entries[id].persist_metadata_info->fields.tail * sizeof(LogEntry);
        if (r_vaddr % general_spdk_info.sector_size != 0) {
            extra = (r_vaddr & ((1ULL << general_spdk_info.sector_bit) - 1)) / sizeof(LogEntry);
            uint64_t r_paddr = LOG_AT_TABLE(id)[(r_vaddr >> SPDK_SEGMENT_BIT) % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH] << SPDK_SEGMENT_BIT;
            io_request_t data_request;
            r_paddr = r_paddr + (r_vaddr & SPDK_SEGMENT_MASK);
            data_request.lba = r_paddr >> general_spdk_info.sector_bit;
            data_request.lba_count = 1;
            void* buf = (void*)(metadata_entries[id].log_write_buffer + (metadata_entries[id].persist_metadata_info->fields.tail - extra)); 
            data_request.buf = buf;
            std::atomic<bool>* completed = new std::atomic<bool>();
            general_cbfn_args* cbfn_args = new general_cbfn_args();
            cbfn_args->completed = completed;
            cbfn_args->dlen = general_spdk_info.sector_size;
            data_request.args = (void*) cbfn_args;
            data_request.cb_fn = read_request_complete;
            data_request.request_type = READ;

            io_queue_mtx.lock();
            io_queue.push(data_request);
            new_io_request.notify_one();
            io_queue_mtx.unlock();

            while(!(*completed));
        }
    }        
    
    // Step 2: Update data info
    if (((next_log_entry->fields.ofst + data_size) >> general_spdk_info.sector_bit) - (metadata_entries[id].last_written_addr >> general_spdk_info.sector_bit) >= DATA_BUFFER_SIZE) {
        throw derecho::derecho_exception("Covering data that have not been persisted.");
    }
    memcpy(metadata_entries[id].data_write_buffer + (next_log_entry->fields.ofst % (DATA_BUFFER_SIZE << general_spdk_info.sector_bit)), pdata, data_size);   

    // Step 3: Update metadata entry
    metadata_entries[id].persist_metadata_info->fields.tail++;
    metadata_entries[id].persist_metadata_info->fields.ver = ver;
    if (metadata_entries[id].in_memory_idx < 0) {
       metadata_entries[id].in_memory_idx = metadata_entries[id].persist_metadata_info->fields.tail - 1 - extra;
       metadata_entries[id].in_memory_addr = next_log_entry->fields.ofst;
    } else {
       metadata_entries[id].in_memory_idx = std::max(metadata_entries[id].in_memory_idx.load(), metadata_entries[id].persist_metadata_info->fields.tail- LOG_BUFFER_SIZE);
       
       metadata_entries[id].in_memory_addr = std::max(metadata_entries[id].in_memory_addr.load(), (uint64_t)((next_log_entry->fields.ofst + next_log_entry->fields.dlen > (DATA_BUFFER_SIZE << general_spdk_info.sector_bit))? (next_log_entry->fields.ofst + next_log_entry->fields.dlen - (DATA_BUFFER_SIZE << general_spdk_info.sector_bit)) : 0)); 
    }
}

const version_t PersistThreads::persist(const uint32_t& id) {
    if (metadata_entries[id].last_submitted_idx == metadata_entries[id].persist_metadata_info->fields.tail - 1) {
        return metadata_entries[id].last_written_ver;
    }
    
    // Update to_write_metadata entry
    to_write_metadata[id].processing.lock();
    to_write_metadata[id].ver = metadata_entries[id].persist_metadata_info->fields.ver;
    to_write_metadata[id].metadata = *(metadata_entries[id].persist_metadata_info);

    metadata_entries[id].log_write_buffer_lock.lock(); 
    uint64_t to_submit_idx_start = metadata_entries[id].last_submitted_idx + 1;
    uint64_t to_submit_idx_end = to_write_metadata[id].metadata.fields.tail - 1;
    metadata_entries[id].last_submitted_idx = to_submit_idx_end;
    metadata_entries[id].log_write_buffer_lock.unlock(); 

    metadata_entries[id].data_write_buffer_lock.lock(); 
    uint64_t to_submit_addr_start = (to_submit_idx_start == 0) ? 0:std::max(metadata_entries[id].in_memory_addr.load(), metadata_entries[id].last_submitted_addr.load() + 1);
    uint64_t to_submit_addr_end = metadata_entries[id].log_write_buffer[(to_write_metadata[id].metadata.fields.tail - 1) % LOG_BUFFER_SIZE].fields.ofst + metadata_entries[id].log_write_buffer[(to_write_metadata[id].metadata.fields.tail - 1) % LOG_BUFFER_SIZE].fields.dlen - 1;
    metadata_entries[id].last_submitted_addr = to_submit_addr_end;
    metadata_entries[id].data_write_buffer_lock.unlock(); 
    to_write_metadata[id].processing.unlock(); 
    
    // Step 0: Extract virtual address span needed to be written 
    uint64_t log_virtaddr_start = (to_submit_idx_start * sizeof(LogEntry)) & general_spdk_info.sector_round_mask; 
    uint64_t log_virtaddr_end = ((to_submit_idx_end * sizeof(LogEntry)) & general_spdk_info.sector_round_mask) + general_spdk_info.sector_size;
    uint64_t data_virtaddr_start = to_submit_addr_start & general_spdk_info.sector_round_mask;
    uint64_t data_virtaddr_end = (to_submit_addr_end & general_spdk_info.sector_round_mask) + general_spdk_info.sector_size;

    // Step 1: Calculate needed segment number
    uint16_t needed_segment = 0;
    uint16_t needed_log_segment = 0;
    uint16_t needed_data_segment = 0;
   
    uint64_t virtual_log_segment_start = log_virtaddr_start >> SPDK_SEGMENT_BIT; 
    uint64_t virtual_log_segment_end = log_virtaddr_end >> SPDK_SEGMENT_BIT;
    needed_segment += virtual_log_segment_end - virtual_log_segment_start;
    needed_log_segment += virtual_log_segment_end - virtual_log_segment_start;
    if(LOG_AT_TABLE(id)[virtual_log_segment_start] == 0) {
        needed_segment++;
        needed_log_segment += 1;
    } else {
        virtual_log_segment_start++;
    }	
    
    uint64_t virtual_data_segment_start = data_virtaddr_start >> SPDK_SEGMENT_BIT; 
    uint64_t virtual_data_segment_end = data_virtaddr_end >> SPDK_SEGMENT_BIT;
    needed_segment += virtual_data_segment_end - virtual_data_segment_start;
    needed_data_segment += virtual_data_segment_end - virtual_data_segment_start;
    if(DATA_AT_TABLE(id)[virtual_data_segment_start] == 0) {
        needed_segment++;
        needed_data_segment += 1;
    } else {
        virtual_data_segment_start++;
    }

    if(pthread_mutex_lock(&segment_assignment_lock) != 0) {
        // failed to grab the lock
        throw derecho::derecho_exception("Failed to grab segment assignment lock.");
    }

    // Step 2: search for available segment
    int seg_index = 1;
    std::set<uint16_t> available_segment_index;
    while(available_segment_index.size() < needed_segment) {
        if(segment_usage_table[seg_index] == 0) {
            available_segment_index.insert(seg_index);
        }
        seg_index++;
    }
    if(available_segment_index.size() < needed_segment) {
        pthread_mutex_unlock(&segment_assignment_lock);
        throw derecho::derecho_exception("Available segments are not enough.");
    }

    // Step 3: Submit data request for segment address translation table update
    std::vector<atomic_sub_req> sub_requests;
    std::set<uint16_t>::iterator it = available_segment_index.begin();
    for (uint64_t i = virtual_log_segment_start; i <= virtual_log_segment_end; i++) {
        LOG_AT_TABLE(id)[i] = *it;
        segment_usage_table[*it] = 1;
        it++;
    }

    if (needed_log_segment > 0) {
        uint64_t virtaddress = (segment_address_log_location(id, virtual_log_segment_start)) & general_spdk_info.sector_round_mask;
        uint32_t dlen = needed_log_segment * sizeof(uint16_t) + (segment_address_log_location(id, virtual_log_segment_start) & general_spdk_info.sector_mask); 
        uint8_t* start = ((uint8_t*)(&LOG_AT_TABLE(id)[virtual_log_segment_start % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH])) - (segment_address_log_location(id, virtual_log_segment_start) & general_spdk_info.sector_mask);
    
        uint32_t cutoff = 0;
        if (virtual_log_segment_start / SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH != virtual_log_segment_end / SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH) {
            cutoff = (SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH - virtual_log_segment_start % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH) * sizeof(uint16_t);
        }
    
        if (cutoff != 0) {
            atomic_sub_req log_at_request_one;
            atomic_sub_req log_at_request_two;

            log_at_request_one.buf = start;
            log_at_request_one.data_length = cutoff;
            log_at_request_one.virtaddress = virtaddress;
            log_at_request_one.content_type = METADATA;
            sub_requests.push_back(log_at_request_one);

            log_at_request_two.buf = (uint8_t*)(&LOG_AT_TABLE(id)[0]);
            log_at_request_two.data_length = dlen - cutoff;
            log_at_request_two.virtaddress = virtaddress + cutoff;
            log_at_request_two.content_type = METADATA;
            sub_requests.push_back(log_at_request_two);
        } else {
            atomic_sub_req log_at_request;
            log_at_request.buf = start;
            log_at_request.data_length = dlen;
            log_at_request.virtaddress = virtaddress;
            log_at_request.content_type = METADATA;
            sub_requests.push_back(log_at_request);
        }
    }

    // TODO:THINK OVER THIS!!!
    uint16_t i = 0;
    while(it != available_segment_index.end()) {
        DATA_AT_TABLE(id)[virtual_data_segment_start + i] = *it;
        segment_usage_table[*it] = 1;
        it++;
        i++;
    }

    if (needed_data_segment >= 0) {
        uint64_t virtaddress = segment_address_data_location(id, virtual_data_segment_start) & general_spdk_info.sector_round_mask;
        uint32_t dlen = needed_data_segment * sizeof(uint16_t) + (segment_address_data_location(id, virtual_data_segment_start) & general_spdk_info.sector_mask);
        uint8_t* start = ((uint8_t*)(&DATA_AT_TABLE(id)[virtual_data_segment_start % SPDK_DATA_ADDRESS_TABLE_LENGTH])) - (segment_address_log_location(id, virtual_data_segment_start) & general_spdk_info.sector_mask);
   
        uint32_t cutoff = 0;
        if (virtual_data_segment_start / SPDK_DATA_ADDRESS_TABLE_LENGTH != virtual_data_segment_end / SPDK_DATA_ADDRESS_TABLE_LENGTH) {
            cutoff = (SPDK_DATA_ADDRESS_TABLE_LENGTH - virtual_data_segment_start % SPDK_DATA_ADDRESS_TABLE_LENGTH) * sizeof(uint16_t);
        }

        if (cutoff != 0) {
            atomic_sub_req data_at_request_one;
            atomic_sub_req data_at_request_two;
            
            data_at_request_one.buf = start;
            data_at_request_one.data_length = cutoff;
            data_at_request_one.virtaddress = virtaddress;
            data_at_request_one.content_type = METADATA;
            sub_requests.push_back(data_at_request_one);

            data_at_request_two.buf = (uint8_t*)(&DATA_AT_TABLE(id)[0]);
            data_at_request_two.data_length = dlen - cutoff;
            data_at_request_two.virtaddress = virtaddress + cutoff;
            data_at_request_two.content_type = METADATA;
            sub_requests.push_back(data_at_request_two);
        } else {
            atomic_sub_req data_at_request;
            data_at_request.buf = start;
            data_at_request.data_length = dlen;
            data_at_request.virtaddress = virtaddress;
            data_at_request.content_type = METADATA;
            sub_requests.push_back(data_at_request);
        }
    }
    pthread_mutex_unlock(&segment_assignment_lock);
    
    // Step 4: Submit data request
    atomic_sub_req log_entry_request;
    uint64_t log_virtaddress = (to_submit_idx_start * sizeof(LogEntry)) & general_spdk_info.sector_round_mask; 
    log_entry_request.buf = (uint8_t*)(&metadata_entries[id].log_write_buffer[to_submit_idx_start % LOG_BUFFER_SIZE]) - ((to_submit_idx_start * sizeof(LogEntry)) & general_spdk_info.sector_mask);
    log_entry_request.data_length = (to_submit_idx_end - to_submit_idx_start + 1) * sizeof(LogEntry) + ((to_submit_idx_start * sizeof(LogEntry)) & general_spdk_info.sector_mask);
    log_entry_request.virtaddress = log_virtaddress;
    log_entry_request.content_type = LOGENTRY;
    sub_requests.push_back(log_entry_request);

    atomic_sub_req data_request;
    data_request.buf = metadata_entries[id].data_write_buffer + (to_submit_addr_start % (DATA_BUFFER_SIZE << general_spdk_info.sector_bit)) - (to_submit_addr_start & general_spdk_info.sector_mask);
    data_request.data_length = (to_submit_addr_end - to_submit_addr_start + 1) + (to_submit_addr_start & general_spdk_info.sector_mask);
    data_request.virtaddress = to_submit_addr_start & general_spdk_info.sector_round_mask;
    data_request.content_type = DATA;
    sub_requests.push_back(data_request);
	
    //Step 4: Submit control request
    atomic_w(sub_requests, to_write_metadata[id].metadata, id, to_submit_idx_end, to_submit_addr_end);
    return metadata_entries[id].last_written_ver;
}

void PersistThreads::update_metadata(const uint32_t& id, PTLogMetadataInfo metadata) {
    std::atomic<int> completed = 0;
    
    // Update to_write_metadata entry
    to_write_metadata[id].processing.lock();
    to_write_metadata[id].ver = metadata.fields.ver;
    to_write_metadata[id].metadata = metadata;
    to_write_metadata[id].processing.unlock();
    
    // Submit io request 
    io_request_t metadata_request;
    metadata_request.buf = spdk_malloc(sizeof(PTLogMetadataInfo), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    std::memcpy(metadata_request.buf, (void*)&metadata, sizeof(PTLogMetadataInfo));
    metadata_request.lba =  (id * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataAddress)) >> get()->general_spdk_info.sector_bit;
    metadata_request.lba_count = PAGE_SIZE >> general_spdk_info.sector_bit;
    metadata_request.cb_fn = metadata_write_request_complete;
    metadata_request.request_type = WRITE;
    metadata_write_cbfn_args* cbfn_args = new metadata_write_cbfn_args();
    cbfn_args->id = id;
    cbfn_args->ver = metadata.fields.ver;
    metadata_request.args = cbfn_args;
    
    metadata_io_queue_mtx.lock();
    metadata_io_queue.push(metadata_request);
    new_metadata_request.notify_one();
    metadata_io_queue_mtx.unlock();    
}

void PersistThreads::load(const std::string& name, LogMetadata* log_metadata) {
    if(!loaded) {
        //Step 0: submit read request for the segment of all global data
        char* buf = (char*)spdk_malloc(SPDK_SEGMENT_SIZE, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        non_atomic_rw(buf, SPDK_SEGMENT_SIZE, 0, BLOCKING, METADATA, false, 0);
        pt_global_metadata = (GlobalMetadata*)spdk_malloc(sizeof(GlobalMetadata), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA); 

        //Step 2: Construct log_name_to_id and segment usage array
        size_t ofst = 0;
        for(size_t id = 0; id < SPDK_NUM_LOGS_SUPPORTED; id++) {
            //Step 2_0: copy data into metadata entry
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH, (uint8_t*)&LOG_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH, (uint8_t*)&DATA_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH;
            pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info = *((PTLogMetadataInfo*)(buf + ofst));
            ofst += sizeof(PTLogMetadataInfo);

            if(pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info.fields.inuse) {
                //Step 2_1: Update log_name_to_id
                log_name_to_id.insert(std::pair<std::string, uint32_t>((char*)pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info.fields.name, id));

                //Step 2_2: Update segment_usage_array
                for(size_t index = 0; index < SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH; index++) {
                    if(LOG_AT_TABLE(id)[index] != 0) {
                        segment_usage_table[LOG_AT_TABLE(id)[index]].flip();
                    }
                }
                for(size_t index = 0; index < SPDK_DATA_ADDRESS_TABLE_LENGTH; index++) {
                    if(DATA_AT_TABLE(id)[index] != 0) {
                        segment_usage_table[DATA_AT_TABLE(id)[index]].flip();
                    }
                }
            }
        }
        spdk_free((void*)buf);
        

        loaded = true;
    }

    //Step 3: Update log_metadata
    try {
        uint32_t id = log_name_to_id.at(name);
        log_metadata->persist_metadata_info = &pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info;

        if (metadata_entries[id].log_write_buffer != nullptr) {
            return;
        }
        metadata_entries[id].persist_metadata_info = &pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info;

        // Step 1: memory metadata entry info
        metadata_entries[id].last_written_ver = pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info.fields.ver;
        metadata_entries[id].last_written_idx = pt_global_metadata->fields.log_metadata_entries[id].fields.log_metadata_info.fields.tail - 1;
        metadata_entries[id].last_submitted_idx = metadata_entries[id].last_written_idx.load();
        metadata_entries[id].in_memory_idx = -1;
        metadata_entries[id].in_memory_addr = 0;
        metadata_entries[id].last_written_addr = 0;
        metadata_entries[id].last_submitted_addr = 0;
      
        // Step 2: allocate buffer area
        metadata_entries[id].log_write_buffer = (LogEntry*)spdk_malloc(LOG_BUFFER_SIZE * general_spdk_info.sector_size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
       
        // Step 2.1: open files for memory mapping 
        metadata_entries[id].dw_fd = open(("/mnt/huge/" + name + ".datawrite").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
        if (metadata_entries[id].dw_fd == -1) {
           throw derecho::derecho_exception("Failed to open data write file.");
        }

        // Step 2.2: write buffer and read buffer for data need to be allocated twice
        metadata_entries[id].data_write_buffer = (uint8_t*)mmap(NULL, DATA_BUFFER_SIZE << (general_spdk_info.sector_bit + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);
        
        if (mmap((void*)metadata_entries[id].data_write_buffer, DATA_BUFFER_SIZE << general_spdk_info.sector_bit, PROT_READ | PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].dw_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map first data memory."); 
        }

        if (mmap((void*)(metadata_entries[id].data_write_buffer + (DATA_BUFFER_SIZE << general_spdk_info.sector_bit)), DATA_BUFFER_SIZE << general_spdk_info.sector_bit, PROT_READ | PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].dw_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map second data memory."); 
        }

        int drc = spdk_mem_register((void*)metadata_entries[id].data_write_buffer, DATA_BUFFER_SIZE << (general_spdk_info.sector_bit + 1));
        if (drc != 0) {
            throw derecho::derecho_exception("Failed to register data memory.");
        }	       

        // Allocate log read buffer
        metadata_entries[id].log_rd_fd = open(("/mnt/huge/readlog" + name + ".log").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
        if (metadata_entries[id].log_rd_fd == -1) {
           throw derecho::derecho_exception("Failed to open read file.");
        }

        metadata_entries[id].log_read_buffer = (uint8_t*)mmap(NULL, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);

        if (mmap((void*)metadata_entries[id].log_read_buffer, READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].log_rd_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map memory."); 
        }

        if (mmap((void*)(metadata_entries[id].log_read_buffer + (READ_BUFFER_SIZE << READ_BATCH_BIT)), READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].log_rd_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map memory."); 
        }
        
        int log_rc = spdk_mem_register((void*)metadata_entries[id].log_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1)); 
        if (log_rc != 0) {
            std::cout << "Error code is " << log_rc << std::endl;
            throw derecho::derecho_exception("Failed to register memory.");
        }

        // Allocate read buffer
        metadata_entries[id].data_rd_fd = open(("/mnt/huge/readdata" + name + ".log").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
        if (metadata_entries[id].data_rd_fd == -1) {
           throw derecho::derecho_exception("Failed to open read file.");
        }

        metadata_entries[id].data_read_buffer = (uint8_t*)mmap(NULL, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);

        if (mmap((void*)metadata_entries[id].data_read_buffer, READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].data_rd_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map memory."); 
        }

        if (mmap((void*)(metadata_entries[id].data_read_buffer + (READ_BUFFER_SIZE << READ_BATCH_BIT)), READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[id].data_rd_fd, 0) == MAP_FAILED) {
            throw derecho::derecho_exception("Failed to map memory."); 
        }
        
        int data_rc = spdk_mem_register((void*)metadata_entries[id].data_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1)); 
        if (data_rc != 0) {
            throw derecho::derecho_exception("Failed to register memory.");
        }

        for (int i = 0; i < READ_BUFFER_SIZE; i++) {
            metadata_entries[id].log_idx_to_numref[i] = 0;
        }

        for (int i = 0; i < READ_BUFFER_SIZE; i++) {
            metadata_entries[id].data_idx_to_numref[i] = 0;
        }

        return;
    } catch (const std::out_of_range& oor) {
        // Find an unused metadata entry
        if(pthread_mutex_lock(&metadata_entry_assignment_lock) != 0) {
            // throw an exception
            throw derecho::derecho_exception("Failed to grab metadata entry assignment lock.");
        }
        for(uint32_t index = 0; index < SPDK_NUM_LOGS_SUPPORTED; index++) {
            if(!pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse) {
                pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse = true;
                pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.id = index;
                pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.head = 0;
                pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.tail = 0;
                pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.ver = -1;

                strcpy((char *)pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info.fields.name, name.c_str());
                memset(pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_address.segment_log_entry_at_table, 0, sizeof(pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_address.segment_log_entry_at_table));
                memset(pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_address.segment_data_at_table, 0, sizeof(pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_address.segment_data_at_table));
                log_metadata->persist_metadata_info= &pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info;
		metadata_entries[index].persist_metadata_info = &pt_global_metadata->fields.log_metadata_entries[index].fields.log_metadata_info;
                log_name_to_id.insert(std::pair<std::string, uint32_t>(name, index));

                pthread_mutex_unlock(&metadata_entry_assignment_lock);

                // Step 1: memory metadata entry info
                metadata_entries[index].last_written_ver = -1;
                metadata_entries[index].last_written_idx = -1; 
                metadata_entries[index].last_submitted_idx = -1;
                metadata_entries[index].in_memory_idx = -1;
                metadata_entries[index].in_memory_addr = 0;
                metadata_entries[index].last_written_addr = 0; 
                metadata_entries[index].last_submitted_addr = 0;

                // Step 2: allocate buffer area
                metadata_entries[index].log_write_buffer = (LogEntry*)spdk_malloc(LOG_BUFFER_SIZE * sizeof(LogEntry), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

                // Step 2.1: open files for memory mapping 
                metadata_entries[index].dw_fd = open(("/mnt/huge/" + name + ".datawrite").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
                if (metadata_entries[index].dw_fd == -1) {
                    throw derecho::derecho_exception("Failed to open data write file.");
                }

                // Step 2.2: write buffer and read buffer for data need to be allocated twice
                metadata_entries[index].data_write_buffer = (uint8_t*)mmap(NULL, DATA_BUFFER_SIZE << (general_spdk_info.sector_bit + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);

                if (mmap((void*)metadata_entries[index].data_write_buffer, DATA_BUFFER_SIZE << general_spdk_info.sector_bit, PROT_READ | PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].dw_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map first data memory."); 
                }

                if (mmap((void*)(metadata_entries[index].data_write_buffer + (DATA_BUFFER_SIZE << general_spdk_info.sector_bit)), DATA_BUFFER_SIZE << general_spdk_info.sector_bit, PROT_READ | PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].dw_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map second data memory."); 
                }

                int drc = spdk_mem_register((void*)metadata_entries[index].data_write_buffer, DATA_BUFFER_SIZE << (general_spdk_info.sector_bit + 1));
                if (drc != 0) {
                    throw derecho::derecho_exception("Failed to register data memory.");
                }

                // Allocate log read buffer
                metadata_entries[index].log_rd_fd = open(("/mnt/huge/readlog" + name + ".log").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
                if (metadata_entries[index].log_rd_fd == -1) {
                    throw derecho::derecho_exception("Failed to open read file.");
                }

                metadata_entries[index].log_read_buffer = (uint8_t*)mmap(NULL, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);

                if (mmap((void*)metadata_entries[index].log_read_buffer, READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].log_rd_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map memory."); 
                }

                if (mmap((void*)(metadata_entries[index].log_read_buffer + (READ_BUFFER_SIZE << READ_BATCH_BIT)), READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].log_rd_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map memory."); 
                }

                int log_rc = spdk_mem_register((void*)metadata_entries[index].log_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1)); 
                if (log_rc != 0) {
                    throw derecho::derecho_exception("Failed to register memory.");
                }

                // Allocate read buffer
                metadata_entries[index].data_rd_fd = open(("/mnt/huge/readdata" + name + ".log").c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
                if (metadata_entries[index].data_rd_fd == -1) {
                    throw derecho::derecho_exception("Failed to open read file.");
                }

                metadata_entries[index].data_read_buffer = (uint8_t*)mmap(NULL, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1), PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE|MAP_HUGETLB, -1, 0);

                if (mmap((void*)metadata_entries[index].data_read_buffer, READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].data_rd_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map memory."); 
                }

                if (mmap((void*)(metadata_entries[index].data_read_buffer + (READ_BUFFER_SIZE << READ_BATCH_BIT)), READ_BUFFER_SIZE << READ_BATCH_BIT, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, metadata_entries[index].data_rd_fd, 0) == MAP_FAILED) {
                    throw derecho::derecho_exception("Failed to map memory."); 
                }

                int data_rc = spdk_mem_register((void*)metadata_entries[index].data_read_buffer, READ_BUFFER_SIZE << (READ_BATCH_BIT + 1)); 
                if (data_rc != 0) {
                    throw derecho::derecho_exception("Failed to register memory.");
                }

                for (int i = 0; i < READ_BUFFER_SIZE; i++) {
                    metadata_entries[index].log_idx_to_numref[i] = 0;
                }

                for (int i = 0; i < READ_BUFFER_SIZE; i++) {
                    metadata_entries[index].data_idx_to_numref[i] = 0;
                }

		return;
            }
        }
        //no available metadata entry
        pthread_mutex_unlock(&metadata_entry_assignment_lock);
        throw derecho::derecho_exception("No available metadata entry.");
    }
}

std::tuple<LogEntry*, PersistThreads::Guard> PersistThreads::read_entry(const uint32_t& id, const int64_t& index) {
    uint64_t virtaddress = index * sizeof(LogEntry);
    // Step 0: Check whether existng
    if (index >= metadata_entries[id].persist_metadata_info->fields.tail) {
        throw derecho::derecho_exception("LogEntry of this idx does not exist");   
    }  
    if (index - metadata_entries[id].last_written_idx > LOG_BUFFER_SIZE) {
        throw derecho::derecho_exception("Run out of memory space for LogEntry");
    }
    
    // Step 1: Check whether in write buffer
    if (metadata_entries[id].in_memory_idx >= 0) {
        if (metadata_entries[id].in_memory_idx <= index && metadata_entries[id].in_memory_idx + LOG_BUFFER_SIZE > index) {
            metadata_entries[id].log_write_buffer_lock.lock();
            Guard guard(0, 0, id, true, true);
            std::tuple<LogEntry*, PersistThreads::Guard> res (&metadata_entries[id].log_write_buffer[index % LOG_BUFFER_SIZE], std::move(guard));
            return res;
        }
    }

    // Step 2: Check whether in read buffer
    uint64_t virt_batch = virtaddress >> READ_BATCH_BIT;
    uint16_t buf_batch = virt_batch % READ_BUFFER_SIZE;
    metadata_entries[id].log_idx_to_mtx[buf_batch].lock();
    if (metadata_entries[id].log_idx_to_batch.find(buf_batch) != metadata_entries[id].log_idx_to_batch.end() && metadata_entries[id].log_idx_to_batch[buf_batch] == virt_batch) {
        // Step 2.1 : Check readed seg length
        if (metadata_entries[id].log_read_buf_index[virt_batch] >= ((virtaddress + sizeof(LogEntry)) & ((1ULL << READ_BATCH_BIT) - 1))) {
            metadata_entries[id].log_idx_to_numref[buf_batch]++;
            Guard guard(buf_batch, 1, id, false, true);
            metadata_entries[id].log_idx_to_mtx[buf_batch].unlock();
            std::tuple<LogEntry*, PersistThreads::Guard> res ((LogEntry*)(metadata_entries[id].log_read_buffer + (buf_batch << READ_BATCH_BIT) + (virtaddress & ((1ULL << READ_BATCH_BIT) - 1))), std::move(guard));
            return res;
        } 
    }
    metadata_entries[id].log_idx_to_mtx[buf_batch].unlock();


    // Step 3: Issue read request
    // Wait for available read buffer slot
    std::unique_lock<std::mutex> lock(metadata_entries[id].log_idx_to_mtx[buf_batch]);
    if (metadata_entries[id].log_idx_to_batch[buf_batch] != virt_batch) {
        while (metadata_entries[id].log_idx_to_numref[buf_batch] > 0) {
            metadata_entries[id].log_idx_to_cv[buf_batch].wait(lock);
        }

        // Claim the available slot
        metadata_entries[id].log_idx_to_batch[buf_batch] = virt_batch;
        metadata_entries[id].log_idx_to_numref[buf_batch] = 1;
    }

    char* buf = (char*)(metadata_entries[id].log_read_buffer + (buf_batch << READ_BATCH_BIT));

    uint64_t r_virtaddress = virtaddress & (~((1ULL << READ_BATCH_BIT) - 1));
    uint32_t size = std::min((uint64_t)(1ULL << READ_BATCH_BIT), (uint64_t)((metadata_entries[id].last_written_idx * sizeof(LogEntry)) - r_virtaddress));
    non_atomic_rw(buf, (uint32_t)(1ULL << READ_BATCH_BIT), r_virtaddress, BLOCKING, LOGENTRY, false, id); 
    // Step 4: Update read buffer info
    metadata_entries[id].log_read_buf_index[virt_batch] = size;
    Guard guard(buf_batch, 1, id, false, true);
    std::tuple<LogEntry*, PersistThreads::Guard> res ((LogEntry*)(buf + (virtaddress & ((1ULL << READ_BATCH_BIT) - 1))), std::move(guard));
    return res; 
}

bool myComp(uint64_t i, uint64_t j) {
    return ((i % READ_BUFFER_SIZE) < (j % READ_BUFFER_SIZE));
}

std::tuple<void*, PersistThreads::Guard> PersistThreads::read_data(const uint32_t& id, const int64_t& index) {
    LogEntry* log_entry;
    Guard guard;
    auto res = read_entry(id, index);
    log_entry = std::get<0>(res);
    guard = std::move(std::get<1>(res));
   
    uint64_t ofst = log_entry->fields.ofst;
    uint64_t dlen = log_entry->fields.dlen;
    guard.~Guard();
    
    uint64_t virtaddress_start = ofst;
    uint64_t virtaddress_end = ofst + dlen - 1;
    uint16_t virt_batch_start = virtaddress_start >> READ_BATCH_BIT;
    uint16_t virt_batch_end = virtaddress_end >> READ_BATCH_BIT; 
    // Sort the order of accessing batchs
    std::vector<uint64_t> virt_bid_set;
    std::vector<uint16_t> bid_set;
    for (uint64_t i = virt_batch_start; i <= virt_batch_end; i++) {
        virt_bid_set.push_back(i);
    }
    std::sort(virt_bid_set.begin(), virt_bid_set.end(), myComp);   
    
    for (uint64_t i : virt_bid_set) {
        uint64_t buf_batch = i % READ_BUFFER_SIZE;
        bid_set.push_back(buf_batch);
        
        // Step 1: Check whether in write buffer
        if (metadata_entries[id].in_memory_idx >= 0) {
            if (metadata_entries[id].in_memory_addr <= (i << READ_BATCH_BIT) && std::min(((i + 1) << READ_BATCH_BIT) - 1, virtaddress_end) < metadata_entries[id].in_memory_addr + DATA_BUFFER_SIZE) {
                std::unique_lock<std::mutex> lock(metadata_entries[id].log_idx_to_mtx[buf_batch]);
                if (metadata_entries[id].data_idx_to_batch[buf_batch] != i) {
                    while (metadata_entries[id].data_idx_to_numref[buf_batch] > 0) {
                        metadata_entries[id].data_idx_to_cv[buf_batch].wait(lock);
                    }
                    metadata_entries[id].data_idx_to_batch[buf_batch] = i;
                    metadata_entries[id].data_idx_to_numref[buf_batch] = 1;
                }
                
                // Copy the content from write buffer to read buffer
                memcpy((char*)(metadata_entries[id].data_read_buffer + (buf_batch << READ_BATCH_BIT)), metadata_entries[id].data_write_buffer + ((i << READ_BATCH_BIT) % DATA_BUFFER_SIZE), 1 << READ_BATCH_BIT);
                metadata_entries[id].data_read_buf_index[i] = std::min(1 << READ_BATCH_BIT, (int)(metadata_entries[id].in_memory_addr + DATA_BUFFER_SIZE  - (i << READ_BATCH_BIT)));
		continue;
            }
        }

        // Step 2: Check whether in read buffer
        metadata_entries[id].data_idx_to_mtx[buf_batch].lock();
        if (metadata_entries[id].data_idx_to_batch.find(buf_batch) != metadata_entries[id].data_idx_to_batch.end() && metadata_entries[id].data_idx_to_batch[buf_batch] == i) {
            if (i == virt_batch_end) {
                if ((virtaddress_end & ((1 << READ_BATCH_BIT) - 1)) <= metadata_entries[id].data_read_buf_index[buf_batch]) {
                    metadata_entries[id].data_idx_to_numref[buf_batch]++;
                    metadata_entries[id].data_idx_to_mtx[buf_batch].unlock();
		    continue;
                }
            } else {
                if (metadata_entries[id].data_read_buf_index[i] == 1 << READ_BATCH_BIT) {
                    metadata_entries[id].data_idx_to_numref[buf_batch]++;
                    metadata_entries[id].data_idx_to_mtx[buf_batch].unlock();
		    continue;
                }
            }
        }
        metadata_entries[id].data_idx_to_mtx[buf_batch].unlock();

        // Step 3: Issue read request
        // Wait for available read buffer slot
        std::unique_lock<std::mutex> lock(metadata_entries[id].data_idx_to_mtx[buf_batch]);
        if (metadata_entries[id].data_idx_to_batch[buf_batch] != i) {
            while (metadata_entries[id].data_idx_to_numref[buf_batch] > 0) {
                metadata_entries[id].data_idx_to_cv[buf_batch].wait(lock);
            }

            // Claim the available slot
            metadata_entries[id].data_idx_to_batch[buf_batch] = i;
            metadata_entries[id].data_idx_to_numref[buf_batch] = 1;
        }

        char* buf = (char*)(metadata_entries[id].data_read_buffer + (buf_batch << READ_BATCH_BIT));

        uint64_t r_virtaddress = (i << READ_BATCH_BIT) & (~((1ULL << READ_BATCH_BIT) - 1));
        // uint32_t size = (i == phys_batch_end) ? (virtaddress_end & ((1 << READ_BATCH_BIT) - 1)):(1ULL << READ_BATCH_BIT);
        uint32_t size;
        if (metadata_entries[id].in_memory_idx < 0) {
            LogEntry* last_entry;
            Guard guard;
            std::tie(last_entry, guard) = read_entry(id, metadata_entries[id].persist_metadata_info->fields.tail - 1);
            size = std::min((uint32_t)(1ULL << READ_BATCH_BIT), (uint32_t)(last_entry->fields.dlen + last_entry->fields.ofst - r_virtaddress));
        } else {
            size = std::min((uint32_t)(1ULL << READ_BATCH_BIT), (uint32_t)(metadata_entries[id].last_written_addr - r_virtaddress));
        } 
        non_atomic_rw(buf, size, r_virtaddress, BLOCKING, DATA, false, id);
        metadata_entries[id].data_read_buf_index[i] = size; 
    }
    
    Guard r_guard(virt_batch_start % READ_BUFFER_SIZE, (virt_batch_end - virt_batch_start + 1), id, false, false);
    char* buf = (char*)(metadata_entries[id].data_read_buffer + ((virt_batch_start % READ_BUFFER_SIZE) << READ_BATCH_BIT) + (virtaddress_start & ((1ULL << READ_BATCH_BIT) - 1)));
    return std::make_tuple((void*)buf, std::move(r_guard));
}

void* PersistThreads::read_lba(const uint64_t& lba_index) {
   io_request_t data_request;
   data_request.lba = lba_index;
   data_request.lba_count = 1;
   void* buf = spdk_malloc(general_spdk_info.sector_size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
   data_request.buf = buf;
   std::atomic<bool>* completed = new std::atomic<bool>();
   general_cbfn_args* cbfn_args = new general_cbfn_args();
   cbfn_args->completed = completed;
   cbfn_args->dlen = general_spdk_info.sector_size;
   data_request.args = (void*) cbfn_args;
   data_request.cb_fn = read_request_complete;
   data_request.request_type = READ;

   io_queue_mtx.lock();
   io_queue.push(data_request);
   new_io_request.notify_one();
   io_queue_mtx.unlock();
  
   while(!(*completed));
   return buf;
}

}  // namespace spdk
}  // namespace persistent
