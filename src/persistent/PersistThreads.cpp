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
#define SPDK_SEGMENT_MASK ((1 << SPDK_SEGMENT_BIT) - 1)
const uint32_t MAX_LBA_COUNT = 130816;

namespace persistent {
namespace spdk {

PersistThreads* PersistThreads::m_PersistThread;
bool PersistThreads::initialized;
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
    spdk_free(cbfn_args->buf);
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
    std::printf("Get new metadata write complete. \n");
    std::cout.flush();
    // if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("control request failed.");
    }
    metadata_write_cbfn_args* cbfn_args = (metadata_write_cbfn_args*) args;
    get()->last_written_ver[cbfn_args->id] = cbfn_args->ver;
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
        //std::printf("Starting lba_index %d.\n", lba_index);
        //std::cout.flush();

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

int PersistThreads::atomic_w(std::vector<atomic_sub_req> sub_requests, PTLogMetadataInfo metadata, uint32_t id){
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
                data_request.buf = spdk_malloc(curr_size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
                std::memcpy(data_request.buf, ((uint8_t*)req.buf) + ofst + curr_ofst, curr_size);
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
    to_write_metadata[id].processing.lock();
    to_write_metadata[id].ver = metadata.fields.ver;
    to_write_metadata[id].metadata = metadata;
    to_write_metadata[id].processing.unlock();

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

int PersistThreads::initialize_threads() {
    // TODO: using first available device and namespace. This should come from configuration file.
    // Step 0: initialize spdk nvme info
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
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
            std::printf("qpair failed.\n");
            std::cout.flush();
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
        	
    // Step 4: initialize io threads
    for(int io_thread_idx = 0; io_thread_idx < NUM_IO_THREAD; io_thread_idx++) {
        io_threads[io_thread_idx] = std::thread([&, io_thread_idx]() {
            const int thread_id = io_thread_idx;
            uncompleted_io_req[thread_id] = 0;
            //TODO: Assuming that each sub req is of size 32KB
            uncompleted_io_sub_req[thread_id] = 0;
            while(true) {
                io_queue_mtx.lock();
                if (!io_queue.empty()) {
                    io_request_t request = io_queue.front();
                    io_queue.pop();
		            io_queue_mtx.unlock();

		            while (uncompleted_io_req[thread_id] > general_spdk_info.qpair_requests) {
		                spdk_nvme_qpair_process_completions(spdk_qpair[thread_id], 0);
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
                                int rc = spdk_nvme_ns_cmd_read(general_spdk_info.ns, spdk_qpair[thread_id], request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
                                if (rc != 0){
			    	                std::fprintf(stderr, "Failed to initialize read io %d with lba_count %d.\n", rc, request.lba_count);
			    	                std::cout.flush();
			                    }
                                uncompleted_io_req[thread_id] += 1;
                            }
                            break;
                        
                        case WRITE:
		                    {
                                int rc = spdk_nvme_ns_cmd_write(general_spdk_info.ns, spdk_qpair[thread_id], request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
                                if (rc != 0){
			    	                std::fprintf(stderr, "Failed to initialize write io %d with lba_count %d.\n", rc, request.lba_count);
			    	                std::cout.flush();
			                    }
                                uncompleted_io_req[thread_id] += 1;
                            }
                            break;

                        default:
                            break;
                    }
                } else if (uncompleted_io_req[thread_id] > 0) {
                    io_queue_mtx.unlock();
                    spdk_nvme_qpair_process_completions(spdk_qpair[thread_id], 0);
                } else if (destructed) {
                    io_queue_mtx.unlock();
                    std::printf("thread destructed. \n");
                    std::cout.flush();
                    spdk_nvme_ctrlr_free_io_qpair(spdk_qpair[thread_id]);
                    exit(0);
                } else {
                    io_queue_mtx.unlock();
                    std::unique_lock<std::mutex> lk(io_queue_mtx);
                    new_io_request.wait(lk, [&]{return (destructed || !io_queue.empty());});
        	}
            }
        });
        io_threads[io_thread_idx].detach();
    }

    // Step 5: initialize metadata thread
    metadata_thread = std::thread([&]() {
        uncompleted_metadata_req = 0;
        while(true) {
            metadata_io_queue_mtx.lock();
            if (!metadata_io_queue.empty()) {
                io_request_t request = metadata_io_queue.front();
                metadata_io_queue.pop();
                metadata_io_queue_mtx.unlock();

                // Check whether space avaliable for the new req
                while (uncompleted_metadata_req > general_spdk_info.qpair_requests) {
                    spdk_nvme_qpair_process_completions(metadata_spdk_qpair, 0);
                }

                if (request.cb_fn == data_write_request_complete) {
                    ((data_write_cbfn_args*)(request.args))->io_thread_id = -1;
                }
                int rc = spdk_nvme_ns_cmd_write(general_spdk_info.ns, metadata_spdk_qpair, request.buf, request.lba, request.lba_count, request.cb_fn, request.args, 0);
                if (rc != 0){
                    std::fprintf(stderr, "Failed to initialize write io %d with lba_count %d.\n", rc, request.lba_count);
                    std::cout.flush();
                }
                uncompleted_metadata_req += 1;
            } else if (uncompleted_metadata_req > 0) {
                metadata_io_queue_mtx.unlock();
                spdk_nvme_qpair_process_completions(metadata_spdk_qpair, 0);
            } else if (destructed) {
                metadata_io_queue_mtx.unlock();
                std::printf("metadata destructed.\n");
                std::cout.flush();
                bool no_uc_data_req = true;
                for (int i = 0; i < NUM_IO_THREAD; i++){
                    if (uncompleted_io_req[no_uc_data_req] > 0) {
                        no_uc_data_req = false;
                        break;
                    } 
                }
                if (no_uc_data_req) {
                    spdk_nvme_ctrlr_free_io_qpair(metadata_spdk_qpair);
                    exit(0);
                }
            } else {
                metadata_io_queue_mtx.unlock();
                std::unique_lock<std::mutex> lk(metadata_io_queue_mtx);
                new_metadata_request.wait(lk, [&]{return (destructed || !metadata_io_queue.empty());});
            }
        }
    });
    metadata_thread.detach();
    return 0;
}

PersistThreads::PersistThreads() {}

PersistThreads::~PersistThreads() {
    destructed = true;
    new_io_request.notify_all();
    new_metadata_request.notify_all();
    // Wait for all requests to be submitted
    while (!io_queue.empty());
    // Wait for all submitted requests to be completed
    for (int i = 0; i < NUM_IO_THREAD; i++) {
        while (uncompleted_io_req[i] > 0);
    }
    // Wait for all metadata requests to be submitted
    while (!metadata_io_queue.empty());
    // Wait for all submitted requests to be completed
    while (uncompleted_io_req > 0);
}

void PersistThreads::append(const uint32_t& id, char* data, 
                            const uint64_t& data_size, void* log,
                            const uint64_t& log_index, PTLogMetadataInfo metadata) {
    // Step 0: extract virtual segment number and sector number
    id_to_log[id][log_index].fields.ofst = (id_to_log[id][log_index].fields.ofst + (general_spdk_info.sector_size - 1)) & general_spdk_info.sector_round_mask;
    uint64_t data_offset = id_to_log[id][log_index].fields.ofst;
    uint64_t virtual_data_segment = (data_offset >> SPDK_SEGMENT_BIT) % SPDK_DATA_ADDRESS_TABLE_LENGTH;
    uint64_t data_sector = (data_offset & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit;
     
    uint64_t virtual_log_segment = ((log_index * sizeof(LogEntry)) >> SPDK_SEGMENT_BIT) % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;

    // Step 1: Calculate needed segment number
    uint16_t needed_segment = 0;
    uint16_t needed_log_segment = 0;
    uint16_t needed_data_segment = 0;
    
    if(LOG_AT_TABLE(id)[virtual_log_segment] == 0) {
        needed_segment++;
        needed_log_segment = 1;
    }
    
    uint16_t needed_data_sector = 1 +  ((data_size - 1) >> general_spdk_info.sector_bit);
    if(DATA_AT_TABLE(id)[virtual_data_segment] == 0) {
        needed_data_segment = 1 + (needed_data_sector >> LBA_PER_SEG_BIT);
        needed_segment += needed_data_segment;
    } else {
        needed_data_segment += ((needed_data_sector + data_sector) >> LBA_PER_SEG_BIT);
        needed_segment += needed_data_segment;
    }

    uint16_t physical_data_segment = DATA_AT_TABLE(id)[virtual_data_segment];
    uint64_t physical_data_sector = physical_data_segment * LBA_PER_SEG + data_sector;

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

    //Calculate part num
    int data_assignment_start;
    if(physical_data_sector == 0) {
        data_assignment_start = virtual_data_segment;
    } else {
        data_assignment_start = virtual_data_segment + 1;
    }

    // Step 3: Submit data request for segment address translation table update
    std::vector<atomic_sub_req> sub_requests;
    std::set<uint16_t>::iterator it = available_segment_index.begin();
    if(needed_log_segment > 0) {
        LOG_AT_TABLE(id)[virtual_log_segment] = *it;
        segment_usage_table[*it] = 1;
        uint64_t virtaddress = (segment_address_log_location(id, virtual_log_segment)) & general_spdk_info.sector_round_mask;
        atomic_sub_req data_request;
        uint32_t dlen = general_spdk_info.sector_size;
        uint8_t* start = ((uint8_t*)(&LOG_AT_TABLE(id)[virtual_log_segment])) - (segment_address_log_location(id, virtual_log_segment) & general_spdk_info.sector_mask);
        data_request.buf = start;
        data_request.data_length = dlen;
        data_request.virtaddress = virtaddress;
        data_request.content_type = METADATA;
        sub_requests.push_back(data_request);
        it++;
    }

    uint16_t i = 0;
    while(it != available_segment_index.end()) {
        DATA_AT_TABLE(id)[data_assignment_start + i] = *it;
        segment_usage_table[*it] = 1;
        it++;
        i++;
    }

    atomic_sub_req data_at_request;
    uint64_t virtaddress = segment_address_data_location(id, data_assignment_start) & general_spdk_info.sector_round_mask;
    uint32_t dlen = (segment_address_data_location(id, data_assignment_start + needed_data_segment - 1) >> general_spdk_info.sector_bit) - (segment_address_data_location(id, data_assignment_start) >> general_spdk_info.sector_bit);
    dlen = (dlen + 1) << general_spdk_info.sector_bit;
    uint8_t* start = ((uint8_t*)(&DATA_AT_TABLE(id)[data_assignment_start])) - (segment_address_log_location(id, data_assignment_start) & general_spdk_info.sector_mask);
    data_at_request.buf = start;
    data_at_request.data_length = dlen;
    data_at_request.virtaddress = virtaddress;
    data_at_request.content_type = METADATA;
    sub_requests.push_back(data_at_request);
    pthread_mutex_unlock(&segment_assignment_lock);
    
    // Step 4: Submit data request
    atomic_sub_req log_entry_request;
    uint64_t log_virtaddress = ((log_index * sizeof(LogEntry)) >> general_spdk_info.sector_bit) << general_spdk_info.sector_bit;
    log_entry_request.buf = &(id_to_log[id][log_virtaddress / sizeof(LogEntry)]);
    log_entry_request.data_length = general_spdk_info.sector_size;
    log_entry_request.virtaddress = log_virtaddress;
    log_entry_request.content_type = LOGENTRY;
    sub_requests.push_back(log_entry_request);

    atomic_sub_req data_request;
    data_request.buf = data;
    data_request.data_length = data_size;
    data_request.virtaddress = data_offset;
    data_request.content_type = DATA;
    sub_requests.push_back(data_request);
	
    //Step 4: Submit control request
    atomic_w(sub_requests, metadata, id);
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
         
        //Step 2: Construct log_name_to_id and segment usage array
        size_t ofst = 0;
        for(size_t id = 0; id < SPDK_NUM_LOGS_SUPPORTED; id++) {
            //Step 2_0: copy data into metadata entry
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH, (uint8_t*)&LOG_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH, (uint8_t*)&DATA_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH;
            global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info = *((PTLogMetadataInfo*)(buf + ofst));
            ofst += sizeof(PTLogMetadataInfo);

            if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.inuse) {
                //Step 2_1: Update log_name_to_id
                log_name_to_id.insert(std::pair<std::string, uint32_t>((char*)global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.name, id));
                last_written_ver[id] = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.ver;

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
        //spdk_free(buf);
        loaded = true;
    }

    //Step 3: Update log_metadata
    try {
        uint32_t id = log_name_to_id.at(name);
        log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info;
        //std::printf("tail %d\n", log_metadata->persist_metadata_info->fields.tail);
        std::cout.flush();
        LogEntry *log_entry = (LogEntry *)malloc(SPDK_LOG_ADDRESS_SPACE << 6);

        // Submit read request
        uint32_t size = (log_metadata->persist_metadata_info->fields.tail - log_metadata->persist_metadata_info->fields.head) * sizeof(LogEntry) + ((log_metadata->persist_metadata_info->fields.head * sizeof(LogEntry)) & (general_spdk_info.sector_size - 1));
        uint64_t virtaddress = ((log_metadata->persist_metadata_info->fields.head * sizeof(LogEntry)) >> general_spdk_info.sector_bit) << general_spdk_info.sector_bit;
        char* buf = (char*)spdk_malloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        //std::printf("Reading logentries with size %d and virtaddress %d\n", size, virtaddress);
	non_atomic_rw(buf, size, virtaddress, BLOCKING, LOGENTRY, false, id);
        //std::printf("Submitted read request for existing log.\n");
        //std::cout.flush();
        std::copy(buf, buf + size, (char*)log_entry + ((virtaddress >> general_spdk_info.sector_bit) << general_spdk_info.sector_bit) );
        spdk_free(buf);	
	for (int idx = log_metadata->persist_metadata_info->fields.head; idx < log_metadata->persist_metadata_info->fields.tail; idx ++ ) {
	    
            std::printf("LogEntry idx: %d ver: %ld, LogEntry dlen: %lu, LogEntry ofst: %lu\n", idx, log_entry[idx].fields.ver, log_entry[idx].fields.dlen, log_entry[idx].fields.ofst);
            std::cout.flush();
	}
        id_to_log[id] = log_entry;
        return;
    } catch(const std::out_of_range& oor) {
        // Find an unused metadata entry
        if(pthread_mutex_lock(&metadata_entry_assignment_lock) != 0) {
            // throw an exception
            throw derecho::derecho_exception("Failed to grab metadata entry assignment lock.");
        }
        for(uint32_t index = 0; index < SPDK_NUM_LOGS_SUPPORTED; index++) {
            if(!global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse) {
                
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse = true;
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.id = index;
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.head = 0;
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.tail = 0;
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.ver = -1;
                last_written_ver[index] = -1;
                
                strcpy((char *)global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.name, name.c_str());
                memset(global_metadata.fields.log_metadata_entries[index].fields.log_metadata_address.segment_log_entry_at_table, 0, sizeof(global_metadata.fields.log_metadata_entries[index].fields.log_metadata_address.segment_log_entry_at_table));
                memset(global_metadata.fields.log_metadata_entries[index].fields.log_metadata_address.segment_data_at_table, 0, sizeof(global_metadata.fields.log_metadata_entries[index].fields.log_metadata_address.segment_data_at_table));
                log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info;
                log_name_to_id.insert(std::pair<std::string, uint32_t>(name, index));
                
                pthread_mutex_unlock(&metadata_entry_assignment_lock);
                
                id_to_log[index] = (LogEntry*)malloc(SPDK_LOG_ADDRESS_SPACE << 6);
                return;
            }
        }
        //no available metadata entry
        pthread_mutex_unlock(&metadata_entry_assignment_lock);
        throw derecho::derecho_exception("No available metadata entry.");
    }
}

LogEntry* PersistThreads::read_entry(const uint32_t& id, const uint64_t& index) {
    return &(id_to_log[id][index % SPDK_LOG_ADDRESS_SPACE]);
}

void* PersistThreads::read_data(const uint32_t& id, const uint64_t& index) {
    LogEntry* log_entry = read_entry(id, index);
    char* buf = (char*)spdk_malloc((1 + (log_entry->fields.dlen >> general_spdk_info.sector_bit)) << general_spdk_info.sector_bit, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    uint64_t data_offset = log_entry->fields.ofst;
    non_atomic_rw(buf, log_entry->fields.dlen, data_offset, BLOCKING, DATA, false, id);
    
    void* res_buf = malloc(log_entry->fields.dlen);
    memcpy(res_buf, buf, log_entry->fields.dlen);
    spdk_free(buf);
    return res_buf;
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
