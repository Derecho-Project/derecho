#include <cmath>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/persistent/detail/PersistThreads.hpp>
#include <iostream>

#define UNBLOCKING 1
#define BLOCKING 2
#define METADATA 0
#define LOGENTRY 1
#define DATA 2
#define LBA_PER_SEG (SPDK_SEGMENT_BIT >> general_spdk_info.sector_bit)
#define SPDK_SEGMENT_MASK ((1 << SPDK_SEGMENT_BIT) - 1)
const uint32_t MAX_LBA_COUNT = 4096;

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
        std::printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
                    spdk_nvme_ns_get_size(ns) / 1000000000);
        std::cout.flush();
        m_PersistThread->general_spdk_info.ns = ns;
        m_PersistThread->general_spdk_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        m_PersistThread->general_spdk_info.sector_bit = std::log2(m_PersistThread->general_spdk_info.sector_size);
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
        throw derecho::derecho_exception("data write request failed.");
    }
    persist_data_request_t* data_request = (persist_data_request_t*)args;
    data_request->completed++;
    if(data_request->completed->load() == data_request->part_num) {
        m_PersistThread->data_request_completed.notify_all();
    }
    free(data_request->buf);
    delete data_request;
}

void PersistThreads::load_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("data write request failed.");
    }
    //std::tuple<atomic<bool>*, bool, void*> argtuple =  *(std::tuple<atomic<bool>*, bool, void*>*)args;
    //*std::get<0>(argtuple) = true;
    //if (std::get<1>(argtuple)) {
	//std::printf("request is write, freeing the buf\n");
	//std::cout.flush();
    //    spdk_free(std::get<2>(argtuple));
    //}
    //std::printf("Freeing the arg tuple %p.\n", (void *)args);
    //std::cout.flush();
    //free(args);
    *(bool*)args = true;
    std::printf("Got completed %p\n", args);
    std::cout.flush();
}

void PersistThreads::dummy_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("dummy request failed.");
    }
}

void PersistThreads::control_write_request_complete(void* args,
        const struct spdk_nvme_cpl* completion) {
    // if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        throw derecho::derecho_exception("control request failed.");
    }
    persist_control_request_t* control_request = (persist_control_request_t*)args;
    PTLogMetadataInfo* metadata_info = (PTLogMetadataInfo*)(control_request->buf);
    m_PersistThread->id_to_last_version.insert(std::pair<uint32_t, int64_t>(metadata_info->fields.id, metadata_info->fields.ver));
    m_PersistThread->compeleted_request_id = control_request->request_id;
    spdk_free(control_request->buf);
    free(control_request->completed);
    free(control_request);
}

int PersistThreads::update_segment(char* buf, uint32_t data_length, uint64_t lba_index, int mode, bool is_write) {
    uint32_t offset = 0;
    std::queue<bool*> completed_queue;
    while (offset < data_length) {
        persist_data_request_t data_request;
        uint32_t size = std::min(data_length - offset, MAX_LBA_COUNT << general_spdk_info.sector_bit);
        data_request.buf = buf + offset;
        data_request.lba = lba_index + (offset >> general_spdk_info.sector_bit);
        data_request.lba_count = size >> general_spdk_info.sector_bit;
        std::cout.flush();
        bool *completed = new bool();
        // Use different callback function based on input mode
        switch (mode) {
            case UNBLOCKING:
                {
                    data_request.cb_fn = dummy_request_complete;
                    data_request.is_write = is_write;
                    *completed = true;
                }                
                break;

            case BLOCKING:
                {                
                    data_request.cb_fn = load_request_complete;
                    //std::tuple<bool*, bool, void*>* argtuple = new std::tuple<bool*, bool, void*>;
                    //*argtuple = std::make_tuple(completed, is_write, buf + offset);
                    //data_request.args = argtuple;
                    data_request.args = completed;
                    completed_queue.push(completed);
                    data_request.is_write = is_write;
                } 
                break;

            default:
                break;
        }
        // Push the request to the data queue
        std::cout.flush();
        std::unique_lock<std::mutex> mlck(data_queue_mtx);
        data_write_queue.push(data_request);
        std::printf("Pushed data request!\n");
        std::cout.flush();
        sem_post(&new_data_request);
        mlck.unlock();
        offset += size;
        //while(!completed);
    }

    // Wait for completion
    switch (mode) {
        case UNBLOCKING:
            return 0;

        case BLOCKING:
            while (completed_queue.size() > 0) {
                bool* completed_ptr = completed_queue.front();
                completed_queue.pop();
                while (!*completed_ptr){
                    spdk_nvme_qpair_process_completions(SpdkQpair_data[0], 0);
                }
                free(completed_ptr);
            }
            return 0;

        default:
            return 0;
    }
}

int PersistThreads::non_atomic_rw(char* buf, uint32_t data_length, uint64_t virtaddress, int blocking_mode, int content_type, bool is_write, int id){
    int num_segment = ((virtaddress + data_length - 1) >> SPDK_SEGMENT_BIT) - (virtaddress >> SPDK_SEGMENT_BIT) + 1;
    uint32_t ofst = 0;
    std::queue<bool*> completed_queue;
    for (int i = 0; i < num_segment; i++) {
        uint32_t curr_ofst = 0;
        uint32_t size = std::max((uint32_t)SPDK_SEGMENT_SIZE, data_length + ofst);
        // Extract physical lba index based on content type using the corresponding address translation table
        uint64_t lba_index;
        switch(content_type){
            case METADATA:
                lba_index = (virtaddress + ofst) >> general_spdk_info.sector_bit;
                break;
            case LOGENTRY:
                lba_index = LOG_AT_TABLE(id)[(virtaddress + ofst) >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + (((virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                break;
            case DATA:
                lba_index = DATA_AT_TABLE(id)[(virtaddress + ofst) >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + (((virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                break;
            default:
                break;
        }

        // Submit data request for the current segment
        while (curr_ofst < size) {  
            persist_data_request_t data_request;
            uint32_t curr_size = std::min(size - curr_ofst, MAX_LBA_COUNT << general_spdk_info.sector_bit);
            data_request.buf = buf + ofst +  curr_ofst;
            data_request.lba = lba_index + (curr_ofst >> general_spdk_info.sector_bit);
            data_request.lba_count = curr_size >> general_spdk_info.sector_bit;

            // Use different callback function based on input mode
            switch (blocking_mode) {
                case UNBLOCKING:
                    {
                        data_request.cb_fn = dummy_request_complete;
                        data_request.is_write = is_write;
                    }                
                    break;

                case BLOCKING:
                    {    
                        bool* completed = new bool();
                        data_request.cb_fn = load_request_complete;
                        data_request.args = completed;                        
                        completed_queue.push(completed);
                        data_request.is_write = is_write;
                    } 
                    break;

                default:
                    break;
            }
            // Push the request to the data queue
            std::cout.flush();
            std::unique_lock<std::mutex> mlck(data_queue_mtx);
            data_write_queue.push(data_request);
            std::printf("Pushed data request!\n");
            std::cout.flush();
            sem_post(&new_data_request);
            mlck.unlock();
            curr_ofst += curr_size;
            //while(!completed);
        }
        ofst += size;
    }

    // Wait for completion
    switch (blocking_mode) {
        case UNBLOCKING:
            return 0;

        case BLOCKING:
            while (completed_queue.size() > 0) {
                bool* completed_ptr = completed_queue.front();
                completed_queue.pop();
                std::printf("Waiting on completed_ptr %p\n", (void*) completed_ptr);
                std::cout.flush();
                while (!*completed_ptr){
                    spdk_nvme_qpair_process_completions(SpdkQpair_data[0], 1);
                }
                std::printf("Freeing completed %p\n", (void *)completed_ptr);
                std::cout.flush();
                //free(completed_ptr);
            }
            return 0;

        default:
            return 0;
    }
}

int PersistThreads::atomic_w(std::vector<atomic_sub_req> sub_requests, char* atomic_buf, uint32_t atomic_dl, uint64_t atomic_virtaddress, int content_type, int id){
    std::queue<bool*> completed_queue;
    //Issue write request for each sub request
    for (atomic_sub_req req : sub_requests) {
        int num_segment = ((req.virtaddress + req.data_length - 1) >> SPDK_SEGMENT_BIT) - (req.virtaddress >> SPDK_SEGMENT_BIT) + 1;
        uint32_t ofst = 0;
        for (int i = 0; i < num_segment; i++) {
            uint32_t curr_ofst = 0;
            uint32_t size = std::max((uint32_t)SPDK_SEGMENT_SIZE, req.data_length - ofst);
            // Extract physical lba index based on content type using the corresponding address translation table
            uint64_t lba_index;
            switch(content_type){
                case METADATA:
                    lba_index = (req.virtaddress + ofst) >> general_spdk_info.sector_bit;
                    break;
                case LOGENTRY:
                    lba_index = LOG_AT_TABLE(id)[(req.virtaddress + ofst) >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + (((req.virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                    break;
                case DATA:
                    lba_index = DATA_AT_TABLE(id)[(req.virtaddress + ofst) >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + (((req.virtaddress + ofst) & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
                    break;
                default:
                    break;
            }

            // Submit data request for the current segment
            while (curr_ofst < size) {  
                persist_data_request_t data_request;
                uint32_t curr_size = std::min(size - curr_ofst, MAX_LBA_COUNT << general_spdk_info.sector_bit);
                data_request.buf = req.buf + ofst + curr_ofst;
                data_request.lba = lba_index + (curr_ofst >> general_spdk_info.sector_bit);
                data_request.lba_count = curr_size >> general_spdk_info.sector_bit;

                bool* completed = new bool();
                data_request.cb_fn = load_request_complete;
                data_request.args = completed;
                data_request.is_write = true;
                completed_queue.push(completed);
                
                // Push the request to the data queue
                std::cout.flush();
                std::unique_lock<std::mutex> mlck(data_queue_mtx);
                data_write_queue.push(data_request);
                std::printf("Pushed data request!\n");
                std::cout.flush();
                sem_post(&new_data_request);
                mlck.unlock();
                curr_ofst += curr_size;
                //while(!completed);
            }
            ofst += size;
        }
    }

    // Submit control request for the current segment
    uint64_t lba_index;
    bool* all_sub_req_completed = new bool();
    switch(content_type){
        case METADATA:
            lba_index = atomic_virtaddress >> general_spdk_info.sector_bit;
            break;
        case LOGENTRY:
            lba_index = LOG_AT_TABLE(id)[atomic_virtaddress >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + ((atomic_virtaddress & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
            break;
        case DATA:
            lba_index = DATA_AT_TABLE(id)[atomic_virtaddress >> SPDK_SEGMENT_BIT] * LBA_PER_SEG + ((atomic_virtaddress & SPDK_SEGMENT_MASK) >> general_spdk_info.sector_bit);
            break;
        default:
            break;
    }
    persist_control_request_t* control_request = new persist_control_request_t;
    control_request->buf = atomic_buf;
    control_request->lba = lba_index;
    control_request->lba_count = atomic_dl >> general_spdk_info.sector_bit;
    control_request->cb_fn = control_write_request_complete;
    control_request->args = control_request;
    control_request->completed = all_sub_req_completed;
    std::unique_lock<std::mutex> clck(control_queue_mtx);
    control_write_queue.push(*control_request);
    clck.release();
    
    // Wait until all sub req completed to trigger control_request
    std::thread thread([&](){
        while (completed_queue.size() > 0) {
            bool* completed_ptr = completed_queue.front();
            completed_queue.pop();
            while (!*completed_ptr) {
                spdk_nvme_qpair_process_completions(SpdkQpair_data[0], 0);
            }
            free(completed_ptr);
        }
        *all_sub_req_completed = true;
        data_request_completed.notify_all(); 
    }); 
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
        std::printf("PersistThreads initialized. Returning it.\n");
        std::cout.flush();
        return m_PersistThread;
    } else {
        std::printf("PersistThreads not initialized at this moment.\n");
        std::cout.flush();
        // Step 0: Grab the initialization lock
        initialization_lock.lock();
        std::printf("Grabbed initialization lock.\n");
        if(initialized) {
            initialization_lock.unlock();
            return m_PersistThread;
        }
        std::printf("PersistThreads not initialized.\n");
        std::cout.flush();
        m_PersistThread = new PersistThreads();
        if(m_PersistThread->initialize_threads() != 0){ 
            //TODO: throw exception
            std::printf("m_PersistThread initialization failed.\n");
            std::cout.flush();
            exit(1);
        }
        initialized = true;
        initialization_lock.unlock();
        std::printf("Released lock.\n");
        std::cout.flush();
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
    std::printf("Initialized nvme namespace.\n");
    std::cout.flush();
    int res = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
    if(res != 0) {
        //initialization of nvme ctrlr failed
        throw derecho::derecho_exception("Failed to initialize nvme ctrlr.");
    }
    std::printf("Initialized nvme ctrlr.");
    std::cout.flush();
        // Step 1: get qpair for each data plane and control plane
        for(int i = 0; i < NUM_DATA_PLANE; i++) {
            std::printf("qpair %d. ", i);
            std::cout.flush();
	    struct spdk_nvme_io_qpair_opts opts;
	    spdk_nvme_ctrlr_get_default_io_qpair_opts(general_spdk_info.ctrlr, &opts, sizeof(opts));
            opts.io_queue_size = 1024;
	    opts.io_queue_requests = 2048;
            SpdkQpair_data[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, &opts, sizeof(opts));
            std::printf("qpair %d allocated.\n ", i);
            std::cout.flush();
            if(SpdkQpair_data[i] == NULL) {
                //qpair initialization failed
                throw derecho::derecho_exception("Failed to initialize data qpair.");
            }
        }
        for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
            std::printf("control qpair %d. ", i);
            std::cout.flush();
	    struct spdk_nvme_io_qpair_opts opts;
            spdk_nvme_ctrlr_get_default_io_qpair_opts(general_spdk_info.ctrlr, &opts, sizeof(opts));
	    opts.io_queue_size = 1024;
	    opts.io_queue_requests = 2048;
	    SpdkQpair_control[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, &opts, sizeof(opts));
            std::printf("control qpair %d allocated.\n", i);
            if(SpdkQpair_control[i] == NULL) {
                //qpair initialization failed
                throw derecho::derecho_exception("Failed to initialize control qpair.");
            }
        }
        std::printf("Initialized qpair.\n");
        std::cout.flush();

        // Step 2: initialize sem and locks
        if(sem_init(&new_data_request, 0, 0) != 0) {
            // sem init failed
            throw derecho::derecho_exception("Failed to initialize new_data_request semaphore.");
        }
        if(pthread_mutex_init(&segment_assignment_lock, NULL) != 0) {
            // mutex init failed
            throw derecho::derecho_exception("Failed to initialize mutex.");
        }
        if(pthread_mutex_init(&metadata_entry_assignment_lock, NULL) != 0) {
            // mutex init failed
            throw derecho::derecho_exception("Failed to initialize mutex.");
        }

        std::printf("Initialized sem and locks.\n");
        std::cout.flush();
        // Step 3: initialize other fields
        compeleted_request_id = -1;
        assigned_request_id = -1;
        segment_usage_table.reset();
        segment_usage_table[0].flip();
        std::printf("Initialized other fields.\n");
        std::cout.flush();
        // Step 3: initialize threads
        for(int i = 0; i < NUM_DATA_PLANE; i++) {
            data_plane[i] = std::thread([&]() {
                do {
                    sem_wait(&new_data_request);
                    std::unique_lock<std::mutex> lck(m_PersistThread->data_queue_mtx);
                    persist_data_request_t data_request = data_write_queue.front();
                    data_write_queue.pop();
                    if(data_request.is_write) {
                        spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_request.cb_fn, data_request.args, 0);
                        lck.unlock();
                    } else {
		        std::printf("Get new read request.\n");
                        std::cout.flush();
                            int rc = spdk_nvme_ns_cmd_read(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_request.cb_fn, data_request.args, 0);
			    if (rc != 0){
			    	std::fprintf(stderr, "Failed to initialize read io %d with lba_count %d.\n", rc, data_request.lba_count);
			    	std::cout.flush();
			    }
                        }
                        lck.unlock();
                    } while(true);
            });
            data_plane[i].detach();
            std::printf("Initialized data_plane %d.\n", i);
            std::cout.flush();
        }
        for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
            control_plane[i] = std::thread([&]() {
                do {
                    // Wait until available compeleted data request
                    std::unique_lock<std::mutex> lck(control_queue_mtx);
                    data_request_completed.wait(lck);
                    persist_control_request_t control_request = control_write_queue.front();
		    do {
                        if(*control_request.completed == true) {
                            spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_request.cb_fn, control_request.args, 0);
                            control_write_queue.pop();
                            if(control_write_queue.size() == 0) {
                                break;
                            } else {
                                control_request = control_write_queue.front();
                            }
                        } else {
                            break;
                        }
                    } while(true);
                    lck.release();
                } while(true);
            });
            control_plane[i].detach();
            std::printf("Initialized control plane %d.\n", i);
            std::cout.flush();
        }
	return 0;
}

PersistThreads::PersistThreads() {}

PersistThreads::~PersistThreads() {}

void PersistThreads::append(const uint32_t& id, char* data, const uint64_t& data_offset,
                            const uint64_t& data_size, void* log,
                            const uint64_t& log_offset, PTLogMetadataInfo metadata) {
    // Step 0: extract virtual segment number and sector number
    uint64_t virtual_data_segment = data_offset >> SPDK_SEGMENT_BIT % SPDK_DATA_ADDRESS_TABLE_LENGTH;
    uint64_t data_sector = data_offset & ((1 << SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;
    uint64_t virtual_log_segment = log_offset >> SPDK_SEGMENT_BIT % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
    uint64_t log_sector = log_offset & ((1 << SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;

    // Step 1: Calculate needed segment number
    //TODO: add ring buffer logic
    uint16_t needed_segment = 0;
    uint16_t part_num = 0;
    if(LOG_AT_TABLE(id)[virtual_log_segment] == 0) {
        needed_segment++;
        part_num++;
    }
    int needed_data_sector = data_size >> general_spdk_info.sector_bit;
    if(DATA_AT_TABLE(id)[virtual_data_segment] == 0) {
        needed_segment += needed_data_sector / (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size);
    } else {
        needed_segment += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) - 1;
    }

    uint16_t physical_data_segment = DATA_AT_TABLE(id)[virtual_data_segment];
    uint64_t physical_data_sector = physical_data_segment * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + data_sector;

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
    //?
    if(available_segment_index.size() > part_num) {
        part_num++;
    }
    part_num += (segment_address_data_location(id, data_assignment_start + available_segment_index.size() - part_num) >> PAGE_SHIFT) - (segment_address_data_location(id, data_assignment_start) >> PAGE_SHIFT);
    if(needed_data_sector > 0) {
        part_num++;
    }
    part_num += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE >> general_spdk_info.sector_bit);

    // Step 3: Submit data request for segment address translation table update
    std::atomic<int> completed = 0;
    uint64_t request_id = assigned_request_id + 1;
    assigned_request_id++;
    uint16_t part_id = 0;

    std::set<uint16_t>::iterator it = available_segment_index.begin();
    if(LOG_AT_TABLE(id)[virtual_log_segment] == 0) {
        LOG_AT_TABLE(id)
        [virtual_log_segment] = *it;
        segment_usage_table[*it] = 1;
        uint64_t offset = segment_address_log_location(id, virtual_log_segment);
        persist_data_request_t data_request;
        data_request.request_id = request_id;
        data_request.completed = &completed;
        data_request.part_id = part_id;
        data_request.buf = new char[PAGE_SIZE];
        data_request.lba = offset >> general_spdk_info.sector_bit;
        data_request.lba_count = PAGE_SIZE / general_spdk_info.sector_size;
        char* start = (char*)LOG_AT_TABLE(id);
        std::copy(start, start + PAGE_SIZE, (char*)data_request.buf);
        std::unique_lock<std::mutex> lck(data_queue_mtx);
        data_write_queue.push(data_request);
        lck.release();
        sem_post(&new_data_request);
        it++;
        part_id++;
    }

    uint64_t next_sector = segment_address_data_location(id, data_assignment_start) >> general_spdk_info.sector_bit;
    while(it != available_segment_index.end()) {
        DATA_AT_TABLE(id)
        [data_assignment_start] = *it;
        segment_usage_table[*it] = 1;
        uint64_t offset = segment_address_data_location(id, data_assignment_start);
        if(offset >> general_spdk_info.sector_bit == next_sector) {
            persist_data_request_t data_request;
            data_request.request_id = request_id;
            data_request.completed = &completed;
            data_request.part_id = part_id;
            data_request.buf = new char[PAGE_SIZE];
            data_request.lba = offset >> general_spdk_info.sector_bit;
            data_request.lba_count = PAGE_SIZE / general_spdk_info.sector_size;
            char* start = (char*)(&DATA_AT_TABLE(id)[data_assignment_start >> (PAGE_SHIFT - 1) << (PAGE_SHIFT - 1)]);
            std::copy(start, start + PAGE_SIZE, (char*)data_request.buf);
            std::unique_lock<std::mutex> lck(data_queue_mtx);
            data_write_queue.push(data_request);
            lck.release();
            sem_post(&new_data_request);
            part_id++;
            next_sector += (PAGE_SIZE - (offset & (PAGE_SIZE - 1))) >> general_spdk_info.sector_bit;
        }
        it++;
        data_assignment_start++;
    }
    pthread_mutex_unlock(&segment_assignment_lock);

    // Step 4: Submit data request
    persist_data_request_t log_entry_request;
    log_entry_request.request_id = request_id;
    log_entry_request.buf = log;
    log_entry_request.part_id = part_id;
    log_entry_request.part_num = part_num;
    log_entry_request.completed = &completed;
    log_entry_request.lba = LOG_AT_TABLE(id)[virtual_log_segment] * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + log_sector;
    log_entry_request.lba_count = 1;
    std::unique_lock<std::mutex> lck(data_queue_mtx);
    data_write_queue.push(log_entry_request);
    lck.release();
    sem_post(&new_data_request);
    part_id++;

    while(part_id < part_num) {
        persist_data_request_t data_request;
        data_request.request_id = request_id;
        data_request.buf = data;
        data_request.part_id = part_id;
        data_request.part_num = part_num;
        data_request.completed = &completed;
        data_request.lba = DATA_AT_TABLE(id)[virtual_data_segment] * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + data_sector;
        data_request.lba_count = (1 >> (SPDK_SEGMENT_BIT - general_spdk_info.sector_bit)) - data_sector;
        std::unique_lock<std::mutex> lck(data_queue_mtx);
        data_write_queue.push(data_request);
        sem_post(&new_data_request);
        lck.release();
        data_sector = 0;
        virtual_data_segment++;
        data += data_request.lba_count * general_spdk_info.sector_size;
    }
    //Step 4: Submit control request
    persist_control_request_t metadata_request;
    metadata_request.request_id = request_id;
    //metadata_request.buf = metadata;
    metadata_request.part_num = part_num;
    //metadata_request.completed = &completed;
    metadata_request.lba = (id * SPDK_LOG_METADATA_SIZE) >> general_spdk_info.sector_bit;
    metadata_request.lba_count = 1 >> (PAGE_SHIFT - general_spdk_info.sector_bit);
    metadata_request.cb_fn = NULL;
    std::unique_lock<std::mutex> clck(control_queue_mtx);
    control_write_queue.push(metadata_request);
    clck.release();
}

void PersistThreads::release_segments(void* args, const struct spdk_nvme_cpl* completion) {
    PTLogMetadataInfo* metadata = (PTLogMetadataInfo*)args;
    m_PersistThread->id_to_last_version.insert(std::pair<uint32_t, int64_t>(metadata->fields.id, metadata->fields.ver));
    //Step 0: release log_segments until metadata.head; release data_segments until
    int log_seg = std::max((metadata->fields.head >> SPDK_SEGMENT_BIT) - 1, (int64_t)0);
    int data_seg = std::max((uint64_t)((m_PersistThread->id_to_log[metadata->fields.id][metadata->fields.head - 1].fields.ofst + m_PersistThread->id_to_log[metadata->fields.id]->fields.dlen) >> SPDK_SEGMENT_BIT) - 1, (uint64_t)0);
    if(pthread_mutex_lock(&m_PersistThread->segment_assignment_lock) != 0) {
        //throw exception
        throw derecho::derecho_exception("Failed to grab segment assignment lock.");
    }
    for(int i = 0; i < log_seg; i++) {
        m_PersistThread->segment_usage_table[LOG_AT_TABLE(metadata->fields.id)[i]] = 0;
    }
    for(int i = 0; i < data_seg; i++) {
        m_PersistThread->segment_usage_table[DATA_AT_TABLE(metadata->fields.id)[i]] = 0;
    }
    std::fill(LOG_AT_TABLE(metadata->fields.id),
              LOG_AT_TABLE(metadata->fields.id) + log_seg,
              0);
    std::fill(DATA_AT_TABLE(metadata->fields.id),
              DATA_AT_TABLE(metadata->fields.id) + data_seg,
              0);

    //Step 1: Submit metadata update request
    uint64_t request_id = m_PersistThread->assigned_request_id + 1;
    m_PersistThread->assigned_request_id++;

    if(log_seg > 0) {
        for(size_t i = 0; i < log_seg * sizeof(uint16_t) / PAGE_SIZE + 1; i++) {
            persist_data_request_t metadata_request;
            metadata_request.request_id = request_id;
            metadata_request.buf = new char[PAGE_SIZE];
            char* start = (char*)&LOG_AT_TABLE(metadata->fields.id)[i * PAGE_SIZE / sizeof(uint16_t)];
            std::copy(start, start + PAGE_SIZE, (char*)metadata_request.buf);
            metadata_request.part_num = 0;
            metadata_request.completed = 0;
            metadata_request.cb_fn = dummy_request_complete;
            metadata_request.is_write = true;
            metadata_request.lba = (metadata->fields.id * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataInfo) + i * PAGE_SIZE) >> m_PersistThread->general_spdk_info.sector_bit;
            metadata_request.lba_count = 1 >> (PAGE_SHIFT - m_PersistThread->general_spdk_info.sector_bit);
            std::unique_lock<std::mutex> lck(m_PersistThread->control_queue_mtx);
            m_PersistThread->data_write_queue.push(metadata_request);
            m_PersistThread->data_request_completed.notify_all();
            lck.release();
        }
    }
    if(data_seg > 0) {
        for(size_t i = 0; i < data_seg * sizeof(uint16_t) / PAGE_SIZE + 1; i++) {
            persist_data_request_t metadata_request;
            metadata_request.request_id = request_id;
            metadata_request.buf = new char[PAGE_SIZE];
            char* start = (char*)&DATA_AT_TABLE(metadata->fields.id)[i * PAGE_SIZE / sizeof(uint16_t)];
            std::copy(start, start + PAGE_SIZE, (char*)metadata_request.buf);
            metadata_request.part_num = 0;
            metadata_request.completed = 0;
            metadata_request.cb_fn = dummy_request_complete;
            metadata_request.is_write = true;
            metadata_request.lba = (metadata->fields.id * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataInfo) + SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH * sizeof(uint16_t) + i * PAGE_SIZE) >> m_PersistThread->general_spdk_info.sector_bit;
            metadata_request.lba_count = 1 >> (PAGE_SHIFT - m_PersistThread->general_spdk_info.sector_bit);
            std::unique_lock<std::mutex> lck(m_PersistThread->data_queue_mtx);
            m_PersistThread->data_write_queue.push(metadata_request);
            m_PersistThread->data_request_completed.notify_all();
            lck.release();
        }
    }
    free(args);
    pthread_mutex_unlock(&m_PersistThread->segment_assignment_lock);
}

void PersistThreads::update_metadata(const uint32_t& id, PTLogMetadataInfo metadata, bool garbage_collection) {
    std::atomic<int> completed = 0;
    uint64_t request_id = assigned_request_id + 1;
    assigned_request_id++;

    persist_control_request_t metadata_request;
    metadata_request.request_id = request_id;
    //metadata_request.buf = metadata;
    metadata_request.part_num = 0;
    //metadata_request.completed = &completed;
    metadata_request.lba = id * (SPDK_LOG_METADATA_SIZE / general_spdk_info.sector_size);
    metadata_request.lba_count = PAGE_SIZE / general_spdk_info.sector_size;
    if(garbage_collection) {
        metadata_request.cb_fn = release_segments;
        metadata_request.args = (void*)&metadata;
    } else {
        metadata_request.cb_fn = NULL;
    }
    std::unique_lock<std::mutex> lck(control_queue_mtx);
    control_write_queue.push(metadata_request);
    data_request_completed.notify_all();
    lck.release();
}

void PersistThreads::load(const std::string& name, LogMetadata* log_metadata) {
    if(!loaded) {
        //Step 0: submit read request for the segment of all global data
        std::printf("metadata not loaded. load the metadata.\n");
        std::cout.flush();
        char* buf = (char*)spdk_malloc(SPDK_SEGMENT_SIZE, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        non_atomic_rw(buf, SPDK_SEGMENT_SIZE, 0, BLOCKING, METADATA, false, 0);
        std::printf("Submitted data request for log metadata.\n");
        std::cout.flush();
         
        std::printf("Read completed.\n");
        std::cout.flush();
        //Step 2: Construct log_name_to_id and segment usage array
        size_t ofst = 0;
        for(size_t id = 0; id < SPDK_NUM_LOGS_SUPPORTED; id++) {
            //Step 2_0: copy data into metadata entry
            global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info = *(PTLogMetadataInfo*)(buf + ofst);
            ofst += sizeof(PTLogMetadataInfo);
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH, (uint8_t*)&LOG_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
            std::copy(buf + ofst, buf + ofst + sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH, (uint8_t*)&DATA_AT_TABLE(id));
            ofst += sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH;

            if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.inuse) {
                //Step 2_1: Update log_name_to_id
                log_name_to_id.insert(std::pair<std::string, uint32_t>((char*)global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.name, id));
                id_to_last_version.insert(std::pair<uint32_t, int64_t>(id, global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.ver));

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
        spdk_free(buf);
        loaded = true;
        std::printf("Metadata segment loaded.\n");
        std::cout.flush();
    }

    //Step 3: Update log_metadata
    try {
        uint32_t id = log_name_to_id.at(name);
        std::printf("Log with existing metadata entry.\n");
	std::cout.flush();
	log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info;
        LogEntry *log_entry = (LogEntry *)malloc(SPDK_LOG_ADDRESS_SPACE);
        int num_log_segment = (log_metadata->persist_metadata_info->fields.tail >> SPDK_SEGMENT_BIT) - (log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT) + 1;
        size_t offset = 0;
	for(int i = 0; i < num_log_segment; i++) {
	    uint64_t lba = (LOG_AT_TABLE(id)[log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT] + i) * LBA_PER_SEG;
	    uint32_t size = SPDK_SEGMENT_SIZE - ((log_metadata->persist_metadata_info->fields.head + offset) & ((1 << SPDK_SEGMENT_BIT) - 1)); 
	    char *buf = (char *)spdk_malloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	    update_segment(buf, size, lba, BLOCKING, false);
	    std::copy(buf, buf + size, (char *)log_entry + (log_metadata->persist_metadata_info->fields.head + offset));
	    offset += size;
	    spdk_free(buf);
        }
        id_to_log.insert(std::pair<uint32_t, LogEntry*>(id, log_entry));
        PTLogMetadataInfo log_metadata = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info;
        release_segments((void*)&log_metadata, NULL);
        return;
    } catch(const std::out_of_range& oor) {
        std::printf("Log without existing metadata entry.\n");
	std::cout.flush();
	// Find an unused metadata entry
        if(pthread_mutex_lock(&metadata_entry_assignment_lock) != 0) {
            // throw an exception
            throw derecho::derecho_exception("Failed to grab metadata entry assignment lock.");
        }
        for(uint32_t index = 0; index < SPDK_NUM_LOGS_SUPPORTED; index++) {
            if(!global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse) {
                std::printf("Found metadata entry not occupied.\n");
		std::cout.flush();
		global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse = true;
	        global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.id = index;
                log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info;
                log_name_to_id.insert(std::pair<std::string, uint32_t>(name, index));
                pthread_mutex_unlock(&metadata_entry_assignment_lock);
                std::printf("Updated metadata inuse field. Released lock.\n");
		std::cout.flush();
		id_to_log[index] = (LogEntry*)malloc(SPDK_LOG_ADDRESS_SPACE);
		//id_to_log.insert(std::pair<uint32_t, LogEntry*>(index, log_entry));
                std::printf("Inserted to id_to_log.\n");
		std::cout.flush();
		return;
            }
        }
        //no available metadata entry
        pthread_mutex_unlock(&metadata_entry_assignment_lock);
        throw derecho::derecho_exception("No available metadata entry.");
    }
}

LogEntry* PersistThreads::read_entry(const uint32_t& id, const uint64_t& index) {
    return &(id_to_log[id][index]);
}

void* PersistThreads::read_data(const uint32_t& id, const uint64_t& index) {
    LogEntry* log_entry = read_entry(id, index);
    char* buf = (char*)spdk_malloc(log_entry->fields.dlen, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    uint64_t data_offset = log_entry->fields.ofst;
    non_atomic_rw(buf, log_entry->fields.dlen, data_offset, BLOCKING, DATA, false, id);
    return buf;
}

}  // namespace spdk
}  // namespace persistent
