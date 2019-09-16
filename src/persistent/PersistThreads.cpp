#include <cmath>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/persistent/detail/PersistThreads.hpp>
#include <iostream>

#define DUMMY_CB 1
#define COMPLETED_CB 2
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
    *(bool*)args = true;
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
    m_PersistThread->id_to_last_version.insert(std::pair<uint32_t, int64_t>(control_request->buf.fields.id, control_request->buf.fields.ver));
    free(control_request->completed);
    m_PersistThread->compeleted_request_id = control_request->request_id;
    delete control_request;
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
	bool completed = false;
        // Use different callback function based on input mode
        switch (mode) {
            case DUMMY_CB:
                {
		    data_request.cb_fn = dummy_request_complete;
                    data_request.is_write = is_write;
		    completed = true;
                }                
                break;

            case COMPLETED_CB:
                {                
                    data_request.cb_fn = load_request_complete;
                    data_request.args = (void*)&completed;
                    completed_queue.push(&completed);
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
	while(!completed);
    }

    // Wait for completion
    switch (mode) {
        case DUMMY_CB:
            return 0;

        case COMPLETED_CB:
            while (completed_queue.size() > 0) {
                bool* completed_ptr = completed_queue.front();
                completed_queue.pop();
                while (!*completed_ptr);
            }
            return 0;

        default:
            return 0;
    }
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
            SpdkQpair_data[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
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
            SpdkQpair_control[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
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
                    std::printf("Waiting for new data request.\n");
                    std::cout.flush();
                    sem_wait(&new_data_request);
                    std::printf("Done waiting for new.\n");
                    std::cout.flush();
                    std::unique_lock<std::mutex> lck(m_PersistThread->data_queue_mtx);
                    persist_data_request_t data_request = data_write_queue.front();
                    if(data_request.is_write) {
                        std::printf("Get new data write request.\n");
                        std::cout.flush();
                        if(data_request.cb_fn == NULL) {
                            spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_write_request_complete, (void*)&data_request, 0);
                        } else {
                            spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_request.cb_fn, data_request.args, 0);
                        }
                        std::printf("Submitted new data write request.\n");
                        std::cout.flush();
                        lck.unlock();
                    } else {
                        std::printf("Get new data read request.\n");
                        std::cout.flush();
                        if(data_request.cb_fn == NULL) {
                            std::printf("Get new data read request without given callback.\n");
                            std::cout.flush();
                            int rc = spdk_nvme_ns_cmd_read(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_request.cb_fn, data_request.args, 0);
                            if (rc != 0){
//                               std::printf("Failed to initialize read io. lba_count: %d, max_io: %d, stripe: %d, qrequest: %d\n", data_request.lba_count, general_spdk_info.ns->sectors_per_max_io, general_spdk_info.ns->sectors_per_stripe, SpdkQpair_data[i]->ctrlr->opts.io_queue_requests);
                               std::cout.flush();
                            }
                        } else {
                            std::printf("Get new data request with given callback.\n");
                            std::cout.flush();
                            int rc = spdk_nvme_ns_cmd_read(general_spdk_info.ns,SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_request.cb_fn, data_request.args, 0);
                            if (rc != 0){
                              std::fprintf(stderr, "Failed to initialize read io %d with lba_count %d.\n", rc, data_request.lba_count);
                              std::cout.flush();
                            }
                        }
                        std::printf("Submitted new data read request.\n");
                        std::cout.flush();
                        lck.unlock();
                    }

                } while(true);
            });
            data_plane[i].detach();
            std::printf("Initialized data_plane %d.\n", i);
            std::cout.flush();
            std::thread thread([&]() {
                do {
                    spdk_nvme_qpair_process_completions(SpdkQpair_data[i], 0);
                } while(true);
            });
            thread.detach();
            std::printf("Initialized polling thread %d.\n", i);
            std::cout.flush();
        }
        for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
            control_plane[i] = std::thread([&]() {
                do {
                    // Wait until available compeleted data request
                    std::unique_lock<std::mutex> lck(control_queue_mtx);
                    data_request_completed.wait(lck);
                    persist_control_request_t control_request =control_write_queue.front();
                    do {
                        if(*control_request.completed == control_request.part_num) {
                            if(control_request.cb_fn == NULL) {
                                spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_write_request_complete, (void*)&control_request, 0);
                            } else {
                                spdk_nvme_ns_cmd_write(general_spdk_info.ns,SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_request.cb_fn, control_request.args, 0);
                            }
                            control_write_queue.pop();
                            if(control_write_queue.size() == 0) {
                                break;
                            } else {
                                control_request =control_write_queue.front();
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
            std::thread thread([&]() {
                do {
                    spdk_nvme_qpair_process_completions(SpdkQpair_control[i], 0);
                } while(true);
            });
            thread.detach();
            std::printf("INitialized polling thread %d.\n", i);
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
    metadata_request.buf = metadata;
    metadata_request.part_num = part_num;
    metadata_request.completed = &completed;
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
    metadata_request.buf = metadata;
    metadata_request.part_num = 0;
    metadata_request.completed = &completed;
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
        update_segment(buf, SPDK_SEGMENT_SIZE, 20, COMPLETED_CB, false);
        std::printf("Submitted data request for log metadata.\n");
        std::cout.flush();
        
         
        //Step 1: Wait until the request is completed
       // while(!completed){
       //   spdk_nvme_qpair_process_completions(SpdkQpair_data[0], 0);
       // }

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
        LogEntry* log_entry = (LogEntry*)malloc(SPDK_LOG_ADDRESS_SPACE);
        int num_log_segment = (log_metadata->persist_metadata_info->fields.tail >> SPDK_SEGMENT_BIT) - (log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT) + 1;
        bool completed[num_log_segment];
        uint64_t request_id = assigned_request_id + 1;
        assigned_request_id++;
        for(int i = 0; i < num_log_segment; i++) {
            persist_data_request_t logentry_request;
            logentry_request.request_id = request_id;
            logentry_request.buf = (void*)&log_entry[(log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT) + i * SPDK_SEGMENT_SIZE / sizeof(uint16_t)];
            logentry_request.lba = LOG_AT_TABLE(id)[(log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT) + i] * (1 >> (SPDK_SEGMENT_BIT >> general_spdk_info.sector_bit));
            logentry_request.lba_count = 1 >> (SPDK_SEGMENT_BIT >> general_spdk_info.sector_bit);
            logentry_request.cb_fn = load_request_complete;
            logentry_request.is_write = false;
            logentry_request.args = (void*)&completed[i];
            std::unique_lock<std::mutex> lck(data_queue_mtx);
            data_write_queue.push(logentry_request);
            lck.release();
        }
        for(int i = 0; i < num_log_segment; i++) {
            while(!completed[i])
                ;
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
    void* buf = malloc(log_entry->fields.dlen);
    uint64_t data_offset = log_entry->fields.ofst;
    uint64_t virtual_data_segment = data_offset >> SPDK_SEGMENT_BIT % SPDK_DATA_ADDRESS_TABLE_LENGTH;
    uint64_t data_sector = data_offset & ((1 << SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;

    int part_num = ((data_offset + log_entry->fields.dlen) >> SPDK_SEGMENT_BIT) - (data_offset >> SPDK_SEGMENT_BIT) + 1;
    bool completed[part_num];
    int request_id = assigned_request_id + 1;
    assigned_request_id++;
    for(int i = 0; i < part_num; i++) {
        persist_data_request_t data_request;
        data_request.request_id = request_id;
        data_request.buf = buf;
        data_request.lba = DATA_AT_TABLE(id)[virtual_data_segment] * (SPDK_SEGMENT_SIZE >> general_spdk_info.sector_bit) + data_sector;
        data_request.lba_count = (SPDK_SEGMENT_SIZE >> general_spdk_info.sector_bit) - data_sector;
        data_request.cb_fn = load_request_complete;
        data_request.is_write = false;
        data_request.args = (void*)&completed[i];
        std::unique_lock<std::mutex> lck(data_queue_mtx);
        data_write_queue.push(data_request);
        lck.release();
        virtual_data_segment++;
        data_sector = 0;
        int inc = ((SPDK_SEGMENT_SIZE >> general_spdk_info.sector_bit) - data_sector) >> general_spdk_info.sector_bit;
        buf = (void*)((char*)buf + inc);
    }

    for(int i = 0; i < part_num; i++) {
        while(!completed[i])
            ;
    }

    buf = (void*)((char*)buf - log_entry->fields.dlen);
    return buf;
}

}  // namespace spdk
}  // namespace persistent
