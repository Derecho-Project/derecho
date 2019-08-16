#include <derecho/persistent/detail/SPDKPersistLog.hpp>
#include <cmath>

using namespace std;

namespace persistent {
namespace spdk {
void SPDKPersistLog::head_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->head_lock) != 0)
        ;
}

void SPDKPersistLog::head_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->head_lock) != 0)
        ;
}

void SPDKPersistLog::head_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->head_lock) != 0)
        ;
}

void SPDKPersistLog::tail_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->tail_lock) != 0)
        ;
}

bool SPDKPersistLog::PersistThread::probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                                             struct spdk_nvme_ctrlr_opts* opts) {
    return true;
}

void SPDKPersistLog::PersistThread::attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                                              struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts) {
    struct spdk_nvme_ns* ns;
    // Step 0: store the ctrlr
    general_spdk_info.ctrlr = ctrlr;
    // Step 1: register one of the namespaces
    int num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
    for(int nsid = 1; nsid <= num_ns; nsid++) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if(ns == NULL) {
            continue;
        }
        if(!spdk_nvme_ns_is_active(ns)) {
            continue;
        }
        general_spdk_info.ns = ns;
        general_spdk_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        general_spdk_info.sector_bit = std::log2(general_spdk_info.sector_size);
        break;
    }
    // TODO: if no available ns, throw an exception
}

void SPDKPersistLog::PersistThread::data_write_request_complete(void* args,
                                                                const struct spdk_nvme_cpl* completion) {
    // TODO: if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        exit(1);
    }
    persist_data_request_t* data_request = (persist_data_request_t*)args;
    data_request->completed++;
    if(data_request->completed->load() == data_request->part_num) {
        data_request_completed.notify_all();
    }
    free(data_request->buf);
    delete data_request;
}

void SPDKPersistLog::PersistThread::load_request_complete(void* args,
                                                          const struct spdk_nvme_cpl* completion) {
    *(bool*)args = true;
}

void SPDKPersistLog::PersistThread::control_write_request_complete(void* args,
                                                                   const struct spdk_nvme_cpl* completion) {
    // TODO: if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        exit(1);
    }
    persist_control_request_t* control_request = (persist_control_request_t*)args;
    free(control_request->completed);
    compeleted_request_id = control_request->request_id;
    delete control_request;
}

uint64_t segment_address_log_location(const uint32_t& id, const uint16_t& virtual_log_segment) {
    return id * sizeof(persist_thread_log_metadata) + virtual_log_segment * sizeof(uint16_t);
}

uint64_t segment_address_data_location(const uint32_t& id, const uint16_t& virtual_data_segment) {
    return id * sizeof(persist_thread_log_metadata) + SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH * sizeof(uint16_t) + virtual_data_segment * sizeof(uint16_t);
}

SPDKPersistLog::PersistThread::PersistThread() {
    // Step 0: initialize spdk nvme info
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    if(spdk_env_init(&opts) < 0) {
        // Failed to initialize spdk env, throw an exception
        return;
    }
    int res = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
    if(res != 0) {
        //TODO: initialization of nvme ctrlr failed
    }
    // Step 1: get qpair for each plane
    for(int i = 0; i < NUM_DATA_PLANE; i++) {
        SpdkQpair_data[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
        if(SpdkQpair_data[i] == NULL) {
            //TODO: qpair initialization failed
        }
    }
    for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
        SpdkQpair_control[i] = spdk_nvme_ctrlr_alloc_io_qpair(general_spdk_info.ctrlr, NULL, 0);
        if(SpdkQpair_control[i] == NULL) {
            //TODO: qpair initialization failed
        }
    }

    // Step 2: initialize sem and locks
    if(sem_init(&new_data_request, 0, 0) != 0) {
        // TODO: sem init failed
    }
    if(pthread_mutex_init(&segment_assignment_lock, NULL) != 0) {
        //TODO: mutex init failed
    }
    if(pthread_mutex_init(&metadata_entry_assignment_lock, NULL) != 0) {
        //TODO: mutex init failed
    }
    // Step 3: initialize other fields
    compeleted_request_id = -1;
    assigned_request_id = -1;
    segment_usage_table.reset();
    segment_usage_table[0].flip();
    initialized = false;
    // Step 3: initialize threads
    for(int i = 0; i < NUM_DATA_PLANE; i++) {
        data_plane[i] = thread([i]() {
            do {
                sem_wait(&new_data_request);
                std::unique_lock<std::mutex> lck(data_queue_mtx);
                persist_data_request_t data_request = data_write_queue.front();
                spdk_nvme_ns_cmd_write(general_spdk_info.ns, SpdkQpair_data[i], data_request.buf, data_request.lba, data_request.lba_count, data_write_request_complete, (void*)&data_request, NULL);
                lck.release();
            } while(true);
        });
        std::thread thread([i]() {
            do {
                spdk_nvme_qpair_process_completions(SpdkQpair_control[i], 0);
            } while(true);
        });
    }
    for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
        control_plane[i] = thread([i]() {
            do {
                // Wait until available compeleted data request
                std::unique_lock<std::mutex> lck(control_queue_mtx);
                data_request_completed.wait(lck);
                persist_control_request_t control_request = control_write_queue.front();
                do {
                    if(*control_request.completed == control_request.part_num) {
                        if(control_request.cb_fn == NULL) {
                            spdk_nvme_ns_cmd_write(general_spdk_info.ns, SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_write_request_complete, (void*)&control_request, NULL);
                        } else {
                            spdk_nvme_ns_cmd_write(general_spdk_info.ns, SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_request.cb_fn, control_request.args, NULL);
                        }
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
        std::thread thread([i]() {
            do {
                spdk_nvme_qpair_process_completions(SpdkQpair_control[i], 0);
            } while(true);
        });
    }
    initialized = true;
}

void SPDKPersistLog::PersistThread::append(const uint32_t& id, char* data, const uint64_t& data_offset,
                                           const uint64_t& data_size, void* log,
                                           const uint64_t& log_offset, PTLogMetadataInfo metadata) {
    // Step 0: extract virtual segment number and sector number
    uint64_t virtual_data_segment = data_offset >> SPDK_SEGMENT_BIT % SPDK_DATA_ADDRESS_TABLE_LENGTH;
    uint64_t data_sector = data_offset & ((1 << SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;
    uint64_t virtual_log_segment = log_offset >> SPDK_SEGMENT_BIT % SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
    uint64_t log_sector = log_offset & ((1 << SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;

    // Step 1: Calculate needed segment number
    //TODO: using data_sector index as condition?
    //TODO: add ring buffer logic
    uint16_t needed_segment = 0;
    uint16_t part_num = 0;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] == 0) {
        needed_segment++;
        part_num++;
    }
    int needed_data_sector = data_size >> general_spdk_info.sector_bit;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] == 0) {
        needed_segment += needed_data_sector / (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size);
    } else {
        needed_segment += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) - 1;
    }

    uint16_t physical_data_segment = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment];
    uint64_t physical_data_sector = physical_data_segment * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + data_sector;

    if(pthread_mutex_lock(&segment_assignment_lock) != 0) {
        // failed to grab the lock
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
        //TODO: throw excpetion
    }

    //Calculate part num
    int data_assignment_start;
    if(physical_data_sector == 0) {
        data_assignment_start = virtual_data_segment;
    } else {
        data_assignment_start = virtual_data_segment + 1;
    }

    if(available_segment_index.size() > part_num) {
        part_num++;
    }
    part_num += (segment_address_data_location(id, data_assignment_start + available_segment_index.size() - part_num) >> PAGE_BIT) - (segment_address_data_location(id, data_assignment_start) >> PAGE_BIT);
    if(needed_data_sector > 0) {
        part_num++;
    }
    part_num += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size);

    // Step 3: Submit data request for segment address translation table update
    std::atomic<int> completed = 0;
    uint64_t request_id = assigned_request_id + 1;
    assigned_request_id++;
    uint16_t part_id = 0;

    std::set<uint16_t>::iterator it = available_segment_index.begin();
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] == 0) {
        global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] = *it;
        segment_usage_table[*it] = 1;
        uint64_t offset = segment_address_log_location(id, virtual_log_segment);
        persist_data_request_t data_request;
        data_request.request_id = request_id;
        data_request.completed = &completed;
        data_request.part_id = part_id;
        data_request.buf = new char[PAGE_SIZE];
        data_request.lba = offset >> general_spdk_info.sector_bit;
        data_request.lba_count = PAGE_SIZE / general_spdk_info.sector_size;
        char* start = (char*)global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table;
        std::copy(start, start + PAGE_SIZE,(char *)data_request.buf);
        std::unique_lock<std::mutex> lck(data_queue_mtx);
        data_write_queue.push(data_request);
        lck.release();
        sem_post(&new_data_request);
        it++;
        part_id++;
    }

    uint64_t next_sector = segment_address_data_location(id, data_assignment_start) >> general_spdk_info.sector_bit;
    while(it != available_segment_index.end()) {
        global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[data_assignment_start] = *it;
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
            char* start = (char*)&global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[data_assignment_start >> (PAGE_BIT - 1) << (PAGE_BIT - 1)];
            std::copy(start, start + PAGE_SIZE,(char *)data_request.buf);
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
    log_entry_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + log_sector;
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
        data_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] * (SPDK_SEGMENT_SIZE / general_spdk_info.sector_size) + data_sector;
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
    metadata_request.lba_count = 1 >> (PAGE_BIT - general_spdk_info.sector_bit);
    metadata_request.cb_fn = NULL;
    std::unique_lock<std::mutex> clck(control_queue_mtx);
    control_write_queue.push(metadata_request);
    clck.release();
}

void SPDKPersistLog::PersistThread::release_segments(void* args, const struct spdk_nvme_cpl* completion) {
    PTLogMetadataInfo* metadata = (PTLogMetadataInfo*)args;
    //Step 0: release log_segments until metadata.head; release data_segments until
    int log_seg = max((metadata->fields.head >> SPDK_SEGMENT_BIT) - 1, (int64_t)0);
    int data_seg = max((uint64_t)((id_to_log[metadata->fields.id][metadata->fields.head - 1].fields.ofst + id_to_log[metadata->fields.id]->fields.dlen) >> SPDK_SEGMENT_BIT) - 1, (uint64_t)0);
    if(pthread_mutex_lock(&segment_assignment_lock) != 0) {
        //TODO: throw exception
    }
    for(int i = 0; i < log_seg; i++) {
        segment_usage_table[global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_log_entry_at_table[i]] = 0;
    }
    for(int i = 0; i < data_seg; i++) {
        segment_usage_table[global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_data_at_table[i]] = 0;
    }
    std::fill(global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_log_entry_at_table,
              global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_log_entry_at_table + log_seg,
              0);
    std::fill(global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_data_at_table,
              global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_data_at_table + data_seg,
              0);

    //Step 1: Submit metadata update request
    uint64_t request_id = assigned_request_id + 1;
    assigned_request_id++;

    if(log_seg > 0) {
        for(int i = 0; i < log_seg * sizeof(uint16_t) / PAGE_SIZE + 1; i++) {
            persist_data_request_t metadata_request;
            metadata_request.request_id = request_id;
            metadata_request.buf = new char[PAGE_SIZE];
	    char* start = (char*)&global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_log_entry_at_table[i * PAGE_SIZE / sizeof(uint16_t)];
            std::copy(start, start + PAGE_SIZE,(char *)metadata_request.buf);
            metadata_request.part_num = 0;
            metadata_request.completed = 0;
            //metadata_request.cb_fn = NULL;
            metadata_request.lba = (metadata->fields.id * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataInfo) + i * PAGE_SIZE) >> general_spdk_info.sector_bit;
            metadata_request.lba_count = 1 >> (PAGE_BIT - general_spdk_info.sector_bit);
            std::unique_lock<std::mutex> lck(control_queue_mtx);
            data_write_queue.push(metadata_request);
            data_request_completed.notify_all();
            lck.release();
        }
    }
    if(data_seg > 0) {
        for(int i = 0; i < data_seg * sizeof(uint16_t) / PAGE_SIZE + 1; i++) {
            persist_data_request_t metadata_request;
            metadata_request.request_id = request_id;
            metadata_request.buf = new char[PAGE_SIZE];
            char* start = (char*)&global_metadata.fields.log_metadata_entries[metadata->fields.id].fields.log_metadata_address.segment_data_at_table[i * PAGE_SIZE / sizeof(uint16_t)];
            std::copy(start, start + PAGE_SIZE, (char *) metadata_request.buf);
            metadata_request.part_num = 0;
            metadata_request.completed = 0;
            //metadata_request.cb_fn = NULL;
            metadata_request.lba = (metadata->fields.id * SPDK_LOG_METADATA_SIZE + sizeof(PTLogMetadataInfo) + SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH * sizeof(uint16_t) + i * PAGE_SIZE) >> general_spdk_info.sector_bit;
            metadata_request.lba_count = 1 >> (PAGE_BIT - general_spdk_info.sector_bit);
            std::unique_lock<std::mutex> lck(control_queue_mtx);
            data_write_queue.push(metadata_request);
            data_request_completed.notify_all();
            lck.release();
        }
    }
    free(args);
    pthread_mutex_unlock(&segment_assignment_lock);
}

void SPDKPersistLog::PersistThread::update_metadata(const uint32_t& id, PTLogMetadataInfo metadata, bool garbage_collection) {
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

void SPDKPersistLog::PersistThread::load(const string& name, LogMetadata* log_metadata) {
    if(!loaded) {
        //Step 0: submit read request for the segment of all global data
        char* buf = (char*)malloc(SPDK_SEGMENT_SIZE);
        bool completed = false;
        spdk_nvme_ns_cmd_read(general_spdk_info.ns, SpdkQpair_control[0], buf, 0, SPDK_SEGMENT_SIZE / general_spdk_info.sector_size, load_request_complete, (void*)&completed, NULL);

        //Step 1: Wait until the request is completed
        while(!completed) {
            spdk_nvme_qpair_process_completions(SpdkQpair_control[0], 0);
        }

        //Step 2: Construct log_name_to_id and segment usage array
        for(int id = 0; id < SPDK_NUM_LOGS_SUPPORTED; id++) {
            //Step 2_0: copy data into metadata entry
            global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info = *(PTLogMetadataInfo*)buf;
            buf += sizeof(PTLogMetadataInfo);
            std::copy(buf, buf + sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH, (uint8_t*)&global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table);
            buf += sizeof(uint16_t) * SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH;
            std::copy(buf, buf + sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH, (uint8_t*)&global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table);
            buf += sizeof(uint16_t) * SPDK_DATA_ADDRESS_TABLE_LENGTH;

            if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.inuse) {
                //Step 2_1: Update log_name_to_id
                log_name_to_id.insert(std::pair<string, uint32_t>((char*)global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info.fields.name, id));

                //Step 2_2: Update segment_usage_array
                for(int index = 0; index < SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH; index++) {
                    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[index] != 0) {
                        segment_usage_table[global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[index]].flip();
                    }
                }
                for(int index = 0; index < SPDK_DATA_ADDRESS_TABLE_LENGTH; index++) {
                    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[index] != 0) {
                        segment_usage_table[global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[index]].flip();
                    }
                }
            }
        }
        free(buf);
        loaded = true;
    }

    //Step 3: Update log_metadata
    try {
        uint32_t id = log_name_to_id.at(name);
        log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info;
        log_metadata->log_entry = (LogEntry*)malloc(SPDK_LOG_ADDRESS_SPACE);
        int num_log_segment = log_metadata->persist_metadata_info->fields.tail >> SPDK_SEGMENT_BIT - log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT + 1;
        bool completed[num_log_segment];
        for(int i = 0; i < num_log_segment; i++) {
            spdk_nvme_ns_cmd_read(general_spdk_info.ns,
                                  SpdkQpair_control[0],
                                  (void*)&log_metadata->log_entry[log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT + i * SPDK_SEGMENT_SIZE / sizeof(uint16_t)],
                                  global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[log_metadata->persist_metadata_info->fields.head >> SPDK_SEGMENT_BIT + i] * (1 >> (SPDK_SEGMENT_BIT >> general_spdk_info.sector_bit)),
                                  1 >> (SPDK_SEGMENT_BIT >> general_spdk_info.sector_bit),
                                  load_request_complete,
                                  (void*)&completed[i],
                                  NULL);
        }
        for(int i = 0; i < num_log_segment; i++) {
            while(!completed[i]) {
                spdk_nvme_qpair_process_completions(SpdkQpair_control[0], 0);
            }
        }
        id_to_log.insert(std::pair<uint32_t, LogEntry*>(id, log_metadata->log_entry));
        PTLogMetadataInfo log_metadata = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_info;
        release_segments((void*)&log_metadata, NULL);
        return;
    } catch(const std::out_of_range& oor) {
        // Find an unused metadata entry
        if(pthread_mutex_lock(&metadata_entry_assignment_lock) != 0) {
            //TODO: throw an exception
        }
        for(int index = 0; index < SPDK_NUM_LOGS_SUPPORTED; index++) {
            if(!global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse) {
                global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info.fields.inuse = true;
                log_metadata->persist_metadata_info = &global_metadata.fields.log_metadata_entries[index].fields.log_metadata_info;
                log_name_to_id.insert(std::pair<std::string, uint32_t>(name, index));
                pthread_mutex_unlock(&metadata_entry_assignment_lock);
                log_metadata->log_entry = (LogEntry*)malloc(SPDK_LOG_ADDRESS_SPACE);
                id_to_log.insert(std::pair<uint32_t, LogEntry*>(index, log_metadata->log_entry));
                return;
            }
        }
        //TODO: no available metadata entry
        pthread_mutex_unlock(&metadata_entry_assignment_lock);
    }
}  // namespace spdk

SPDKPersistLog::SPDKPersistLog(const std::string& name) noexcept(true) : PersistLog(name) {
    while(!persist_thread.initialized)
        ;
    //Initialize locks
    if(pthread_rwlock_init(&this->head_lock, NULL) != 0) {
        //TODO
    }
    if(pthread_rwlock_init(&this->head_lock, NULL) != 0) {
        //TODO
    }
    head_wlock();
    tail_wlock();
    if(pthread_mutex_lock(&persist_thread.metadata_load_lock)) {
        //TODO
    }
    persist_thread.load(name, &this->m_currLogMetadata);
    pthread_mutex_unlock(&persist_thread.metadata_load_lock);
    tail_unlock();
    head_unlock();
}

void SPDKPersistLog::append(const void* pdata,
                            const uint64_t& size, const version_t& ver,
                            const HLC& mhlc) {
    head_rlock();
    tail_wlock();
    if(ver <= METADATA.ver) {
        //TODO: throw an exception
        tail_unlock();
        head_unlock();
    }
    NEXT_LOG_ENTRY.fields.dlen = size;
    NEXT_LOG_ENTRY.fields.ver = ver;
    NEXT_LOG_ENTRY.fields.hlc_l = mhlc.m_rtc_us;
    NEXT_LOG_ENTRY.fields.hlc_l = mhlc.m_logic;
    if(METADATA.tail - METADATA.head == 0) {
        NEXT_LOG_ENTRY.fields.ofst = 0;
    } else {
        NEXT_LOG_ENTRY.fields.ofst = LOG[METADATA.tail - 1].fields.ofst + LOG[METADATA.tail - 1].fields.dlen;
    }

    METADATA.ver = ver;
    METADATA.tail++;

    persist_thread.append(METADATA.id,
                          (char*)pdata, LOG[METADATA.tail - 1].fields.ofst,
                          LOG[METADATA.tail - 1].fields.dlen, &LOG[METADATA.tail - 1],
                          METADATA.tail - 1,
                          *m_currLogMetadata.persist_metadata_info);

    tail_unlock();
    head_unlock();
}

void SPDKPersistLog::advanceVersion(const version_t& ver) {
    head_rlock();
    tail_wlock();
    if(ver <= METADATA.ver) {
        //TODO: throw an exception
        tail_unlock();
        head_unlock();
    }
    METADATA.ver = ver;
    persist_thread.update_metadata(METADATA.id, *m_currLogMetadata.persist_metadata_info, false);
}

int64_t
SPDKPersistLog::getLength() noexcept(false) {
    head_rlock();
    tail_rlock();
    int64_t len = (METADATA.tail - METADATA.head);
    tail_unlock();
    head_unlock();

    return len;
}

int64_t SPDKPersistLog::getEarliestIndex() noexcept(false) {
    head_rlock();
    int64_t idx = METADATA.head;
    head_unlock();

    return idx;
}

int64_t SPDKPersistLog::getLatestIndex() noexcept(false) {
    tail_rlock();
    int64_t idx = METADATA.tail;
    tail_unlock();

    return idx;
}

int64_t SPDKPersistLog::getVersionIndex(const version_t& ver) {
    head_rlock();
    tail_rlock();
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    int res = -1;
    while(begin <= end) {
        int64_t curr_ver = LOG[(begin + end) / 2].fields.ver;
        if(curr_ver == ver) {
            res = (begin + end) / 2;
            break;
        } else if(curr_ver > ver) {
            begin = (begin + end) / 2 + 1;
        } else if(curr_ver < ver) {
            end = (begin + end) / 2 - 1;
        }
    }
    if(res == -1) {
        // TODO: Failed to find the version
    }
    head_unlock();
    tail_unlock();
    return res;
}

version_t SPDKPersistLog::getEarliestVersion() noexcept(false) {
    head_rlock();
    version_t ver = LOG[METADATA.head].fields.ver;
    head_unlock();
    return ver;
}

version_t SPDKPersistLog::getLatestVersion() noexcept(false) {
    tail_rlock();
    version_t ver = METADATA.ver;
    tail_unlock();
    return ver;
}

int64_t SPDKPersistLog::upper_bound(const version_t& ver) {
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
        if(ver >= LOG[mid].fields.ver) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}

int64_t SPDKPersistLog::upper_bound(const HLC& hlc) {
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
        if(!(LOG[mid].fields.hlc_r > hlc.m_rtc_us || (LOG[mid].fields.hlc_r == hlc.m_rtc_us && LOG[mid].fields.hlc_r > hlc.m_logic))) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}

void SPDKPersistLog::trimByIndex(const int64_t& idx) {
    head_wlock();
    tail_rlock();
    if(idx < METADATA.head || idx >= METADATA.tail) {
        tail_unlock();
        head_unlock();
        return;
    }

    METADATA.head = idx + 1;
    persist_thread.update_metadata(METADATA.id, *m_currLogMetadata.persist_metadata_info, true);
    tail_unlock();
    head_unlock();
}

void SPDKPersistLog::trim(const version_t& ver) {
    int64_t idx = upper_bound(ver);
    trimByIndex(idx);
}

void SPDKPersistLog::trim(const HLC& hlc) {
    int64_t idx = upper_bound(hlc);
    trimByIndex(idx);
}

}  // namespace spdk
}  // namespace persistent
