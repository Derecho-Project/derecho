#include <derecho/persistent/detail/SPDKPersistLog.hpp>

using namespace std;

namespace persistent {
namespace spdk {
void SPDKPersistLog::head_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->m_currLogMetadata.head_lock) != 0)
        ;
}

void SPDKPersistLog::head_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->m_currLogMetadata.head_lock) != 0)
        ;
}

void SPDKPersistLog::head_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->m_currLogMetadata.head_lock) != 0)
        ;
}

void SPDKPersistLog::tail_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->m_currLogMetadata.tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->m_currLogMetadata.tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->m_currLogMetadata.tail_lock) != 0)
        ;
}

void SPDKPersistLog::advanceVersion(const int64_t& ver) noexcept(false) {
    tail_wlock();
    // Step 0: update metadata entry
    this->m_currLogMetadata.persist_metadata_info->ver = ver;
    // Step 1: write the update to disk

    tail_unlock();
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
        if(ns = NULL) {
            continue;
        }
        if(!spdk_nvme_ns_is_active(ns)) {
            continue;
        }
        general_spdk_info.ns = ns;
        general_spdk_info.sector_bit = log2(spdk_nvme_ns_get_sector_size(ns));
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

SPDKPersistLog::PersistThread::PersistThread() noexcept(true) {
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
    }
    for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
        control_plane[i] = thread([i]() {
            do {
                // Wait until available compeleted data request
                std::unique_lock<std::mutex> lck(control_queue_mtx);
                data_request_completed.wait(lck);
                persist_control_request_t control_request = control_write_queue.front();
                do {
                    if(control_request.completed->load() == control_request.part_num) {
                        spdk_nvme_ns_cmd_write(general_spdk_info.ns, SpdkQpair_control[i], &control_request.buf, control_request.lba, control_request.lba_count, control_write_request_complete, (void*)&control_request, NULL);
                        control_write_queue.pop();
                    } else {
                        break;
                    }
                } while(true);
                lck.release();
            } while(true);
        });
    }
}

void SPDKPersistLog::PersistThread::append(const uint32_t& id, char* data, const uint64_t& data_offset,
                                           const uint64_t& data_size, void* log,
                                           const uint64_t& log_offset, PTLogMetadataInfo metadata) {
    // Step 0: extract virtual segment number and sector number
    uint64_t virtual_data_segment = data_offset >> SPDK_SEGMENT_BIT;
    uint64_t data_sector = data_offset & (1 << SPDK_SEGMENT_BIT - 1) >> general_spdk_info.sector_bit;
    uint64_t virtual_log_segment = log_offset >> SPDK_SEGMENT_BIT;
    uint64_t log_sector = log_offset & (1 << SPDK_SEGMENT_BIT - 1) >> general_spdk_info.sector_bit;

    // Step 1: Calculate needed segment number
    //TODO: using data_sector index as condition?
    int needed_segment = 0;
    int part_num = 0;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] == 0) {
        needed_segment++;
        part_num++;
    }
    int needed_data_sector = data_size >> general_spdk_info.sector_bit;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] == 0) {
        needed_segment += needed_data_sector / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit));
    } else {
        needed_segment += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) - 1;
    }

    uint16_t physical_data_segment = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment];
    uint64_t physical_data_sector = physical_data_segment * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + data_sector;

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
    part_num += segment_address_data_location(id, data_assignment_start + available_segment_index.size() - part_num) >> general_spdk_info.sector_bit - segment_address_data_location(id, data_assignment_start) >> general_spdk_info.sector_bit;
    part_num += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit));

    // Step 3: Submit data request for segment address translation table update
    std::atomic<int> completed;
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
        data_request.buf = new char[1 >> general_spdk_info.sector_bit];
        data_request.lba = offset >> general_spdk_info.sector_bit;
        data_request.lba_count = 1;
        char* start = (char*)&global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] - (offset & (1 << general_spdk_info.sector_bit - 1));
        std::copy(start, start + (1 << general_spdk_info.sector_bit), data_request.buf);
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
            data_request.buf = new char[1 >> general_spdk_info.sector_bit];
            data_request.lba = offset >> general_spdk_info.sector_bit;
            data_request.lba_count = 1;
            char* start = (char*)&global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[data_assignment_start] - (offset & (1 << general_spdk_info.sector_bit - 1));
            std::copy(start, start + (1 << general_spdk_info.sector_bit), data_request.buf);
            std::unique_lock<std::mutex> lck(data_queue_mtx);
            data_write_queue.push(data_request);
            lck.release();
            sem_post(&new_data_request);
            part_id++;
            next_sector += (1 >> general_spdk_info.sector_bit - offset & (1 << general_spdk_info.sector_bit - 1)) / sizeof(uint16_t);
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
    log_entry_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + log_sector;
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
        data_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + data_sector;
        data_request.lba_count = (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) - data_sector;
        std::unique_lock<std::mutex> lck(data_queue_mtx);
        data_write_queue.push(data_request);
        sem_post(&new_data_request);
        lck.release();
        data_sector = 0;
        virtual_data_segment++;
        data += data_request.lba_count * (1 << general_spdk_info.sector_bit);
    }
    //Step 4: Submit control request
    persist_control_request_t metadata_request;
    metadata_request.request_id = request_id;
    metadata_request.buf = metadata;
    metadata_request.part_num = part_num;
    metadata_request.completed = &completed;
    metadata_request.lba = id * (SPDK_LOG_METADATA_SIZE / (1 << general_spdk_info.sector_bit));
    metadata_request.lba_count = 1;
    control_write_queue.push(metadata_request);
}

int64_t
SPDKPersistLog::getLength() noexcept(false) {
    head_rlock();
    tail_rlock();
    int64_t len = (this->m_currLogMetadata.persist_metadata_info->tail - this->m_currLogMetadata.persist_metadata_info->head) / 64;
    tail_unlock();
    head_unlock();

    return len;
}

int64_t SPDKPersistLog::getEarliestIndex() noexcept(false) {
    head_rlock();
    int64_t idx = this->m_currLogMetadata.persist_metadata_info->head / 64;
    head_unlock();

    return idx;
}

int64_t SPDKPersistLog::getLatestIndex() noexcept(false) {
    tail_rlock();
    int64_t idx = this->m_currLogMetadata.persist_metadata_info->tail / 64;
    tail_unlock();

    return idx;
}

}  // namespace spdk
}  // namespace persistent
