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
}

void SPDKPersistLog::PersistThread::control_write_request_complete(void* args,
                                                                   const struct spdk_nvme_cpl* completion) {
    // TODO: if error occured
    if(spdk_nvme_cpl_is_error(completion)) {
        exit(1);
    }
    persist_control_request_t* control_request = (persist_control_request_t*)args;
    while(data_write_queue.front().request_id == control_request->request_id) {
        free(data_write_queue.front().buf);
        data_write_queue.pop();
    }
    free(control_request->completed);
    compeleted_request_id = control_request->request_id;
    control_write_queue.pop();
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
    // Step 3: initialize threads
    for(int i = 0; i < NUM_DATA_PLANE; i++) {
        data_plane[i] = thread([]() {
            do {
                sem_wait(&new_data_request);
                std::unique_lock<std::mutex> lck(data_queue_mtx);
                // TODO: Process data write request
                lck.release();
            } while(true);
        });
    }
    for(int i = 0; i < NUM_CONTROL_PLANE; i++) {
        control_plane[i] = thread([]() {
            do {
                // Wait until available compeleted data request
                std::unique_lock<std::mutex> lck(control_queue_mtx);
                data_request_completed.wait(lck);
                persist_control_request_t control_request = control_write_queue.front();
                do {
                    if(control_request.completed->load() == control_request.part_num) {
                        if(!control_request.handled) {
                            control_request.handled = true;
                            //TODO: submit control write request
                        }
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
    uint64_t data_sector = data_offset & (1 << (64 - SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;
    uint64_t virtual_log_segment = log_offset >> SPDK_SEGMENT_BIT;
    uint64_t log_sector = log_offset & (1 << (64 - SPDK_SEGMENT_BIT) - 1) >> general_spdk_info.sector_bit;

    // Step 1: Calculate needed segment number
    int needed_segment = 0;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] == 0) {
        needed_segment++;
    }
    int needed_data_sector = data_size >> general_spdk_info.sector_bit;
    if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] == 0) {
        needed_segment += needed_data_sector / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit));
    } else {
        needed_segment += (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) - 1;
    }

    uint16_t physical_data_segment = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment];
    uint64_t physical_data_sector = physical_data_segment * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + data_sector;

    // Step 2: Assign needed segments
    if(needed_segment > 0) {
        if(pthread_mutex_lock(&segment_assignment_lock) != 0) {
            // failed to grab the lock
        }
        int available_segment = 0;
        for(int i = 0; i < SPDK_NUM_SEGMENTS; i++) {
            if(segment_usage_table[i] == 0) {
                available_segment++;
                if(available_segment == needed_segment) {
                    break;
                }
            }
        }
        // Not enough available segments
        if(available_segment < needed_segment) {
            //TODO: throw exception
        }
        // Assign segements
        if(global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] == 0) {
            for(int i = 0; i < SPDK_NUM_SEGMENTS; i++) {
                if(segment_usage_table[i] == 0) {
                    segment_usage_table[i] = 1;
                    global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] = i;
                    needed_segment--;
                    break;
                }
            }
        }
        if(needed_segment > 0) {
            int data_assignment_start;
            if(physical_data_sector == 0) {
                data_assignment_start = virtual_data_segment;
            } else {
                data_assignment_start = virtual_data_segment + 1;
            }
            for(int i = 0; i < SPDK_NUM_SEGMENTS; i++) {
                if(segment_usage_table[i] == 0) {
                    segment_usage_table[i] = 1;
                    global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[data_assignment_start] = i;
                    needed_segment--;
                    data_assignment_start++;
                    if(needed_segment == 0) {
                        break;
                    }
                }
            }
        }
    }
    // Step 3: Submit data request
    uint64_t request_id = assigned_request_id + 1;
    assigned_request_id++;
    uint16_t part_num = (needed_data_sector + data_sector) / (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit));
    std::atomic<int> completed;
    persist_data_request_t log_entry_request;
    log_entry_request.request_id = request_id;
    log_entry_request.buf = log;
    log_entry_request.part_id = 0;
    log_entry_request.part_num = part_num;
    log_entry_request.completed = &completed;
    log_entry_request.handled = false;
    log_entry_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_log_entry_at_table[virtual_log_segment] * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + log_sector;
    log_entry_request.lba_count = 1;
    data_write_queue.push(log_entry_request);
    sem_post(&new_data_request);

    for(int i = 1; i < part_num; i++) {
        persist_data_request_t data_request;
        data_request.request_id = request_id;
        data_request.buf = data;
        data_request.part_id = i;
        data_request.part_num = part_num;
        data_request.completed = &completed;
        data_request.handled = false;
        data_request.lba = global_metadata.fields.log_metadata_entries[id].fields.log_metadata_address.segment_data_at_table[virtual_data_segment] * (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) + data_sector;
        data_request.lba_count = (SPDK_SEGMENT_SIZE / (1 << general_spdk_info.sector_bit)) - data_sector;
        data_write_queue.push(data_request);
        sem_post(&new_data_request);
        data_sector = 0;
        virtual_data_segment++;
        data += data_request.lba_count * (1 << general_spdk_info.sector_bit);
    }
    //Step 4: Submit control request
    //TODO: metadata address
    persist_control_request_t metadata_request;
    metadata_request.request_id = request_id;
    metadata_request.buf = metadata;
    metadata_request.part_num = part_num;
    metadata_request.completed = &completed;
    metadata_request.lba;
    metadata_request.lba_count = 1;
    metadata_request.handled = false;
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
