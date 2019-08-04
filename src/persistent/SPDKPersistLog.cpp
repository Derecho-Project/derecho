#include <derecho/persistent/detail/SPDKPersistLog.hpp>

using namespace std;

namespace persistent {
namespace spdk {
void SPDKPersistLog::head_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->m_currLogMetadata.fields.head_lock) != 0)
        ;
}

void SPDKPersistLog::head_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->m_currLogMetadata.fields.head_lock) != 0)
        ;
}

void SPDKPersistLog::head_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->m_currLogMetadata.fields.head_lock) != 0)
        ;
}

void SPDKPersistLog::tail_rlock() noexcept(false) {
    while(pthread_rwlock_rdlock(&this->m_currLogMetadata.fields.tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_wlock() noexcept(false) {
    while(pthread_rwlock_wrlock(&this->m_currLogMetadata.fields.tail_lock) != 0)
        ;
}

void SPDKPersistLog::tail_unlock() noexcept(false) {
    while(pthread_rwlock_unlock(&this->m_currLogMetadata.fields.tail_lock) != 0)
        ;
}

void SPDKPersistLog::advanceVersion(const int64_t& ver) noexcept(false) {
    tail_wlock();
    // Step 0: update metadata entry
    this->m_currLogMetadata.fields.ver = ver;
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
        general_spdk_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
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
    free(control_request->buf);
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
    // Step 3: initialize threads
    for(int i = 0; i < NUM_DATA_PLANE; i++) {
        data_plane[i] = thread([]() {
            do {
                sem_wait(&new_data_request);
                // TODO: Process data write request
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

int64_t SPDKPersistLog::getLength() noexcept(false) {
    head_rlock();
    tail_rlock();
    int64_t len = (this->m_currLogMetadata.fields.tail - this->m_currLogMetadata.fields.head) / 64;
    tail_unlock();
    head_unlock();

    return len;
}

int64_t SPDKPersistLog::getEarliestIndex() noexcept(false) {
    head_rlock();
    int64_t idx = this->m_currLogMetadata.fields.head / 64;
    head_unlock();

    return idx;
}

int64_t SPDKPersistLog::getLatestIndex() noexcept(false) {
    tail_rlock();
    int64_t idx = this->m_currLogMetadata.fields.tail / 64;
    tail_unlock();

    return idx;
}

}  // namespace spdk
}  // namespace persistent
