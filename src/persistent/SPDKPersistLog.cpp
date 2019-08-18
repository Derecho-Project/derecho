#include <derecho/persistent/detail/SPDKPersistLog.hpp>

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
