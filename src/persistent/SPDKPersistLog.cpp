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
    LogEntry* next_log_entry = persist_thread.read_entry(METADATA.id, METADATA.tail);
    next_log_entry->fields.dlen = size;
    next_log_entry->fields.ver = ver;
    next_log_entry->fields.hlc_l = mhlc.m_rtc_us;
    next_log_entry->fields.hlc_l = mhlc.m_logic;
    LogEntry* last_entry = persist_thread.read_entry(METADATA.id, METADATA.tail - 1);
    if(METADATA.tail - METADATA.head == 0) {
        next_log_entry->fields.ofst = 0;
    } else {
        next_log_entry->fields.ofst = last_entry->fields.ofst + last_entry->fields.dlen;
    }

    METADATA.ver = ver;
    METADATA.tail++;

    persist_thread.append(METADATA.id,
                          (char*)pdata, last_entry->fields.ofst,
                          last_entry->fields.dlen, &next_log_entry,
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
    tail_unlock();
    head_unlock();
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
        int64_t mid = (begin + end) / 2;
        LogEntry* mid_entry = persist_thread.read_entry(METADATA.id, mid);
        int64_t curr_ver = mid_entry->fields.ver;
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
    LogEntry* earliest_entry = persist_thread.read_entry(METADATA.id, METADATA.head);
    version_t ver = earliest_entry->fields.ver;
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
        LogEntry* mid_entry = persist_thread.read_entry(METADATA.id, mid);
        int64_t curr_ver = mid_entry->fields.ver;
        if(ver >= curr_ver) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}

int64_t SPDKPersistLog::lower_bound(const version_t& ver) {
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
        LogEntry* mid_entry = persist_thread.read_entry(METADATA.id, mid);
        int64_t curr_ver = mid_entry->fields.ver;
        if(ver <= curr_ver) {
            end = mid - 1;
        } else {
            begin = mid;
        }
    }
    return begin;
}

int64_t SPDKPersistLog::upper_bound(const HLC& hlc) {
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
        LogEntry* mid_entry = persist_thread.read_entry(METADATA.id, mid);
        if(!(mid_entry->fields.hlc_r > hlc.m_rtc_us || (mid_entry->fields.hlc_r == hlc.m_rtc_us && mid_entry->fields.hlc_r > hlc.m_logic))) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}

int64_t SPDKPersistLog::lower_bound(const HLC& hlc) {
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
        LogEntry* mid_entry = persist_thread.read_entry(METADATA.id, mid);
        int64_t curr_ver = mid_entry->fields.ver;
        if(!(mid_entry->fields.hlc_r < hlc.m_rtc_us || (mid_entry->fields.hlc_r == hlc.m_rtc_us && mid_entry->fields.hlc_r < hlc.m_logic))) {
            end = mid - 1;
        } else {
            begin = mid;
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
    int64_t idx = lower_bound(ver);
    trimByIndex(idx);
}

void SPDKPersistLog::trim(const HLC& hlc) {
    int64_t idx = lower_bound(hlc);
    trimByIndex(idx);
}

const version_t SPDKPersistLog::getLastPersisted() {
    return persist_thread.id_to_last_version[METADATA.id];
}

const version_t SPDKPersistLog::persist(bool preLocked) noexcept(false) {
    return persist_thread.id_to_last_version[METADATA.id];
}

const void* SPDKPersistLog::getEntry(const version_t& ver) noexcept(false) {
    head_rlock();
    tail_rlock();
    int64_t index = lower_bound(ver);
    void* buf = persist_thread.read_data(METADATA.id, index);
    tail_unlock();
    head_unlock();
    return buf;
}

const void* SPDKPersistLog::getEntry(const HLC& hlc) noexcept(false) {
    head_rlock();
    tail_rlock();
    int64_t index = lower_bound(hlc);
    void* buf = persist_thread.read_data(METADATA.id, index);
    tail_unlock();
    head_unlock();
    return buf;
}

const void* SPDKPersistLog::getEntryByIndex(const int64_t& eno) noexcept(false) {
    head_rlock();
    tail_rlock();
    void* buf = persist_thread.read_data(METADATA.id, eno);
    tail_unlock();
    head_unlock();
    return buf;
}

size_t SPDKPersistLog::bytes_size(const int64_t& ver) {
    head_rlock();
    tail_rlock();
    int64_t index = upper_bound(ver);
    size_t bsize = sizeof(int64_t) + sizeof(int64_t);
    if(index != INVALID_INDEX) {
        while(index < METADATA.tail) {
            LogEntry* log_entry = persist_thread.read_entry(METADATA.id, index);
            bsize += sizeof(LogEntry) + log_entry->fields.dlen;
            index++;
        }
    }
    head_unlock();
    tail_unlock();
    return bsize;
}

size_t SPDKPersistLog::to_bytes(char* buf, const version_t& ver) {
    head_rlock();
    tail_rlock();
    int64_t index = upper_bound(ver);
    size_t ofst = 0;
    // latest version
    *(int64_t*)(buf + ofst) = METADATA.ver;
    ofst += sizeof(int64_t);
    // num of log entries
    *(int64_t*)(buf + ofst) = (index == INVALID_INDEX) ? 0 : (METADATA.tail - index);
    ofst += sizeof(int64_t);
    if(index != INVALID_INDEX) {
        while(index < METADATA.tail) {
            // Write log entry
            LogEntry* log_entry = persist_thread.read_entry(METADATA.id, index);
            std::copy((LogEntry*)(buf + ofst), (LogEntry*)(buf + ofst + sizeof(LogEntry)), log_entry);
            ofst += sizeof(LogEntry);
            // Write data
            void* data = persist_thread.read_data(METADATA.id, index);
            std::copy((char*)(buf + ofst), (char*)(buf + ofst + log_entry->fields.dlen), (char*)data);
            ofst += log_entry->fields.dlen;
            index++;
        }
    }
    tail_unlock();
    head_unlock();
    return ofst;
}

void SPDKPersistLog::post_object(const std::function<void(char const* const, std::size_t)>& f,
                                 const version_t& ver) {
    head_rlock();
    tail_rlock();
    int64_t index = upper_bound(ver);
    //latest version
    int64_t latest_version = METADATA.ver;
    f((char*)&latest_version, sizeof(int64_t));
    //num logs
    int64_t nr_log_entry = (index == INVALID_INDEX) ? 0 : (METADATA.tail - index);
    f((char*)&nr_log_entry, sizeof(int64_t));
    if(index != INVALID_INDEX) {
        while(index < METADATA.tail) {
            // Post Log entry
            LogEntry* log_entry = persist_thread.read_entry(METADATA.id, index);
            f((char*)&log_entry, sizeof(LogEntry));
            // Post data
            void* data = persist_thread.read_data(METADATA.id, index);
            f((char*)&data, log_entry->fields.dlen);
            index++;
        }
    }
    tail_unlock();
    head_unlock();
}

void SPDKPersistLog::applyLogTail(char const* v) {
    head_rlock();
    tail_wlock();
    size_t ofst = 0;
    //latest version
    int64_t latest_version = *(int64_t*)(v + ofst);
    ofst += sizeof(int64_t);
    //num logs
    int64_t nr_log_entry = *(int64_t*)(v + ofst);
    ofst += sizeof(int64_t);
    while(nr_log_entry--) {
        LogEntry* log_entry = (LogEntry*)(v + ofst);
        ofst += sizeof(LogEntry);
        if(log_entry->fields.ver <= METADATA.ver) {
            ofst += log_entry->fields.dlen;
            continue;
        } else {
            void* data = (void*)(v + ofst);

            LogEntry* next_log_entry
                    = persist_thread.read_entry(METADATA.id, METADATA.tail);
            std::copy(next_log_entry, next_log_entry + sizeof(LogEntry), log_entry);
            LogEntry* last_entry = persist_thread.read_entry(METADATA.id, METADATA.tail - 1);
            if(METADATA.tail - METADATA.head == 0) {
                next_log_entry->fields.ofst = 0;
            } else {
                next_log_entry->fields.ofst = last_entry->fields.ofst + last_entry->fields.dlen;
            }

            METADATA.ver = log_entry->fields.ver;
            METADATA.tail++;

            persist_thread.append(METADATA.id,
                                  (char*)data, last_entry->fields.ofst,
                                  last_entry->fields.dlen, &next_log_entry,
                                  METADATA.tail - 1,
                                  *m_currLogMetadata.persist_metadata_info);
            ofst += log_entry->fields.dlen;
        }
    }
    tail_unlock();
    head_unlock();
}

void SPDKPersistLog::truncate(const version_t& ver) {
    head_rlock();
    tail_wlock();
    int64_t index = upper_bound(ver);
    METADATA.tail = index;
    persist_thread.update_metadata(METADATA.id, *m_currLogMetadata.persist_metadata_info, false);
    tail_unlock();
    head_unlock();
}

SPDKPersistLog::~SPDKPersistLog() {
    free(persist_thread.id_to_log[METADATA.id]);
}

}  // namespace spdk
}  // namespace persistent
