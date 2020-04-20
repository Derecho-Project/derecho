#include <derecho/core/derecho_exception.hpp>
#include <derecho/persistent/detail/SPDKPersistLog.hpp>
#include <iostream>

using namespace std;

namespace persistent {
namespace spdk {

SPDKPersistLog::SPDKPersistLog(const std::string& name) noexcept(false) : PersistLog(name) {
    //Initialize locks
    std::unique_lock head_wlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    PersistThreads::get();
    if(pthread_mutex_lock(&PersistThreads::get()->metadata_load_lock)) {
        throw derecho::derecho_exception("Failed to grab metadata_load_lock");
    }
    std::cout << "Loading log name: " << name << std::endl;
    PersistThreads::get()->load(name, &this->m_currLogMetadata);
    pthread_mutex_unlock(&PersistThreads::get()->metadata_load_lock);
}

void SPDKPersistLog::append(const void* pdata,
                            const uint64_t& size, const version_t& ver,
                            const HLC& mhlc) noexcept(false) {
    std::shared_lock head_rlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    if(ver <= METADATA.ver) {
        //throw an exception
        throw derecho::derecho_exception("the version to append is smaller than the current version.");
    }
    
    if((((sizeof(LogEntry) * METADATA.tail) >> SPDK_SEGMENT_BIT) - ((sizeof(LogEntry) * METADATA.head) >> SPDK_SEGMENT_BIT)) > SPDK_LOG_ENTRY_ADDRESS_TABLE_LENGTH) {
        //throw an exception
        throw derecho::derecho_exception("Ran out of log space.");
    }
    PersistThreads::get()->append(METADATA.id,
                                  (char*)pdata, 
                                  size, ver,
                                  mhlc);
}

void SPDKPersistLog::advanceVersion(const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    if(ver <= METADATA.ver) {
        throw derecho::derecho_exception("the version to append is smaller than the current version.");
    }
    METADATA.ver = ver;
}

int64_t
SPDKPersistLog::getLength() noexcept(false) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    return (METADATA.tail - METADATA.head);
}

int64_t SPDKPersistLog::getEarliestIndex() noexcept(false) {
    std::shared_lock head_rlock(head_mutex);
    return METADATA.head;
}

int64_t SPDKPersistLog::getLatestIndex() noexcept(false) {
    std::shared_lock tail_rlock(tail_mutex);
    return METADATA.tail - 1;
}

int64_t SPDKPersistLog::getVersionIndex(const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    return binarySearch<int64_t>(
         [&](const LogEntry* ple) {
             return ple->fields.ver;
         },
         ver);
}

int64_t SPDKPersistLog::getHLCIndex(const HLC& hlc) noexcept(false) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    int64_t begin = METADATA.head;
    int64_t end = METADATA.tail - 1;
    int64_t res = -1;
    while(begin <= end) {
        int mid = (begin + end) / 2;
	LogEntry* mid_entry;
	PersistThreads::Guard entry_guard;
	std::tie(mid_entry, entry_guard) = PersistThreads::get()->read_entry(METADATA.id, mid);
        if(mid_entry->fields.hlc_r == hlc.m_rtc_us) {
            res = mid;
            break;
        } else if(!(mid_entry->fields.hlc_r > hlc.m_rtc_us || (mid_entry->fields.hlc_r == hlc.m_rtc_us && mid_entry->fields.hlc_r > hlc.m_logic))) {
            begin = mid + 1;
        } else {
            end = mid - 1;
        }
    }

    if(res == -1) {
        // Failed to find the version
        throw derecho::derecho_exception("Failed to find the hlc.");
    }

    return res;
}

version_t SPDKPersistLog::getEarliestVersion() noexcept(false) {
    std::shared_lock head_rlock(head_mutex);
    LogEntry* earliest_entry;
    PersistThreads::Guard entry_guard;
    std::tie(earliest_entry, entry_guard) = PersistThreads::get()->read_entry(METADATA.id, METADATA.head);
    return earliest_entry->fields.ver;
}

version_t SPDKPersistLog::getLatestVersion() noexcept(false) {
    std::shared_lock tail_rlock(tail_mutex);
    return METADATA.ver;
}

int64_t SPDKPersistLog::lower_bound(const HLC& hlc) {
	int64_t begin = METADATA.head;
	int64_t end = METADATA.tail - 1;
	while(begin <= end) {
		int mid = (begin + end) / 2;
		LogEntry* mid_entry;
		PersistThreads::Guard entry_guard;
		std::tie(mid_entry, entry_guard) = PersistThreads::get()->read_entry(METADATA.id, mid);
		if(!(mid_entry->fields.hlc_r < hlc.m_rtc_us || (mid_entry->fields.hlc_r == hlc.m_rtc_us && mid_entry->fields.hlc_l < hlc.m_logic))) {
			end = mid - 1;
		} else {
			begin = mid + 1;
		}
	}
	if (end != METADATA.tail - 1) {
		LogEntry* entry;
		PersistThreads::Guard guard;
        std::tie(entry, guard) = PersistThreads::get()->read_entry(METADATA.id, end + 1);
        if (entry->fields.hlc_r == hlc.m_rtc_us && entry->fields.hlc_l == hlc.m_logic) {
            end = end + 1;
        }
    }
    return end;
}

int64_t SPDKPersistLog::getMinimumIndexBeyondVersion(const int64_t& ver) noexcept(false) {
    int64_t rIndex = INVALID_INDEX;
    
    if (ver == INVALID_VERSION) {
        return METADATA.head;
    }
    
    int64_t idx = binarySearch<int64_t>(
        [&](const LogEntry* ple) {
            return ple->fields.ver;
        },
        ver);

    if (idx == -1) {
       rIndex = METADATA.head;
    } else if (idx + 1 == METADATA.tail) {
       rIndex = INVALID_INDEX;
    } else {
       rIndex = idx + 1;
    }

    return rIndex;
}

void SPDKPersistLog::trimByIndex(const int64_t& idx) {
    std::shared_lock head_rlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    if(idx < METADATA.head || idx >= METADATA.tail) {
        return;
    }

    METADATA.head = idx + 1;
}

void SPDKPersistLog::trim(const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    int64_t idx = binarySearch<int64_t>(
        [&](const LogEntry* ple){
            return ple->fields.ver;
        },
        ver);

    tail_rlock.unlock();
    head_rlock.unlock();
    
    if (idx == -1) {
        return;
    }
    
    head_rlock.lock();
    std::unique_lock tail_wlock(tail_mutex);
    idx = binarySearch<int64_t>(
        [&](const LogEntry* ple){
            return ple->fields.ver;
        },
        ver);
    if(idx != -1) {
        METADATA.head = idx + 1;
    }
    std::printf("Trimming by %ld\n", idx);
    std::cout.flush();
}

void SPDKPersistLog::trim(const HLC& hlc) {
    int64_t idx = lower_bound(hlc);
    trimByIndex(idx);
}

const version_t SPDKPersistLog::getLastPersisted() {
    return PersistThreads::get()->getLastPersisted(METADATA.id); 
}

const version_t SPDKPersistLog::persist(bool preLocked) noexcept(false) {
    if (!preLocked) {	    
        std::unique_lock head_wlock(head_mutex);
        std::unique_lock tail_wlock(tail_mutex);
    }
    return PersistThreads::get()->persist(METADATA.id);
}

void* SPDKPersistLog::getLBA(const uint64_t& lba_index) {
    void* buf = PersistThreads::get()->read_lba(lba_index);
    return buf;
}

// TODO: this is not safe or efficient:
// 1 - returning a struct causes copy
// 2 - what will happen if read_entry read null??
LogEntry SPDKPersistLog::getLogEntry(const int64_t& idx) {
    std::cout << "Waiting on lock " << std::endl; 
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    std::cout << "Grabbed lock " << std::endl;
    LogEntry* entry;
    PersistThreads::Guard guard;
    std::tie(entry, guard) = PersistThreads::get()->read_entry(METADATA.id, idx);
    return *entry;
}

size_t SPDKPersistLog::bytes_size(const int64_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    int64_t index = getMinimumIndexBeyondVersion(ver);
    size_t bsize = sizeof(int64_t) + sizeof(int64_t);
    if(index != INVALID_INDEX) {
        while(index < METADATA.tail) {
            LogEntry* log_entry;
	    PersistThreads::Guard guard;
            std::tie(log_entry, guard) = PersistThreads::get()->read_entry(METADATA.id, index);
            bsize += sizeof(LogEntry) + log_entry->fields.dlen;
            index++;
        }
    }
    return bsize;
}

size_t SPDKPersistLog::to_bytes(char* buf, const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    int64_t index = getMinimumIndexBeyondVersion(ver);
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
            LogEntry* log_entry;
	    PersistThreads::Guard log_guard; 
            std::tie(log_entry, log_guard) = PersistThreads::get()->read_entry(METADATA.id, index);
            std::copy((LogEntry*)(buf + ofst), (LogEntry*)(buf + ofst + sizeof(LogEntry)), log_entry);
            ofst += sizeof(LogEntry);
            // Write data
            void* data;
	    PersistThreads::Guard data_guard;
            std::tie(data, data_guard) = PersistThreads::get()->read_data(METADATA.id, index);
            std::copy((char*)(buf + ofst), (char*)(buf + ofst + log_entry->fields.dlen), (char*)data);
            ofst += log_entry->fields.dlen;
            index++;
        }
    }
    return ofst;
}

void SPDKPersistLog::post_object(const std::function<void(char const* const, std::size_t)>& f,
                                 const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::shared_lock tail_rlock(tail_mutex);
    int64_t index = getMinimumIndexBeyondVersion(ver);
    //latest version
    int64_t latest_version = METADATA.ver;
    f((char*)&latest_version, sizeof(int64_t));
    //num logs
    int64_t nr_log_entry = (index == INVALID_INDEX) ? 0 : (METADATA.tail - index);
    f((char*)&nr_log_entry, sizeof(int64_t));
    if(index != INVALID_INDEX) {
        while(index < METADATA.tail) {
            // Post Log entry
            LogEntry* log_entry;
	    PersistThreads::Guard log_guard;
            std::tie(log_entry, log_guard) = PersistThreads::get()->read_entry(METADATA.id, index);
            f((const char*)log_entry, sizeof(LogEntry));
            // Post data
            void* data;
	    PersistThreads::Guard data_guard;
            std::tie(data, data_guard) = PersistThreads::get()->read_data(METADATA.id, index);
            f((const char*)data, log_entry->fields.dlen);
            index++;
        }
    }
}

void SPDKPersistLog::applyLogTail(char const* v) {
    std::shared_lock head_rlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    size_t ofst = 0;
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
            HLC mhlc;
	    mhlc.m_rtc_us = log_entry->fields.hlc_l;
	    mhlc.m_logic = log_entry->fields.hlc_r;
            PersistThreads::get()->append(METADATA.id,
                                          (char*)data, log_entry->fields.dlen, log_entry->fields.ver,
                                          mhlc);
            ofst += log_entry->fields.dlen;
        }
    }
}

void SPDKPersistLog::truncate(const version_t& ver) {
    std::shared_lock head_rlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    int64_t index = binarySearch<int64_t>(
        [&](const LogEntry* ple) {
            return ple->fields.ver;
        },
        ver);
    if (index == -1) {
        METADATA.tail = METADATA.head;
    } else {
        METADATA.tail = index + 1;
    }
    if (METADATA.ver > ver) {
	METADATA.ver = ver;
    }
}

void SPDKPersistLog::zeroout() {
    std::unique_lock head_wlock(head_mutex);
    std::unique_lock tail_wlock(tail_mutex);
    METADATA.head = 0;
    METADATA.tail = 0;
    METADATA.ver = -1;
    METADATA.inuse = false;
    PersistThreads::get()->update_metadata(METADATA.id, *m_currLogMetadata.persist_metadata_info);
}

SPDKPersistLog::~SPDKPersistLog() {
    //free(PersistThreads::get()->id_to_log[METADATA.id]);
}

}  // namespace spdk
}  // namespace persistent
