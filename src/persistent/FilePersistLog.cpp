#include "derecho/persistent/detail/FilePersistLog.hpp"

#include "derecho/conf/conf.hpp"
#include "derecho/persistent/detail/util.hpp"
#include "derecho/persistent/detail/logger.hpp"
#include "derecho/persistent/PersistException.hpp"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if __GNUC__ > 7
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace std;

namespace persistent {

/////////////////////////
// internal structures //
/////////////////////////

////////////////////////
// visible to outside //
////////////////////////

FilePersistLog::FilePersistLog(const string& name, const string& dataPath, bool enableSignatures)
        : PersistLog(name, enableSignatures),
          m_sDataPath(dataPath),
          m_sMetaFile(dataPath + "/" + name + "." + META_FILE_SUFFIX),
          m_sLogFile(dataPath + "/" + name + "." + LOG_FILE_SUFFIX),
          m_sDataFile(dataPath + "/" + name + "." + DATA_FILE_SUFFIX),
          m_iMaxLogEntry(derecho::getConfUInt64(CONF_PERS_MAX_LOG_ENTRY)),
          m_iMaxDataSize(derecho::getConfUInt64(CONF_PERS_MAX_DATA_SIZE)),
          m_logger(PersistLogger::get()),
          m_iLogFileDesc(-1),
          m_iDataFileDesc(-1),
          m_pLog(MAP_FAILED),
          m_pData(MAP_FAILED) {
    if(pthread_rwlock_init(&this->m_rwlock, NULL) != 0) {
        throw persistent_lock_error("rwlock_init failed", errno);
    }
    if(pthread_mutex_init(&this->m_perslock, NULL) != 0) {
        throw persistent_lock_error("mutex_init failed", errno);
    }
    dbg_trace(m_logger, "{0} constructor: before load()", name);
    if(derecho::getConfBoolean(CONF_PERS_RESET)) {
        reset();
    }
    load();
    dbg_trace(m_logger, "{0} constructor: after load()", name);
}

void FilePersistLog::reset() {
    dbg_trace(m_logger, "{0} reset state...begin", this->m_sName);
    if(fs::exists(this->m_sMetaFile)) {
        if(!fs::remove(this->m_sMetaFile)) {
            dbg_error(m_logger, "{0} reset failed to remove the file:{1}", this->m_sName, this->m_sMetaFile);
            throw persistent_file_error("Failed to remove file.", errno);
        }
        if(!fs::remove(this->m_sLogFile)) {
            dbg_error(m_logger, "{0} reset failed to remove the file:{1}", this->m_sName, this->m_sLogFile);
            throw persistent_file_error("Failed to remove file.", errno);
        }
        if(!fs::remove(this->m_sDataFile)) {
            dbg_error(m_logger, "{0} reset failed to remove the file:{1}", this->m_sName, this->m_sDataFile);
            throw persistent_file_error("Failed to remove file.", errno);
        }
    }
    dbg_trace(m_logger, "{0} reset state...done", this->m_sName);
}

void FilePersistLog::load() {
    dbg_trace(m_logger, "{0}:load state...begin", this->m_sName);
    // STEP 0: check if data path exists
    checkOrCreateDir(this->m_sDataPath);
    dbg_trace(m_logger, "{0}:checkOrCreateDir passed.", this->m_sName);
    // STEP 1: check and create files.
    bool bCreate = checkOrCreateMetaFile();
    checkOrCreateLogFile();
    checkOrCreateDataFile();
    dbg_trace(m_logger, "{0}:checkOrCreateDataFile passed.", this->m_sName);
    // STEP 2: open files
    this->m_iLogFileDesc = open(this->m_sLogFile.c_str(), O_RDWR);
    if(this->m_iLogFileDesc == -1) {
        throw persistent_file_error("Failed to open file.", errno);
    }
    this->m_iDataFileDesc = open(this->m_sDataFile.c_str(), O_RDWR);
    if(this->m_iDataFileDesc == -1) {
        throw persistent_file_error("Failed to open file.", errno);
    }
    // STEP 3: mmap to memory
    //// we map the log entry and data twice to faciliate the search and data
    //// retrieving then the data is rewinding across the buffer end as follow:
    //// [1][2][3][4][5][6][1][2][3][4][5][6]
    this->m_pLog = mmap(NULL, MAX_LOG_SIZE << 1, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(this->m_pLog == MAP_FAILED) {
        dbg_error(m_logger, "{0}:reserve map space for log failed.", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    if(mmap(this->m_pLog, MAX_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iLogFileDesc, 0) == MAP_FAILED) {
        dbg_error(m_logger, "{0}:map ringbuffer space for the first half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    if(mmap((void*)((uint64_t)this->m_pLog + MAX_LOG_SIZE), MAX_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iLogFileDesc, 0) == MAP_FAILED) {
        dbg_error(m_logger, "{0}:map ringbuffer space for the second half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    //// data ringbuffer
    this->m_pData = mmap(NULL, (size_t)(MAX_DATA_SIZE << 1), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(this->m_pData == MAP_FAILED) {
        dbg_error(m_logger, "{0}:reserve map space for data failed.", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    if(mmap(this->m_pData, (size_t)(MAX_DATA_SIZE), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iDataFileDesc, 0) == MAP_FAILED) {
        dbg_error(m_logger, "{0}:map ringbuffer space for the first half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    if(mmap((void*)((uint64_t)this->m_pData + MAX_DATA_SIZE), (size_t)MAX_DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iDataFileDesc, 0) == MAP_FAILED) {
        dbg_error(m_logger, "{0}:map ringbuffer space for the second half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
        throw persistent_file_error("mmap failed.", errno);
    }
    dbg_trace(m_logger, "{0}:data/meta file mapped to memory", this->m_sName);
    // STEP 4: initialize the header for new created Metafile
    if(bCreate) {
        m_currMetaHeader.fields.head = 0ll;
        m_currMetaHeader.fields.tail = 0ll;
        m_currMetaHeader.fields.ver = INVALID_VERSION;
        m_persMetaHeader.fields.head = INVALID_INDEX;
        m_persMetaHeader.fields.tail = INVALID_INDEX;
        m_persMetaHeader.fields.ver = INVALID_VERSION;
        // persist the header
        FPL_RDLOCK;
        FPL_PERS_LOCK;

        try {
            persistMetaHeaderAtomically(&m_currMetaHeader);
        } catch(std::exception& e) {
            FPL_PERS_UNLOCK;
            FPL_UNLOCK;
            throw;
        }
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        dbg_info(m_logger, "{0}:new header initialized.", this->m_sName);
    } else {  // load meta header from disk
        FPL_WRLOCK;
        FPL_PERS_LOCK;
        try {
            int fd = open(this->m_sMetaFile.c_str(), O_RDONLY);
            if(fd == -1) {
                throw persistent_file_error("Failed to open file.", errno);
            }
            ssize_t nRead = read(fd, (void*)&m_persMetaHeader, sizeof(MetaHeader));
            if(nRead != sizeof(MetaHeader)) {
                close(fd);
                throw persistent_file_error("Failed to read file.", errno);
            }
            close(fd);
            m_currMetaHeader = m_persMetaHeader;
            // update mhlc index
            for(int64_t idx = m_currMetaHeader.fields.head; idx < m_currMetaHeader.fields.tail; idx++) {
                struct hlc_index_entry _ent;
                _ent.hlc.m_rtc_us = LOG_ENTRY_AT(idx)->fields.hlc_r;
                _ent.hlc.m_logic = LOG_ENTRY_AT(idx)->fields.hlc_l;
                _ent.log_idx = idx;
                this->hidx.insert(_ent);
            }
        } catch(std::exception& e) {
            FPL_PERS_UNLOCK;
            FPL_UNLOCK;
            throw;
        }

        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
    }
    // STEP 5: update m_hlcLE with the latest event: we don't need this anymore
    //if (m_currMetaHeader.fields.eno >0) {
    //  if (this->m_hlcLE.m_rtc_us < CURR_LOG_ENTRY->fields.hlc_r &&
    //    this->m_hlcLE.m_logic < CURR_LOG_ENTRY->fields.hlc_l){
    //    this->m_hlcLE.m_rtc_us = CURR_LOG_ENTRY->fields.hlc_r;
    //    this->m_hlcLE.m_logic = CURR_LOG_ENTRY->fields.hlc_l;
    //  }
    //}
    dbg_trace(m_logger, "{0}:load state...done", this->m_sName);
}

FilePersistLog::~FilePersistLog() noexcept(true) {
    pthread_rwlock_destroy(&this->m_rwlock);
    pthread_mutex_destroy(&this->m_perslock);
    if(this->m_pData != MAP_FAILED) {
        munmap(m_pData, (size_t)(MAX_DATA_SIZE << 1));
    }
    this->m_pData = nullptr;  // prevent ~MemLog() destructor to release it again.
    if(this->m_pLog != MAP_FAILED) {
        munmap(m_pLog, MAX_LOG_SIZE);
    }
    this->m_pLog = nullptr;  // prevent ~MemLog() destructor to release it again.
    if(this->m_iLogFileDesc != -1) {
        close(this->m_iLogFileDesc);
    }
    if(this->m_iDataFileDesc != -1) {
        close(this->m_iDataFileDesc);
    }
}

inline void FilePersistLog::do_append_validation(const uint64_t size, const int64_t ver) {
    if(NUM_FREE_SLOTS < 1) {
        dbg_error(m_logger, "{0}-append exception no free slots in log! NUM_FREE_SLOTS={1}",
                  this->m_sName, NUM_FREE_SLOTS);
        dbg_flush(m_logger);
        FPL_UNLOCK;
        std::cerr << "No space in log: FREESLOT=" << NUM_FREE_SLOTS << ",version=" << ver << std::endl;
        throw persistent_log_full("No free slots in the log.");
    }
    if(NUM_FREE_BYTES < (signature_size + size)) {
        dbg_error(m_logger, "{0}-append exception no space for data: NUM_FREE_BYTES={1}, size={2}, signature_size={3}",
                  this->m_sName, NUM_FREE_BYTES, size, signature_size);
        dbg_flush(m_logger);
        FPL_UNLOCK;
        std::cerr << "No space for data: FREE:" << NUM_FREE_BYTES << ",size=" << size
                  << ",signature_size=" << signature_size << std::endl;
        throw persistent_log_full("Insufficient space in the log for the data.");
    }
    if((CURR_LOG_IDX != INVALID_INDEX) && (m_currMetaHeader.fields.ver >= ver)) {
        int64_t cver = m_currMetaHeader.fields.ver;
        dbg_error(m_logger, "{0}-append version already exists! cur_ver:{1} new_ver:{2}", this->m_sName,
                  (int64_t)cver, (int64_t)ver);
        dbg_flush(m_logger);
        FPL_UNLOCK;
        std::cerr << "Invalid version: cver=" << cver << ",ver=" << ver << std::endl;
        throw persistent_invalid_version(ver);
    }
}

void FilePersistLog::append(const void* pdat, uint64_t size, version_t ver, const HLC& mhlc) {
    dbg_trace(m_logger, "{0} append event ({1},{2})", this->m_sName, mhlc.m_rtc_us, mhlc.m_logic);
    FPL_RDLOCK;

    do_append_validation(size, ver);

    FPL_UNLOCK;
    dbg_trace(m_logger, "{0} append:validate check1 Finished.", this->m_sName);

    FPL_WRLOCK;
    do_append_validation(size, ver);
    dbg_trace(m_logger, "{0} append:validate check2 Finished.", this->m_sName);

    // copy data
    // we reserve the first 'signature_size' bytes at the beginning of NEXT_DATA.
    memcpy(reinterpret_cast<void*>(reinterpret_cast<uint64_t>(NEXT_DATA) + signature_size), pdat, size);
    dbg_trace(m_logger, "{0} append:data ({1} bytes) is copied to log.", this->m_sName, size);

    // fill the log entry
    NEXT_LOG_ENTRY->fields.ver = ver;
    NEXT_LOG_ENTRY->fields.sdlen = signature_size + size;
    NEXT_LOG_ENTRY->fields.ofst = NEXT_DATA_OFST;
    NEXT_LOG_ENTRY->fields.hlc_r = mhlc.m_rtc_us;
    NEXT_LOG_ENTRY->fields.hlc_l = mhlc.m_logic;
    /* No Sync required here. */

    // update meta header
    this->hidx.insert(hlc_index_entry{mhlc, m_currMetaHeader.fields.tail});
    m_currMetaHeader.fields.tail++;
    m_currMetaHeader.fields.ver = ver;
    dbg_trace(m_logger, "{0} append:log entry and meta data are updated.", this->m_sName);
    /* No sync */
    dbg_trace(m_logger, "{0} append a log ver:{1} hlc:({2},{3})", this->m_sName,
              ver, mhlc.m_rtc_us, mhlc.m_logic);
    FPL_UNLOCK;
}

void FilePersistLog::advanceVersion(version_t ver) {
    FPL_WRLOCK;
    dbg_trace(m_logger, "{} advance version to {}.", this->m_sName, ver);
    if(m_currMetaHeader.fields.ver < ver) {
        m_currMetaHeader.fields.ver = ver;
    } else {
        FPL_UNLOCK;
        throw persistent_invalid_version(ver);
    }
    FPL_UNLOCK;
}

version_t FilePersistLog::persist(version_t ver, bool preLocked) {
    int64_t ver_ret = INVALID_VERSION;
    if(!preLocked) {
        FPL_PERS_LOCK;
        FPL_RDLOCK;
    }

    if(m_currMetaHeader == m_persMetaHeader) {
        if(CURR_LOG_IDX != INVALID_INDEX) {
            //ver_ret = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver;
            ver_ret = m_currMetaHeader.fields.ver;
        }
        if(!preLocked) {
            FPL_UNLOCK;
            FPL_PERS_UNLOCK;
        }
        dbg_trace(m_logger, "{} persist returning early with version {}", this->m_sName, ver_ret);
        return ver_ret;
    }

    //flush data
    dbg_trace(m_logger, "{0} flush data,log,and meta.", this->m_sName);
    try {
        // shadow the current state
        void *flush_dstart = nullptr, *flush_lstart = nullptr;
        size_t flush_dlen = 0, flush_llen = 0;
        MetaHeader shadow_header = m_currMetaHeader;
        if((NUM_USED_SLOTS > 0) && (NEXT_LOG_ENTRY > NEXT_LOG_ENTRY_PERS)) {
            flush_dlen = (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + LOG_ENTRY_AT(CURR_LOG_IDX)->fields.sdlen - NEXT_LOG_ENTRY_PERS->fields.ofst);
            // flush data
            flush_dstart = ALIGN_TO_PAGE(NEXT_DATA_PERS);
            flush_dlen += ((int64_t)NEXT_DATA_PERS) % PAGE_SIZE;
            // flush log
            flush_lstart = ALIGN_TO_PAGE(NEXT_LOG_ENTRY_PERS);
            flush_llen = ((size_t)NEXT_LOG_ENTRY - (size_t)NEXT_LOG_ENTRY_PERS) + ((int64_t)NEXT_LOG_ENTRY_PERS) % PAGE_SIZE;
        }
        if(NUM_USED_SLOTS > 0) {
            //get the latest flushed version
            //ver_ret = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver;
            ver_ret = m_currMetaHeader.fields.ver;
        }
        if(!preLocked) {
            FPL_UNLOCK;
        }
        if(flush_dlen > 0) {
            if(msync(flush_dstart, flush_dlen, MS_SYNC) != 0) {
                throw persistent_file_error("msync failed.", errno);
            }
        }
        if(flush_llen > 0) {
            if(msync(flush_lstart, flush_llen, MS_SYNC) != 0) {
                throw persistent_file_error("msync failed.", errno);
            }
        }
        // flush meta data
        this->persistMetaHeaderAtomically(&shadow_header);
    } catch(std::exception& e) {
        if(!preLocked) {
            FPL_PERS_UNLOCK;
        }
        throw;
    }
    dbg_trace(m_logger, "{0} flush data,log,and meta...done.", this->m_sName);

    if(!preLocked) {
        FPL_PERS_UNLOCK;
    }
    return ver_ret;
}

void FilePersistLog::addSignature(version_t version,
                                  const uint8_t* signature,
                                  version_t prev_signed_ver) {
    if(signature_size == 0) {
        return;
    }
    LogEntry* ple = nullptr;

    FPL_RDLOCK;

    //binary search
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            version,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);
    ple = (l_idx == -1) ? nullptr : LOG_ENTRY_AT(l_idx);
    FPL_UNLOCK;

    if(ple != nullptr && ple->fields.ver == version) {
        memcpy(LOG_ENTRY_SIGNATURE(ple), signature, signature_size);

        ple->fields.prev_signed_ver = prev_signed_ver;
    }
}

bool FilePersistLog::getSignature(version_t version, uint8_t* signature, version_t& previous_signed_version) {
    if(signature_size == 0) {
        return false;
    }
    LogEntry* ple = nullptr;

    FPL_RDLOCK;

    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            version,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);
    ple = (l_idx == -1) ? nullptr : LOG_ENTRY_AT(l_idx);

    FPL_UNLOCK;

    if(ple != nullptr && ple->fields.ver == version) {
        memcpy(signature, LOG_ENTRY_SIGNATURE(ple), signature_size);
        previous_signed_version = ple->fields.prev_signed_ver;
        return true;
    }
    return false;
}

bool FilePersistLog::getSignatureByIndex(int64_t index, uint8_t* signature, version_t& previous_signed_version) {
    if(signature_size == 0) {
        return false;
    }

    LogEntry* entry_ptr;

    //Logic copied from getEntryByIndex. I'm not sure what ridx means.
    FPL_RDLOCK;

    int64_t ridx = (index < 0) ? (m_currMetaHeader.fields.tail + index) : index;

    if(m_currMetaHeader.fields.tail <= ridx || ridx < m_currMetaHeader.fields.head) {
        FPL_UNLOCK;
        return false;
    }
    FPL_UNLOCK;

    entry_ptr = LOG_ENTRY_AT(ridx);
    memcpy(signature, LOG_ENTRY_SIGNATURE(entry_ptr), signature_size);
    previous_signed_version = entry_ptr->fields.prev_signed_ver;
    return true;
}

int64_t FilePersistLog::getLength() {
    FPL_RDLOCK;
    int64_t len = NUM_USED_SLOTS;
    FPL_UNLOCK;

    return len;
}

int64_t FilePersistLog::getEarliestIndex() {
    FPL_RDLOCK;
    int64_t idx = (NUM_USED_SLOTS == 0) ? INVALID_INDEX : m_currMetaHeader.fields.head;
    FPL_UNLOCK;
    return idx;
}

int64_t FilePersistLog::getLatestIndex() {
    FPL_RDLOCK;
    int64_t idx = CURR_LOG_IDX;
    FPL_UNLOCK;
    return idx;
}

version_t FilePersistLog::getEarliestVersion() {
    FPL_RDLOCK;
    int64_t idx = (NUM_USED_SLOTS == 0) ? INVALID_INDEX : m_currMetaHeader.fields.head;
    version_t ver = (idx == INVALID_INDEX) ? INVALID_VERSION : (LOG_ENTRY_AT(idx)->fields.ver);
    FPL_UNLOCK;
    return ver;
}

version_t FilePersistLog::getLatestVersion() {
    FPL_RDLOCK;
    int64_t idx = CURR_LOG_IDX;
    version_t ver = (idx == INVALID_INDEX) ? INVALID_VERSION : (LOG_ENTRY_AT(idx)->fields.ver);
    FPL_UNLOCK;
    return ver;
}

version_t FilePersistLog::getLastPersistedVersion() {
    version_t last_persisted = INVALID_VERSION;
    ;
    FPL_PERS_LOCK;

    last_persisted = m_persMetaHeader.fields.ver;

    FPL_PERS_UNLOCK;
    return last_persisted;
}

int64_t FilePersistLog::getVersionIndex(version_t ver, bool exact) {
    FPL_RDLOCK;

    //binary search
    dbg_trace(m_logger, "{0} - begin binary search.", this->m_sName);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);
    dbg_trace(m_logger, "{0} - end binary search.", this->m_sName);

    FPL_UNLOCK;

    if((l_idx != INVALID_INDEX) && (LOG_ENTRY_AT(l_idx)->fields.ver != ver) && exact) {
        l_idx = INVALID_INDEX;
    }

    dbg_trace(m_logger, "{0} getVersionIndex({1}) at index {2}", this->m_sName, ver, l_idx);

    return l_idx;
}

const void* FilePersistLog::getEntryByIndex(int64_t eidx) {
    FPL_RDLOCK;
    dbg_trace(m_logger, "{0}-getEntryByIndex-head:{1},tail:{2},eidx:{3}",
              this->m_sName, m_currMetaHeader.fields.head, m_currMetaHeader.fields.tail, eidx);

    int64_t ridx = (eidx < 0) ? (m_currMetaHeader.fields.tail + eidx) : eidx;

    if(m_currMetaHeader.fields.tail <= ridx || ridx < m_currMetaHeader.fields.head) {
        FPL_UNLOCK;
        throw persistent_invalid_index(eidx);
    }
    FPL_UNLOCK;

    dbg_trace(m_logger, "{0} getEntryByIndex at idx:{1} ver:{2} time:({3},{4})",
              this->m_sName,
              ridx,
              (int64_t)(LOG_ENTRY_AT(ridx)->fields.ver),
              (LOG_ENTRY_AT(ridx))->fields.hlc_r,
              (LOG_ENTRY_AT(ridx))->fields.hlc_l);

    return LOG_ENTRY_DATA(LOG_ENTRY_AT(ridx));
}

const void* FilePersistLog::getEntry(version_t ver, bool exact) {
    LogEntry* ple = nullptr;

    FPL_RDLOCK;

    //binary search
    dbg_trace(m_logger, "{0} - begin binary search.", this->m_sName);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);
    ple = (l_idx == INVALID_INDEX) ? nullptr : LOG_ENTRY_AT(l_idx);
    dbg_trace(m_logger, "{0} - end binary search.", this->m_sName);

    FPL_UNLOCK;

    // no object exists before the requested timestamp.
    if(ple == nullptr || (exact && (ple->fields.ver != ver))) {
        return nullptr;
    }

    dbg_trace(m_logger, "{0} getEntry at ({1},{2})", this->m_sName, ple->fields.hlc_r, ple->fields.hlc_l);

    return LOG_ENTRY_DATA(ple);
}

int64_t FilePersistLog::getHLCIndex(const HLC& rhlc) {
    FPL_RDLOCK;
    dbg_trace(m_logger, "getHLCIndex for hlc({0},{1})", rhlc.m_rtc_us, rhlc.m_logic);
    struct hlc_index_entry skey(rhlc, 0);
    auto key = this->hidx.upper_bound(skey);
    FPL_UNLOCK;

    if(key != this->hidx.begin() && this->hidx.size() > 0) {
        key--;
        dbg_trace(m_logger, "getHLCIndex returns: hlc:({0},{1}),idx:{2}", key->hlc.m_rtc_us, key->hlc.m_logic, key->log_idx);
        return key->log_idx;
    }

    // no object exists before the requested timestamp.

    dbg_trace(m_logger, "{0} getHLCIndex found no entry at ({1},{2})", this->m_sName, rhlc.m_rtc_us, rhlc.m_logic);

    return INVALID_INDEX;
}

version_t FilePersistLog::getHLCVersion(const HLC& rhlc) {
    int64_t idx = getHLCIndex(rhlc);

    if (idx != INVALID_INDEX) {
        return LOG_ENTRY_AT(idx)->fields.ver;
    }

    return INVALID_VERSION;
}

version_t FilePersistLog::getPreviousVersionOf(version_t ver) {
    int64_t idx = getVersionIndex(ver,false);
    version_t prev_ver = INVALID_VERSION;
    if (idx != INVALID_INDEX) {
        if(LOG_ENTRY_AT(idx)->fields.ver < ver) {
            prev_ver = LOG_ENTRY_AT(idx)->fields.ver;
        } else {
            FPL_RDLOCK;
            if (idx > m_currMetaHeader.fields.head) {
                prev_ver = LOG_ENTRY_AT(idx - 1)->fields.ver;
            }
            FPL_UNLOCK;
        }
    }

    return prev_ver;
}

version_t FilePersistLog::getNextVersionOf(version_t ver) {
    int64_t idx = getVersionIndex(ver,false);
    version_t next_ver = INVALID_VERSION;
    if (idx != INVALID_INDEX) {
        FPL_RDLOCK;
        if (idx < (m_currMetaHeader.fields.tail - 1)) {
            next_ver = LOG_ENTRY_AT(idx+1)->fields.ver;
        }
        FPL_UNLOCK;
    } else {
        FPL_RDLOCK;
        if (NUM_USED_SLOTS > 0) {
            next_ver = LOG_ENTRY_AT(m_currMetaHeader.fields.head)->fields.ver;
        }
        FPL_UNLOCK;
    }

    return next_ver;
}

const void* FilePersistLog::getEntry(const HLC& rhlc) {
    LogEntry* ple = nullptr;

    int64_t idx = getHLCIndex(rhlc);

    if(idx != INVALID_INDEX) {
        ple = LOG_ENTRY_AT(idx);
    }

    // no object exists before the requested timestamp.
    if(ple == nullptr) {
        return nullptr;
    }

    dbg_trace(m_logger, "{0} getEntry at ({1},{2})", this->m_sName, ple->fields.hlc_r, ple->fields.hlc_l);

    return LOG_ENTRY_DATA(ple);
}

void FilePersistLog::processEntryAtVersion(version_t ver,
                                           const std::function<void(const void*, std::size_t)>& func) {
    LogEntry* ple = nullptr;
    dbg_trace(m_logger, "{} - process entry at version {}", m_sName, ver);
    FPL_RDLOCK;

    //binary search
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);
    ple = (l_idx == -1) ? nullptr : LOG_ENTRY_AT(l_idx);

    FPL_UNLOCK;

    if(ple != nullptr && ple->fields.ver == ver) {
        func(LOG_ENTRY_DATA(ple), static_cast<size_t>(ple->fields.sdlen - this->signature_size));
    }
}

// trim by index
void FilePersistLog::trimByIndex(int64_t idx) {
    dbg_trace(m_logger, "{0} trim at index: {1}", this->m_sName, idx);
    FPL_RDLOCK;
    // validate check
    if(idx < m_currMetaHeader.fields.head || idx >= m_currMetaHeader.fields.tail) {
        FPL_UNLOCK;
        return;
    }
    FPL_UNLOCK;

    FPL_PERS_LOCK;
    FPL_WRLOCK;
    //validate check again
    if(idx < m_currMetaHeader.fields.head || idx >= m_currMetaHeader.fields.tail) {
        FPL_UNLOCK;
        FPL_PERS_UNLOCK;
        return;
    }
    m_currMetaHeader.fields.head = idx + 1;
    try {
        //What version number should be supplied to persist in this case?
        // CAUTION:
        // The persist API is changed for Edward's convenience by adding a version parameter
        // This has a widespreading on the design and needs extensive test before replying on
        // it.
        persist(LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver, true);
    } catch(std::exception& e) {
        FPL_UNLOCK;
        FPL_PERS_UNLOCK;
        throw;
    }
    //TODO: remove entry from index...this is tricky because HLC
    // order does not agree with index order.
    FPL_UNLOCK;
    FPL_PERS_UNLOCK;
    dbg_trace(m_logger, "{0} trim at index: {1}...done", this->m_sName, idx);
}

void FilePersistLog::trim(version_t ver) {
    dbg_trace(m_logger, "{0} trim at version: {1}", this->m_sName, ver);
    this->trim<int64_t>(ver,
                        [&](const LogEntry* ple) { return ple->fields.ver; });
    dbg_trace(m_logger, "{0} trim at version: {1}...done", this->m_sName, ver);
}

void FilePersistLog::trim(const HLC& hlc) {
    dbg_trace(m_logger, "{0} trim at time: {1}.{2}", this->m_sName, hlc.m_rtc_us, hlc.m_logic);
    //    this->trim<unsigned __int128>(
    //      ((((const unsigned __int128)hlc.m_rtc_us)<<64) | hlc.m_logic),
    //      [&](int64_t idx) {
    //        return ((((const unsigned __int128)LOG_ENTRY_AT(idx)->fields.hlc_r)<<64) |
    //          LOG_ENTRY_AT(idx)->fields.hlc_l);
    //      });
    //TODO: This is hard because HLC order does not agree with index order.
    throw persistent_not_implemented("trim(const HLC&)");
    dbg_trace(m_logger, "{0} trim at time: {1}.{2}...done", this->m_sName, hlc.m_rtc_us, hlc.m_logic);
}

void FilePersistLog::persistMetaHeaderAtomically(MetaHeader* pShadowHeader) {
    // STEP 1: get file name
    const string swpFile = this->m_sMetaFile + "." + SWAP_FILE_SUFFIX;

    // STEP 2: write current meta header to swap file
    int fd = open(swpFile.c_str(), O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if(fd == -1) {
        throw persistent_file_error("Failed to open file.", errno);
    }
    ssize_t nWrite = write(fd, pShadowHeader, sizeof(MetaHeader));
    if(nWrite != sizeof(MetaHeader)) {
        throw persistent_file_error("Failed to write file.", errno);
    }
    close(fd);

    // STEP 3: atomically update the meta file
    if(rename(swpFile.c_str(), this->m_sMetaFile.c_str()) != 0) {
        throw persistent_file_error("Failed to rename file.", errno);
    }

    // STEP 4: update the persisted header in memory
    m_persMetaHeader = *pShadowHeader;
}

int64_t FilePersistLog::getMinimumIndexBeyondVersion(version_t ver) {
    int64_t rIndex = INVALID_INDEX;

    dbg_trace(m_logger, "{0}[{1}] - request version {2}", this->m_sName, __func__, ver);

    if(NUM_USED_SLOTS == 0) {
        dbg_trace(m_logger, "{0}[{1}] - request on an empty log, return INVALID_INDEX.", this->m_sName, __func__);
        return rIndex;
    }

    if(ver == INVALID_VERSION) {
        dbg_trace(m_logger, "{0}[{1}] - request all logs", this->m_sName, __func__);
        // return the earliest log we have.
        return m_currMetaHeader.fields.head;
    }

    // binary search
    dbg_trace(m_logger, "{0}[{1}] - begin binary search.", this->m_sName, __func__);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            m_currMetaHeader.fields.head,
            m_currMetaHeader.fields.tail);

    if(l_idx == INVALID_INDEX) {
        // if binary search failed, it means the requested version is earlier
        // than the earliest available log so we return the earliest log entry
        // we have.
        rIndex = m_currMetaHeader.fields.head;
        dbg_trace(m_logger, "{0}[{1}] - binary search failed, return the earliest version {2}", this->m_sName, __func__, ver);
    } else if((l_idx + 1) == m_currMetaHeader.fields.tail) {
        // if binary search found the last one, it means ver is in the future return INVALID_INDEX.
        // use the default rIndex value (INVALID_INDEX)
        dbg_trace(m_logger, "{0}[{1}] - binary search returns the last entry in the log. return INVALID_INDEX.", this->m_sName, __func__);
    } else {
        // binary search found some entry earlier than the last one. return l_idx+1:
        dbg_trace(m_logger, "{0}[{1}] - binary search returns an entry earlier than the last one, return ldx+1:{2}", this->m_sName, __func__, l_idx + 1);
        rIndex = l_idx + 1;
    }

    return rIndex;
}

// format for the logs:
// [latest_version(int64_t)][nr_log_entry(int64_t)][log_enty1][log_entry2]...
// the log entry is from the earliest to the latest.
// two functions for serialization/deserialization for log entries:
// 1) size_t byteSizeOfLogEntry(const LogEntry * ple);
// 2) size_t writeLogEntryToByteArray(const LogEntry * ple, uint8_t * ba);
// 3) size_t postLogEntry(const std::function<void (uint8_t const *const, std::size_t)> f, const LogEntry *ple);
// 4) size_t mergeLogEntryFromByteArray(const uint8_t * ba);
size_t FilePersistLog::bytes_size(version_t ver) {
    size_t bsize = (sizeof(int64_t) + sizeof(int64_t));
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    if(idx != INVALID_INDEX) {
        while(idx < m_currMetaHeader.fields.tail) {
            bsize += byteSizeOfLogEntry(LOG_ENTRY_AT(idx));
            idx++;
        }
    }
    return bsize;
}

size_t FilePersistLog::to_bytes(uint8_t* buf, version_t ver) {
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    size_t ofst = 0;
    // latest_version
    int64_t latest_version = this->getLatestVersion();
    *(int64_t*)(buf + ofst) = latest_version;
    ofst += sizeof(int64_t);
    // nr_log_entry
    *(int64_t*)(buf + ofst) = (idx == INVALID_INDEX) ? 0 : (m_currMetaHeader.fields.tail - idx);
    ofst += sizeof(int64_t);
    // log_entries
    if(idx != INVALID_INDEX) {
        while(idx < m_currMetaHeader.fields.tail) {
            ofst += writeLogEntryToByteArray(LOG_ENTRY_AT(idx), buf + ofst);
            idx++;
        }
    }
    return ofst;
}

void FilePersistLog::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f,
                                 version_t ver) {
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    // latest_version
    int64_t latest_version = this->getLatestVersion();
    f((uint8_t*)&latest_version, sizeof(int64_t));
    // nr_log_entry
    int64_t nr_log_entry = (idx == INVALID_INDEX) ? 0 : (m_currMetaHeader.fields.tail - idx);
    f((uint8_t*)&nr_log_entry, sizeof(int64_t));
    // log_entries
    if(idx != INVALID_INDEX) {
        while(idx < m_currMetaHeader.fields.tail) {
            postLogEntry(f, LOG_ENTRY_AT(idx));
            idx++;
        }
    }
}

void FilePersistLog::applyLogTail(uint8_t const* v) {
    size_t ofst = 0;
    // latest_version
    int64_t latest_version = *(const int64_t*)(v + ofst);
    ofst += sizeof(int64_t);
    // nr_log_entry
    int64_t nr_log_entry = *(const int64_t*)(v + ofst);
    ofst += sizeof(int64_t);
    // log_entries
    while(nr_log_entry--) {
        ofst += mergeLogEntryFromByteArray(v + ofst);
    }
    // update the latest version.
    m_currMetaHeader.fields.ver = latest_version;
}

size_t FilePersistLog::byteSizeOfLogEntry(const LogEntry* ple) {
    return sizeof(LogEntry) + ple->fields.sdlen;
}

size_t FilePersistLog::writeLogEntryToByteArray(const LogEntry* ple, uint8_t* ba) {
    size_t nr_written = 0;
    memcpy(ba, ple, sizeof(LogEntry));
    nr_written += sizeof(LogEntry);
    if(ple->fields.sdlen > 0) {
        memcpy((void*)(ba + nr_written), (void*)LOG_ENTRY_SIGNATURE(ple), ple->fields.sdlen);
        nr_written += ple->fields.sdlen;
    }
    return nr_written;
}

size_t FilePersistLog::postLogEntry(const std::function<void(uint8_t const* const, std::size_t)>& f, const LogEntry* ple) {
    size_t nr_written = 0;
    f((const uint8_t*)ple, sizeof(LogEntry));
    nr_written += sizeof(LogEntry);
    if(ple->fields.sdlen > 0) {
        f((const uint8_t*)LOG_ENTRY_SIGNATURE(ple), ple->fields.sdlen);
        nr_written += ple->fields.sdlen;
    }
    return nr_written;
}

size_t FilePersistLog::mergeLogEntryFromByteArray(const uint8_t* ba) {
    const LogEntry* cple = (const LogEntry*)ba;
    // valid check
    // 0) version grows monotonically.
    if(cple->fields.ver <= m_currMetaHeader.fields.ver) {
        dbg_trace(m_logger, "{0} skip log entry version {1}, we are at {2}.", __func__, cple->fields.ver, m_currMetaHeader.fields.ver);
        return cple->fields.sdlen + sizeof(LogEntry);
    }
    // 1) do we have space to merge it?
    if(NUM_FREE_SLOTS == 0) {
        dbg_trace(m_logger, "{0} failed to merge log entry, we don't empty log entry.", __func__);
        throw persistent_log_full("No free log entries");
    }
    if(NUM_FREE_BYTES < cple->fields.sdlen) {
        dbg_trace(m_logger, "{0} failed to merge log entry, we need {1} bytes data space, but we have only {2} bytes.", __func__, cple->fields.sdlen, NUM_FREE_BYTES);
        throw persistent_log_full("Insufficient space for log data");
    }
    // 2) merge it!
    memcpy(NEXT_DATA, (const void*)(ba + sizeof(LogEntry)), cple->fields.sdlen);
    memcpy(NEXT_LOG_ENTRY, cple, sizeof(LogEntry));
    NEXT_LOG_ENTRY->fields.ofst = NEXT_DATA_OFST;
    this->hidx.insert(hlc_index_entry{HLC{cple->fields.hlc_r, cple->fields.hlc_l}, m_currMetaHeader.fields.tail});
    m_currMetaHeader.fields.tail++;
    m_currMetaHeader.fields.ver = cple->fields.ver;
    dbg_trace(m_logger, "{0} merge log:log entry and meta data are updated.", __func__);
    return cple->fields.sdlen + sizeof(LogEntry);
}
//////////////////////////
// invisible to outside //
//////////////////////////

bool FilePersistLog::checkOrCreateMetaFile() {
    return checkOrCreateFileWithSize(this->m_sMetaFile, META_SIZE);
}

bool FilePersistLog::checkOrCreateLogFile() {
    return checkOrCreateFileWithSize(this->m_sLogFile, MAX_LOG_SIZE);
}

bool FilePersistLog::checkOrCreateDataFile() {
    return checkOrCreateFileWithSize(this->m_sDataFile, MAX_DATA_SIZE);
}

void FilePersistLog::truncate(version_t ver) {
    dbg_trace(m_logger, "{0} truncate at version: {1}.", this->m_sName, ver);
    FPL_WRLOCK;
    // STEP 1: search for the log entry
    // TODO
    //binary search
    int64_t head = m_currMetaHeader.fields.head % MAX_LOG_ENTRY;
    int64_t tail = m_currMetaHeader.fields.tail % MAX_LOG_ENTRY;
    if(tail < head) tail += MAX_LOG_ENTRY;
    dbg_trace(m_logger, "{0} - begin binary search.", this->m_sName);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver, head, tail);
    dbg_trace(m_logger, "{0} - end binary search.", this->m_sName);
    // STEP 2: update META_HEADER
    if(l_idx == INVALID_INDEX) {  // not adequate log found. We need to remove all logs.
        // TODO: this may not be safe in case the log has been trimmed beyond 'ver' !!!
        m_currMetaHeader.fields.tail = m_currMetaHeader.fields.head;
    } else {
        int64_t _idx = (m_currMetaHeader.fields.head + l_idx - head) + ((head > l_idx) ? MAX_LOG_ENTRY : 0);
        m_currMetaHeader.fields.tail = _idx + 1;
    }
    if(m_currMetaHeader.fields.ver > ver)
        m_currMetaHeader.fields.ver = ver;
    // STEP 3: update PERSISTENT STATE
    FPL_PERS_LOCK;
    try {
        persistMetaHeaderAtomically(&m_currMetaHeader);
    } catch(std::exception& e) {
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        throw;
    }
    FPL_PERS_UNLOCK;
    FPL_UNLOCK;
    dbg_trace(m_logger, "{0} truncate at version: {1}....done", this->m_sName, ver);
}

const uint64_t FilePersistLog::getMinimumLatestPersistedVersion(const std::string& prefix) {
    // STEP 1: list all meta files in the path
    DIR* dir = opendir(getPersFilePath().c_str());
    if(dir == NULL) {
        // We cannot open the persistent directory, so just return error.
        dbg_error(PersistLogger::get(), "{}:{} failed to open the directory. errno={}, err={}.",
                  __FILE__, __func__, errno, strerror(errno));
        return INVALID_VERSION;
    }
    // STEP 2: get through the meta header for the minimum
    struct dirent* dent;
    bool found = false;
    int64_t ver = INVALID_VERSION;
    while((dent = readdir(dir)) != NULL) {
        uint32_t name_len = strlen(dent->d_name);
        if(name_len > prefix.length() && strncmp(prefix.c_str(), dent->d_name, prefix.length()) == 0 && strncmp("." META_FILE_SUFFIX, dent->d_name + name_len - strlen(META_FILE_SUFFIX) - 1, strlen(META_FILE_SUFFIX) + 1) == 0) {
            MetaHeader mh;
            char fn[1024];
            sprintf(fn, "%s/%s", getPersFilePath().c_str(), dent->d_name);
            int fd = open(fn, O_RDONLY);
            if(fd < 0) {
                dbg_warn(PersistLogger::get(), "{}:{} cannot read file:{}, errno={}, err={}.",
                         __FILE__, __func__, errno, strerror(errno));
                continue;
            }
            int nRead = read(fd, (void*)&mh, sizeof(mh));
            if(nRead != sizeof(mh)) {
                dbg_warn(PersistLogger::get(), "{}:{} cannot load meta header from file:{}, errno={}, err={}",
                         __FILE__, __func__, errno, strerror(errno));
                close(fd);
                continue;
            }
            close(fd);
            if(!found || ver > mh.fields.ver)
                ver = mh.fields.ver;
        }
    }
    return ver;
}
}  // namespace persistent
