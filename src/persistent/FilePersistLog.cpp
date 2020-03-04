#include <derecho/persistent/detail/FilePersistLog.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <derecho/conf/conf.hpp>
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

// MACROS
#define META_FILE_SUFFIX "meta"
#define LOG_FILE_SUFFIX "log"
#define DATA_FILE_SUFFIX "data"
#define SWAP_FILE_SUFFIX "swp"


using namespace std;

namespace persistent {
namespace file {

/////////////////////////
// internal structures //
/////////////////////////

////////////////////////
// visible to outside //
////////////////////////

FilePersistLog::FilePersistLog(const string& name, const string& dataPath) noexcept(false) : PersistLog(name),
                                                                                             m_sDataPath(dataPath),
                                                                                             m_sMetaFile(dataPath + "/" + name + "." + META_FILE_SUFFIX),
                                                                                             m_sLogFile(dataPath + "/" + name + "." + LOG_FILE_SUFFIX),
                                                                                             m_sDataFile(dataPath + "/" + name + "." + DATA_FILE_SUFFIX),
                                                                                             m_iMaxLogEntry(derecho::getConfUInt64(CONF_PERS_FS_MAX_LOG_ENTRY)),
                                                                                             m_iMaxDataSize(derecho::getConfUInt64(CONF_PERS_FS_MAX_DATA_SIZE)),
                                                                                             m_iLogFileDesc(-1),
                                                                                             m_iDataFileDesc(-1),
                                                                                             m_pLog(MAP_FAILED),
                                                                                             m_pData(MAP_FAILED) {
    if(pthread_rwlock_init(&this->m_rwlock, NULL) != 0) {
        throw PERSIST_EXP_RWLOCK_INIT(errno);
    }
    if(pthread_mutex_init(&this->m_perslock, NULL) != 0) {
        throw PERSIST_EXP_MUTEX_INIT(errno);
    }
    dbg_default_trace("{0} constructor: before load()", name);
    if(derecho::getConfBoolean(CONF_PERS_RESET)) {
        reset();
    }
    load();
    dbg_default_trace("{0} constructor: after load()", name);
}

void FilePersistLog::reset() noexcept(false) {
    dbg_default_trace("{0} reset state...begin", this->m_sName);
    if(fs::exists(this->m_sMetaFile)) {
        if(!fs::remove(this->m_sMetaFile)) {
            dbg_default_error("{0} reset failed to remove the file:{1}", this->m_sName, this->m_sMetaFile);
            throw PERSIST_EXP_REMOVE_FILE(errno);
        }
        if(!fs::remove(this->m_sLogFile)) {
            dbg_default_error("{0} reset failed to remove the file:{1}", this->m_sName, this->m_sLogFile);
            throw PERSIST_EXP_REMOVE_FILE(errno);
        }
        if(!fs::remove(this->m_sDataFile)) {
            dbg_default_error("{0} reset failed to remove the file:{1}", this->m_sName, this->m_sDataFile);
            throw PERSIST_EXP_REMOVE_FILE(errno);
        }
    }
    dbg_default_trace("{0} reset state...done", this->m_sName);
}

void FilePersistLog::load() noexcept(false) {
    dbg_default_trace("{0}:load state...begin", this->m_sName);
    // STEP 0: check if data path exists
    checkOrCreateDir(this->m_sDataPath);
    dbg_default_trace("{0}:checkOrCreateDir passed.", this->m_sName);
    // STEP 1: check and create files.
    bool bCreate = checkOrCreateMetaFile();
    checkOrCreateLogFile();
    checkOrCreateDataFile();
    dbg_default_trace("{0}:checkOrCreateDataFile passed.", this->m_sName);
    // STEP 2: open files
    this->m_iLogFileDesc = open(this->m_sLogFile.c_str(), O_RDWR);
    if(this->m_iLogFileDesc == -1) {
        throw PERSIST_EXP_OPEN_FILE(errno);
    }
    this->m_iDataFileDesc = open(this->m_sDataFile.c_str(), O_RDWR);
    if(this->m_iDataFileDesc == -1) {
        throw PERSIST_EXP_OPEN_FILE(errno);
    }
    // STEP 3: mmap to memory
    //// we map the log entry and data twice to faciliate the search and data
    //// retrieving then the data is rewinding across the buffer end as follow:
    //// [1][2][3][4][5][6][1][2][3][4][5][6]
    this->m_pLog = mmap(NULL, FS_MAX_LOG_SIZE << 1, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(this->m_pLog == MAP_FAILED) {
        dbg_default_error("{0}:reserve map space for log failed.", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap(this->m_pLog, FS_MAX_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iLogFileDesc, 0) == MAP_FAILED) {
        dbg_default_error("{0}:map ringbuffer space for the first half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap((void*)((uint64_t)this->m_pLog + FS_MAX_LOG_SIZE), FS_MAX_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iLogFileDesc, 0) == MAP_FAILED) {
        dbg_default_error("{0}:map ringbuffer space for the second half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    //// data ringbuffer
    this->m_pData = mmap(NULL, (size_t)(FS_MAX_DATA_SIZE << 1), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(this->m_pData == MAP_FAILED) {
        dbg_default_error("{0}:reserve map space for data failed.", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap(this->m_pData, (size_t)(FS_MAX_DATA_SIZE), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iDataFileDesc, 0) == MAP_FAILED) {
        dbg_default_error("{0}:map ringbuffer space for the first half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap((void*)((uint64_t)this->m_pData + FS_MAX_DATA_SIZE), (size_t)FS_MAX_DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, this->m_iDataFileDesc, 0) == MAP_FAILED) {
        dbg_default_error("{0}:map ringbuffer space for the second half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
        throw PERSIST_EXP_MMAP_FILE(errno);
    }
    dbg_default_trace("{0}:data/meta file mapped to memory", this->m_sName);
    // STEP 4: initialize the header for new created Metafile
    if(bCreate) {
        FS_META_HEADER->fields.head = 0ll;
        FS_META_HEADER->fields.tail = 0ll;
        FS_META_HEADER->fields.ver = INVALID_VERSION;
        FS_META_HEADER_PERS->fields.head = -1ll;  // -1 means uninitialized
        FS_META_HEADER_PERS->fields.tail = -1ll;  // -1 means uninitialized
        FS_META_HEADER_PERS->fields.ver = INVALID_VERSION;
        // persist the header
        FPL_RDLOCK;
        FPL_PERS_LOCK;

        try {
            persistMetaHeaderAtomically(FS_META_HEADER);
        } catch(uint64_t e) {
            FPL_PERS_UNLOCK;
            FPL_UNLOCK;
            throw e;
        }
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        dbg_default_info("{0}:new header initialized.", this->m_sName);
    } else {  // load FS_META_HEADER from disk
        FPL_WRLOCK;
        FPL_PERS_LOCK;
        try {
            int fd = open(this->m_sMetaFile.c_str(), O_RDONLY);
            if(fd == -1) {
                throw PERSIST_EXP_OPEN_FILE(errno);
            }
            ssize_t nRead = read(fd, (void*)FS_META_HEADER_PERS, sizeof(MetaHeader));
            if(nRead != sizeof(MetaHeader)) {
                close(fd);
                throw PERSIST_EXP_READ_FILE(errno);
            }
            close(fd);
            *FS_META_HEADER = *FS_META_HEADER_PERS;
            // update mhlc index
            for(int64_t idx = FS_META_HEADER->fields.head; idx < FS_META_HEADER->fields.tail; idx++) {
                struct hlc_index_entry _ent;
                _ent.hlc.m_rtc_us = FS_LOG_ENTRY_AT(idx)->fields.hlc_r;
                _ent.hlc.m_logic = FS_LOG_ENTRY_AT(idx)->fields.hlc_l;
                _ent.log_idx = idx;
                this->hidx.insert(_ent);
            }
        } catch(uint64_t e) {
            FPL_PERS_UNLOCK;
            FPL_UNLOCK;
            throw e;
        }

        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
    }
    // STEP 5: update m_hlcLE with the latest event: we don't need this anymore
    //if (FS_META_HEADER->fields.eno >0) {
    //  if (this->m_hlcLE.m_rtc_us < CURR_LOG_ENTRY->fields.hlc_r &&
    //    this->m_hlcLE.m_logic < CURR_LOG_ENTRY->fields.hlc_l){
    //    this->m_hlcLE.m_rtc_us = CURR_LOG_ENTRY->fields.hlc_r;
    //    this->m_hlcLE.m_logic = CURR_LOG_ENTRY->fields.hlc_l;
    //  }
    //}
    dbg_default_trace("{0}:load state...done", this->m_sName);
}

FilePersistLog::~FilePersistLog() noexcept(true) {
    pthread_rwlock_destroy(&this->m_rwlock);
    pthread_mutex_destroy(&this->m_perslock);
    if(this->m_pData != MAP_FAILED) {
        munmap(m_pData, (size_t)(FS_MAX_DATA_SIZE << 1));
    }
    this->m_pData = nullptr;  // prevent ~MemLog() destructor to release it again.
    if(this->m_pLog != MAP_FAILED) {
        munmap(m_pLog, FS_MAX_LOG_SIZE);
    }
    this->m_pLog = nullptr;  // prevent ~MemLog() destructor to release it again.
    if(this->m_iLogFileDesc != -1) {
        close(this->m_iLogFileDesc);
    }
    if(this->m_iDataFileDesc != -1) {
        close(this->m_iDataFileDesc);
    }
}

void FilePersistLog::append(const void* pdat, const uint64_t& size, const int64_t& ver, const HLC& mhlc) noexcept(false) {
    dbg_default_trace("{0} append event ({1},{2})", this->m_sName, mhlc.m_rtc_us, mhlc.m_logic);
    FPL_RDLOCK;

#define __DO_VALIDATION                                                                                    \
    do {                                                                                                   \
        if(FS_NUM_FREE_SLOTS < 1) {                                                                           \
            dbg_default_error("{0}-append exception no free slots in log! FS_NUM_FREE_SLOTS={1}",             \
                              this->m_sName, FS_NUM_FREE_SLOTS);                                              \
            dbg_default_flush();                                                                           \
            FPL_UNLOCK;                                                                                    \
            std::cerr << "PERSIST_EXP_NOSPACE_LOG: FREESLOT=" << FS_NUM_FREE_SLOTS << ",version=" << ver << std::endl;              \
            throw PERSIST_EXP_NOSPACE_LOG;                                                                 \
        }                                                                                                  \
        if(FS_NUM_FREE_BYTES < size) {                                                                        \
            dbg_default_error("{0}-append exception no space for data: FS_NUM_FREE_BYTES={1}, size={2}",      \
                              this->m_sName, FS_NUM_FREE_BYTES, size);                                        \
            dbg_default_flush();                                                                           \
            FPL_UNLOCK;                                                                                    \
            std::cerr << "PERSIST_EXP_NOSPACE_DATA: FREE:" << FS_NUM_FREE_BYTES << ",size=" << size << std::endl; \
            throw PERSIST_EXP_NOSPACE_DATA;                                                                \
        }                                                                                                  \
        if((FS_CURR_LOG_IDX != -1) && (FS_META_HEADER->fields.ver >= ver)) {                                     \
            int64_t cver = FS_META_HEADER->fields.ver;                                                        \
            dbg_default_error("{0}-append version already exists! cur_ver:{1} new_ver:{2}", this->m_sName, \
                              (int64_t)cver, (int64_t)ver);                                                \
            dbg_default_flush();                                                                           \
            FPL_UNLOCK;                                                                                    \
            std::cerr << "PERSIST_EXP_INV_VERSION:cver=" << cver << ",ver=" << ver << std::endl;           \
            throw PERSIST_EXP_INV_VERSION;                                                                 \
        }                                                                                                  \
    } while(0)

#pragma GCC diagnostic ignored "-Wunused-variable"
    __DO_VALIDATION;
#pragma GCC diagnostic pop
    FPL_UNLOCK;
    dbg_default_trace("{0} append:validate check1 Finished.", this->m_sName);

    FPL_WRLOCK;
//check
#pragma GCC diagnostic ignored "-Wunused-variable"
    __DO_VALIDATION;
#pragma GCC diagnostic pop
    dbg_default_trace("{0} append:validate check2 Finished.", this->m_sName);

    // copy data
    memcpy(FS_NEXT_DATA, pdat, size);
    dbg_default_trace("{0} append:data is copied to log.", this->m_sName);

    // fill the log entry
    FS_NEXT_LOG_ENTRY->fields.ver = ver;
    FS_NEXT_LOG_ENTRY->fields.dlen = size;
    FS_NEXT_LOG_ENTRY->fields.ofst = FS_NEXT_DATA_OFST;
    FS_NEXT_LOG_ENTRY->fields.hlc_r = mhlc.m_rtc_us;
    FS_NEXT_LOG_ENTRY->fields.hlc_l = mhlc.m_logic;
    /* No Sync required here.
    if (msync(ALIGN_TO_PAGE(FS_NEXT_LOG_ENTRY), 
        sizeof(LogEntry) + (((uint64_t)FS_NEXT_LOG_ENTRY) % PAGE_SIZE),MS_SYNC) != 0) {
      FPL_UNLOCK;
      throw PERSIST_EXP_MSYNC(errno);
    }
*/

    // update meta header
    this->hidx.insert(hlc_index_entry{mhlc, FS_META_HEADER->fields.tail});
    FS_META_HEADER->fields.tail++;
    FS_META_HEADER->fields.ver = ver;
    dbg_default_trace("{0} append:log entry and meta data are updated.", this->m_sName);
    /* No sync
    if (msync(this->m_pMeta,sizeof(MetaHeader),MS_SYNC) != 0) {
      FPL_UNLOCK;
      throw PERSIST_EXP_MSYNC(errno);
    }
*/
    dbg_default_debug("{0} append a log ver:{1} hlc:({2},{3})", this->m_sName,
                      ver, mhlc.m_rtc_us, mhlc.m_logic);
    FPL_UNLOCK;
}

void FilePersistLog::advanceVersion(const int64_t& ver) noexcept(false) {
    FPL_WRLOCK;
    if(FS_META_HEADER->fields.ver < ver) {
        FS_META_HEADER->fields.ver = ver;
    } else {
        FPL_UNLOCK;
        throw PERSIST_EXP_INV_VERSION;
    }
    FPL_UNLOCK;
}

const int64_t FilePersistLog::persist(const bool preLocked) noexcept(false) {
    int64_t ver_ret = INVALID_VERSION;
    if(!preLocked) {
        FPL_PERS_LOCK;
        FPL_RDLOCK;
    }

    if(*FS_META_HEADER == *FS_META_HEADER_PERS) {
        if(FS_CURR_LOG_IDX != -1) {
            //ver_ret = FS_LOG_ENTRY_AT(FS_CURR_LOG_IDX)->fields.ver;
            ver_ret = FS_META_HEADER->fields.ver;
        }
        if(!preLocked) {
            FPL_UNLOCK;
            FPL_PERS_UNLOCK;
        }
        return ver_ret;
    }

    //flush data
    dbg_default_trace("{0} flush data,log,and meta.", this->m_sName);
    try {
        // shadow the current state
        void *flush_dstart = nullptr, *flush_lstart = nullptr;
        size_t flush_dlen = 0, flush_llen = 0;
        MetaHeader shadow_header = *FS_META_HEADER;
        if((FS_NUM_USED_SLOTS > 0) && (FS_NEXT_LOG_ENTRY > FS_NEXT_LOG_ENTRY_PERS)) {
            flush_dlen = (FS_LOG_ENTRY_AT(FS_CURR_LOG_IDX)->fields.ofst + FS_LOG_ENTRY_AT(FS_CURR_LOG_IDX)->fields.dlen - FS_NEXT_LOG_ENTRY_PERS->fields.ofst);
            // flush data
            flush_dstart = ALIGN_TO_PAGE(FS_NEXT_DATA_PERS);
            flush_dlen += ((int64_t)FS_NEXT_DATA_PERS) % PAGE_SIZE;
            // flush log
            flush_lstart = ALIGN_TO_PAGE(FS_NEXT_LOG_ENTRY_PERS);
            flush_llen = ((size_t)FS_NEXT_LOG_ENTRY - (size_t)FS_NEXT_LOG_ENTRY_PERS) + ((int64_t)FS_NEXT_LOG_ENTRY_PERS) % PAGE_SIZE;
        }
        if(FS_NUM_USED_SLOTS > 0) {
            //get the latest flushed version
            //ver_ret = FS_LOG_ENTRY_AT(FS_CURR_LOG_IDX)->fields.ver;
            ver_ret = FS_META_HEADER->fields.ver;
        }
        if(!preLocked) {
            FPL_UNLOCK;
        }
        if(flush_dlen > 0) {
            if(msync(flush_dstart, flush_dlen, MS_SYNC) != 0) {
                throw PERSIST_EXP_MSYNC(errno);
            }
        }
        if(flush_llen > 0) {
            if(msync(flush_lstart, flush_llen, MS_SYNC) != 0) {
                throw PERSIST_EXP_MSYNC(errno);
            }
        }
        // flush meta data
        this->persistMetaHeaderAtomically(&shadow_header);
    } catch(uint64_t e) {
        if(!preLocked) {
            FPL_PERS_UNLOCK;
        }
        throw e;
    }
    dbg_default_trace("{0} flush data,log,and meta...done.", this->m_sName);

    if(!preLocked) {
        FPL_PERS_UNLOCK;
    }
    return ver_ret;
}

int64_t FilePersistLog::getLength() noexcept(false) {
    FPL_RDLOCK;
    int64_t len = FS_NUM_USED_SLOTS;
    FPL_UNLOCK;

    return len;
}

int64_t FilePersistLog::getEarliestIndex() noexcept(false) {
    FPL_RDLOCK;
    int64_t idx = (FS_NUM_USED_SLOTS == 0) ? INVALID_INDEX : FS_META_HEADER->fields.head;
    FPL_UNLOCK;
    return idx;
}

int64_t FilePersistLog::getLatestIndex() noexcept(false) {
    FPL_RDLOCK;
    int64_t idx = FS_CURR_LOG_IDX;
    FPL_UNLOCK;
    return idx;
}

version_t FilePersistLog::getEarliestVersion() noexcept(false) {
    FPL_RDLOCK;
    int64_t idx = (FS_NUM_USED_SLOTS == 0) ? INVALID_INDEX : FS_META_HEADER->fields.head;
    version_t ver = (idx == INVALID_INDEX) ? INVALID_VERSION : (FS_LOG_ENTRY_AT(idx)->fields.ver);
    FPL_UNLOCK;
    return ver;
}

version_t FilePersistLog::getLatestVersion() noexcept(false) {
    FPL_RDLOCK;
    int64_t idx = FS_CURR_LOG_IDX;
    version_t ver = (idx == INVALID_INDEX) ? INVALID_VERSION : (FS_LOG_ENTRY_AT(idx)->fields.ver);
    FPL_UNLOCK;
    return ver;
}

const version_t FilePersistLog::getLastPersisted() noexcept(false) {
    version_t last_persisted = INVALID_VERSION;
    ;
    FPL_PERS_LOCK;

    last_persisted = FS_META_HEADER_PERS->fields.ver;

    FPL_PERS_UNLOCK;
    return last_persisted;
}

int64_t FilePersistLog::getVersionIndex(const version_t& ver) {
    FPL_RDLOCK;

    //binary search
    dbg_default_trace("{0} - begin binary search.", this->m_sName);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            FS_META_HEADER->fields.head,
            FS_META_HEADER->fields.tail);
    dbg_default_trace("{0} - end binary search.", this->m_sName);

    FPL_UNLOCK;

    dbg_default_trace("{0} getVersionIndex({1}) at index {2}", this->m_sName, ver, l_idx);

    return l_idx;
}

int64_t FilePersistLog::getHLCIndex(const HLC& rhlc) noexcept(false) {
    FPL_RDLOCK;
    dbg_default_trace("getHLCIndex for hlc({0},{1})", rhlc.m_rtc_us, rhlc.m_logic);
    struct hlc_index_entry skey(rhlc, 0);
    auto key = this->hidx.upper_bound(skey);
    FPL_UNLOCK;

    if(key != this->hidx.begin() && this->hidx.size() > 0) {
        dbg_default_trace("getHLCIndex returns: hlc:({0},{1}),idx:{2}", key->hlc.m_rtc_us, key->hlc.m_logic, key->log_idx);
        return key->log_idx;
    }

    // no object exists before the requested timestamp.

    dbg_default_trace("{0} getHLCIndex found no entry at ({1},{2})", this->m_sName, rhlc.m_rtc_us, rhlc.m_logic);

    return INVALID_INDEX;
}

// trim by index
void FilePersistLog::trimByIndex(const int64_t& idx) noexcept(false) {
    dbg_default_trace("{0} trim at index: {1}", this->m_sName, idx);
    FPL_RDLOCK;
    // validate check
    if(idx < FS_META_HEADER->fields.head || idx >= FS_META_HEADER->fields.tail) {
        FPL_UNLOCK;
        return;
    }
    FPL_UNLOCK;

    FPL_PERS_LOCK;
    FPL_WRLOCK;
    //validate check again
    if(idx < FS_META_HEADER->fields.head || idx >= FS_META_HEADER->fields.tail) {
        FPL_UNLOCK;
        FPL_PERS_UNLOCK;
        return;
    }
    FS_META_HEADER->fields.head = idx + 1;
    try {
        persist(true);
    } catch(uint64_t e) {
        FPL_UNLOCK;
        FPL_PERS_UNLOCK;
        throw e;
    }
    //TODO: remove entry from index...this is tricky because HLC
    // order does not agree with index order.
    FPL_UNLOCK;
    FPL_PERS_UNLOCK;
    // throw PERSIST_EXP_UNIMPLEMENTED;
    dbg_default_trace("{0} trim at index: {1}...done", this->m_sName, idx);
}

void FilePersistLog::trim(const int64_t& ver) noexcept(false) {
    dbg_default_trace("{0} trim at version: {1}", this->m_sName, ver);
    this->trim<int64_t>(ver,
                        [&](const LogEntry* ple) { return ple->fields.ver; });
    dbg_default_trace("{0} trim at version: {1}...done", this->m_sName, ver);
}

void FilePersistLog::trim(const HLC& hlc) noexcept(false) {
    dbg_default_trace("{0} trim at time: {1}.{2}", this->m_sName, hlc.m_rtc_us, hlc.m_logic);
    //    this->trim<unsigned __int128>(
    //      ((((const unsigned __int128)hlc.m_rtc_us)<<64) | hlc.m_logic),
    //      [&](int64_t idx) {
    //        return ((((const unsigned __int128)FS_LOG_ENTRY_AT(idx)->fields.hlc_r)<<64) |
    //          FS_LOG_ENTRY_AT(idx)->fields.hlc_l);
    //      });
    //TODO: This is hard because HLC order does not agree with index order.
    throw PERSIST_EXP_UNIMPLEMENTED;
    dbg_default_trace("{0} trim at time: {1}.{2}...done", this->m_sName, hlc.m_rtc_us, hlc.m_logic);
}

void FilePersistLog::persistMetaHeaderAtomically(MetaHeader* pShadowHeader) noexcept(false) {
    // STEP 1: get file name
    const string swpFile = this->m_sMetaFile + "." + SWAP_FILE_SUFFIX;

    // STEP 2: write current meta header to swap file
    int fd = open(swpFile.c_str(), O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if(fd == -1) {
        throw PERSIST_EXP_OPEN_FILE(errno);
    }
    ssize_t nWrite = write(fd, pShadowHeader, sizeof(MetaHeader));
    if(nWrite != sizeof(MetaHeader)) {
        throw PERSIST_EXP_WRITE_FILE(errno);
    }
    close(fd);

    // STEP 3: atomically update the meta file
    if(rename(swpFile.c_str(), this->m_sMetaFile.c_str()) != 0) {
        throw PERSIST_EXP_RENAME_FILE(errno);
    }

    // STEP 4: update the persisted header in memory
    *FS_META_HEADER_PERS = *pShadowHeader;
}

int64_t FilePersistLog::getMinimumIndexBeyondVersion(const int64_t& ver) noexcept(false) {
    int64_t rIndex = INVALID_INDEX;

    dbg_default_trace("{0}[{1}] - request version {2}", this->m_sName, __func__, ver);

    if(FS_NUM_USED_SLOTS == 0) {
        dbg_default_trace("{0}[{1}] - request on an empty log, return INVALID_INDEX.", this->m_sName, __func__);
        return rIndex;
    }

    if(ver == INVALID_VERSION) {
        dbg_default_trace("{0}[{1}] - request all logs", this->m_sName, __func__);
        // return the earliest log we have.
        return FS_META_HEADER->fields.head;
    }

    // binary search
    dbg_default_trace("{0}[{1}] - begin binary search.", this->m_sName, __func__);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver,
            FS_META_HEADER->fields.head,
            FS_META_HEADER->fields.tail);

    if(l_idx == -1) {
        // if binary search failed, it means the requested version is earlier
        // than the earliest available log so we return the earliest log entry
        // we have.
        rIndex = FS_META_HEADER->fields.head;
        dbg_default_trace("{0}[{1}] - binary search failed, return the earliest version {2}", this->m_sName, __func__, ver);
    } else if((l_idx + 1) == FS_META_HEADER->fields.tail) {
        // if binary search found the last one, it means ver is in the future return INVALID_INDEX.
        // use the default rIndex value (INVALID_INDEX)
        dbg_default_trace("{0}[{1}] - binary search returns the last entry in the log. return INVALID_INDEX.", this->m_sName, __func__);
    } else {
        // binary search found some entry earlier than the last one. return l_idx+1:
        dbg_default_trace("{0}[{1}] - binary search returns an entry earlier than the last one, return ldx+1:{2}", this->m_sName, __func__, l_idx + 1);
        rIndex = l_idx + 1;
    }

    return rIndex;
}

// format for the logs:
// [latest_version(int64_t)][nr_log_entry(int64_t)][log_enty1][log_entry2]...
// the log entry is from the earliest to the latest.
// two functions for serialization/deserialization for log entries:
// 1) size_t byteSizeOfLogEntry(const LogEntry * ple);
// 2) size_t writeLogEntryToByteArray(const LogEntry * ple, char * ba);
// 3) size_t postLogEntry(const std::function<void (char const *const, std::size_t)> f, const LogEntry *ple);
// 4) size_t mergeLogEntryFromByteArray(const char * ba);
size_t FilePersistLog::bytes_size(const int64_t& ver) noexcept(false) {
    size_t bsize = (sizeof(int64_t) + sizeof(int64_t));
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    if(idx != INVALID_INDEX) {
        while(idx < FS_META_HEADER->fields.tail) {
            bsize += byteSizeOfLogEntry(FS_LOG_ENTRY_AT(idx));
            idx++;
        }
    }
    return bsize;
}

size_t FilePersistLog::to_bytes(char* buf, const int64_t& ver) noexcept(false) {
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    size_t ofst = 0;
    // latest_version
    int64_t latest_version = this->getLatestVersion();
    *(int64_t*)(buf + ofst) = latest_version;
    ofst += sizeof(int64_t);
    // nr_log_entry
    *(int64_t*)(buf + ofst) = (idx == INVALID_INDEX) ? 0 : (FS_META_HEADER->fields.tail - idx);
    ofst += sizeof(int64_t);
    // log_entries
    if(idx != INVALID_INDEX) {
        while(idx < FS_META_HEADER->fields.tail) {
            ofst += writeLogEntryToByteArray(FS_LOG_ENTRY_AT(idx), buf + ofst);
            idx++;
        }
    }
    return ofst;
}

void FilePersistLog::post_object(const std::function<void(char const* const, std::size_t)>& f,
                                 const int64_t& ver) noexcept(false) {
    int64_t idx = this->getMinimumIndexBeyondVersion(ver);
    // latest_version
    int64_t latest_version = this->getLatestVersion();
    f((char*)&latest_version, sizeof(int64_t));
    // nr_log_entry
    int64_t nr_log_entry = (idx == INVALID_INDEX) ? 0 : (FS_META_HEADER->fields.tail - idx);
    f((char*)&nr_log_entry, sizeof(int64_t));
    // log_entries
    if(idx != INVALID_INDEX) {
        while(idx < FS_META_HEADER->fields.tail) {
            postLogEntry(f, FS_LOG_ENTRY_AT(idx));
            idx++;
        }
    }
}

void FilePersistLog::applyLogTail(char const* v) noexcept(false) {
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
    FS_META_HEADER->fields.ver = latest_version;
}

size_t FilePersistLog::byteSizeOfLogEntry(const LogEntry* ple) noexcept(false) {
    return sizeof(LogEntry) + ple->fields.dlen;
}

size_t FilePersistLog::writeLogEntryToByteArray(const LogEntry* ple, char* ba) noexcept(false) {
    size_t nr_written = 0;
    memcpy(ba, ple, sizeof(LogEntry));
    nr_written += sizeof(LogEntry);
    if(ple->fields.dlen > 0) {
        memcpy((void*)(ba + nr_written), (void*)FS_LOG_ENTRY_DATA(ple), ple->fields.dlen);
        nr_written += ple->fields.dlen;
    }
    return nr_written;
}

size_t FilePersistLog::postLogEntry(const std::function<void(char const* const, std::size_t)>& f, const LogEntry* ple) noexcept(false) {
    size_t nr_written = 0;
    f((const char*)ple, sizeof(LogEntry));
    nr_written += sizeof(LogEntry);
    if(ple->fields.dlen > 0) {
        f((const char*)FS_LOG_ENTRY_DATA(ple), ple->fields.dlen);
        nr_written += ple->fields.dlen;
    }
    return nr_written;
}

size_t FilePersistLog::mergeLogEntryFromByteArray(const char* ba) noexcept(false) {
    const LogEntry* cple = (const LogEntry*)ba;
    // valid check
    // 0) version grows monotonically.
    if(cple->fields.ver <= FS_META_HEADER->fields.ver) {
        dbg_default_trace("{0} skip log entry version {1}, we are at {2}.", __func__, cple->fields.ver, FS_META_HEADER->fields.ver);
        return cple->fields.dlen + sizeof(LogEntry);
    }
    // 1) do we have space to merge it?
    if(FS_NUM_FREE_SLOTS == 0) {
        dbg_default_trace("{0} failed to merge log entry, we don't empty log entry.", __func__);
        throw PERSIST_EXP_NOSPACE_LOG;
    }
    if(FS_NUM_FREE_BYTES < cple->fields.dlen) {
        dbg_default_trace("{0} failed to merge log entry, we need {1} bytes data space, but we have only {2} bytes.", __func__, cple->fields.dlen, FS_NUM_FREE_BYTES);
        throw PERSIST_EXP_NOSPACE_DATA;
    }
    // 2) merge it!
    memcpy(FS_NEXT_DATA, (const void*)(ba + sizeof(LogEntry)), cple->fields.dlen);
    memcpy(FS_NEXT_LOG_ENTRY, cple, sizeof(LogEntry));
    FS_NEXT_LOG_ENTRY->fields.ofst = FS_NEXT_DATA_OFST;
    this->hidx.insert(hlc_index_entry{HLC{cple->fields.hlc_r, cple->fields.hlc_l}, FS_META_HEADER->fields.tail});
    FS_META_HEADER->fields.tail++;
    FS_META_HEADER->fields.ver = cple->fields.ver;
    dbg_default_trace("{0} merge log:log entry and meta data are updated.", __func__);
    return cple->fields.dlen + sizeof(LogEntry);
}
//////////////////////////
// invisible to outside //
//////////////////////////
/* -- moved to util.hpp
  void checkOrCreateDir(const string & dirPath) 
  noexcept(false) {
    struct stat sb;
    if (stat(dirPath.c_str(),&sb) == 0) {
      if (! S_ISDIR(sb.st_mode)) {
        throw PERSIST_EXP_INV_PATH;
      }
    } else { 
      // create it
      if (mkdir(dirPath.c_str(),0700) != 0) {
        throw PERSIST_EXP_CREATE_PATH(errno);
      }
    }
  }

  bool checkOrCreateFileWithSize(const string & file, uint64_t size)
  noexcept(false) {
    bool bCreate = false;
    struct stat sb;
    int fd;

    if (stat(file.c_str(),&sb) == 0) {
      if(! S_ISREG(sb.st_mode)) {
        throw PERSIST_EXP_INV_FILE;
      }
    } else {
      // create it
      bCreate = true;
    }

    fd = open(file.c_str(), O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
    if (fd < 0) {
      throw PERSIST_EXP_CREATE_FILE(errno);
    }

    if (ftruncate(fd,size) != 0) {
      throw PERSIST_EXP_TRUNCATE_FILE(errno);
    }
    close(fd);
    return bCreate;
  }
*/
bool FilePersistLog::checkOrCreateMetaFile() noexcept(false) {
    return checkOrCreateFileWithSize(this->m_sMetaFile, FS_META_SIZE);
}

bool FilePersistLog::checkOrCreateLogFile() noexcept(false) {
    return checkOrCreateFileWithSize(this->m_sLogFile, FS_MAX_LOG_SIZE);
}

bool FilePersistLog::checkOrCreateDataFile() noexcept(false) {
    return checkOrCreateFileWithSize(this->m_sDataFile, FS_MAX_DATA_SIZE);
}

void FilePersistLog::truncate(const int64_t& ver) noexcept(false) {
    dbg_default_trace("{0} truncate at version: {1}.", this->m_sName, ver);
    FPL_WRLOCK;
    // STEP 1: search for the log entry
    // TODO
    //binary search
    int64_t head = FS_META_HEADER->fields.head % FS_MAX_LOG_ENTRY;
    int64_t tail = FS_META_HEADER->fields.tail % FS_MAX_LOG_ENTRY;
    if(tail < head) tail += FS_MAX_LOG_ENTRY;
    dbg_default_trace("{0} - begin binary search.", this->m_sName);
    int64_t l_idx = binarySearch<int64_t>(
            [&](const LogEntry* ple) {
                return ple->fields.ver;
            },
            ver, head, tail);
    dbg_default_trace("{0} - end binary search.", this->m_sName);
    // STEP 2: update FS_META_HEADER
    if(l_idx == -1) {  // not adequate log found. We need to remove all logs.
        // TODO: this may not be safe in case the log has been trimmed beyond 'ver' !!!
        FS_META_HEADER->fields.tail = FS_META_HEADER->fields.head;
    } else {
        int64_t _idx = (FS_META_HEADER->fields.head + l_idx - head) + ((head > l_idx) ? FS_MAX_LOG_ENTRY : 0);
        FS_META_HEADER->fields.tail = _idx + 1;
    }
    if(FS_META_HEADER->fields.ver > ver)
        FS_META_HEADER->fields.ver = ver;
    // STEP 3: update PERSISTENT STATE
    FPL_PERS_LOCK;
    try {
        persistMetaHeaderAtomically(FS_META_HEADER);
    } catch(uint64_t e) {
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        throw e;
    }
    FPL_PERS_UNLOCK;
    FPL_UNLOCK;
    dbg_default_trace("{0} truncate at version: {1}....done", this->m_sName, ver);
}

const uint64_t FilePersistLog::getMinimumLatestPersistedVersion(const std::string& prefix) {
    // STEP 1: list all meta files in the path
    DIR* dir = opendir(getPersFilePath().c_str());
    if(dir == NULL) {
        // We cannot open the persistent directory, so just return error.
        dbg_default_error("{}:{} failed to open the directory. errno={}, err={}.",
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
                dbg_default_warn("{}:{} cannot read file:{}, errno={}, err={}.",
                                 __FILE__, __func__, errno, strerror(errno));
                continue;
            }
            int nRead = read(fd, (void*)&mh, sizeof(mh));
            if(nRead != sizeof(mh)) {
                dbg_default_warn("{}:{} cannot load meta header from file:{}, errno={}, err={}",
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
}
}  // namespace persistent
