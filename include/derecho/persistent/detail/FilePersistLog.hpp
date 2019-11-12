#ifndef FILE_PERSIST_LOG_HPP
#define FILE_PERSIST_LOG_HPP

#include "PersistLog.hpp"
#include "util.hpp"
#include <derecho/utils/logger.hpp>
#include <pthread.h>
#include <string>

namespace persistent {

#define META_FILE_SUFFIX "meta"
#define LOG_FILE_SUFFIX "log"
#define DATA_FILE_SUFFIX "data"
#define SWAP_FILE_SUFFIX "swp"

// meta header format
typedef union meta_header {
    struct {
        int64_t head;  // the head index
        int64_t tail;  // the tail index
        int64_t ver;   // the latest version number.
                       // uint64_t d_head;  // the data head offset
                       // uint64_t d_tail;  // the data tail offset
    } fields;
    uint8_t bytes[256];
    bool operator==(const union meta_header& other) {
        return (this->fields.head == other.fields.head) && (this->fields.tail == other.fields.tail) && (this->fields.ver == other.fields.ver);
    };
} MetaHeader;

// log entry format
typedef union log_entry {
    struct {
        int64_t ver;     // version of the data
        uint64_t dlen;   // length of the data
        uint64_t ofst;   // offset of the data in the memory buffer
        uint64_t hlc_r;  // realtime component of hlc
        uint64_t hlc_l;  // logic component of hlc
    } fields;
    uint8_t bytes[64];
} LogEntry;

// TODO: make this hard-wired number configurable.
// Currently, we allow 1M(2^20-1) log entries and
// 512GB data size. The max log entry and max size are
// both from the configuration file:
// CONF_PERS_MAX_LOG_ENTRY - "PERS/max_log_entry"
// CONF_PERS_MAX_DATA_SIZE - "PERS/max_data_size"
#define MAX_LOG_ENTRY (this->m_iMaxLogEntry)
#define MAX_LOG_SIZE (sizeof(LogEntry) * MAX_LOG_ENTRY)
#define MAX_DATA_SIZE (this->m_iMaxDataSize)
#define META_SIZE (sizeof(MetaHeader))

// helpers:
///// READ or WRITE LOCK on LOG REQUIRED to use the following MACROs!!!!
#define META_HEADER ((MetaHeader*)(&(this->m_currMetaHeader)))
#define META_HEADER_PERS ((MetaHeader*)(&(this->m_persMetaHeader)))
#define LOG_ENTRY_ARRAY ((LogEntry*)(this->m_pLog))

#define NUM_USED_SLOTS (META_HEADER->fields.tail - META_HEADER->fields.head)
// #define NUM_USED_SLOTS_PERS   (META_HEADER_PERS->tail - META_HEADER_PERS->head)
#define NUM_FREE_SLOTS (MAX_LOG_ENTRY - 1 - NUM_USED_SLOTS)
// #define NUM_FREE_SLOTS_PERS   (MAX_LOG_ENTRY - 1 - NUM_USERD_SLOTS_PERS)

#define LOG_ENTRY_AT(idx) (LOG_ENTRY_ARRAY + (int)((idx) % MAX_LOG_ENTRY))
#define NEXT_LOG_ENTRY LOG_ENTRY_AT(META_HEADER->fields.tail)
#define NEXT_LOG_ENTRY_PERS LOG_ENTRY_AT( \
        MAX(META_HEADER_PERS->fields.tail, META_HEADER->fields.head))
#define CURR_LOG_IDX ((NUM_USED_SLOTS == 0) ? -1 : META_HEADER->fields.tail - 1)
#define LOG_ENTRY_DATA(e) ((void*)((uint8_t*)this->m_pData + (e)->fields.ofst % MAX_DATA_SIZE))

#define NEXT_DATA_OFST ((CURR_LOG_IDX == -1) ? 0 : (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen))
#define NEXT_DATA ((void*)((uint64_t) this->m_pData + NEXT_DATA_OFST % MAX_DATA_SIZE))
#define NEXT_DATA_PERS ((NEXT_LOG_ENTRY > NEXT_LOG_ENTRY_PERS) ? LOG_ENTRY_DATA(NEXT_LOG_ENTRY_PERS) : NULL)

#define NUM_USED_BYTES ((NUM_USED_SLOTS == 0) ? 0 : (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen - LOG_ENTRY_AT(META_HEADER->fields.head)->fields.ofst))
#define NUM_FREE_BYTES (MAX_DATA_SIZE - NUM_USED_BYTES)

#define PAGE_SIZE (getpagesize())
#define ALIGN_TO_PAGE(x) ((void*)(((uint64_t)(x)) - ((uint64_t)(x)) % PAGE_SIZE))

// declaration for binary search util. see cpp file for comments.
template <typename TKey, typename KeyGetter>
int64_t binarySearch(const KeyGetter&, const TKey&, const int64_t&, const int64_t&);

// FilePersistLog is the default persist Log
class FilePersistLog : public PersistLog {
protected:
    // the current meta header
    MetaHeader m_currMetaHeader;
    // the persisted meta header
    MetaHeader m_persMetaHeader;
    // path of the data files
    const std::string m_sDataPath;
    // full meta file name
    const std::string m_sMetaFile;
    // full log file name
    const std::string m_sLogFile;
    // full data file name
    const std::string m_sDataFile;
    // max number of log entry
    const uint64_t m_iMaxLogEntry;
    // max data size
    const uint64_t m_iMaxDataSize;

    // the log file descriptor
    int m_iLogFileDesc;
    // the data file descriptor
    int m_iDataFileDesc;

    // memory mapped Log RingBuffer
    void* m_pLog;
    // memory mapped Data RingBuffer
    void* m_pData;
    // read/write lock
    pthread_rwlock_t m_rwlock;
    // persistent lock
    pthread_mutex_t m_perslock;

// lock macro
#define FPL_WRLOCK                                        \
    do {                                                  \
        if(pthread_rwlock_wrlock(&this->m_rwlock) != 0) { \
            throw PERSIST_EXP_RWLOCK_WRLOCK(errno);       \
        }                                                 \
    } while(0)

#define FPL_RDLOCK                                        \
    do {                                                  \
        if(pthread_rwlock_rdlock(&this->m_rwlock) != 0) { \
            throw PERSIST_EXP_RWLOCK_WRLOCK(errno);       \
        }                                                 \
    } while(0)

#define FPL_UNLOCK                                        \
    do {                                                  \
        if(pthread_rwlock_unlock(&this->m_rwlock) != 0) { \
            throw PERSIST_EXP_RWLOCK_UNLOCK(errno);       \
        }                                                 \
    } while(0)

#define FPL_PERS_LOCK                                    \
    do {                                                 \
        if(pthread_mutex_lock(&this->m_perslock) != 0) { \
            throw PERSIST_EXP_MUTEX_LOCK(errno);         \
        }                                                \
    } while(0)

#define FPL_PERS_UNLOCK                                    \
    do {                                                   \
        if(pthread_mutex_unlock(&this->m_perslock) != 0) { \
            throw PERSIST_EXP_MUTEX_UNLOCK(errno);         \
        }                                                  \
    } while(0)

    // load the log from files. This method may through exceptions if read from
    // file failed.
    virtual void load() noexcept(false);

    // reset the logs. This will remove the existing persisted data.
    virtual void reset() noexcept(false);

    // Persistent the Metadata header, we assume
    // FPL_PERS_LOCK is acquired.
    virtual void persistMetaHeaderAtomically(MetaHeader*) noexcept(false);

public:
    //Constructor
    FilePersistLog(const std::string& name, const std::string& dataPath) noexcept(false);
    FilePersistLog(const std::string& name) noexcept(false) : FilePersistLog(name, getPersFilePath()){};
    //Destructor
    virtual ~FilePersistLog() noexcept(true);

    //Derived from PersistLog
    virtual void append(const void* pdata,
                        const uint64_t& size, const int64_t& ver,
                        const HLC& mhlc) noexcept(false);
    virtual void advanceVersion(const int64_t& ver) noexcept(false);
    virtual int64_t getLength() noexcept(false);
    virtual int64_t getEarliestIndex() noexcept(false);
    virtual int64_t getLatestIndex() noexcept(false);
    virtual int64_t getVersionIndex(const version_t& ver) noexcept(false);
    virtual int64_t getHLCIndex(const HLC& hlc) noexcept(false);
    virtual version_t getEarliestVersion() noexcept(false);
    virtual version_t getLatestVersion() noexcept(false);
    virtual const version_t getLastPersisted() noexcept(false);
    virtual const void* getEntryByIndex(const int64_t& eno) noexcept(false);
    virtual const void* getEntry(const version_t& ver) noexcept(false);
    virtual const void* getEntry(const HLC& hlc) noexcept(false);
    virtual const version_t persist(const bool preLocked = false) noexcept(false);
    virtual void trimByIndex(const int64_t& eno) noexcept(false);
    virtual void trim(const version_t& ver) noexcept(false);
    virtual void trim(const HLC& hlc) noexcept(false);
    virtual void truncate(const version_t& ver) noexcept(false);
    virtual size_t bytes_size(const version_t& ver) noexcept(false);
    virtual size_t to_bytes(char* buf, const version_t& ver) noexcept(false);
    virtual void post_object(const std::function<void(char const* const, std::size_t)>& f,
                             const version_t& ver) noexcept(false);
    virtual void applyLogTail(char const* v) noexcept(false);

    template <typename TKey, typename KeyGetter>
    void trim(const TKey& key, const KeyGetter& keyGetter) noexcept(false) {
        int64_t idx;
        // RDLOCK for validation
        FPL_RDLOCK;
        idx = binarySearch<TKey>(keyGetter, key, META_HEADER->fields.head, META_HEADER->fields.tail);
        if(idx == -1) {
            FPL_UNLOCK;
            return;
        }
        FPL_UNLOCK;
        // do binary search again in case some concurrent trim() and
        // append() happens. TODO: any optimization to avoid the second
        // search?
        // WRLOCK for trim
        FPL_WRLOCK;
        idx = binarySearch<TKey>(keyGetter, key, META_HEADER->fields.head, META_HEADER->fields.tail);
        if(idx != -1) {
            META_HEADER->fields.head = (idx + 1);
            FPL_PERS_LOCK;
            try {
                persist(true);
            } catch(uint64_t e) {
                FPL_UNLOCK;
                FPL_PERS_UNLOCK;
                throw e;
            }
            FPL_PERS_UNLOCK;
            //TODO:remove delete entries from the index. This is tricky because
            // HLC order and idex order does not agree with each other.
            // throw PERSIST_EXP_UNIMPLEMENTED;
        } else {
            FPL_UNLOCK;
            return;
        }
        FPL_UNLOCK;
    }

    /**
     * Get the minimum latest persisted version for a subgroup/shard with prefix
     * @PARAM prefix the subgroup/shard prefix
     * @RETURN the minimum latest persisted version
     */
    static const uint64_t getMinimumLatestPersistedVersion(const std::string& prefix);

private:
     /** verify the existence of the meta file */
     bool checkOrCreateMetaFile() noexcept(false);

     /** verify the existence of the log file */
     bool checkOrCreateLogFile() noexcept(false);

     /** verify the existence of the data file */
     bool checkOrCreateDataFile() noexcept(false);

    /**
     * Get the minimum index greater than a given version
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ver the given version. INVALID_VERSION means to return the earliest index.
     * @RETURN the minimum index since the given version. INVALID_INDEX means 
     *         that no log entry is available for the requested version.
     */
    int64_t getMinimumIndexBeyondVersion(const int64_t& ver) noexcept(false);
    /**
     * get the byte size of log entry
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ple - pointer to the log entry
     * @RETURN the number of bytes required for the serialized data.
     */
    size_t byteSizeOfLogEntry(const LogEntry* ple) noexcept(false);
    /**
     * serialize the log entry to a byte array
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ple - the pointer to the log entry
     * @RETURN the number of bytes written to the byte array
     */
    size_t writeLogEntryToByteArray(const LogEntry* ple, char* ba) noexcept(false);
    /**
     * post the log entry to a serialization function accepting a byte array
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM f - funciton
     * @PARAM ple - pointer to the log entry
     * @RETURN the number of bytes posted.
     */
    size_t postLogEntry(const std::function<void(char const* const, std::size_t)>& f, const LogEntry* ple) noexcept(false);
    /**
     * merge the log entry to current state.
     * Note: no lock protected, use FPL_WRLOCK
     * @PARAM ba - serialize form of the entry
     * @RETURN - number of size read from the entry.
     */
    size_t mergeLogEntryFromByteArray(const char* ba) noexcept(false);

    /**
     * binary search through the log, return the maximum index of the entries
     * whose key <= @param key. Note that indexes used here is 'virtual'.
     *
     * [ ][ ][ ][ ][X][X][X][X][ ][ ][ ]
     *              ^logHead   ^logTail
     * @param keyGetter: function which get the key from LogEntry
     * @param key: the key to be search
     * @param logArr: log array
     * @param len: log length
     * @return index of the log entry found or -1 if not found.
     */
    template <typename TKey, typename KeyGetter>
    int64_t binarySearch(const KeyGetter& keyGetter, const TKey& key,
                         const int64_t& logHead, const int64_t& logTail) noexcept(false) {
        if(logTail <= logHead) {
            dbg_default_trace("binary Search failed...EMPTY LOG");
            return (int64_t)-1L;
        }
        int64_t head = logHead, tail = logTail - 1;
        int64_t pivot = 0;
        while(head <= tail) {
            pivot = (head + tail) / 2;
            dbg_default_trace("Search range: {0}->[{1},{2}]", pivot, head, tail);
            const TKey p_key = keyGetter(LOG_ENTRY_AT(pivot));
            if(p_key == key) {
                break;  // found
            } else if(p_key < key) {
                if(pivot + 1 >= logTail) {
                    break;  // found - the last element
                } else if(keyGetter(LOG_ENTRY_AT(pivot + 1)) > key) {
                    break;  // found - the next one is greater than key
                } else {    // search right
                    head = pivot + 1;
                }
            } else {  // search left
                tail = pivot - 1;
                if(head > tail) {
                    dbg_default_trace("binary Search failed...Object does not exist.");
                    return (int64_t)-1L;
                }
            }
        }
        return pivot;
    }

#ifdef DERECHO_DEBUG
    //dbg functions
    void dbgDumpMeta() {
        dbg_default_trace("m_pData={0},m_pLog={1}", (void*)this->m_pData, (void*)this->m_pLog);
        dbg_default_trace("MEAT_HEADER:head={0},tail={1}", (int64_t)META_HEADER->fields.head, (int64_t)META_HEADER->fields.tail);
        dbg_default_trace("MEAT_HEADER_PERS:head={0},tail={1}", (int64_t)META_HEADER_PERS->fields.head, (int64_t)META_HEADER_PERS->fields.tail);
        dbg_default_trace("NEXT_LOG_ENTRY={0},NEXT_LOG_ENTRY_PERS={1}", (void*)NEXT_LOG_ENTRY, (void*)NEXT_LOG_ENTRY_PERS);
    }
#endif  //DERECHO_DEBUG
};
}

#endif  //FILE_PERSIST_LOG_HPP
