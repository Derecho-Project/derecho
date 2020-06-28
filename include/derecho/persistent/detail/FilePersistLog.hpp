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
//Every log entry will be padded out to this size, which must be page-aligned
#define MAX_LOG_ENTRY_SIZE (1024)
//Similarly, the size of a meta header must be page-aligned
#define META_HEADER_SIZE (256)

// meta header format
union MetaHeader {
    struct {
        int64_t head;  // the head index
        int64_t tail;  // the tail index
        int64_t ver;   // the latest version number.
    } fields;
    uint8_t bytes[META_HEADER_SIZE];
    bool operator==(const MetaHeader& other) {
        return (this->fields.head == other.fields.head) && (this->fields.tail == other.fields.tail)
               && (this->fields.ver == other.fields.ver);
    };
};

// log entry format
union LogEntry {
    struct {
        int64_t ver;       // version of the data
        uint64_t dlen;     // length of the data
        uint64_t ofst;     // offset of the data in the memory buffer
        uint64_t hlc_r;    // realtime component of hlc
        uint64_t hlc_l;    // logic component of hlc
        int64_t prev_signed_ver; // previous signed version, whose signature is included in this version's signature
        char signature[];  // signature
    } fields;
    uint8_t bytes[MAX_LOG_ENTRY_SIZE];
};

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
#define LOG_ENTRY_ARRAY ((LogEntry*)(this->m_pLog))

#define NUM_USED_SLOTS (m_currMetaHeader.fields.tail - m_currMetaHeader.fields.head)
// #define NUM_USED_SLOTS_PERS   (m_persMetaHeader.tail - m_persMetaHeader.head)
#define NUM_FREE_SLOTS (MAX_LOG_ENTRY - 1 - NUM_USED_SLOTS)
// #define NUM_FREE_SLOTS_PERS   (MAX_LOG_ENTRY - 1 - NUM_USERD_SLOTS_PERS)

#define LOG_ENTRY_AT(idx) (LOG_ENTRY_ARRAY + (int)((idx) % MAX_LOG_ENTRY))
#define NEXT_LOG_ENTRY LOG_ENTRY_AT(m_currMetaHeader.fields.tail)
#define NEXT_LOG_ENTRY_PERS LOG_ENTRY_AT( \
        MAX(m_persMetaHeader.fields.tail, m_currMetaHeader.fields.head))
#define CURR_LOG_IDX ((NUM_USED_SLOTS == 0) ? INVALID_INDEX : m_currMetaHeader.fields.tail - 1)
#define LOG_ENTRY_DATA(e) ((void*)((uint8_t*)this->m_pData + (e)->fields.ofst % MAX_DATA_SIZE))

#define NEXT_DATA_OFST ((CURR_LOG_IDX == INVALID_INDEX) ? 0 : (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen))
#define NEXT_DATA ((void*)((uint64_t)this->m_pData + NEXT_DATA_OFST % MAX_DATA_SIZE))
#define NEXT_DATA_PERS ((NEXT_LOG_ENTRY > NEXT_LOG_ENTRY_PERS) ? LOG_ENTRY_DATA(NEXT_LOG_ENTRY_PERS) : NULL)

#define NUM_USED_BYTES ((NUM_USED_SLOTS == 0) ? 0 : (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen - LOG_ENTRY_AT(m_currMetaHeader.fields.head)->fields.ofst))
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
    virtual void load();

    // reset the logs. This will remove the existing persisted data.
    virtual void reset();

    // Persistent the Metadata header, we assume
    // FPL_PERS_LOCK is acquired.
    virtual void persistMetaHeaderAtomically(MetaHeader*);

public:
    //Constructor
    FilePersistLog(const std::string& name, const std::string& dataPath);
    FilePersistLog(const std::string& name) : FilePersistLog(name, getPersFilePath()){};
    //Destructor
    virtual ~FilePersistLog() noexcept(true);

    //Derived from PersistLog
    virtual void append(const void* pdata,
                        uint64_t size, version_t ver,
                        const HLC& mhlc);
    virtual void advanceVersion(int64_t ver);
    virtual int64_t getLength();
    virtual int64_t getEarliestIndex();
    virtual int64_t getLatestIndex();
    virtual int64_t getVersionIndex(version_t ver);
    virtual int64_t getHLCIndex(const HLC& hlc);
    virtual version_t getEarliestVersion();
    virtual version_t getLatestVersion();
    virtual const version_t getLastPersistedVersion();
    virtual const void* getEntryByIndex(int64_t eno);
    virtual const void* getEntry(version_t ver);
    virtual const void* getEntry(const HLC& hlc);
    virtual const version_t persist(version_t ver,
                                    const bool preLocked = false);
    virtual void processEntryAtVersion(version_t ver, const std::function<void(const void*, std::size_t)>& func);
    virtual void addSignature(version_t ver, const unsigned char* signature, version_t previous_signed_version);
    virtual bool getSignature(version_t ver, unsigned char* signature, version_t& previous_signed_version);
    virtual void trimByIndex(int64_t eno);
    virtual void trim(version_t ver);
    virtual void trim(const HLC& hlc);
    virtual void truncate(version_t ver);
    virtual size_t bytes_size(version_t ver);
    virtual size_t to_bytes(char* buf, version_t ver);
    virtual void post_object(const std::function<void(char const* const, std::size_t)>& f,
                             version_t ver);
    virtual void applyLogTail(char const* v);

    template <typename TKey, typename KeyGetter>
    void trim(const TKey& key, const KeyGetter& keyGetter) {
        int64_t idx;
        // RDLOCK for validation
        FPL_RDLOCK;
        idx = binarySearch<TKey>(keyGetter, key, m_currMetaHeader.fields.head, m_currMetaHeader.fields.tail);
        if(idx == INVALID_INDEX) {
            FPL_UNLOCK;
            return;
        }
        FPL_UNLOCK;
        // do binary search again in case some concurrent trim() and
        // append() happens. TODO: any optimization to avoid the second
        // search?
        // WRLOCK for trim
        FPL_WRLOCK;
        idx = binarySearch<TKey>(keyGetter, key, m_currMetaHeader.fields.head, m_currMetaHeader.fields.tail);
        if(idx != INVALID_INDEX) {
            m_currMetaHeader.fields.head = (idx + 1);
            FPL_PERS_LOCK;
            try {
                // What version number should be supplied to persist in this case?
                // CAUTION:
                // The persist API is changed for Edward's convenience by adding a version parameter
                // This has a widespreading on the design and needs extensive test before replying on
                // it.
                version_t ver = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver;
                persist(ver, true);
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
     bool checkOrCreateMetaFile();

     /** verify the existence of the log file */
     bool checkOrCreateLogFile();

     /** verify the existence of the data file */
     bool checkOrCreateDataFile();

    /**
     * Get the minimum index greater than a given version
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ver the given version. INVALID_VERSION means to return the earliest index.
     * @RETURN the minimum index since the given version. INVALID_INDEX means
     *         that no log entry is available for the requested version.
     */
    int64_t getMinimumIndexBeyondVersion(version_t ver);
    /**
     * get the byte size of log entry
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ple - pointer to the log entry
     * @RETURN the number of bytes required for the serialized data.
     */
    size_t byteSizeOfLogEntry(const LogEntry* ple);
    /**
     * serialize the log entry to a byte array
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM ple - the pointer to the log entry
     * @RETURN the number of bytes written to the byte array
     */
    size_t writeLogEntryToByteArray(const LogEntry* ple, char* ba);
    /**
     * post the log entry to a serialization function accepting a byte array
     * Note: no lock protected, use FPL_RDLOCK
     * @PARAM f - funciton
     * @PARAM ple - pointer to the log entry
     * @RETURN the number of bytes posted.
     */
    size_t postLogEntry(const std::function<void(char const* const, std::size_t)>& f, const LogEntry* ple);
    /**
     * merge the log entry to current state.
     * Note: no lock protected, use FPL_WRLOCK
     * @PARAM ba - serialize form of the entry
     * @RETURN - number of size read from the entry.
     */
    size_t mergeLogEntryFromByteArray(const char* ba);

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
     * @return index of the log entry found or INVALID_INDEX if not found.
     */
    template <typename TKey, typename KeyGetter>
    int64_t binarySearch(const KeyGetter& keyGetter, const TKey& key,
                         const int64_t& logHead, const int64_t& logTail) {
        if(logTail <= logHead) {
            dbg_default_trace("binary Search failed...EMPTY LOG");
            return INVALID_INDEX;
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
                    return INVALID_INDEX;
                }
            }
        }
        return pivot;
    }

    /* Validate the log before we append. It will throw exception if
     * - there is no space
     * - the version is not monotonic
     * @param size: size of the data to be append in this log entry
     * @param ver: version of the new log entry
     */
    void do_append_validation(const uint64_t size, const int64_t ver);

#ifndef NDEBUG
    //dbg functions
    void dbgDumpMeta() {
        dbg_default_trace("m_pData={0},m_pLog={1}", (void*)this->m_pData, (void*)this->m_pLog);
        dbg_default_trace("META_HEADER:head={0},tail={1}", (int64_t)m_currMetaHeader.fields.head, (int64_t)m_currMetaHeader.fields.tail);
        dbg_default_trace("META_HEADER_PERS:head={0},tail={1}", (int64_t)m_persMetaHeader.fields.head, (int64_t)m_persMetaHeader.fields.tail);
        dbg_default_trace("NEXT_LOG_ENTRY={0},NEXT_LOG_ENTRY_PERS={1}", (void*)NEXT_LOG_ENTRY, (void*)NEXT_LOG_ENTRY_PERS);
    }
#endif  // NDEBUG
};
}  // namespace persistent

#endif  //FILE_PERSIST_LOG_HPP
