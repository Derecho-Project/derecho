#ifndef FILE_PERSIST_LOG_HPP
#define FILE_PERSIST_LOG_HPP

#include <pthread.h>
#include <string>
#include "util.hpp"
#include "PersistLog.hpp"

namespace ns_persistent {

  // TODO:hardwired definitions need go to configuration file
  #define DEFAULT_FILE_PERSIST_LOG_DATA_PATH (".plog")
  #define META_FILE_SUFFIX ("meta")
  #define LOG_FILE_SUFFIX  ("log")
  #define DATA_FILE_SUFFIX ("data")
  #define SWAP_FILE_SUFFIX ("swp")

  // meta header format
  typedef union meta_header {
    struct {
      int64_t head;     // the head index
      int64_t tail;     // the tail index
      // uint64_t d_head;  // the data head offset
      // uint64_t d_tail;  // the data tail offset
    } fields;
    uint8_t bytes[256];
    bool operator == (const union meta_header & other) {
      return (this->fields.head == other.fields.head) && 
             (this->fields.tail == other.fields.tail);
    };
  } MetaHeader;

  // log entry format
  typedef union log_entry {
    struct {
      __int128 ver;      // version of the data
      uint64_t dlen;    // length of the data
      uint64_t ofst;    // offset of the data in the memory buffer
      uint64_t hlc_r;   // realtime component of hlc
      uint64_t hlc_l;   // logic component of hlc
    } fields;
    uint8_t bytes[64];
  } LogEntry;

  // TODO: make this hard-wired number configurable.
  // Currently, we allow 16383(2^14-1) log entries and
  // 16M data size.
  // #define MAX_LOG_ENTRY         (1UL<<14)
  #define MAX_LOG_ENTRY         (1UL<<6)
  #define MAX_LOG_SIZE          (sizeof(LogEntry)*MAX_LOG_ENTRY)
  // #define MAX_DATA_SIZE         (1UL<<26)
  #define MAX_DATA_SIZE         (1UL<<12)
  #define META_SIZE             (sizeof(MetaHeader))

  // helpers:
  ///// READ or WRITE LOCK on LOG REQUIRED to use the following MACROs!!!!
  #define META_HEADER           ((MetaHeader*)(&(this->m_currMetaHeader)))
  #define META_HEADER_PERS      ((MetaHeader*)(&(this->m_persMetaHeader)))
  #define LOG_ENTRY_ARRAY       ((LogEntry*)(this->m_pLog))

  #define NUM_USED_SLOTS        (META_HEADER->fields.tail - META_HEADER->fields.head)
  // #define NUM_USED_SLOTS_PERS   (META_HEADER_PERS->tail - META_HEADER_PERS->head)
  #define NUM_FREE_SLOTS        (MAX_LOG_ENTRY - 1 - NUM_USED_SLOTS)
  // #define NUM_FREE_SLOTS_PERS   (MAX_LOG_ENTRY - 1 - NUM_USERD_SLOTS_PERS)

  #define LOG_ENTRY_AT(idx)     (LOG_ENTRY_ARRAY + (int)((idx)%MAX_LOG_ENTRY))
  #define NEXT_LOG_ENTRY        LOG_ENTRY_AT(META_HEADER->fields.tail)
  #define NEXT_LOG_ENTRY_PERS   LOG_ENTRY_AT( \
    MIN(META_HEADER_PERS->fields.tail,META_HEADER->fields.head))
  #define CURR_LOG_IDX        ((NUM_USED_SLOTS == 0)? -1 : META_HEADER->fields.tail - 1)
  #define LOG_ENTRY_DATA(e)     ((void *)((uint8_t *)this->m_pData + \
    (e)->fields.ofst%MAX_DATA_SIZE))

  #define NEXT_DATA_OFST        ((CURR_LOG_IDX == -1)? 0 : \
    (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + \
     LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen))
  #define NEXT_DATA             ((void *)((uint64_t)this->m_pData + NEXT_DATA_OFST%MAX_DATA_SIZE))
  #define NEXT_DATA_PERS        ((NEXT_LOG_ENTRY > NEXT_LOG_ENTRY_PERS) ? \
    LOG_ENTRY_DATA(NEXT_LOG_ENTRY_PERS) : NULL)

  #define NUM_USED_BYTES        ((NUM_USED_SLOTS == 0)? 0 : \
    ( LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst + \
      LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen - \
      LOG_ENTRY_AT(META_HEADER->fields.head)->fields.ofst ))
  #define NUM_FREE_BYTES        (MAX_DATA_SIZE - NUM_USED_BYTES)

  #define PAGE_SIZE             (getpagesize())
  #define ALIGN_TO_PAGE(x)      ((void *)(((uint64_t)(x))-((uint64_t)(x))%PAGE_SIZE))

  // declaration for binary search util. see cpp file for comments.
  template<typename TKey,typename KeyGetter>
    int64_t binarySearch(const KeyGetter &, const TKey &, const int64_t&, const int64_t&);

  // FilePersistLog is the default persist Log
  class FilePersistLog : public PersistLog {
  protected:
    // the current meta header
    MetaHeader m_currMetaHeader;
    // the persisted meta header
    MetaHeader m_persMetaHeader;
   // path of the data files
    const string m_sDataPath;
    // full meta file name
    const string m_sMetaFile;
    // full log file name
    const string m_sLogFile;
    // full data file name
    const string m_sDataFile;

    // the log file descriptor
    int m_iLogFileDesc;
    // the data file descriptor
    int m_iDataFileDesc;

    // memory mapped Log RingBuffer
    void * m_pLog;
    // memory mapped Data RingBuffer
    void * m_pData;
    // read/write lock
    pthread_rwlock_t m_rwlock;
    // persistent lock
    pthread_mutex_t m_perslock;
    // lock macro
    #define FPL_WRLOCK \
    do { \
      if (pthread_rwlock_wrlock(&this->m_rwlock) != 0) { \
        throw PERSIST_EXP_RWLOCK_WRLOCK(errno); \
      } \
      dbg_trace("FPL_WRLOCK"); \
    } while (0)

    #define FPL_RDLOCK \
    do { \
      if (pthread_rwlock_rdlock(&this->m_rwlock) != 0) { \
        throw PERSIST_EXP_RWLOCK_WRLOCK(errno); \
      } \
      dbg_trace("FPL_RDLOCK"); \
    } while (0)

    #define FPL_UNLOCK \
    do { \
      if (pthread_rwlock_unlock(&this->m_rwlock) != 0) { \
        throw PERSIST_EXP_RWLOCK_UNLOCK(errno); \
      } \
      dbg_trace("FPL_UNLOCK"); \
    } while (0)

    #define FPL_PERS_LOCK \
    do { \
      if (pthread_mutex_lock(&this->m_perslock) != 0) { \
        throw PERSIST_EXP_MUTEX_LOCK(errno); \
      } \
      dbg_trace("PERS_LOCK"); \
    } while (0)

    #define FPL_PERS_UNLOCK \
    do { \
      if (pthread_mutex_unlock(&this->m_perslock) != 0) { \
        throw PERSIST_EXP_MUTEX_UNLOCK(errno); \
      } \
      dbg_trace("PERS_UNLOCK"); \
    } while (0)

 
    // load the log from files. This method may through exceptions if read from
    // file failed.
    virtual void load() noexcept(false);

    // Persistent the Metadata header, we assume 
    // 1) FPL_RDLOCK or FPL_WRLOCK is acquired.
    // 2) FPL_PERS_LOCK is acquired.
    virtual void persistMetaHeaderAtomically() noexcept(false);

  public:

    //Constructor
    FilePersistLog(const string &name,const string &dataPath) noexcept(false);
    FilePersistLog(const string &name) noexcept(false):
      FilePersistLog(name,DEFAULT_FILE_PERSIST_LOG_DATA_PATH){
    };
    //Destructor
    virtual ~FilePersistLog() noexcept(true);

    //Derived from PersistLog
    virtual void append(const void * pdata,
      const uint64_t & size, const __int128 & ver,
      const HLC &mhlc) noexcept(false);
    virtual int64_t getLength() noexcept(false);
    virtual int64_t getEarliestIndex() noexcept(false);
    virtual const __int128 getLastPersisted() noexcept(false);
    virtual const void* getEntryByIndex(const int64_t &eno) noexcept(false);
    virtual const void* getEntry(const __int128 & ver) noexcept(false);
    virtual const void* getEntry(const HLC &hlc) noexcept(false);
    //virtual const __int128 persist(const __int128 & ver = -1) noexcept(false);
    virtual const __int128 persist() noexcept(false);
    virtual void trim(const int64_t &eno) noexcept(false);
    virtual void trim(const __int128 &ver) noexcept(false);
    virtual void trim(const HLC & hlc) noexcept(false);

    template <typename TKey,typename KeyGetter>
    void trim(const TKey &key,const KeyGetter &keyGetter) noexcept(false) {
      int64_t head,tail,idx;
      // RDLOCK for validation
      FPL_RDLOCK;
      head = META_HEADER->fields.head % MAX_LOG_ENTRY;
      tail = META_HEADER->fields.tail % MAX_LOG_ENTRY;
      if (tail < head) tail += MAX_LOG_ENTRY;
      idx = binarySearch<TKey>(keyGetter,key,head,tail);
      if (idx == -1) {
        FPL_UNLOCK;
        return;
      }
      FPL_UNLOCK;
      // do binary search again in case some concurrent trim() and
      // append() happens. TODO: any optimization to avoid the second
      // search?
      // WRLOCK for trim
      FPL_WRLOCK;
      head = META_HEADER->fields.head % MAX_LOG_ENTRY;
      tail = META_HEADER->fields.tail % MAX_LOG_ENTRY;
      if (tail < head) tail += MAX_LOG_ENTRY;
      idx = binarySearch<TKey>(keyGetter,key,head,tail);
      if (idx != -1) {
        META_HEADER->fields.head += (idx-head+1);
      } else {
        FPL_UNLOCK;
        return;
      }
      FPL_UNLOCK;
    }

  private:
#ifdef _DEBUG
    //dbg functions
    void dbgDumpMeta() {
      dbg_trace("m_pData={0},m_pLog={1}",(void*)this->m_pData,(void*)this->m_pLog);
      dbg_trace("MEAT_HEADER:head={0},tail={1}",(int64_t)META_HEADER->fields.head,(int64_t)META_HEADER->fields.tail);
      dbg_trace("MEAT_HEADER_PERS:head={0},tail={1}",(int64_t)META_HEADER_PERS->fields.head,(int64_t)META_HEADER_PERS->fields.tail);
      dbg_trace("NEXT_LOG_ENTRY={0},NEXT_LOG_ENTRY_PERS={1}",(void*)NEXT_LOG_ENTRY,(void*)NEXT_LOG_ENTRY_PERS);
    }
#endif
  };
}

#endif//FILE_PERSIST_LOG_HPP
