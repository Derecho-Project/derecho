#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <string>
#include "util.hpp"
#include "FilePersistLog.hpp"

using namespace std;

namespace ns_persistent{

  /////////////////////////
  // internal structures //
  /////////////////////////

  // verify the existence of the meta file
  static bool checkOrCreateMetaFile(const string & metaFile) noexcept(false);

  // verify the existence of the log file
  static bool checkOrCreateLogFile(const string & logFile) noexcept(false);

  // verify the existence of the data file
  static bool checkOrCreateDataFile(const string & dataFile) noexcept(false);

  ////////////////////////
  // visible to outside //
  ////////////////////////

  FilePersistLog::FilePersistLog(const string &name, const string &dataPath) 
  noexcept(false) : PersistLog(name),
    m_sDataPath(dataPath),
    m_sMetaFile(dataPath + "/" + name + "." + META_FILE_SUFFIX),
    m_sLogFile(dataPath + "/" + name + "." + LOG_FILE_SUFFIX),
    m_sDataFile(dataPath + "/" + name + "." + DATA_FILE_SUFFIX),
    m_iLogFileDesc(-1),
    m_iDataFileDesc(-1),
    m_pLog(MAP_FAILED),
    m_pData(MAP_FAILED) {
#ifdef _DEBUG
    spdlog::set_level(spdlog::level::trace);
#endif
    if (pthread_rwlock_init(&this->m_rwlock,NULL) != 0) {
      throw PERSIST_EXP_RWLOCK_INIT(errno);
    }
    if (pthread_mutex_init(&this->m_perslock,NULL) != 0) {
      throw PERSIST_EXP_MUTEX_INIT(errno);
    }
    dbg_trace("{0} constructor: before load()",name);
    load();
    dbg_trace("{0} constructor: after load()",name);
  }

  void FilePersistLog::load()
  noexcept(false){
    dbg_trace("{0}:load state...begin",this->m_sName);
    // STEP 0: check if data path exists
    checkOrCreateDir(this->m_sDataPath);
    dbg_trace("{0}:checkOrCreateDir passed.",this->m_sName);
    // STEP 1: check and create files.
    bool bCreate = checkOrCreateMetaFile(this->m_sMetaFile);
    checkOrCreateLogFile(this->m_sLogFile);
    checkOrCreateDataFile(this->m_sDataFile);
    dbg_trace("{0}:checkOrCreateDataFile passed.",this->m_sName);
    // STEP 2: open files
    this->m_iLogFileDesc = open(this->m_sLogFile.c_str(),O_RDWR);
    if (this->m_iLogFileDesc == -1) {
      throw PERSIST_EXP_OPEN_FILE(errno);
    }
    this->m_iDataFileDesc = open(this->m_sDataFile.c_str(),O_RDWR);
    if (this->m_iDataFileDesc == -1) {
      throw PERSIST_EXP_OPEN_FILE(errno);
    }
    // STEP 3: mmap to memory
    //// we map the log entry and data twice to faciliate the search and data
    //// retrieving then the data is rewinding across the buffer end as follow:
    //// [1][2][3][4][5][6][1][2][3][4][5][6]
    this->m_pLog = mmap(NULL,MAX_LOG_SIZE<<1,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
    if(this->m_pLog == MAP_FAILED) {
      dbg_trace("{0}:reserve map space for log failed.", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap(this->m_pLog,MAX_LOG_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_FIXED,this->m_iLogFileDesc,0) == MAP_FAILED) {
      dbg_trace("{0}:map ringbuffer space for the first half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap((void*)((uint64_t)this->m_pLog+MAX_LOG_SIZE),MAX_LOG_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_FIXED,this->m_iLogFileDesc,0) == MAP_FAILED) {
      dbg_trace("{0}:map ringbuffer space for the second half of log failed. Is the size of log ringbuffer aligned to page?", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    //// data ringbuffer
    this->m_pData = mmap(NULL,MAX_DATA_SIZE<<1,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
    if (this->m_pData == MAP_FAILED) {
      dbg_trace("{0}:reserve map space for data failed.", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap(this->m_pData,MAX_DATA_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_FIXED,this->m_iDataFileDesc,0) == MAP_FAILED) {
      dbg_trace("{0}:map ringbuffer space for the first half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    if(mmap((void*)((uint64_t)this->m_pData + MAX_DATA_SIZE),MAX_DATA_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_FIXED,this->m_iDataFileDesc,0) == MAP_FAILED) {
      dbg_trace("{0}:map ringbuffer space for the second half of data failed. Is the size of data ringbuffer aligned to page?", this->m_sName);
      throw PERSIST_EXP_MMAP_FILE(errno);
    }
    dbg_trace("{0}:data/meta file mapped to memory",this->m_sName);
    // STEP 4: initialize the header for new created Metafile
    if (bCreate) {
      META_HEADER->fields.head = 0ll;
      META_HEADER->fields.tail = 0ll;
      META_HEADER_PERS->fields.head = -1ll; // -1 means uninitialized
      META_HEADER_PERS->fields.tail = -1ll; // -1 means uninitialized
      // persist the header
      FPL_RDLOCK;
      FPL_PERS_LOCK;

      try {
        persistMetaHeaderAtomically();
      } catch (uint64_t e) {
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        throw e;
      }
      FPL_PERS_UNLOCK;
      FPL_UNLOCK;
      dbg_info("{0}:new header initialized.",this->m_sName);
    } else { // load META_HEADER from disk
      FPL_WRLOCK;
      FPL_PERS_LOCK;
      try {
        int fd = open(this->m_sMetaFile.c_str(), O_RDONLY);
        if (fd == -1) {
          throw PERSIST_EXP_OPEN_FILE(errno);
        }
        ssize_t nRead = read(fd, (void*)META_HEADER_PERS, sizeof(MetaHeader));
        if(nRead != sizeof(MetaHeader)) {
          close(fd);
          throw PERSIST_EXP_READ_FILE(errno);
        }
        close(fd);
        *META_HEADER = *META_HEADER_PERS;
      } catch (uint64_t e) {
        FPL_PERS_UNLOCK;
        FPL_UNLOCK;
        throw e;
      }

      FPL_PERS_UNLOCK;
      FPL_UNLOCK;
    }
    // STEP 5: update m_hlcLE with the latest event: we don't need this anymore
    //if (META_HEADER->fields.eno >0) {
    //  if (this->m_hlcLE.m_rtc_us < CURR_LOG_ENTRY->fields.hlc_r &&
    //    this->m_hlcLE.m_logic < CURR_LOG_ENTRY->fields.hlc_l){
    //    this->m_hlcLE.m_rtc_us = CURR_LOG_ENTRY->fields.hlc_r;
    //    this->m_hlcLE.m_logic = CURR_LOG_ENTRY->fields.hlc_l;
    //  }
    //}
    dbg_trace("{0}:load state...done",this->m_sName);
  }

  FilePersistLog::~FilePersistLog()
  noexcept(true){
    pthread_rwlock_destroy(&this->m_rwlock);
    pthread_mutex_destroy(&this->m_perslock);
    if (this->m_pData != MAP_FAILED){
      munmap(m_pData,MAX_DATA_SIZE<<1);
    }
    this->m_pData = nullptr; // prevent ~MemLog() destructor to release it again.
    if (this->m_pLog != MAP_FAILED){
      munmap(m_pLog,MAX_LOG_SIZE);
    }
    this->m_pLog = nullptr; // prevent ~MemLog() destructor to release it again.
    if (this->m_iLogFileDesc != -1){
      close(this->m_iLogFileDesc);
    }
    if (this->m_iDataFileDesc != -1){
      close(this->m_iDataFileDesc);
    }
  }

  void FilePersistLog::append(const void *pdat, const uint64_t & size, const __int128 &ver, const HLC & mhlc)
  noexcept(false) {
    dbg_trace("{0} append event ({1},{2})",this->m_sName, mhlc.m_rtc_us, mhlc.m_logic);
    FPL_RDLOCK;

#define __DO_VALIDATION \
    do { \
      if (NUM_FREE_SLOTS < 1 ) { \
        FPL_UNLOCK; \
        throw PERSIST_EXP_NOSPACE_LOG; \
      } \
      if (NUM_FREE_BYTES < size) { \
        dbg_trace("{0}-append exception no space for data: NUM_FREE_BYTES={1}, size={2}", \
          this->m_sName, NUM_FREE_BYTES, size); \
        FPL_UNLOCK; \
        throw PERSIST_EXP_NOSPACE_DATA; \
      } \
      if ((CURR_LOG_IDX != -1) && \
          (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver >= ver)) { \
        __int128 cver = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver; \
        dbg_trace("{0}-append cur_ver:{1}.{2} new_ver:{3}.{4}", this->m_sName, \
          (int64_t)(cver>>64),(int64_t)cver,(int64_t)(ver>>64),(int64_t)ver); \
        FPL_UNLOCK; \
        throw PERSIST_EXP_INV_VERSION; \
      } \
    } while (0)

    __DO_VALIDATION;
    FPL_UNLOCK;
    dbg_trace("{0} append:validate check1 Finished.",this->m_sName);

    FPL_WRLOCK;
    //check
    __DO_VALIDATION;
    dbg_trace("{0} append:validate check2 Finished.",this->m_sName);

    // copy data
    memcpy(NEXT_DATA,pdat,size);
    dbg_trace("{0} append:data is copied to log.",this->m_sName);

    // fill the log entry
    NEXT_LOG_ENTRY->fields.ver = ver;
    NEXT_LOG_ENTRY->fields.dlen = size;
    NEXT_LOG_ENTRY->fields.ofst = NEXT_DATA_OFST;
    NEXT_LOG_ENTRY->fields.hlc_r = mhlc.m_rtc_us;
    NEXT_LOG_ENTRY->fields.hlc_l = mhlc.m_logic;
/* No Sync required here.
    if (msync(ALIGN_TO_PAGE(NEXT_LOG_ENTRY), 
        sizeof(LogEntry) + (((uint64_t)NEXT_LOG_ENTRY) % PAGE_SIZE),MS_SYNC) != 0) {
      FPL_UNLOCK;
      throw PERSIST_EXP_MSYNC(errno);
    }
*/

    // update meta header
    META_HEADER->fields.tail ++;
    dbg_trace("{0} append:log entry and meta data are updated.",this->m_sName);
/* No sync
    if (msync(this->m_pMeta,sizeof(MetaHeader),MS_SYNC) != 0) {
      FPL_UNLOCK;
      throw PERSIST_EXP_MSYNC(errno);
    }
*/
    dbg_trace("{0} append a log ver:{1}.{2} hlc:({3},{4})",this->m_sName, 
      HIGH__int128(ver), LOW__int128(ver),  mhlc.m_rtc_us, mhlc.m_logic);
    FPL_UNLOCK;
  }

  const __int128 FilePersistLog::persist()
    noexcept(false) {
    __int128 ver_ret = INVALID_VERSION;
    FPL_RDLOCK;
    FPL_PERS_LOCK;

    if(*META_HEADER == *META_HEADER_PERS) {
      if (CURR_LOG_IDX != -1){
        ver_ret = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver;
      }
      FPL_PERS_UNLOCK;
      FPL_UNLOCK;
      return ver_ret;
    }

    //flush data
    dbg_trace("{0} flush data,log,and meta.", this->m_sName);
    try {
      if ((NUM_USED_SLOTS > 0) && 
          (NEXT_LOG_ENTRY > NEXT_LOG_ENTRY_PERS)){
        size_t flush_len = (LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ofst +
          LOG_ENTRY_AT(CURR_LOG_IDX)->fields.dlen - 
          NEXT_LOG_ENTRY_PERS->fields.ofst);
        // flush data
        void * flush_start;
        flush_start = ALIGN_TO_PAGE(NEXT_DATA_PERS);
        flush_len += ((int64_t)NEXT_DATA_PERS)%PAGE_SIZE;
        if (msync(flush_start,flush_len,MS_SYNC) != 0) {
          throw PERSIST_EXP_MSYNC(errno);
        }
        // flush log
        flush_start = ALIGN_TO_PAGE(NEXT_LOG_ENTRY_PERS);
        flush_len = ((size_t)NEXT_LOG_ENTRY-(size_t)NEXT_LOG_ENTRY_PERS) + 
          ((int64_t)NEXT_LOG_ENTRY_PERS)%PAGE_SIZE;
        if (msync(flush_start,flush_len,MS_SYNC) != 0) {
          throw PERSIST_EXP_MSYNC(errno);
        }
      }
      // flush meta data
      this->persistMetaHeaderAtomically();
    } catch (uint64_t e) {
      FPL_PERS_UNLOCK;
      FPL_UNLOCK;
      throw e;
    }
    dbg_trace("{0} flush data,log,and meta...done.", this->m_sName);

    //get the latest flushed version
    if (NUM_USED_SLOTS > 0) {
      ver_ret = LOG_ENTRY_AT(CURR_LOG_IDX)->fields.ver;
    }

    FPL_PERS_UNLOCK;
    FPL_UNLOCK;
    return ver_ret;
  }

  int64_t FilePersistLog::getLength ()
  noexcept(false) {

    FPL_RDLOCK;
    int64_t len = NUM_USED_SLOTS;
    FPL_UNLOCK;

    return len;
  }

  int64_t FilePersistLog::getEarliestIndex ()
  noexcept(false) {
    FPL_RDLOCK;
    int64_t idx = (NUM_FREE_SLOTS == 0)? INVALID_INDEX:META_HEADER->fields.head;
    FPL_UNLOCK;
    return idx;
  }

  const __int128 FilePersistLog::getLastPersisted ()
  noexcept(false) {
    __int128 last_persisted = INVALID_VERSION;
    FPL_RDLOCK;
    FPL_PERS_LOCK;

    if ((META_HEADER_PERS->fields.tail > META_HEADER_PERS->fields.head) &&
       (META_HEADER->fields.head < META_HEADER_PERS->fields.tail)) {
      last_persisted = LOG_ENTRY_AT(META_HEADER_PERS->fields.tail - 1)->fields.ver;
    }

    FPL_PERS_UNLOCK;
    FPL_UNLOCK
    return last_persisted;
  }

  const void * FilePersistLog::getEntryByIndex (const int64_t &eidx)
    noexcept(false) {

    FPL_RDLOCK;
    dbg_trace("{0}-getEntryByIndex-head:{1},tail:{2},eidx:{3}",
      this->m_sName,META_HEADER->fields.head,META_HEADER->fields.tail,eidx);

    int64_t ridx = (eidx < 0)?(META_HEADER->fields.tail + eidx):eidx;

    if (META_HEADER->fields.tail <= ridx || ridx < META_HEADER->fields.head ) {
      FPL_UNLOCK;
      throw PERSIST_EXP_INV_ENTRY_IDX(eidx);
    }
    FPL_UNLOCK;

    dbg_trace("{0} getEntryByIndex at idx:{1} ver:{2}.{3} time:({4},{5})",
     this->m_sName,
       ridx,
       (int64_t)(LOG_ENTRY_AT(ridx)->fields.ver>>64),
       (int64_t)(LOG_ENTRY_AT(ridx)->fields.ver),
       (LOG_ENTRY_AT(ridx))->fields.hlc_r,
       (LOG_ENTRY_AT(ridx))->fields.hlc_l);

    return LOG_ENTRY_DATA(LOG_ENTRY_AT(ridx));
  }

  // binary search through the log
  /**
   * binary search through the log
   * [ ][ ][ ][ ][X][X][X][X][ ][ ][ ]
   *              ^logHead   ^logTail
   * @param keyGetter: function which get the key from LogEntry
   * @param key: the key to be search
   * @param logArr: log array
   * @param len: log length
   * @return index of the log entry found or -1 if not found.
   */
  template<typename TKey,typename KeyGetter>
  static int64_t binarySearch(const KeyGetter & keyGetter, const TKey & key,
    const int64_t & logHead, const int64_t & logTail) noexcept(false) {
    if (logTail <= logHead) {
      dbg_trace("binary Search failed...EMPTY LOG");
      return (int64_t)-1L;
    }
    int64_t head = logHead, tail = logTail - 1;
    int64_t pivot = 0;
    while (head <= tail) {
      pivot = (head + tail)/2;
      dbg_trace("Search range: {0}->[{1},{2}]",pivot,head,tail);
      const TKey p_key = keyGetter(pivot);
      if (p_key == key) {
        break; // found
      } else if (p_key < key) {
        if (pivot + 1 >= logTail) {
          break; // found - the last element
        } else if (keyGetter(pivot+1) > key) {
          break; // found - the next one is greater than key
        } else { // search right
          head = pivot + 1;
        }
      } else { // search left
        tail = pivot - 1;
        if (head > tail) {
          dbg_trace("binary Search failed...Object does not exist.");
          return (int64_t)-1L;
        }
      }
    }
    return pivot;
  }

  const void * FilePersistLog::getEntry(const __int128& ver)
  noexcept(false) {

    LogEntry * ple = nullptr;

    FPL_RDLOCK;

    //binary search
    int64_t head = META_HEADER->fields.head % MAX_LOG_ENTRY;
    int64_t tail = META_HEADER->fields.tail % MAX_LOG_ENTRY;
    if (tail < head) tail += MAX_LOG_ENTRY;
    dbg_trace("{0} - begin binary search.",this->m_sName);
    int64_t l_idx = binarySearch<__int128>(
      [&](int64_t idx){
        return LOG_ENTRY_AT(idx)->fields.ver;
      },
      ver,head,tail);
    ple = (l_idx == -1) ? nullptr : LOG_ENTRY_AT(l_idx);
    dbg_trace("{0} - end binary search.",this->m_sName);

    FPL_UNLOCK;

    // no object exists before the requested timestamp.
    if (ple == nullptr){
      return nullptr;
    }

    dbg_trace("{0} getEntry at ({1},{2})",this->m_sName,ple->fields.hlc_r,ple->fields.hlc_l);

    return LOG_ENTRY_DATA(ple);
  }

  const void * FilePersistLog::getEntry(const HLC &rhlc)
  noexcept(false) {

    LogEntry * ple = nullptr;
    unsigned __int128 key = ((((unsigned __int128)rhlc.m_rtc_us)<<64) | rhlc.m_logic);

    FPL_RDLOCK;

    //binary search
    int64_t head = META_HEADER->fields.head % MAX_LOG_ENTRY;
    int64_t tail = META_HEADER->fields.tail % MAX_LOG_ENTRY;
    if (tail < head) tail += MAX_LOG_ENTRY; //because we mapped it twice
    dbg_trace("{0} - begin binary search.",this->m_sName);
    int64_t l_idx = binarySearch<unsigned __int128>(
      [&](int64_t idx){
        return ((((unsigned __int128)LOG_ENTRY_AT(idx)->fields.hlc_r)<<64) | LOG_ENTRY_AT(idx)->fields.hlc_l);
      },
      key,head,tail);
    dbg_trace("{0} - end binary search.",this->m_sName);
    ple = (l_idx == -1) ? nullptr : LOG_ENTRY_AT(l_idx);
    FPL_UNLOCK;

    // no object exists before the requested timestamp.
    if (ple == nullptr){
      return nullptr;
    }

    dbg_trace("{0} getEntry at ({1},{2})",this->m_sName,ple->fields.hlc_r,ple->fields.hlc_l);

    return LOG_ENTRY_DATA(ple);
  }

  // trim by index
  void FilePersistLog::trim(const int64_t &idx) noexcept(false) {
    dbg_trace("{0} trim at index: {1}",this->m_sName,idx);
    FPL_RDLOCK;
    // validate check
    if (idx < META_HEADER->fields.head || idx >= META_HEADER->fields.tail){
      FPL_UNLOCK;
      return;
    }
    FPL_UNLOCK;

    FPL_WRLOCK;
    //validate check again
    if (idx < META_HEADER->fields.head || idx >= META_HEADER->fields.tail){
      FPL_UNLOCK;
      return;
    }
    META_HEADER->fields.head = idx + 1;
    FPL_UNLOCK;
    dbg_trace("{0} trim at index: {1}...done",this->m_sName,idx);
  }

  void FilePersistLog::trim(const __int128 &ver) noexcept(false) {
    dbg_trace("{0} trim at version: {1}.{2}",this->m_sName,(int64_t)(ver>>64),(int64_t)ver);
    this->trim<__int128>(ver,
      [&](int64_t idx){return LOG_ENTRY_AT(idx)->fields.ver;});
    dbg_trace("{0} trim at version: {1}.{2}...done",this->m_sName,(int64_t)(ver>>64),(int64_t)ver);
  }

  void FilePersistLog::trim(const HLC & hlc) noexcept(false) {
    dbg_trace("{0} trim at time: {1}.{2}",this->m_sName,hlc.m_rtc_us,hlc.m_logic);
    this->trim<unsigned __int128>(
      ((((const unsigned __int128)hlc.m_rtc_us)<<64) | hlc.m_logic),
      [&](int64_t idx) {
        return ((((const unsigned __int128)LOG_ENTRY_AT(idx)->fields.hlc_r)<<64) | 
          LOG_ENTRY_AT(idx)->fields.hlc_l);
      });
    dbg_trace("{0} trim at time: {1}.{2}...done",this->m_sName,hlc.m_rtc_us,hlc.m_logic);
  }

  void FilePersistLog::persistMetaHeaderAtomically() noexcept(false) {
    // STEP 1: get file name
    const string swpFile = this->m_sMetaFile + "." + SWAP_FILE_SUFFIX;
   
    // STEP 2: write current meta header to swap file
    int fd = open(swpFile.c_str(),O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
    if (fd == -1) {
      throw PERSIST_EXP_OPEN_FILE(errno);
    }
    ssize_t nWrite = write(fd,META_HEADER,sizeof(MetaHeader));
    if (nWrite != sizeof(MetaHeader)) {
      throw PERSIST_EXP_WRITE_FILE(errno);
    }
    close(fd);

    // STEP 3: atomically update the meta file 
    if (rename(swpFile.c_str(),this->m_sMetaFile.c_str()) != 0) {
      throw PERSIST_EXP_RENAME_FILE(errno);
    }

    // STEP 4: update the persisted header in memory
    *META_HEADER_PERS = *META_HEADER;
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
  bool checkOrCreateMetaFile(const string & metaFile)
  noexcept(false) {
    return checkOrCreateFileWithSize(metaFile,META_SIZE);
  }

  bool checkOrCreateLogFile(const string & logFile)
  noexcept(false) {
    return checkOrCreateFileWithSize(logFile,MAX_LOG_SIZE);
  }

  bool checkOrCreateDataFile(const string & dataFile)
  noexcept(false) {
    return checkOrCreateFileWithSize(dataFile,MAX_DATA_SIZE);
  }
}
