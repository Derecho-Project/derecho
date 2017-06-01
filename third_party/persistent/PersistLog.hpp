#ifndef PERSIST_LOG_HPP
#define	PERSIST_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
  #error PersistLog.hpp only works with clang and gnu compilers
#endif

#include <stdio.h>
#include <inttypes.h>
#include <map>
#include <string>
#include "PersistException.hpp"
#include "HLC.hpp"

using namespace std;

namespace ns_persistent {

  // Storage type:
  enum StorageType{
    ST_FILE=0,
    ST_MEM,
    ST_3DXP
  };

  #define INVALID_VERSION ((__int128)-1L)
  #define INVALID_INDEX INT64_MAX

  // Persistent log interfaces
  class PersistLog{
  protected:
    // LogName
    const string m_sName;
  public:
    // Constructor:
    // Remark: the constructor will check the persistent storage
    // to make sure if this named log(by "name" in the template 
    // parameters) is already there. If it is, load it from disk.
    // Otherwise, create the log.
    PersistLog(const string &name) noexcept(true);
    virtual ~PersistLog() noexcept(true);
    /** Persistent Append
     * @param pdata - serialized data to be append
     * @param size - length of the data
     * @param ver - version of the data, the implementation is responsible for
     *              making sure it grows monotonically.
     * @param mhlc - the hlc clock of the data, the implementation is 
     *               responsible for making sure it grows monotonically.
     * Note that the entry appended can only become persistent till the persist()
     * is called on that entry. 
     */
    virtual void append(const void * pdata, 
      const uint64_t & size, const __int128 & ver, 
      const HLC & mhlc) noexcept(false) = 0;

    // Get the length of the log 
    virtual int64_t getLength() noexcept(false) = 0;

    // Get the Earliest Index
    virtual int64_t getEarliestIndex() noexcept(false) = 0;

    // Get a version by entry number
    virtual const void* getEntryByIndex(const int64_t & eno) noexcept(false) = 0;

    // Get the latest version equal or earlier than ver.
    virtual const void* getEntry(const __int128 & ver) noexcept(false) = 0;

    // Get the latest version - deprecated.
    // virtual const void* getEntry() noexcept(false) = 0;
    // Get a version specified by hlc
    virtual const void* getEntry(const HLC & hlc) noexcept(false) = 0;

    /**
     * Persist the log till specified version
     * @return - the version till which has been persisted.
     * Note that the return value could be higher than the the version asked
     * is lower than the log that has been actually persisted.
     */
    virtual const __int128 persist() noexcept(false) = 0;

    /**
     * Trim the log till entry number eno, inclusively.
     * For exmaple, there is a log: [7,8,9,4,5,6]. After trim(3), it becomes [5,6]
     * @param eno -  the log number to be trimmed
     */
    virtual void trim(const int64_t &idx) noexcept(false) = 0;

    /**
     * Trim the log till version, inclusively.
     * @param ver - all log entry before ver will be trimmed.
     */
    virtual void trim(const __int128 &ver) noexcept(false) = 0;

    /**
     * Trim the log till HLC clock, inclusively.
     * @param hlc - all log entry before hlc will be trimmed.
     */
    virtual void trim(const HLC & hlc) noexcept(false) = 0;
  };
}

#endif//PERSIST_LOG_HPP
