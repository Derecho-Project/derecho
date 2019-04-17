#ifndef SPDK_PERSIST_LOG_HPP
#define SPDK_PERSIST_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error SPDKPersistLog.hpp only works with clang and gnu compilers
#endif

#include "PersistLog.hpp"
#include <bitset>
#include <unordered_map>

namespace persistent {

namespace spdk {

/**
 * Global Metadata on NVMe
 * +-------------------+     
 * |  log meta data 0  |
 * +-------------------+
 * |  log meta data 1  |
 * +-------------------+
 * |  log meta data 2  |
 * +-------------------+
 * |         *         |
 * |         *         |
 * |         *         |
 * +-------------------+
 * |  log meta data N  | <-- N = SPDK_NUM_LOGS_SUPPORTED
 * +-------------------+
 * |                   |
 * 
 */



// TODO hardcoded sizes for now
#define SPDK_NUM_LOGS_SUPPORTED (1UL<<10)  // support 1024 logs
#define SPDK_SEGMENT_SIZE (1ULL<<23)       // segment size is 8 MB
#define SPDK_LOG_ADDRESS_SPACE (1ULL<<40)  // address space per log is 1TB
#define SPDK_NUM_SEGMENTS \
        ((SPDK_LOG_ADDRESS_SPACE/SPDK_NUM_LOGS_SUPPORTED) - \
        256) // maximum number of segments in one log = (128 K - 256),
             // the space for 256 index (or 1KB) is reserved for the other
             // metadata
#define SPDK_LOG_METADATA_SIZE \
        ((SPDK_LOG_ADDRESS_SPACE/SPDK_NUM_LOGS_SUPPORTED)*sizeof(uint32_t))
        // SPDK_LOG_METADATA_SIZE = 512 KB

// per log metadata
typedef union log_metadata {
    struct {
        uint8_t name[256]; // name of the log
        uint32_t id;       // log index
        uint32_t segment_table[SPDK_NUM_SEGMENTS]; // (128 K entries) x 4 Bytes/entry = 512KB
        int64_t head;  // head index
        int64_t tail;  // tail index
        int64_t ver;   // latest version number
    } fields;
    uint8_t bytes[SPDK_LOG_METADATA_SIZE];
    // bool operator
    bool operator==(const union log_metadata& other) {
        return (this->fields.head == other.fields.head) && (this->fields.tail == other.fields.tail) && (this->fields.ver == other.fields.ver);
    }
} LogMetadata;

// global metadata include segment management information. It exists both in
// NVMe and memory.
// - GlobalMetaDataPers is the data struct in NVMe.
typedef struct global_metadata_pers {
    LogMetadata log_metadata_entries[SPDK_NUM_LOGS_SUPPORTED];
} GlobalMetadataPers;
// SPDK_NUM_RESERVED_SEGMENTS are the number of segments reserved for the
// global metadata at the beginning of SPDK address space.
#define SPDK_NUM_RESERVED_SEGMENTS (((sizeof(GlobalMetadataPers)-1)/SPDK_SEGMENT_SIZE) + 1)
// - GlobalMetaData is the data structure in memory, not necessary to be
//   aligned with segments. This data needs to be constructed from the NVMe
//   contents during initialization.
typedef union global_metadata {
    std::unordered_map<std::string, uint32_t> log_name_to_id;
    LogMetadata logs[SPDK_NUM_LOGS_SUPPORTED];
    std::bitset<SPDK_NUM_SEGMENTS> segments;
} GlobalMetadata;

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

// Persistent log interfaces
class SPDKPersistLog : public PersistLog {
protected:
    // log metadata
    LogMetadata m_currLogMetadata;

public:
    // Constructor:
    // Remark: the constructor will check the persistent storage
    // to make sure if this named log(by "name" in the template
    // parameters) is already there. If it is, load it from disk.
    // Otherwise, create the log.
    SPDKPersistLog(const std::string& name) noexcept(true) : PersistLog(name){};
    // Destructor
    virtual ~SPDKPersistLog() noexcept(true);
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
    virtual void append(const void* pdata,
                        const uint64_t& size, const version_t& ver,
                        const HLC& mhlc) noexcept(false);

    /**
     * Advance the version number without appendding a log. This is useful
     * to create gap between versions.
     */
    // virtual void advanceVersion(const __int128 & ver) noexcept(false) = 0;
    virtual void advanceVersion(const version_t& ver) noexcept(false);

    // Get the length of the log
    virtual int64_t getLength() noexcept(false);

    // Get the Earliest Index
    virtual int64_t getEarliestIndex() noexcept(false);

    // Get the Latest Index
    virtual int64_t getLatestIndex() noexcept(false);

    // Get the Index corresponding to a version
    virtual int64_t getVersionIndex(const version_t& ver);

    // Get the Earlist version
    virtual version_t getEarliestVersion() noexcept(false);

    // Get the Latest version
    virtual version_t getLatestVersion() noexcept(false);

    // return the last persisted value
    virtual const version_t getLastPersisted() noexcept(false);

    // Get a version by entry number return both length and buffer
    virtual const void* getEntryByIndex(const int64_t& eno) noexcept(false);

    // Get the latest version equal or earlier than ver.
    virtual const void* getEntry(const version_t& ver) noexcept(false);

    // Get the latest version - deprecated.
    // virtual const void* getEntry() noexcept(false) = 0;
    // Get a version specified by hlc
    virtual const void* getEntry(const HLC& hlc) noexcept(false);

    /**
     * Persist the log till specified version
     * @return - the version till which has been persisted.
     * Note that the return value could be higher than the the version asked
     * is lower than the log that has been actually persisted.
     */
    virtual const version_t persist(const bool preLocked = false) noexcept(false);

    /**
     * Trim the log till entry number eno, inclusively.
     * For exmaple, there is a log: [7,8,9,4,5,6]. After trim(3), it becomes [5,6]
     * @param eno -  the log number to be trimmed
     */
    virtual void trimByIndex(const int64_t& idx) noexcept(false);

    /**
     * Trim the log till version, inclusively.
     * @param ver - all log entry before ver will be trimmed.
     */
    virtual void trim(const version_t& ver) noexcept(false);

    /**
     * Trim the log till HLC clock, inclusively.
     * @param hlc - all log entry before hlc will be trimmed.
     */
    virtual void trim(const HLC& hlc) noexcept(false);

    /**
     * Calculate the byte size required for serialization
     * @PARAM ver - from which version the detal begins(tail log) 
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t bytes_size(const version_t& ver);

    /**
     * Write the serialized log bytes to the given buffer
     * @PARAM buf - the buffer to receive serialized bytes
     * @PARAM ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual size_t to_bytes(char* buf, const version_t& ver);

    /**
     * Post the serialized log bytes to a function
     * @PARAM f - the function to handle the serialzied bytes
     * @PARAM ver - from which version the detal begins(tail log)
     *   INVALID_VERSION means to include all of the tail logs
     */
    virtual void post_object(const std::function<void(char const* const, std::size_t)>& f,
                             const version_t& ver);

    /**
     * Check/Merge the LogTail to the existing log.
     * @PARAM dsm - deserialization manager
     * @PARAM v - serialized log bytes to be apllied
     */
    virtual void applyLogTail(char const* v);

    /**
     * Truncate the log strictly newer than 'ver'.
     * @param ver - all log entry strict after ver will be truncated.
     */
    virtual void truncate(const version_t& ver) noexcept(false);
};

}  // namespace spdk
}  // namespace persistent
#endif  //SPDK_PERSIST_LOG_HPP
