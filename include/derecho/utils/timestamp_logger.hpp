#pragma once

#include <derecho/config.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <vector>

namespace derecho {

struct TimestampEvent {
    uint64_t event_tag;
    uint64_t message_id;
    int64_t version;
    std::chrono::time_point<std::chrono::system_clock> time;
};

class TimestampLogger {
private:
    /**
     * Lock protecting access to the event log and current index. Implemented
     * as a spinlock because each thread's access to the log should be very brief,
     * and we don't want to incur a context switch when trying to record timing
     * information.
     */
    pthread_spinlock_t log_mutex;
    std::vector<TimestampEvent> event_log;
    std::size_t cur_index;
    /** The node ID of the local node. Read from the Conf system when TimestampLogger is initialized. */
    const uint32_t my_node_id;
    static std::unique_ptr<TimestampLogger> instance;
    static std::atomic<uint32_t> singleton_initialized_flag;
    void instance_log(uint64_t event_tag, uint64_t message_id, int64_t version);
    void instance_dump(std::ostream& output_stream);
    TimestampLogger();

public:
    ~TimestampLogger();
    /**
     * The maximum number of entries in the timestamp log; memory will
     * be reserved for this many entries when the singleton is initialized.
     */
    static constexpr uint64_t max_log_size = 1ull << 20;
    /**
     * Atomic, idempotent singleton initializer. Must be called at least once (at
     * system startup) to initialize the logger before calling log(). Note that
     * log() does not call initialize(), to avoid putting the is-initialized check
     * on the time-sensitive path, so log() will fail if it is called before the
     * logger is initialized.
     */
    static void initialize();
    /**
     * Records an event in the timestamp log with the current system time.
     * This method is thread-safe.
     *
     * @param event_tag The numeric tag identifying the event
     * @param message_id The message ID of the message that generated the event,
     * if known. Set to 0 if not known or not available.
     * @param version The persistent version related to the event, if known.
     */
    static void log(uint64_t event_tag, uint64_t message_id, int64_t version);
    /**
     * Dumps a string representation of the timestamp log to an output stream.
     * Each log entry will become one line of output with the following format:
     * <tag> <node ID> <message ID> <timestamp> <version>
     *
     * This should only be called after all timestamps are done being recorded,
     * since it holds a lock on the log for the duration of printing the output
     * (preventing any new timestamps from being logged).
     *
     * @param output_stream The output stream to print the log to (i.e. a file
     * output stream if saving the log to a file).
     */
    static void dump(std::ostream& output_stream);

    // Numeric constants for timestamp event tags

    static constexpr uint64_t MULTICAST_MESSAGE_RECEIVED = 3001;
    static constexpr uint64_t MULTICAST_VERSION_MESSAGE = 3002;
    // PersistenceManager events
    static constexpr uint64_t PERSISTENCE_MAKE_VERSION_BEGIN = 5003;
    static constexpr uint64_t PERSISTENCE_MAKE_VERSION_END = 5004;
    static constexpr uint64_t PERSISTENCE_REQUEST_POSTED = 5010;
    static constexpr uint64_t HANDLE_PERSIST_REQUEST_BEGIN = 5011;
    static constexpr uint64_t HANDLE_PERSIST_REQUEST_END = 5014;
    static constexpr uint64_t VERIFY_REQUEST_POSTED = 5020;
    static constexpr uint64_t HANDLE_VERIFY_REQUEST_BEGIN = 5021;
    static constexpr uint64_t HANDLE_VERIFY_REQUEST_END = 5024;
    // PersistentRegistry events
    static constexpr uint64_t REGISTRY_MAKE_VERSION_BEGIN = 6003;
    static constexpr uint64_t REGISTRY_MAKE_VERSION_END = 6004;
    static constexpr uint64_t REGISTRY_SIGN_BEGIN = 6012;
    static constexpr uint64_t REGISTRY_SIGN_END = 6013;
    static constexpr uint64_t REGISTRY_UPDATE_SIGNATURE_BEGIN = 6112;
    static constexpr uint64_t REGISTRY_UPDATE_SIGNATURE_END = 6113;
    static constexpr uint64_t REGISTRY_FINALIZE_SIGNATURE_BEGIN = 6114;
    static constexpr uint64_t REGISTRY_FINALIZE_SIGNATURE_END = 6115;
    static constexpr uint64_t REGISTRY_ADD_SIGNATURE_BEGIN = 6116;
    static constexpr uint64_t REGISTRY_ADD_SIGNATURE_END = 6117;
    static constexpr uint64_t REGISTRY_PERSIST_BEGIN = 6014;
    static constexpr uint64_t REGISTRY_PERSIST_END = 6015;
    // Persistent<T> events
    static constexpr uint64_t PERSISTENT_MAKE_VERSION_BEGIN = 7003;
    static constexpr uint64_t PERSISTENT_MAKE_VERSION_END = 7004;
    static constexpr uint64_t PERSISTENT_UPDATE_SIGNATURE_BEGIN = 7012;
    static constexpr uint64_t PERSISTENT_UPDATE_SIGNATURE_END = 7013;
    static constexpr uint64_t PERSISTENT_PERSIST_BEGIN = 7014;
    static constexpr uint64_t PERSISTENT_PERSIST_END = 7015;
};

// Macro wrappers for the static functions so they can be completely disabled with a #define

#ifdef ENABLE_TIMESTAMPS

#define TIMESTAMP_LOG(...) derecho::TimestampLogger::log(__VA_ARGS__)
#define TIMESTAMP_INIT() derecho::TimestampLogger::initialize()

#else

#define TIMESTAMP_LOG(...)
#define TIMESTAMP_INIT()

#endif

}  //namespace derecho
