#include "derecho/utils/timestamp_logger.hpp"
#include "derecho/conf/conf.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace derecho {

constexpr uint32_t logger_uninitialized = 0;
constexpr uint32_t logger_initializing = 1;
constexpr uint32_t logger_initialized = 2;

std::unique_ptr<TimestampLogger> TimestampLogger::instance;

std::atomic<uint32_t> TimestampLogger::singleton_initialized_flag = logger_uninitialized;

TimestampLogger::TimestampLogger()
        : event_log(max_log_size),
          cur_index(0),
          my_node_id(getConfUInt32(Conf::DERECHO_LOCAL_ID)) {
    if(pthread_spin_init(&log_mutex, PTHREAD_PROCESS_PRIVATE) != 0) {
        // pthread_spin_init can, technically, fail if the system is out of memory
        throw std::runtime_error("pthread_spin_init failed!");
    }
}

TimestampLogger::~TimestampLogger() {
    pthread_spin_destroy(&log_mutex);
}

void TimestampLogger::instance_log(uint64_t event_tag, uint64_t message_id, int64_t version) {
    if(pthread_spin_lock(&log_mutex) != 0) {
        // There's only one error that can result in a nonzero return for pthread_spin_lock
        throw std::runtime_error("Deadlock detected in pthread_spin_lock");
    }
    event_log[cur_index] = {event_tag, message_id, version, std::chrono::system_clock::now()};
    // If we run out of log space, loop around and overwrite old entries instead of exploding
    cur_index = (cur_index + 1) % max_log_size;
    pthread_spin_unlock(&log_mutex);
}

void TimestampLogger::instance_dump(std::ostream& output_stream) {
    using namespace std::chrono;
    if(pthread_spin_lock(&log_mutex) != 0) {
        throw std::runtime_error("Deadlock detected in pthread_spin_lock");
    }
    for(std::size_t i = 0; i < cur_index; ++i) {
        output_stream << event_log[i].event_tag << " "
                      << duration_cast<nanoseconds>(event_log[i].time.time_since_epoch()).count() << " "
                      << my_node_id << " "
                      << event_log[i].message_id << " "
                      << event_log[i].version << std::endl;
    }
    pthread_spin_unlock(&log_mutex);
}

void TimestampLogger::initialize() {
    uint32_t expected_uninitialized = logger_uninitialized;
    if(!singleton_initialized_flag.compare_exchange_strong(expected_uninitialized, logger_initializing,
                                                           std::memory_order_acq_rel)) {
        if(!instance) {
            // make_unique doesn't work with private constructors
            instance = std::unique_ptr<TimestampLogger>(new TimestampLogger());
        }
        singleton_initialized_flag.store(logger_initialized, std::memory_order_acq_rel);
    }
    while(singleton_initialized_flag.load(std::memory_order_acquire) != logger_initialized) {
    }
}

void TimestampLogger::log(uint64_t event_tag, uint64_t message_id, int64_t version) {
    instance->log(event_tag, message_id, version);
}

void TimestampLogger::dump(std::ostream& output_stream) {
    instance->dump(output_stream);
}
}  //namespace derecho
