#include "derecho/persistent/detail/logger.hpp"
#include "derecho/conf/conf.hpp"

#include <atomic>
#include <cstdint>
#include <memory>

namespace persistent {

constexpr uint32_t logger_uninitialized = 0;
constexpr uint32_t logger_initializing = 1;
constexpr uint32_t logger_initialized = 2;

std::atomic<uint32_t> PersistLogger::initialize_state = logger_uninitialized;
std::shared_ptr<spdlog::logger> PersistLogger::logger;

void PersistLogger::initialize() {
    uint32_t expected = logger_uninitialized;
    if(initialize_state.compare_exchange_strong(expected, logger_initializing, std::memory_order_acq_rel)) {
        logger = LoggerFactory::createLogger(LoggerFactory::PERSISTENT_LOGGER_NAME,
                                             derecho::getConfString(derecho::Conf::LOGGER_PERSISTENCE_LOG_LEVEL));
        initialize_state.store(logger_initialized, std::memory_order_acq_rel);
    }
    while(initialize_state.load(std::memory_order_acquire) != logger_initialized) {
    }
}

std::shared_ptr<spdlog::logger>& PersistLogger::get() {
    initialize();
    return logger;
}

}  //namespace persistent