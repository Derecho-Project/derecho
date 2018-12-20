#include <atomic>
#include "spdlog/async.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "logger.hpp"
#include "conf/conf.hpp"

#define LOGGER_FACTORY_UNINITIALIZED	(0)
#define LOGGER_FACTORY_INITIALIZING	(1)
#define LOGGER_FACTORY_INITIALIZED	(2)

std::atomic<uint32_t> LoggerFactory::_initialize_state = LOGGER_FACTORY_UNINITIALIZED;
std::shared_ptr<spdlog::logger> LoggerFactory::_default_logger;

std::shared_ptr<spdlog::logger> LoggerFactory::_create_logger(
    const std::string &logger_name,
    spdlog::level::level_enum log_level) {
    std::vector<spdlog::sink_ptr> log_sinks;
    log_sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        logger_name + ".log",1L<<20, 3));
    log_sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
    std::shared_ptr<spdlog::logger> log = std::make_shared<spdlog::async_logger>(
        logger_name,
        log_sinks.begin(),
        log_sinks.end(),
        spdlog::thread_pool(),
        spdlog::async_overflow_policy::block);
    spdlog::register_logger(log);
    log->set_pattern("[%H:%M:%S.%f] [%n] [Thread %t] [%^%l%$] %v");
    log->set_level(log_level);
    return log;
}

void LoggerFactory::_initialize() {
    // static initialization
    uint32_t expected = LOGGER_FACTORY_UNINITIALIZED;
    if (_initialize_state.compare_exchange_strong(
        expected,LOGGER_FACTORY_INITIALIZING,std::memory_order_acq_rel)){
        // 1 - initialize the thread pool
        spdlog::init_thread_pool(1L<<20, 1); // 1MB buffer, 1 thread
        // 2 - initialize the default Logger
        std::string default_logger_name = derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_NAME);
        std::string default_log_level = derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_LEVEL);
        _default_logger = _create_logger(default_logger_name,
            spdlog::level::from_str(default_log_level));
        // 3 - change state to initialized
        _initialize_state.store(LOGGER_FACTORY_INITIALIZED,std::memory_order_acq_rel);
    }
    // make sure initialization finished by concurrent callers
    while (_initialize_state.load(std::memory_order_acquire)!=LOGGER_FACTORY_INITIALIZED) {
    }
    auto start_ms = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch());
    _default_logger->debug("Program start time (microseconds): {}", start_ms.count());
}

std::shared_ptr<spdlog::logger> LoggerFactory::createLogger(
    const std::string &logger_name,
    spdlog::level::level_enum log_level) {
    return _create_logger(logger_name,log_level);
}

std::shared_ptr<spdlog::logger>& LoggerFactory::getDefaultLogger() {
    return _default_logger;
}
