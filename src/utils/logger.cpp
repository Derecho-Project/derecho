#include "derecho/utils/logger.hpp"

#include "derecho/conf/conf.hpp"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <atomic>
#include <memory>

#define LOGGER_FACTORY_UNINITIALIZED	(0)
#define LOGGER_FACTORY_INITIALIZING	(1)
#define LOGGER_FACTORY_INITIALIZED	(2)

const std::string LoggerFactory::PERSISTENT_LOGGER_NAME = "persistent";
const std::string LoggerFactory::SST_LOGGER_NAME = "derecho_sst";
const std::string LoggerFactory::RPC_LOGGER_NAME = "derecho_rpc";
std::atomic<uint32_t> LoggerFactory::_initialize_state = LOGGER_FACTORY_UNINITIALIZED;
std::shared_ptr<spdlog::details::thread_pool> LoggerFactory::_thread_pool_holder;
std::shared_ptr<spdlog::logger> LoggerFactory::_default_logger;
std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> LoggerFactory::_file_sink;
std::shared_ptr<spdlog::sinks::stdout_color_sink_mt> LoggerFactory::_stdout_sink;
std::mutex LoggerFactory::get_create_mutex;

std::shared_ptr<spdlog::logger> LoggerFactory::_create_logger(
    const std::string &logger_name,
    spdlog::level::level_enum log_level) {
    std::vector<spdlog::sink_ptr> log_sinks;
    log_sinks.push_back(_file_sink);
    if(derecho::getConfBoolean(CONF_LOGGER_LOG_TO_TERMINAL)) {
        log_sinks.push_back(_stdout_sink);
    }
    std::shared_ptr<spdlog::logger> log = std::make_shared<spdlog::async_logger>(
        logger_name,
        log_sinks.begin(),
        log_sinks.end(),
        _thread_pool_holder,
        spdlog::async_overflow_policy::block);
    log->set_pattern("[%H:%M:%S.%f] [%n] [Thread %t] [%^%l%$] %v");
    log->set_level(log_level);
    spdlog::register_logger(log);
    // Sanity check: Can I get the logger I just registered?
    assert(spdlog::get(logger_name));
    return log;
}

// re-entrant and idempotent initializer.
void LoggerFactory::_initialize() {
    // static initialization
    uint32_t expected = LOGGER_FACTORY_UNINITIALIZED;
    if (_initialize_state.compare_exchange_strong(
        expected,LOGGER_FACTORY_INITIALIZING,std::memory_order_acq_rel)){
        // 1 - initialize the thread pool
        spdlog::init_thread_pool(1L<<20, 1); // 1MB buffer, 1 thread
        _thread_pool_holder = spdlog::thread_pool();
        // 2 - initialize the sink objects that will be shared by all loggers
        std::string default_logger_name = derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_NAME);
        _file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            default_logger_name + ".log", 1L<<20, derecho::getConfUInt32(CONF_LOGGER_LOG_FILE_DEPTH));
        _stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        // 3 - initialize the default Logger
        std::string default_log_level = derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_LEVEL);
        _default_logger = _create_logger(default_logger_name,
            spdlog::level::from_str(default_log_level));
        // 3 - change state to initialized
        _initialize_state.store(LOGGER_FACTORY_INITIALIZED,std::memory_order_acq_rel);
        auto start_ms = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch());
        _default_logger->debug("Program start time (microseconds): {}", start_ms.count());
    }
    // make sure initialization finished by concurrent callers
    while (_initialize_state.load(std::memory_order_acquire)!=LOGGER_FACTORY_INITIALIZED) {
    }
}

std::shared_ptr<spdlog::logger> LoggerFactory::createLogger(
        const std::string& logger_name,
        spdlog::level::level_enum log_level) {
    _initialize();
    return _create_logger(logger_name, log_level);
}

std::shared_ptr<spdlog::logger> LoggerFactory::createLogger(
        const std::string& logger_name,
        const std::string& log_level_string) {
    return createLogger(logger_name, spdlog::level::from_str(log_level_string));
}

std::shared_ptr<spdlog::logger> LoggerFactory::createIfAbsent(
        const std::string& logger_name,
        const std::string& log_level_string) {
    _initialize();
    std::lock_guard get_check_lock(get_create_mutex);
    auto log_ptr = spdlog::get(logger_name);
    if(log_ptr) {
        return log_ptr;
    } else {
        return _create_logger(logger_name, spdlog::level::from_str(log_level_string));
    }
}

std::shared_ptr<spdlog::logger>& LoggerFactory::getDefaultLogger() {
    _initialize();
    return _default_logger;
}
