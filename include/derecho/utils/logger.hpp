#pragma once
#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <derecho/config.h>
#include "container_ostreams.hpp"

#include <string>
#include <atomic>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/fmt/std.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#ifndef NDEBUG
#undef NOLOG
#endif
/**
 * @brief Logger factory class
 */
class LoggerFactory {
private:
    static std::atomic<uint32_t> _initialize_state;
    static std::shared_ptr<spdlog::details::thread_pool> _thread_pool_holder;
    static std::shared_ptr<spdlog::logger> _default_logger;
    static std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> _file_sink;
    static std::shared_ptr<spdlog::sinks::stdout_color_sink_mt> _stdout_sink;
    // mutex for the createIfAbsent method
    static std::mutex get_create_mutex;
    static void _initialize();
    static std::shared_ptr<spdlog::logger> _create_logger(
            const std::string& logger_name,
            spdlog::level::level_enum log_level);

public:
    /** create a logger
     * @param logger_name
     *     Name of the logger. This name can be used in later calls to spdlog::get().
     * @param log_level
     *     The level of the logger.
     * @return
     *     The created logger.
     */
    static std::shared_ptr<spdlog::logger> createLogger(
            const std::string& logger_name,
            spdlog::level::level_enum log_level = spdlog::level::info);
    /**
     * Create a logger using a string instead of a level_enum
     *
     * @param logger_name Name of the logger. This name can be used in later calls to spdlog::get().
     * @param log_level_string String representation of the log level, which will be parsed by
     * spdlog::level::from_str().
     * @return The created logger.
     */
    static std::shared_ptr<spdlog::logger> createLogger(
            const std::string& logger_name,
            const std::string& log_level_string);
    /**
     * Thread-safe method that creates a logger only if it is not already present
     * in spdlog's global registry. This uses a mutex to make a call to spdlog::get()
     * atomic with a call to createLogger().
     *
     * @param logger_name Name of the logger. This name can be used in later calls to spdlog::get().
     * @param log_level_string String representation of the log level, which will be parsed by
     * spdlog::level::from_str().
     * @return The created logger, or a pointer to the existing logger if it exists.
     */
    static std::shared_ptr<spdlog::logger> createIfAbsent(
            const std::string& logger_name,
            const std::string& log_level_string);
    /** get the default logger */
    static std::shared_ptr<spdlog::logger>& getDefaultLogger();

    //String constants for the names of sub-module loggers
    static const std::string PERSISTENT_LOGGER_NAME;
    static const std::string SST_LOGGER_NAME;
    static const std::string RPC_LOGGER_NAME;
    static const std::string VIEWMANAGER_LOGGER_NAME;
};

#ifndef NOLOG
  // Heavy logging version
  #define dbg_trace(logger, ...) logger->trace(__VA_ARGS__)
  #define dbg_default_trace(...) dbg_trace(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_debug(logger, ...) logger->debug(__VA_ARGS__)
  #define dbg_default_debug(...) dbg_debug(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_info(logger, ...) logger->info(__VA_ARGS__)
  #define dbg_default_info(...) dbg_info(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_warn(logger, ...) logger->warn(__VA_ARGS__)
  #define dbg_default_warn(...) dbg_warn(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_error(logger, ...) logger->error(__VA_ARGS__)
  #define dbg_default_error(...) dbg_error(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_crit(logger, ...) logger->critical(__VA_ARGS__)
  #define dbg_default_crit(...) dbg_crit(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
  #define dbg_flush(logger) logger->flush()
  #define dbg_default_flush() LoggerFactory::getDefaultLogger()->flush()
#else
  // Log-disabled version
  #define dbg_trace(logger, ...)
  #define dbg_default_trace(...)
  #define dbg_debug(logger, ...)
  #define dbg_default_debug(...)
  #define dbg_info(logger, ...)
  #define dbg_default_info(...)
  #define dbg_warn(logger, ...)
  #define dbg_default_warn(...)
  #define dbg_error(logger, ...)
  #define dbg_default_error(...)
  #define dbg_crit(logger, ...)
  #define dbg_default_crit(...)
  #define dbg_flush(logger)
  #define dbg_default_flush()
#endif

// Log-in-release macros. These will not be compiled out in benchmark mode, so use carefully.
#define rls_info(logger, ...) logger->info(__VA_ARGS__)
#define rls_default_info(...) rls_info(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
#define rls_warn(logger, ...) logger->warn(__VA_ARGS__)
#define rls_default_warn( ... ) rls_warn(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
#define rls_error(logger, ...) logger->error(__VA_ARGS__)
#define rls_default_error( ... ) rls_error(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
#define rls_crit(logger, ...) logger->critical(__VA_ARGS__)
#define rls_default_crit( ... ) rls_crit(LoggerFactory::getDefaultLogger(), __VA_ARGS__)
#define rls_flush(logger) logger->flush()
#define rls_default_flush() LoggerFactory::getDefaultLogger()->flush()

#endif//LOGGER_HPP
