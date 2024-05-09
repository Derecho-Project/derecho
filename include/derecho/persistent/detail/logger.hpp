#pragma once

#include <derecho/config.h>
#include <derecho/utils/logger.hpp>

#include <memory>
#include <atomic>

namespace persistent {

/**
 * A static class that atomically initializes and holds a reference to the
 * Persistence module logger. Depends on LoggerFactory, and works similarly.
 * This is for debug logging, and has nothing to do with PersistLog.
 */
class PersistLogger {
    static std::shared_ptr<spdlog::logger> logger;
    static std::atomic<uint32_t> initialize_state;
    static void initialize();
public:
    static std::shared_ptr<spdlog::logger>& get();
};


}
