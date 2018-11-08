#include "conf.hpp"
#include <cstdlib>
#include <sys/stat.h>
#include <unistd.h>
#ifndef NDEBUG
#include <spdlog/sinks/stdout_color_sinks.h>
#endif//NDEBUG

namespace derecho {

static const char* default_conf_file = "derecho.cfg";

std::unique_ptr<Conf> Conf::singleton = nullptr;

std::atomic<uint32_t> Conf::singleton_initialized_flag = 0;
#define CONF_UNINITIALIZED (0)
#define CONF_INITIALIZING (1)
#define CONF_INITIALIZED (2)

#ifndef NDEBUG
inline auto dbgConsole() {
    static auto con = spdlog::stdout_color_mt("conf");
    return con;
}
#define dbg_trace(...) dbgConsole()->trace(__VA_ARGS__)
#define dbg_debug(...) dbgConsole()->debug(__VA_ARGS__)
#define dbg_info(...) dbgConsole()->info(__VA_ARGS__)
#define dbg_warn(...) dbgConsole()->warn(__VA_ARGS__)
#define dbg_error(...) dbgConsole()->error(__VA_ARGS__)
#define dbg_crit(...) dbgConsole()->critical(__VA_ARGS__)
#define dbg_flush() dbgConsole()->flush()
#else
#define dbg_trace(...)
#define dbg_debug(...)
#define dbg_info(...)
#define dbg_warn(...)
#define dbg_error(...)
#define dbg_crit(...)
#define dbg_flush()
#endif  //NDEBUG

void Conf::initialize(const char* conf_file) {
    uint32_t expected = CONF_UNINITIALIZED;
    // if not initialized(0), set the flag to under initialization ...
    if(Conf::singleton_initialized_flag.compare_exchange_strong(
               expected, CONF_INITIALIZING, std::memory_order_acq_rel)) {
        // 1 - get configuration file path
        std::string real_conf_file;
        struct stat buffer;
        if(conf_file)
            real_conf_file = conf_file;
        else if(std::getenv("DERECHO_CONF_FILE"))
            // try environment variable: DERECHO_CONF_FILE
            real_conf_file = std::getenv("DERECHO_CONF_FILE");
        else if(stat(default_conf_file, &buffer) == 0) {
            if(S_ISREG(buffer.st_mode) && (S_IRUSR | buffer.st_mode)) {
                real_conf_file = default_conf_file;
            }
        } else
            real_conf_file.clear();

        // 2 - load configuration
        GetPot* cfg = nullptr;
        if(!real_conf_file.empty()) {
            dbg_trace("load configuration from file:{0}.", real_conf_file);
            cfg = new GetPot(real_conf_file);
        } else
            dbg_trace("no configuration is found...load defaults.");
        Conf::singleton = std::make_unique<Conf>(cfg);
        delete cfg;

        // 3 - set the flag to initialized
        Conf::singleton_initialized_flag.store(CONF_INITIALIZED, std::memory_order_acq_rel);
    }
}

const Conf* Conf::get() noexcept {
    while(Conf::singleton_initialized_flag.load(std::memory_order_acquire) != CONF_INITIALIZED)
        Conf::initialize(nullptr);
    return Conf::singleton.get();
}

const std::string& getConfString(const std::string& key) {
    return Conf::get()->getString(key);
}

const int32_t getConfInt32(const std::string& key) {
    return Conf::get()->getInt32(key);
}

const int64_t getConfInt64(const std::string& key) {
    return Conf::get()->getInt64(key);
}

const float getConfFloat(const std::string& key) {
    return Conf::get()->getFloat(key);
}

const double getConfDouble(const std::string& key) {
    return Conf::get()->getDouble(key);
}
}
