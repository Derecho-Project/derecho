#include <derecho/conf/conf.hpp>
#include <cstdlib>
#include <sys/stat.h>
#include <unistd.h>
#ifndef NDEBUG
#include <spdlog/sinks/stdout_color_sinks.h>
#endif  //NDEBUG

namespace derecho {

static const char* default_conf_file = "derecho.cfg";

const std::vector<std::string> Conf::subgroupProfileFields = {
        "max_payload_size",
	"max_reply_payload_size",
        "max_smc_payload_size",
        "block_size",
        "window_size",
        "rdmc_send_algorithm"
};

std::unique_ptr<Conf> Conf::singleton = nullptr;

std::atomic<uint32_t> Conf::singleton_initialized_flag = 0;
#define CONF_UNINITIALIZED (0)
#define CONF_INITIALIZING (1)
#define CONF_INITIALIZED (2)

#define MAKE_LONG_OPT_ENTRY(x) \
    { x, required_argument, 0, 0 }
struct option Conf::long_options[] = {
        // [DERECHO]
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_LEADER_IP),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_LEADER_GMS_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_LEADER_EXTERNAL_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_LOCAL_ID),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_LOCAL_IP),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_GMS_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_RPC_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_SST_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_RDMC_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_EXTERNAL_PORT),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_HEARTBEAT_MS),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_SST_POLL_CQ_TIMEOUT_MS),
        MAKE_LONG_OPT_ENTRY(CONF_DERECHO_DISABLE_PARTITIONING_SAFETY),
	MAKE_LONG_OPT_ENTRY(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE),
	MAKE_LONG_OPT_ENTRY(CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE),
	MAKE_LONG_OPT_ENTRY(CONF_DERECHO_P2P_WINDOW_SIZE),
        // [SUBGROUP/<subgroup name>]
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_RDMC_SEND_ALGORITHM),
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE),
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_MAX_REPLY_PAYLOAD_SIZE),
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE),
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_BLOCK_SIZE),
        MAKE_LONG_OPT_ENTRY(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE),
        // [RDMA]
        MAKE_LONG_OPT_ENTRY(CONF_RDMA_PROVIDER),
        MAKE_LONG_OPT_ENTRY(CONF_RDMA_DOMAIN),
        MAKE_LONG_OPT_ENTRY(CONF_RDMA_TX_DEPTH),
        MAKE_LONG_OPT_ENTRY(CONF_RDMA_RX_DEPTH),
        // [PERS]
        MAKE_LONG_OPT_ENTRY(CONF_PERS_FILE_PATH),
        MAKE_LONG_OPT_ENTRY(CONF_PERS_RAMDISK_PATH),
        MAKE_LONG_OPT_ENTRY(CONF_PERS_RESET),
        MAKE_LONG_OPT_ENTRY(CONF_PERS_MAX_LOG_ENTRY),
        MAKE_LONG_OPT_ENTRY(CONF_PERS_MAX_DATA_SIZE),
        {0, 0, 0, 0}};

void Conf::initialize(int argc, char* argv[], const char* conf_file) {
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
            cfg = new GetPot(real_conf_file);
        }
        Conf::singleton = std::make_unique<Conf>(argc, argv, cfg);
        delete cfg;

        // 3 - set the flag to initialized
        Conf::singleton_initialized_flag.store(CONF_INITIALIZED, std::memory_order_acq_rel);
    }
}

// should we force the user to call Conf::initialize() by throw an expcetion
// for uninitialized configuration?
const Conf* Conf::get() noexcept {
    while(Conf::singleton_initialized_flag.load(std::memory_order_acquire) != CONF_INITIALIZED) {
        char *empty_arg[1] = {nullptr};
        Conf::initialize(0, empty_arg, nullptr);
    }
    return Conf::singleton.get();
}

const std::string& getConfString(const std::string& key) {
    return Conf::get()->getString(key);
}

const int32_t getConfInt32(const std::string& key) {
    return Conf::get()->getInt32(key);
}

const uint32_t getConfUInt32(const std::string& key) {
    return Conf::get()->getUInt32(key);
}

const int16_t getConfInt16(const std::string& key) {
    return Conf::get()->getInt16(key);
}

const uint16_t getConfUInt16(const std::string& key) {
    return Conf::get()->getUInt16(key);
}

const int64_t getConfInt64(const std::string& key) {
    return Conf::get()->getInt64(key);
}

const uint64_t getConfUInt64(const std::string& key) {
    return Conf::get()->getUInt64(key);
}

const float getConfFloat(const std::string& key) {
    return Conf::get()->getFloat(key);
}

const double getConfDouble(const std::string& key) {
    return Conf::get()->getDouble(key);
}

const bool getConfBoolean(const std::string& key) {
    return Conf::get()->getBoolean(key);
}

const bool hasCustomizedConfKey(const std::string& key) {
    return Conf::get()->hasCustomizedKey(key);
}
}
