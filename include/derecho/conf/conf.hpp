#ifndef CONF_HPP
#define CONF_HPP

#include <atomic>
#include "getpot/GetPot"
#include <getopt.h>
#include <inttypes.h>
#include <map>
#include <memory>
#include <stdio.h>
#include <unistd.h>

namespace derecho {

#define CONF_ENTRY_INTEGER(name, section, string)

/** The single configuration file for derecho **/
class Conf {
private:
    // Configuration Table:
    // config name --> default value
#define CONF_DERECHO_LEADER_IP "DERECHO/leader_ip"
#define CONF_DERECHO_LEADER_GMS_PORT "DERECHO/leader_gms_port"
#define CONF_DERECHO_LOCAL_ID "DERECHO/local_id"
#define CONF_DERECHO_LOCAL_IP "DERECHO/local_ip"
#define CONF_DERECHO_GMS_PORT "DERECHO/gms_port"
#define CONF_DERECHO_RPC_PORT "DERECHO/rpc_port"
#define CONF_DERECHO_SST_PORT "DERECHO/sst_port"
#define CONF_DERECHO_RDMC_PORT "DERECHO/rdmc_port"
#define CONF_DERECHO_HEARTBEAT_MS "DERECHO/heartbeat_ms"
#define CONF_DERECHO_SST_POLL_CQ_TIMEOUT_MS "DERECHO/sst_poll_cq_timeout_ms"
#define CONF_DERECHO_DISABLE_PARTITIONING_SAFETY "DERECHO/disable_partitioning_safety"

#define CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE "DERECHO/max_p2p_request_payload_size"
#define CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE "DERECHO/max_p2p_reply_payload_size"
#define CONF_DERECHO_P2P_WINDOW_SIZE "DERECHO/p2p_window_size"

#define CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE "SUBGROUP/DEFAULT/max_payload_size"
#define CONF_SUBGROUP_DEFAULT_MAX_REPLY_PAYLOAD_SIZE "SUBGROUP/DEFAULT/max_reply_payload_size"
#define CONF_SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE "SUBGROUP/DEFAULT/max_smc_payload_size"
#define CONF_SUBGROUP_DEFAULT_BLOCK_SIZE "SUBGROUP/DEFAULT/block_size"
#define CONF_SUBGROUP_DEFAULT_WINDOW_SIZE "SUBGROUP/DEFAULT/window_size"
#define CONF_SUBGROUP_DEFAULT_RDMC_SEND_ALGORITHM "SUBGROUP/DEFAULT/rdmc_send_algorithm"

#define CONF_RDMA_PROVIDER "RDMA/provider"
#define CONF_RDMA_DOMAIN "RDMA/domain"
#define CONF_RDMA_TX_DEPTH "RDMA/tx_depth"
#define CONF_RDMA_RX_DEPTH "RDMA/rx_depth"
#define CONF_PERS_FILE_PATH "PERS/file_path"
#define CONF_PERS_RAMDISK_PATH "PERS/ramdisk_path"
#define CONF_PERS_RESET "PERS/reset"
#define CONF_PERS_MAX_LOG_ENTRY "PERS/max_log_entry"
#define CONF_PERS_MAX_DATA_SIZE "PERS/max_data_size"
#define CONF_LOGGER_DEFAULT_LOG_NAME "LOGGER/default_log_name"
#define CONF_LOGGER_DEFAULT_LOG_LEVEL "LOGGER/default_log_level"

    std::map<const std::string, std::string> config = {
            // [DERECHO]
            {CONF_DERECHO_LEADER_IP, "127.0.0.1"},
            {CONF_DERECHO_LEADER_GMS_PORT, "23580"},
            {CONF_DERECHO_LOCAL_ID, "0"},
            {CONF_DERECHO_LOCAL_IP, "127.0.0.1"},
            {CONF_DERECHO_GMS_PORT, "23580"},
            {CONF_DERECHO_RPC_PORT, "28366"},
            {CONF_DERECHO_SST_PORT, "37683"},
            {CONF_DERECHO_RDMC_PORT, "31675"},
            {CONF_SUBGROUP_DEFAULT_RDMC_SEND_ALGORITHM, "binomial_send"},
            {CONF_DERECHO_SST_POLL_CQ_TIMEOUT_MS, "2000"},
            {CONF_DERECHO_DISABLE_PARTITIONING_SAFETY, "true"},
	    {CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE, "10240"},
	    {CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE, "10240"},
	    {CONF_DERECHO_P2P_WINDOW_SIZE, "16"},
            // [SUBGROUP/<subgroupname>]
            {CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE, "10240"},
            {CONF_SUBGROUP_DEFAULT_MAX_REPLY_PAYLOAD_SIZE, "10240"},
            {CONF_SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE, "10240"},
            {CONF_SUBGROUP_DEFAULT_BLOCK_SIZE, "1048576"},
            {CONF_SUBGROUP_DEFAULT_WINDOW_SIZE, "16"},
            {CONF_DERECHO_HEARTBEAT_MS, "1"},
            // [RDMA]
            {CONF_RDMA_PROVIDER, "sockets"},
            {CONF_RDMA_DOMAIN, "eth0"},
            {CONF_RDMA_TX_DEPTH, "256"},
            {CONF_RDMA_RX_DEPTH, "256"},
            // [PERS]
            {CONF_PERS_FILE_PATH, ".plog"},
            {CONF_PERS_RAMDISK_PATH, "/dev/shm/volatile_t"},
            {CONF_PERS_RESET, "false"},
            {CONF_PERS_MAX_LOG_ENTRY, "1048576"}, // 1M log entries.
            {CONF_PERS_MAX_DATA_SIZE, "549755813888"}, // 512G total data size.
            // [LOGGER]
            {CONF_LOGGER_DEFAULT_LOG_NAME, "derecho_debug"},
            {CONF_LOGGER_DEFAULT_LOG_LEVEL, "info"}};

public:
    // the option for parsing command line with getopt(not GetPot!!!)
    static struct option long_options[];

public:
    /** Constructor:
   *  Conf can read configure from multiple sources
   *  - the command line argument has the highest priority, then,
   *  - the configuration files
   *  - the default values.
   **/
    Conf(int argc, char* argv[], GetPot* getpotcfg = nullptr) noexcept {
        // 1 - load configuration from configuration file
        if(getpotcfg != nullptr) {
            for(const std::string& key : getpotcfg->get_variable_names()) {
                this->config[key] = (*getpotcfg)(key, "");
            }
        }
        // 2 - load configuration from the command line
        int c;
        while(1) {
            int option_index = 0;

            c = getopt_long(argc, argv, "", long_options, &option_index);
            if(c == -1) {
                break;
            }

            switch(c) {
                case 0:
                    this->config[long_options[option_index].name] = optarg;
                    break;

                case '?':
                    break;

                default:
                    std::cerr << "ignore unknown commandline code:" << c << std::endl;
            }
        }
    }
    /** get configuration **/
    const std::string& getString(const std::string& key) const {
        return this->config.at(key);
    }
    const int16_t getInt16(const std::string& key) const {
        return (const int16_t)std::stoi(this->config.at(key));
    }
    const uint16_t getUInt16(const std::string& key) const {
        return (const uint16_t)std::stoi(this->config.at(key));
    }
    const int32_t getInt32(const std::string& key) const {
        return (const int32_t)std::stoi(this->config.at(key));
    }
    const uint32_t getUInt32(const std::string& key) const {
        return (const uint32_t)std::stoi(this->config.at(key));
    }
    const int64_t getInt64(const std::string& key) const {
        return (const int64_t)std::stoll(this->config.at(key));
    }
    const uint64_t getUInt64(const std::string& key) const {
        return (const uint64_t)std::stoll(this->config.at(key));
    }
    const float getFloat(const std::string& key) const {
        return (const float)std::stof(this->config.at(key));
    }
    const double getDouble(const std::string& key) const {
        return (const float)std::stod(this->config.at(key));
    }
    const bool getBoolean(const std::string& key) const {
        return (this->config.at(key) == "true") || (this->config.at(key) == "yes") || (this->config.at(key) == "1");
    }
    // Please check if a customized key exists, otherwise the getXXX() function
    // will throw an exception on unknown keys.
    const bool hasCustomizedKey(const std::string& key) const {
        return (this->config.find(key) != this->config.end());
    }
    // Initialize the singleton from the command line and the configuration file.
    // The command line has higher priority than the configuration file
    // The process we find the configuration file:
    // 1) if conf_file is not null, use it, otherwise,
    // 2) try DERECHO_CONF_FILE environment, otherwise,
    // 3) use "derecho.cfg" at local, otherwise,
    // 4) use all default settings.
    // Note: initialize will be called on the first 'get()' if it is not called
    // before.
    static void initialize(int argc, char* argv[],
                           const char* conf_file = nullptr);
    static const Conf* get() noexcept;

    // Defines fields used for loading subgroup profiles in multicast_group.h
    static const std::vector<std::string> subgroupProfileFields;

private:
    // singleton
    static std::unique_ptr<Conf> singleton;
    static std::atomic<uint32_t> singleton_initialized_flag;
};

// helpers
const std::string& getConfString(const std::string& key);
const int16_t getConfInt16(const std::string& key);
const uint16_t getConfUInt16(const std::string& key);
const int32_t getConfInt32(const std::string& key);
const uint32_t getConfUInt32(const std::string& key);
const int64_t getConfInt64(const std::string& key);
const uint64_t getConfUInt64(const std::string& key);
const float getConfFloat(const std::string& key);
const double getConfDouble(const std::string& key);
const bool getConfBoolean(const std::string& key);
const bool hasCustomizedConfKey(const std::string& key);
}  // namespace derecho
#endif  // CONF_HPP
