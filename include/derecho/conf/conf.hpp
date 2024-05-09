#pragma once
#ifndef CONF_HPP
#define CONF_HPP

#include <derecho/config.h>
#include "getpot/GetPot"
#include <atomic>
#include <getopt.h>
#include <inttypes.h>
#include <map>
#include <memory>
#include <optional>
#include <stdio.h>
#include <string>
#include <unistd.h>

namespace derecho {


// This configuration option is built-in, based on code in remote_invocable.hpp, and can't be changed
// It's declared here so that it can be compared to the supplied config options
constexpr std::size_t DERECHO_MIN_RPC_RESPONSE_SIZE = 128;

/** The single configuration file for derecho **/
class Conf {
public:
    //String constants for config options
    static constexpr const char* DERECHO_CONTACT_IP = "DERECHO/contact_ip";
    static constexpr const char* DERECHO_CONTACT_PORT = "DERECHO/contact_port";
    static constexpr const char* DERECHO_LEADER_EXTERNAL_PORT = "DERECHO/leader_external_port";
    static constexpr const char* DERECHO_RESTART_LEADERS = "DERECHO/restart_leaders";
    static constexpr const char* DERECHO_RESTART_LEADER_PORTS = "DERECHO/restart_leader_ports";
    static constexpr const char* DERECHO_LOCAL_ID = "DERECHO/local_id";
    static constexpr const char* DERECHO_LOCAL_IP = "DERECHO/local_ip";
    static constexpr const char* DERECHO_GMS_PORT = "DERECHO/gms_port";
    static constexpr const char* DERECHO_STATE_TRANSFER_PORT = "DERECHO/state_transfer_port";
    static constexpr const char* DERECHO_SST_PORT = "DERECHO/sst_port";
    static constexpr const char* DERECHO_RDMC_PORT = "DERECHO/rdmc_port";
    static constexpr const char* DERECHO_EXTERNAL_PORT = "DERECHO/external_port";
    static constexpr const char* DERECHO_HEARTBEAT_MS = "DERECHO/heartbeat_ms";
    static constexpr const char* DERECHO_P2P_LOOP_BUSY_WAIT_BEFORE_SLEEP_MS = "DERECHO/p2p_loop_busy_wait_before_sleep_ms";
    static constexpr const char* DERECHO_SST_POLL_CQ_TIMEOUT_MS = "DERECHO/sst_poll_cq_timeout_ms";
    static constexpr const char* DERECHO_RESTART_TIMEOUT_MS = "DERECHO/restart_timeout_ms";
    static constexpr const char* DERECHO_ENABLE_BACKUP_RESTART_LEADERS = "DERECHO/enable_backup_restart_leaders";
    static constexpr const char* DERECHO_DISABLE_PARTITIONING_SAFETY = "DERECHO/disable_partitioning_safety";
    static constexpr const char* DERECHO_MAX_NODE_ID = "DERECHO/max_node_id";

    static constexpr const char* DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE = "DERECHO/max_p2p_request_payload_size";
    static constexpr const char* DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE = "DERECHO/max_p2p_reply_payload_size";
    static constexpr const char* DERECHO_P2P_WINDOW_SIZE = "DERECHO/p2p_window_size";

    static constexpr const char* SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE = "SUBGROUP/DEFAULT/max_payload_size";
    static constexpr const char* SUBGROUP_DEFAULT_MAX_REPLY_PAYLOAD_SIZE = "SUBGROUP/DEFAULT/max_reply_payload_size";
    static constexpr const char* SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE = "SUBGROUP/DEFAULT/max_smc_payload_size";
    static constexpr const char* SUBGROUP_DEFAULT_BLOCK_SIZE = "SUBGROUP/DEFAULT/block_size";
    static constexpr const char* SUBGROUP_DEFAULT_WINDOW_SIZE = "SUBGROUP/DEFAULT/window_size";
    static constexpr const char* SUBGROUP_DEFAULT_RDMC_SEND_ALGORITHM = "SUBGROUP/DEFAULT/rdmc_send_algorithm";

    static constexpr const char* RDMA_PROVIDER = "RDMA/provider";
    static constexpr const char* RDMA_DOMAIN = "RDMA/domain";
    static constexpr const char* RDMA_TX_DEPTH = "RDMA/tx_depth";
    static constexpr const char* RDMA_RX_DEPTH = "RDMA/rx_depth";
    static constexpr const char* PERS_FILE_PATH = "PERS/file_path";
    static constexpr const char* PERS_RAMDISK_PATH = "PERS/ramdisk_path";
    static constexpr const char* PERS_RESET = "PERS/reset";
    static constexpr const char* PERS_MAX_LOG_ENTRY = "PERS/max_log_entry";
    static constexpr const char* PERS_MAX_DATA_SIZE = "PERS/max_data_size";
    static constexpr const char* PERS_PRIVATE_KEY_FILE = "PERS/private_key_file";
    static constexpr const char* LOGGER_DEFAULT_LOG_NAME = "LOGGER/default_log_name";
    static constexpr const char* LOGGER_DEFAULT_LOG_LEVEL = "LOGGER/default_log_level";
    static constexpr const char* LOGGER_SST_LOG_LEVEL = "LOGGER/sst_log_level";
    static constexpr const char* LOGGER_RPC_LOG_LEVEL = "LOGGER/rpc_log_level";
    static constexpr const char* LOGGER_VIEWMANAGER_LOG_LEVEL = "LOGGER/viewmanager_log_level";
    static constexpr const char* LOGGER_PERSISTENCE_LOG_LEVEL = "LOGGER/persistence_log_level";
    static constexpr const char* LOGGER_LOG_TO_TERMINAL = "LOGGER/log_to_terminal";
    static constexpr const char* LOGGER_LOG_FILE_DEPTH = "LOGGER/log_file_depth";

    static constexpr const char* LAYOUT_JSON_LAYOUT = "LAYOUT/json_layout";
    static constexpr const char* LAYOUT_JSON_LAYOUT_FILE = "LAYOUT/json_layout_file";

private:
    // Configuration Table:
    // config name --> default value
    std::map<const std::string, std::string> config = {
            // [DERECHO]
            {DERECHO_CONTACT_IP, "127.0.0.1"},
            {DERECHO_CONTACT_PORT, "23580"},
            {DERECHO_LEADER_EXTERNAL_PORT, "32645"},
            {DERECHO_RESTART_LEADERS, "127.0.0.1"},
            {DERECHO_RESTART_LEADER_PORTS, "23580"},
            {DERECHO_LOCAL_ID, "0"},
            {DERECHO_LOCAL_IP, "127.0.0.1"},
            {DERECHO_GMS_PORT, "23580"},
            {DERECHO_STATE_TRANSFER_PORT, "28366"},
            {DERECHO_SST_PORT, "37683"},
            {DERECHO_RDMC_PORT, "31675"},
            {DERECHO_EXTERNAL_PORT, "32645"},
            {SUBGROUP_DEFAULT_RDMC_SEND_ALGORITHM, "binomial_send"},
            {DERECHO_P2P_LOOP_BUSY_WAIT_BEFORE_SLEEP_MS, "250"},
            {DERECHO_SST_POLL_CQ_TIMEOUT_MS, "2000"},
            {DERECHO_RESTART_TIMEOUT_MS, "2000"},
            {DERECHO_DISABLE_PARTITIONING_SAFETY, "true"},
            {DERECHO_ENABLE_BACKUP_RESTART_LEADERS, "false"},
            {DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE, "10240"},
            {DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE, "10240"},
            {DERECHO_P2P_WINDOW_SIZE, "16"},
            {DERECHO_MAX_NODE_ID, "1024"},
            // [SUBGROUP/<subgroupname>]
            {SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE, "10240"},
            {SUBGROUP_DEFAULT_MAX_REPLY_PAYLOAD_SIZE, "10240"},
            {SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE, "10240"},
            {SUBGROUP_DEFAULT_BLOCK_SIZE, "1048576"},
            {SUBGROUP_DEFAULT_WINDOW_SIZE, "16"},
            {DERECHO_HEARTBEAT_MS, "1"},
            // [RDMA]
            {RDMA_PROVIDER, "sockets"},
            {RDMA_DOMAIN, "eth0"},
            {RDMA_TX_DEPTH, "256"},
            {RDMA_RX_DEPTH, "256"},
            // [PERS]
            {PERS_FILE_PATH, ".plog"},
            {PERS_RAMDISK_PATH, "/dev/shm/volatile_t"},
            {PERS_RESET, "false"},
            {PERS_MAX_LOG_ENTRY, "1048576"},       // 1M log entries.
            {PERS_MAX_DATA_SIZE, "549755813888"},  // 512G total data size.
            {PERS_PRIVATE_KEY_FILE, "private_key.pem"},
            // [LOGGER]
            {LOGGER_DEFAULT_LOG_NAME, "derecho_debug"},
            {LOGGER_DEFAULT_LOG_LEVEL, "info"},
            {LOGGER_LOG_TO_TERMINAL, "true"},
            {LOGGER_LOG_FILE_DEPTH, "3"}};

public:
    // the option for parsing command line with getopt(not GetPot!!!)
    static struct option long_options[];

public:
    /**
     * The name of the default configuration file that will be loaded if none is specified
     */
    static constexpr const char* default_conf_file = "derecho.cfg";
    /**
     * The name of the default node-local configuration file that will be loaded
     * if none is specified
     */
    static constexpr const char* default_node_conf_file = "derecho_node.cfg";

    /**
     * Constructor:
     * Conf can read configuration from multiple sources
     *  - the command line argument has the highest priority, then,
     *  - the "group" configuration file
     *  - the "node" (local) configuration file
     *  - the default values.
     **/
    Conf(int argc, char* argv[], std::optional<std::string> group_conf_file,
         std::optional<std::string> node_conf_file) noexcept(true);

    /**
     * Loads configuration options from a file and adds them to the
     * configuration table. If the file contains options that were already
     * present in the in-memory configuration table, they will overwrite the
     * values in memory. This method can be called after initialization, but
     * it is not thread-safe, so the caller must ensure it is only called once.
     * Also, this method does not check if the file exists before attempting to
     * parse it with GetPot, so it may fail with an exception from GetPot if
     * called with a nonexistant file.
     *
     * @param file_name The name/path of the file to read
     */
    void loadFromFile(const std::string& file_name);

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
    /**
     * Initialize the singleton from the command line and the configuration files.
     * The command line has higher priority than the configuration files, and the
     * node-local configuration file has higher priority than the group
     * configuration file. The process for finding the group configuration file
     * and node-local configuration file is:
     * 1) if conf_file/node_conf_file is not null, use it, otherwise,
     * 2) try DERECHO_CONF_FILE environment variable, otherwise,
     * 3) use default_conf_file if it exists, otherwise,
     * 4) use all default settings.
     * Note: initialize will be called on the first 'get()' if it is not called
     * before.
     */
    static void initialize(int argc, char* argv[],
                           const char* conf_file = nullptr,
                           const char* node_conf_file = nullptr);
    /** Gets a const pointer to the singleton instance, initializing it if necessary. */
    static const Conf* get() noexcept(true);
    /**
     * Loads an additional configuration file into the Conf object, besides the
     * two standard Derecho configuration files. Options set in the new config
     * file have higher priority than (will overwrite) any options already set
     * in the first initialization. If the optional env_var_name parameter is
     * provided (non-null), it will be used as the name of an environment
     * variable to check for the name of the file, and this filename will be
     * used in preference to the file_name parameter if it exists. This function
     * should be called after initialization, but unlike initialize() it is not
     * thread-safe, so the caller must ensure it is only called once.
     *
     * @param default_file_name The "default" name of the configuration file to
     * load. This is the file that will be loaded if env_var_name is not
     * provided, is not set, or is set but points to a nonexistant file.
     * @param env_var_name The name of an environment variable to check for a
     * non-default file name
     * @throw A std::runtime_error if the file could not be found after checking
     * both the environment variable and the default file name.
     */
    static void loadExtraFile(const std::string& default_file_name,
                              const char* env_var_name = nullptr);

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
// advanced helpers
const std::string getAbsoluteFilePath(const std::string& filename);

/**
 * Splits a string into a vector of strings using a delimiting string. This is
 * helpful for parsing "list-like" config options, which are comma-delimited
 * sequences of strings or numbers (so the default delimiter is ",").
 * @param str The string to split
 * @param delimiter The string to use as the delimiter for splitting
 * @return A vector of substrings of the input string, partitioned on the
 * delimiter. The delimiter is not included in any substring.
 */
std::vector<std::string> split_string(const std::string& str, const std::string& delimiter = ",");

}  // namespace derecho
#endif  // CONF_HPP
