#ifndef CONF_HPP
#define CONF_HPP

#include <memory>
#include <atomic>
#include <inttypes.h>
#include <conf/getpot/GetPot>
#include <map>

namespace derecho {

#define CONF_ENTRY_INTEGER(name,section,string) \

  /** The single configuration file for derecho **/
  class Conf {
  private:
    // Configuration Table: 
    // config name --> default value
#define CONF_DERECHO_LOCAL_IP   "DERECHO/local_ip"
#define CONF_DERECHO_GMS_PORT   "DERECHO/gms_port"
#define CONF_DERECHO_RPC_PORT   "DERECHO/rpc_port"
#define CONF_DERECHO_SST_PORT   "DERECHO/sst_port"
#define CONF_DERECHO_RDMC_PORT  "DERECHO/rdmc_port"

#define CONF_RDMA_PROVIDER      "RDMA/provider"
#define CONF_RDMA_DOMAIN        "RDMA/domain"
#define CONF_RDMA_TX_DEPTH      "RDMA/tx_depth"
#define CONF_RDMA_RX_DEPTH      "RDMA/rx_depth"

#define CONF_PERS_FILE_PATH     "PERS/file_path"
#define CONF_PERS_RAMDISK_PATH  "PERS/ramdisk_path"

    std::map<const std::string, std::string> config = {
      // [DERECHO]
      {CONF_DERECHO_LOCAL_IP,   "DERECHO/local_ip"},
      {CONF_DERECHO_GMS_PORT,   "23580"},
      {CONF_DERECHO_RPC_PORT,   "28366"},
      {CONF_DERECHO_SST_PORT,   "37683"},
      {CONF_DERECHO_RDMC_PORT,  "31675"},
      // [RDMA]
      {CONF_RDMA_PROVIDER,      "sockets"},
      {CONF_RDMA_DOMAIN,        "eth0"},
      {CONF_RDMA_TX_DEPTH,      "256"},
      {CONF_RDMA_RX_DEPTH,      "256"},
      // [PERS]
      {CONF_PERS_FILE_PATH,     ".plog"},
      {CONF_PERS_RAMDISK_PATH,  "/dev/shm/volatile_t"}
    };
  public:
    /** Constructor **/
    Conf(GetPot * getpotcfg) noexcept {
      // load configuration
      if (getpotcfg != nullptr) {
        for (std::map<const std::string, std::string>::iterator it = this->config.begin();it != this->config.end(); it++) {
          this->config[it->first] = (*getpotcfg)(it->first,it->second);
        }
      }
    }
    /** get configuration **/
    const std::string & getString(const std::string & key) const {
      return this->config.at(key);
    }
    const int16_t getInt32(const std::string & key) const {
      return (const int16_t)std::stoi(this->config.at(key));
    }
    const int32_t getInt32(const std::string & key) const {
      return (const int32_t)std::stoi(this->config.at(key));
    }
    const int64_t getInt64(const std::string & key) const {
      return (const int64_t)std::stoll(this->config.at(key));
    }
    const float getFloat(const std::string & key) const {
      return (const float)std::stof(this->config.at(key));
    }
    const double getDouble(const std::string & key ) const {
      return (const float)std::stod(this->config.at(key));
    }
    // Initialize the singleton from the configuration file
    // 1) if conf_file is not null, use it, otherwise,
    // 2) try DERECHO_CONF_FILE environment, otherwise,
    // 3) use "derecho.cfg" at local, otherwise,
    // 4) use all default settings.
    // Note: initialize will be called on the first 'get()' if it is not called before.
    static void initialize(const char * conf_file = nullptr);
    static const Conf* get() noexcept;

  private:
    // singleton
    static std::unique_ptr<Conf> singleton;
    static std::atomic<uint32_t> singleton_initialized_flag;
  };

  // helpers
  const std::string & getConfString(const std::string & key);
  const int16_t getConfInt16(const std::string & key);
  const int32_t getConfInt32(const std::string & key);
  const int64_t getConfInt64(const std::string & key);
  const float getConfFloat(const std::string & key);
  const double getConfDouble(const std::string & key);
}
#endif//CONF_HPP
