#ifndef CONF_HPP
#define CONF_HPP

#include <memory>
#include <atomic>
#include <inttypes.h>
#include <GetPot>
#include <map>

namespace derecho {

#define CONF_ENTRY_INTEGER(name,section,string) \

  /** RMDA Hardware Configuration **/
  class RDMAHwConf {
  public:
    RDMAHwConf(GetPot &cfg);
    const char * provider;
    const char * domain;
    const uint32_t rx_depth;
    const uint32_t tx_depth;
  };

  /** SST Configuration **/
  class SSTConf {
  public:
    constexpr uint32_t getSSTPort() const noexcept;
  };

  /** RDMC Configuration **/
  class RDMCConf {
  public:
    constexpr uint32_t getRDMCPort() const noexcept;
  };

  /** Persistent Configuration **/
  class PersistentConf {
  public:
    constexpr uint32_t getPersMedia() const noexcept;
  };

  /** The single configuration file for derecho **/
  class Conf {
  private:
    // Configuration Table: 
    // config name --> default value
    std::map<const std::string, std::string> config = {
      {"RDMA/provider",         "sockets"},
      {"RDMA/domain",           "eth0"},
      {"RDMA/tx_depth",         "256"},
      {"RDMA/rx_depth",         "256"}
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
}
#endif//CONF_HPP
