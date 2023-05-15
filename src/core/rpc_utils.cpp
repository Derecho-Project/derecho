#include "derecho/core/detail/rpc_utils.hpp"
#include "derecho/utils/logger.hpp"

namespace derecho {
namespace rpc {

std::shared_ptr<spdlog::logger> RpcLoggerPtr::logger;

void RpcLoggerPtr::initialize() {
    logger = spdlog::get(LoggerFactory::RPC_LOGGER_NAME);
    // This function must be called after RPC_LOGGER_NAME is already in the registry
    assert(logger);
}

std::shared_ptr<spdlog::logger>& RpcLoggerPtr::get() {
    return logger;
}

}  //namespace rpc
}  //namespace derecho
