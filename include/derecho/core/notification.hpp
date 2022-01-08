#pragma once

#include "bytes_object.hpp"
#include "derecho/mutils-serialization/SerializationMacros.hpp"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "register_rpc_functions.hpp"

#include <functional>
#include <vector>

namespace derecho {

struct NotificationSupport {
public:
    std::vector<std::function<void(const Bytes&)>> handlers;

    void notify(const Bytes& msg) const {
        for(auto func : handlers) {
            func(msg);
        }
    }

    // extend blob to add a tag, header and body

    // REGISTER_RPC_FUNCTIONS(NotificationSupport, P2P_TARGETS(notify));

    // extend blob to add a tag, header and body

    // merge NotificationSupport and "ConcreteNotificationSupport"
    // server side: class A:// opcode hardcoding?
    // virtual void notify(const Bytes& msg) const {NotificationSupport::notify(msg);}
    // REGISTER_RPC_FUNCTIONS(A, P2P_TARGETS(notify));
    // external client:

    // DEFAULT_SERIALIZATION_SUPPORT(NotificationSupport, handler);
    void add_notification_handler(std::function<void(const Bytes&)> func) {
        handlers.push_back(func);
    }
};

}  // namespace derecho
