#pragma once

#include <cstdint>
#include<functional>

#include "node/node.hpp"

namespace msg {
typedef int64_t msg_id_t;
typedef std::function<void(char*, size_t)> msg_generator_t;
typedef std::function<void(node::node_id_t, msg_id_t, char*, size_t)> recv_upcall_t;
}  // namespace msg
