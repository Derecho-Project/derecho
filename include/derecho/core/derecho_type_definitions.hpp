#pragma once

#include "derecho/config.h"

#include <string>
#include <cstdint>

namespace derecho {

/** Type alias for IP addresses, currently stored as strings. */
using ip_addr_t = std::string;
/** Type alias for Node IDs in a Derecho group. */
using node_id_t = uint32_t;
#define INVALID_NODE_ID (0xffffffff)

/**
 * The memory region's attributes. This information enables device memories like those in GPU
 */
struct memory_attribute_t {
    enum memory_type_t {
        SYSTEM, // Normal system memory
        CUDA,   // NVIDIA CUDA GPU memory
        ROCM,   // AMD ROCm GPU memory
        L0,     // Intel level zero device memory
    } type;
    union {
        uint64_t    reserved;
        int         cuda;
        int         rocm;
        int         l0;
    } device;
};

}// namespace derecho
