#pragma once

#include "max_msg_size.h"

namespace sst {
struct Message {
    char buf[max_msg_size];
    uint32_t size;
    uint64_t next_seq;
};
}
