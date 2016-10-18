#pragma once

#include <string>
#include <cstdint>
#include "derecho_row.h"

namespace derecho {

namespace persistence {

struct message {
    char *data;
    uint64_t length;
    uint32_t view_id;
    uint32_t sender;
    uint64_t index;
    bool cooked;
};

struct __attribute__((__packed__)) header {
    uint8_t magic[8];
    uint32_t version;
};

struct __attribute__((__packed__)) message_metadata {
    uint32_t view_id;
    uint8_t is_cooked;
    uint8_t padding0;
    uint16_t padding1;
    uint32_t sender;
    uint32_t padding2;
    uint64_t index;

    uint64_t offset;
    uint64_t length;
};

static const std::string METADATA_EXTENSION = ".metadata";
static const std::string PAXOS_STATE_EXTENSION = ".paxosstate";
static const std::string SWAP_FILE_EXTENSION = ".swp";

}  // namespace persistence
}  // namespace derecho
