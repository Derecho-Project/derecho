#pragma once

#include <memory>
#include <byteswap.h>

#include "rdma_connection_manager.hpp"

namespace rdma {

//from verbs.cpp
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither
__LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

class MRConnectionData {
public:
    uint64_t           mr_key; // local memory key
    uint64_t           vaddr;  // virtual addr
};

class MemoryRegion {
    node::node_id_t remote_id;
    std::weak_ptr<RDMAConnection> rdma_connection;

    // send_buf is paired with remote recv_buf and vice-versa
    char* send_buf;
    char* recv_buf;
    char* remote_send_buf;
    char* remote_recv_buf;

    /** memory region for remote writer */
    struct fid_mr* write_mr;
    /** memory region for remote writer */
    struct fid_mr* read_mr;
    /** key for local read buffer */
    uint64_t mr_lrkey;
    /** key for local write buffer */
    uint64_t mr_lwkey;
    /** key for remote write buffer */
    uint64_t mr_rwkey;
    /** remote write memory address */
    fi_addr_t remote_fi_addr;

    size_t size;

public:
    // constructor
    MemoryRegion(node_id_t remote_id, char* send_buf, char* recv_buf, size_t size);
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion(MemoryRegion&&) = delete;

    // size = 0 writes the entire region
    bool write_remote(size_t offset = 0, size_t size = 0, bool with_completion = false);

    bool sync() const;
};
}  // namespace rdma
