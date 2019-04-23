#pragma once

#include <memory>

#include "rdma_connection_manager.hpp"

namespace rdma {
class MRConnectionData {
};

class MemoryRegion {
    std::weak_ptr<RDMAConnection> rdma_connection;

    // send_buf is paired with remote recv_buf and vice-versa
    char* send_buf;
    char* recv_buf;
    char* remote_send_buf;
    char* remote_recv_buf;

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
