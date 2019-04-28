#include "memory_region.hpp"
#include "exception/rdma_exceptions.hpp"

#include <memory>
#include <tuple>

namespace rdma {
MemoryRegion::MemoryRegion(node::node_id_t remote_id, char* send_buf, char* recv_buf, size_t size)
        : remote_id(remote_id),
          rdma_connection(RDMAConnectionManager::get(remote_id)),
          send_buf(send_buf),
          recv_buf(recv_buf) {
    std::shared_ptr<RDMAConnection> shared_rdma_connection = rdma_connection.lock();
    if(!shared_rdma_connection) {
        throw RDMAConnectionRemoved("RDMA Connection to " + std::to_string(remote_id) + " has been removed");
    }
    if(shared_rdma_connection->is_broken) {
        throw RDMAConnectionBroken("RDMA Connection to " + std::to_string(remote_id) + " is broken");
    }
    // exchange memory addresses
    MRConnectionData local_data;
    MRConnectionData remote_data;
    tcp_exchange(remote_id, local_data, remote_data);

    // initialize remote_send_buf and remote_recv_buf
}

bool MemoryRegion::write_remote(size_t offset, size_t size, bool with_completion) {
    std::shared_ptr<RDMAConnection> shared_rdma_connection = rdma_connection.lock();
    if(!shared_rdma_connection) {
      throw RDMAConnectionRemoved("RDMA Connection to " + std::to_string(remote_id) + " has been removed");
    }
    assert(offset + size <= this->size);
    return shared_rdma_connection->write_remote(send_buf + offset, remote_recv_buf + offset,
                                                size, with_completion);
}

bool MemoryRegion::sync() const {
    std::shared_ptr<RDMAConnection> shared_rdma_connection = rdma_connection.lock();
    if(!shared_rdma_connection) {
        throw RDMAConnectionRemoved("RDMA Connection to " + std::to_string(remote_id) + " has been removed");
    }
    assert(shared_rdma_connection);
    return shared_rdma_connection->sync();
}
}  // namespace rdma
