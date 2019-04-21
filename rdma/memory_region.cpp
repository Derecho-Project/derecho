#include "memory_region.h"

#include <memory>
#include <tuple>

#include "rdma_connection_manager.h"

namespace rdma {
MemoryRegion::MemoryRegion(node_id_t remote_id, char* send_buf, char* recv_buf, size_t size)
        : rdma_connection(RDMAConnectionManager::get(remote_id)),
          send_buf(send_buf),
          recv_buf(recv_buf) {
    // exchange memory addresses
    MRConnectionData local_data;
    MRConnectionData remote_data;
    connections->exchange(remote_id, local_data, remote_data);

    // initialize remote_send_buf and remote_recv_buf
}

bool MemoryRegion::write_remote(size_t offset, size_t size, bool with_completion) {
    std::shared_ptr<RDMAConnection> shared_rdma_connection = rdma_connection.lock();
    assert(shared_rdma_connection);
    assert(offset + size <= this->size);
    return shared_rdma_connection->write_remote(send_buf + offset, remote_recv_buf + offset,
                                                size, with_completion);
}

void MemoryRegion::sync() const {
    std::shared_ptr<RDMAConnection> shared_rdma_connection = rdma_connection.lock();
    assert(shared_rdma_connection);
    shared_rdma_connection->sync();
}
}  // namespace rdma
