#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <functional>

#include "tcp/tcp_connection_manager.hpp"

namespace rdma {
using node::node_id_t;
typedef std::function<void(node_id_t)> failure_upcall_t;

struct RDMAConnectionData {
    // low-level libfabric fields - not shown
};

void initialize(node_id_t my_id, const std::map<node_id_t, std::pair<tcp::ip_addr_t, tcp::port_t>>& ip_addrs_and_ports, const failure_upcall_t& failure_upcall = nullptr);

// instead of defining add, remove etc. separately, I have defined get_connections to access the static pointer connections
// call add_node, remove_node etc. directly on the return value of get_connections, for example,
// get_connections().add_node(new_id, new_ip_addr_and_port)
tcp::TCPConnectionManager* get_connections();

template <typename T>
bool tcp_exchange(node_id_t remote_id, T local, T& remote) {
    return get_connections()->exchange(remote_id, local, remote);
}

class RDMAConnection {
    friend class RDMAConnectionManager;
    friend class MemoryRegion;

    // id of the remote node
    node_id_t remote_id;

    // if the remote node has failed
    std::atomic<bool> is_broken = false;

    // libfabric endpoint
    struct fid_ep* ep;
    // libfabric event queue
    struct fid_eq* eq;

    // private constructor
    // no one except RDMAConnectionManager can create an RDMAConnection
    RDMAConnection(node_id_t remote_id);
    RDMAConnection(const RDMAConnection&) = delete;
    RDMAConnection(RDMAConnection&&) = delete;

    void breakConnection();

    // update remote_addr with data from local_addr for size size
    bool write_remote(char* local_addr, char* remote_addr, size_t size, bool with_completion);

    // barrier with the remote end
    bool sync() const;
};

class RDMAConnectionManager {
    friend class MemoryRegion;
    static std::map<node_id_t, std::shared_ptr<RDMAConnection>> rdma_connections;
    static std::mutex rdma_connections_mutex;
    static std::shared_ptr<RDMAConnection> get(node_id_t remote_id);

public:
    static failure_upcall_t failure_upcall;
    RDMAConnectionManager(const RDMAConnectionManager&) = delete;
    RDMAConnectionManager(RDMAConnectionManager&&) = delete;
    static void add(node_id_t remote_id);
    static void remove(node_id_t remote_id);
};
}  // namespace rdma
