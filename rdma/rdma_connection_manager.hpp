#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <functional>
#include <rdma/fabric.h>
#include <rdma/fi_eq.h>

#include "tcp/tcp_connection_manager.hpp"

#ifndef LF_VERSION
#define LF_VERSION FI_VERSION(1, 5)
#endif

namespace rdma {

using node::node_id_t;
typedef std::function<void(node_id_t)> failure_upcall_t;

struct RDMAConnectionData {
    // low-level libfabric fields - not shown
    #define MAX_LF_ADDR_SIZE ((128) - sizeof(uint32_t) - 2 * sizeof(uint64_t))
    uint32_t pep_addr_len;  // local endpoint address length
    char pep_addr[MAX_LF_ADDR_SIZE];
} __attribute__((packed));

// instead of defining add, remove etc. separately, I have defined get_connections to access the static pointer connections
// call add_node, remove_node etc. directly on the return value of get_connections, for example,
// get_connections()->add_node(new_id, new_ip_addr_and_port)
tcp::TCPConnectionManager* get_connections();

template <typename T>
bool tcp_exchange(node_id_t remote_id, T local, T& remote) {
    return get_connections()->exchange(remote_id, local, remote);
}

class RDMAConnection {
    friend class RDMAConnectionManager;
    friend class MemoryRegion;

    // my own id
    node_id_t my_id;
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
    RDMAConnection(node_id_t my_id, node_id_t remote_id);
    RDMAConnection(const RDMAConnection&) = delete;
    RDMAConnection(RDMAConnection&&) = delete;

    void breakConnection();

    // update remote_addr with data from local_addr for size size
    bool write_remote(char* local_addr, char* remote_addr, size_t size, bool with_completion);

    // barrier with the remote end
    bool sync() const;

private:
    int init_endpoint(struct fi_info* fi);
};

void initialize(node_id_t my_id, const std::map<node_id_t, std::pair<tcp::ip_addr_t, tcp::port_t>>& ip_addrs_and_ports, const failure_upcall_t& failure_upcall = nullptr);
void lf_destroy();

/**
   * Global States
   */
class lf_ctxt {
public:
    // libfabric resources
    struct fi_info* hints;      // hints
    struct fi_info* fi;         // fabric information
    struct fid_fabric* fabric;  // fabric handle
    struct fid_domain* domain;  // domain handle
    struct fid_pep* pep;        // passive endpoint for receiving connection
    struct fid_eq* peq;         // event queue for connection management
    // struct fid_eq      * eq;           // event queue for transmitting events --> now move to resources.
    struct fid_cq* cq;    // completion queue for all rma operations
    size_t pep_addr_len;  // length of local pep address
    char pep_addr[MAX_LF_ADDR_SIZE];
    // local pep address
    // configuration resources
    struct fi_eq_attr eq_attr;  // event queue attributes
    struct fi_cq_attr cq_attr;  // completion queue attributes
    // #define DEFAULT_TX_DEPTH            (4096)
    // uint32_t           tx_depth;          // transfer depth
    // #define DEFAULT_RX_DEPTH            (4096)
    // uint32_t           rx_depth;          // transfer depth
    // #define DEFAULT_SGE_BATCH_SIZE      (8)
    // uint32_t           sge_bat_size;      // maximum scatter/gather batch size
    virtual ~lf_ctxt() {
        lf_destroy();
    }
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
    static void add(node_id_t my_id, node_id_t remote_id);
    static void remove(node_id_t remote_id);
    static lf_ctxt g_ctxt;
};
}  // namespace rdma
