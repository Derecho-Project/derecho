#include "rdma_connection_manager.hpp"

#include <cassert>

namespace rdma {
static tcp::TCPConnectionManager* connections;
failure_upcall_t RDMAConnectionManager::failure_upcall;

void initialize(node_id_t my_id, const std::map<node_id_t, std::pair<tcp::ip_addr_t, tcp::port_t>>& ip_addrs_and_ports, const failure_upcall_t& failure_upcall) {
    connections = new tcp::TCPConnectionManager(my_id, ip_addrs_and_ports);
    // global libfabric initialization - not shown

    RDMAConnectionManager::failure_upcall = failure_upcall;
    for(auto p : ip_addrs_and_ports) {
        if(my_id != p.first) {
            RDMAConnectionManager::add(p.first);
        }
    }
}

tcp::TCPConnectionManager* get_connections() {
    return connections;
}

RDMAConnection::RDMAConnection(node_id_t remote_id) : remote_id(remote_id) {
    // first create the connection data to exchange
    // now do the exchange
    RDMAConnectionData local_connection_data;
    RDMAConnectionData remote_connection_data;
    connections->exchange(remote_id, local_connection_data, remote_connection_data);

    // proceed with the rest of the connection - create endpoints etc.
}

// not complete - will need to provide local/remote mr_key etc.
bool RDMAConnection::write_remote(char* local_addr, char* remote_addr, size_t size, bool with_completion) {
    if(!is_broken) {
        // post a remote write to the NIC
        // not shown
        bool failure = false;
        /* if a failure happens*/
        if(failure) {
            // call the failure upcall only once
            if(!is_broken.exchange(true)) {
                if(RDMAConnectionManager::failure_upcall) {
                    RDMAConnectionManager::failure_upcall(remote_id);
                }
            }
        }
    }
    // if the connection is broken, ignore
    return false;
}

bool RDMAConnection::sync() const {
    return false;
}

std::map<node_id_t, std::shared_ptr<RDMAConnection>> RDMAConnectionManager::rdma_connections;
std::mutex RDMAConnectionManager::rdma_connections_mutex;

void RDMAConnectionManager::add(node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    assert(rdma_connections.find(remote_id) == rdma_connections.end());
    rdma_connections[remote_id] = std::shared_ptr<RDMAConnection>(new RDMAConnection(remote_id));
}

void RDMAConnectionManager::remove(node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    rdma_connections.erase(remote_id);
}

std::shared_ptr<RDMAConnection> RDMAConnectionManager::get(node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    return rdma_connections.at(remote_id);
}
}  // namespace rdma
