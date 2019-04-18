void RDMAConnections::add(node_id_t remote_id, TcpConnection remote_socket) {
    rdma_connections.emplace(remote_id, RdmaConnection(remote_id, remote_socket));
}

void RDMAConnections::remove(node_id_t remote_id) {
    rdma_connections.remove(remote_id);
}

RDMAConnection& RDMAConnections::get(node_id_t remote_id) {
    return rdma_connections.at(remote_id);
}

bool RDMAConnections::exists(node_id_t remote_id) {
    return rdma_connections.find(remote_id) != rdma_connections.end();
}
