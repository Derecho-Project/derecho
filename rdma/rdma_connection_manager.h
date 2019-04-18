class RDMAConnections {
    std::map<node_id_t, rdma_connection> rdma_connections;

public:
    void add(node_id_t remote_id, TcpConnection remote_socket);
    void remove(node_id_t remote_id_to_remove);
    // note that this is provided by reference
    rdma_connection& get(node_id_t remote_id);
    bool exists(node_id_t remote_id);
};
