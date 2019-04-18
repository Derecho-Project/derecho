struct ConnectionData {
    // low-level libfabric fields - not shown
};

class MemoryRegion {
    const RDMAConnection& rdma_connection;

    // write_buf is paired with remote read_buf and vice-versa
    std::pair<char*, size_t> write_buf;
    std::pair<char*, size_t> read_buf;

public:
    // constructor
    MemoryRegion(const RDMAConnection& rdma_connection, std::pair<char*, size_t> write_buf, std::pair<char*, size_t> read_buf);

    // size = 0 writes the entire region
    bool write_remote(size_t offset = 0, size_t size = 0, bool with_completion = false);

public:
    MemoryRegion(std::pair<char*, size_t> write_buf, std::pair<char*, size_t> read_buf);
};

class RdmaConnection {
private:
    // globally unique id of the remote node
    int remote_id;
    // remote tcp connection - required for out-of-band communication
    // for exchanging connection data as well as memory addresses
    TcpConnection remote_socket;

    // if the remote node has failed
    std::atomic<bool> is_broken = false;
    // upcall to application
    const failure_upcall_t& failure_upcall;

    // libfabric endpoint
    struct fid_ep* ep;
    // libfabric event queue
    struct fid_eq* eq;

    // manages memory region ids
    IdManager mr_id_manager;
    // map of mr_id to memory regions
    std::map<mr_id, MemoryRegion> memory_regions;

protected:
    void break();

    friend class MemoryRegion;

public:
    // constructor
    RdmaConnection(int remote_id, TcpConnection remote_socket, const failure_upcall_t& failure_upcall);

    // add a new memory region, mr_id is just a typedef for size_t
    mr_id add_memory_region(std::pair<char*, size_t> write_buf, std::pair<char*, size_t> read_buf);
    // remove a memory region
    void remove_memory_region(mr_id mr_id);

    bool isBroken();
};
