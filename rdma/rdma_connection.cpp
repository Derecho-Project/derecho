MemoryRegion::MemoryRegion(const RDMAConnection& rdma_connection,
                           std::pair<char*, size_t> write_buf,
                           std::pair<char*, size_t> read_buf) : rdma_connection(rdma_connection),
                                                                write_buf(write_buf),
                                                                read_buf(read_buf) {
}

bool MemoryRegion:: write_remote(size_t offset, size_t size, bool with_completion) {
    if(!rdma_connection.isBroken) {
        // uses the reference to rdma_connection to post a remote write to the NIC
        // not shown
        if(/* a failure happens */) {
            rdma_connections.break();
        }
    }
    // if the connection is broken, ignore
    return false;
}

RDMAConnection::RDMAConnection(int remote_id, TcpConnection remote_socket,
                               const failure_upcall_t& failure_upcall) : remote_id(remote_id),
                                                                         remote_socket(remote_socket),
                                                                         failure_upcall(failure_upcall) {
    // first create the connection data to exchange
    // now do the exchange
    remote_soocket.exchange(connection_data);

    // proceed with the rest of the connection - create endpoints etc.
}

mr_id RDMAConnection::add_memory_region(std::pair<char*, size_t> write_buf, std::pair<char*, size_t> read_buf) {
    mr_id new_id = mr_id_manager.get_new_id();
    memory_regions.emplace(new_id, MemoryRegion(*this, write_buf, read_buf));
    return new_id;
}

void RDMAConnection::remove_memory_region(mr_id mr_id) {
    memory_regions.remove(mr_id);
    mr_id_manager.free_id(mr_id);
}

bool RDMAConnection::isBroken() {
    return is_broken;
}

void RDMAConnection::break() {
    // call the failure upcall only once
    if(!is_broken.exchange(true)) {
        failure_upcall(remote_id);
    }
}
