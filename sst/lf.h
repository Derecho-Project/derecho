#ifndef LF_H
#define LF_H

/**
 * @file lf.h
 * Contains declarations needed for working with RDMA using LibFabric libraries,
 * including the Resources class and global setup functions.
 */

#include <map>
#include <rdma/fabric.h>

namespace sst {

/**
 * Represents the set of RDMA resources needed to maintain a two-way connection
 * to a single remote node.
 */
class resources {
private:
    /** Initializes the queue pair. */
    void set_qp_initialized();
    /** Transitions the queue pair to the ready-to-receive state. */
    void set_qp_ready_to_receive();
    /** Transitions the queue pair to the ready-to-send state. */
    void set_qp_ready_to_send();
    /** Connect the queue pairs. */
    void connect_qp();
    /** Post a remote RDMA operation. */
    int post_remote_send(const uint32_t id, const long long int offset, const long long int size, const int op, const bool completion);

public:
    /** Index of the remote node. */
    int remote_index;
    /** Handle for the LibFabric endpoint. */
    struct fid_ep *ep;
    /** Pointer to the memory buffer used for local writes.*/
    char *write_buf;
    /** Pointer to the memory buffer used for the results of RDMA remote reads. */
    char *read_buf;
    /** remote memory rkey */
    uint64_t rkey;
    /** remote memory address */
    fi_addr_t remote_fi_addr;

    /** Constructor; initializes libfabric endpoint, Memory Regions, memory access keys.*/
    resources(int r_index, char *write_addr, char *read_addr, int size_w,
              int size_r);
    /** Destroys the resources. */
    virtual ~resources();
    /*
      wrapper functions that make up the user interface
      all call post_remote_send with different parameters
    */
    /** Post an RDMA read at the beginning address of remote memory. */
    void post_remote_read(const uint32_t id, const long long int size);
    /** Post an RDMA read at an offset into remote memory. */
    void post_remote_read(const uint32_t id, const long long int offset, const long long int size);
    /** Post an RDMA write at the beginning address of remote memory. */
    void post_remote_write(const uint32_t id, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write(const uint32_t id, const long long int offset, long long int size);
    void post_remote_write_with_completion(const uint32_t id, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write_with_completion(const uint32_t id, const long long int offset, const long long int size);
};

bool add_node(uint32_t new_id, const std::string new_ip_addr);
bool sync(uint32_t r_index);
/** Initializes the global libfabric resources. */
void lf_initialize(const std::map<uint32_t, std::string> &ip_addrs,
                      uint32_t node_rank);
/** Polls for completion of a single posted remote write. */
std::pair<uint32_t, std::pair<int, int>> lf_poll_completion();
void shutdown_polling_thread();
/** Destroys the global libfabric resources. */
void lf_destroy();

}  // namespace sst

#endif  // LF_H
