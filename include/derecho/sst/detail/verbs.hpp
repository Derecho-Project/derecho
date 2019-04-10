#ifndef VERBS_HPP
#define VERBS_HPP

/**
 * @file verbs.h
 * Contains declarations needed for working with RDMA using InfiniBand Verbs,
 * including the Resources class and global setup functions.
 */

#include <map>

#include <infiniband/verbs.h>

namespace sst {

/** Structure to exchange the data needed to connect the Queue Pairs */
struct cm_con_data_t {
    /** Buffer address */
    uint64_t addr;
    /** Remote key */
    uint32_t rkey;
    /** Queue Pair number */
    uint32_t qp_num;
    /** LID of the InfiniBand port */
    uint16_t lid;
    /** GID */
    uint8_t gid[16];
} __attribute__((packed));

struct verbs_sender_ctxt {
  uint32_t      remote_id; // id of the remote node
  uint32_t      ce_idx; // index into the completion entry list
};

/**
 * Represents the set of RDMA resources needed to maintain a two-way connection
 * to a single remote node.
 */
class _resources {
private:
    /** Initializes the queue pair. */
    void set_qp_initialized();
    /** Transitions the queue pair to the ready-to-receive state. */
    void set_qp_ready_to_receive();
    /** Transitions the queue pair to the ready-to-send state. */
    void set_qp_ready_to_send();
    /** Connect the queue pairs. */
    void connect_qp();

protected:
    /** Post a remote RDMA operation. */
    int post_remote_send(const uint32_t id, const long long int offset, const long long int size, const int op, const bool completion);

public:
    /** Index of the remote node. */
    int remote_index;
    /** Handle for the IB Verbs Queue Pair object. */
    struct ibv_qp *qp;
    /** Memory Region handle for the write buffer. */
    struct ibv_mr *write_mr;
    /** Memory Region handle for the read buffer. */
    struct ibv_mr *read_mr;
    /** Connection data values needed to connect to remote side. */
    struct cm_con_data_t remote_props;
    /** Pointer to the memory buffer used for local writes.*/
    char *write_buf;
    /** Pointer to the memory buffer used for the results of RDMA remote reads.
     */
    char *read_buf;

    /** Constructor; initializes Queue Pair, Memory Regions, and `remote_props`.
     */
    _resources(int r_index, char *write_addr, char *read_addr, int size_w,
               int size_r);
    /** Destroys the resources. */
    virtual ~_resources();
};

class resources : public _resources {
public:
    resources(int r_index, char *write_addr, char *read_addr, int size_w,
              int size_r);
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

class resources_two_sided : public _resources {
    int post_receive(const uint32_t id, const long long int offset, const long long int size);

public:
    resources_two_sided(int r_index, char *write_addr, char *read_addr, int size_w,
                        int size_r);
    void post_two_sided_send(const uint32_t id, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send(const uint32_t id, const long long int offset, long long int size);
    void post_two_sided_send_with_completion(const uint32_t id, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send_with_completion(const uint32_t id, const long long int offset, const long long int size);
    void post_two_sided_receive(const uint32_t id, const long long int size);
    void post_two_sided_receive(const uint32_t id, const long long int offset, const long long int size);
};

bool add_node(uint32_t new_id, const std::string new_ip_addr);
bool remove_node(uint32_t node_id);
/**
 * Blocks the current thread until both this node and a remote node reach this
 * function, which exchanges some trivial data over a TCP connection.
 * @param r_index The node rank of the node to exchange data with.
 */
bool sync(uint32_t r_index);
/** Initializes the global verbs resources. */
void verbs_initialize(const std::map<uint32_t, std::string> &ip_addrs,
                      uint32_t node_rank);
/** Polls for completion of a single posted remote write. */
std::pair<uint32_t, std::pair<int, int>> verbs_poll_completion();
void shutdown_polling_thread();
/** Destroys the global verbs resources. */
void verbs_destroy();

}  // namespace sst

#endif  // VERBS_HPP
