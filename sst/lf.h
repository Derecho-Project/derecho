#ifndef LF_H
#define LF_H

/**
 * @file lf.h
 * Contains declarations needed for working with RDMA using LibFabric libraries,
 * including the Resources class and global setup functions.
 */

#include <map>
#include <thread>
#include <rdma/fabric.h>

#include "derecho/derecho_type_definitions.h"

#define LF_VERSION FI_VERSION(1,5)

namespace sst {

struct lf_sender_ctxt {
  uint32_t      ce_idx; // index into the comepletion entry vector. - 0xFFFFFFFF for invalid
  uint32_t      remote_id; // thread id of the sender
};

/**
 * Represents the set of RDMA resources needed to maintain a two-way connection
 * to a single remote node.
 */
class _resources {
private:
    /** Connect the queue pair
     * 
     * @param is_lf_server This parameter decide local role in connection.
     *     If is_lf_server is true, it waits on PEP for connection from remote
     *     side. Otherwise, it initiate a connection to remote side.
     */
    void connect_endpoint(bool is_lf_server);
    /** Initialize resource endpoint using fi_info
     *
     * @param fi The fi_info object
     * @return 0 for success.
     */
    int init_endpoint(struct fi_info *fi);

protected:
    /** 
     * post read/write request
     * 
     * @param ctxt - pointer to the sender context, caller should maintain the
     *     ownership of this context until completion.
     * @param offset - The offset within the remote buffer to read/write
     * @param size - The number of bytes to read/write
     * @param op - 0 for read and 1 for write
     * @param return the return code for operation.
     */
    int post_remote_send(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size,
                         const int op, const bool completion);
public:
    /** ID of the remote node. */
    int remote_id;
    /** tx/rx completion queue */
    // struct fid_cq *txcq, *rxcq; - moved to g_ctxt
    /** Handle for the LibFabric endpoint. */
    struct fid_ep *ep;
    /** memory region for remote writer */
    struct fid_mr *write_mr;
    /** memory region for remote writer */
    struct fid_mr *read_mr;
    /** Pointer to the memory buffer used for local writes.*/
    char *write_buf;
    /** Pointer to the memory buffer used for the results of RDMA remote reads. */
    char *read_buf;
    /** key for local read buffer */
    uint64_t mr_lrkey;
    /** key for local write buffer */
    uint64_t mr_lwkey;
    /** key for remote write buffer */
    uint64_t mr_rwkey;
    /** remote write memory address */
    fi_addr_t remote_fi_addr;
    /** the event queue */
    struct fid_eq * eq;

    /**
     * Constructor
     * Initializes the resources. Registers write_addr and read_addr as the read
     * and write buffers and connects a queue pair with the specified remote node.
     *
     * @param r_id The node id of the remote node to connect to.
     * @param write_addr A pointer to the memory to use as the write buffer. This
     * is where data should be written locally in order to send it in an RDMA write
     * to the remote node.
     * @param read_addr A pointer to the memory to use as the read buffer. This is
     * where the results of RDMA reads from the remote node will arrive.
     * @param size_w The size of the write buffer (in bytes).
     * @param size_r The size of the read buffer (in bytes).
     * @param is_lf_server Is local node a libfabric server or client. A libfabric
     *         client initiates connection to the passive endpoint of the remote 
     *         node, while a libfabric server waiting for the conneciton using its
     *         local passive endpoint.
     */
    _resources(int r_id, char *write_addr, char *read_addr, int size_w,
              int size_r, int is_lf_server);
    /** Destroys the resources. */
    virtual ~_resources();
};

class resources : public _resources {
  public:
    /** constructor */
    resources(int r_id, char *write_addr, char *read_addr, int size_w,
              int size_r, int is_lf_server) : 
      _resources(r_id,write_addr,read_addr,size_w,size_r,is_lf_server) {
    }

    /*
      wrapper functions that make up the user interface
      all call post_remote_send with different parameters
    */

    /** Post an RDMA read at the beginning address of remote memory. */
    void post_remote_read(const long long int size);
    /** Post an RDMA read at an offset into remote memory. */
    void post_remote_read(const long long int offset, const long long int size);
    /** Post an RDMA write at the beginning address of remote memory. */
    void post_remote_write(const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write(const long long int offset, long long int size);
    void post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size);
};

class resources_two_sided : public _resources {
    int post_receive(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size);

public:
    /** constructor */
    resources_two_sided(int r_id, char *write_addr, char *read_addr, int size_w,
              int size_r, int is_lf_server) : 
      _resources(r_id,write_addr,read_addr,size_w,size_r,is_lf_server) {
    }

    void post_two_sided_send(const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send(const long long int offset, long long int size);
    void post_two_sided_send_with_completion(struct lf_sender_ctxt *ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send_with_completion(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size);
    void post_two_sided_receive(struct lf_sender_ctxt *ctxt, const long long int size);
    void post_two_sided_receive(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size);
};

/**
 * add a new node to sst_connection set.
 */
bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
/**
 * Removes a node from the SST TCP connections set
 */
bool remove_node(uint32_t node_id);
/** sync
 * @param r_id - ID of the node to exchange data with.
 */
bool sync(uint32_t r_id);
/** 
 * Initializes the global libfabric resources. Must be called before creating
 * or using any SST instance. 
 * 
 * @param ip_addres A map from rank to string??
 * @param node_rank rank of this node.
 */
  void lf_initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> &ip_addrs_and_ports,
                      uint32_t node_rank);
/** Polls for completion of a single posted remote write. */
std::pair<uint32_t, std::pair<int32_t, int32_t>> lf_poll_completion(); 
/** Shutdown the polling thread. */
void shutdown_polling_thread();
/** Destroys the global libfabric resources. */
void lf_destroy();
}  // namespace sst

#endif  // LF_H
