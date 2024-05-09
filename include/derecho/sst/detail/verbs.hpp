#ifndef VERBS_HPP
#define VERBS_HPP

/**
 * @file verbs.hpp
 * Contains declarations needed for working with RDMA using InfiniBand Verbs,
 * including the Resources class and global setup functions.
 */
#include <derecho/config.h>
#include <derecho/core/derecho_type_definitions.hpp>

#include <atomic>
#include <infiniband/verbs.h>
#include <map>
#include <stdexcept>
#include <sys/uio.h>
#include <vector>

namespace sst {

using memory_attribute_t    =   derecho::memory_attribute_t;
using ip_addr_t             =   derecho::ip_addr_t;
using node_id_t             =   derecho::node_id_t;

struct unsupported_operation_exception : public std::logic_error {
    unsupported_operation_exception(const std::string& message) : logic_error(message) {}
};

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

class _resources;

struct verbs_sender_ctxt {
    enum verbs_sender_ctxt_type {
        INTERNAL_FLOW_CONTROL,
        EXPLICIT_SEND_WITH_COMPLETION
    } type
            = EXPLICIT_SEND_WITH_COMPLETION;  // the type of the sender context, default to EXPLICIT_SEND_WITH_COMPLETION.
    union {
        // for INTERNAL_FLOW_CONTROL type:
        _resources* res;
        // for EXPLICIT_SEND_WOTHCOMPLETION type:
        struct {
            uint32_t remote_id;  // id of the remote node
            uint32_t ce_idx;     // index into the completion entry list
        } sender_info;
    } ctxt;
    // getters
    uint32_t remote_id() { return ctxt.sender_info.remote_id; }
    uint32_t ce_idx() { return ctxt.sender_info.ce_idx; }
    // setters
    void set_remote_id(const uint32_t& rid) { ctxt.sender_info.remote_id = rid; }
    void set_ce_idx(const uint32_t& cidx) { ctxt.sender_info.ce_idx = cidx; }
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
    std::atomic<bool> remote_failed;
    /** Post a remote RDMA operation. 
     * @param   sctxt       sender context pointer
     * @param   offset      offset
     * @param   size        size to send
     * @param   op          operation
     * @param   completion  completion
     */
    int post_remote_send(verbs_sender_ctxt* sctxt, const long long int offset, const long long int size, const int op, const bool completion);

public:
    /** ID of the remote node. */
    int remote_id;
    /** Handle for the IB Verbs Queue Pair object. */
    struct ibv_qp* qp;
    /** Memory Region handle for the write buffer. */
    struct ibv_mr* write_mr;
    /** Memory Region handle for the read buffer. */
    struct ibv_mr* read_mr;
    /** Connection data values needed to connect to remote side. */
    struct cm_con_data_t remote_props;
    /** Pointer to the memory buffer used for local writes.*/
    uint8_t* write_buf;
    /** Pointer to the memory buffer used for the results of RDMA remote reads.
     */
    uint8_t* read_buf;
    /** the number of ops without completion in the queue pair. */
    std::atomic<uint32_t> without_completion_send_cnt;
    /** the maximum number of ops without completion allowed in the queue pair. */
    uint32_t without_completion_send_capacity;
    /** how many ops without completion to send before signaling. */
    uint32_t without_completion_send_signal_interval;
    /** context for the polling thread */
    verbs_sender_ctxt without_completion_sender_ctxt;

    /** Constructor; initializes Queue Pair, Memory Regions, and `remote_props`.
     */
    _resources(int r_id, uint8_t* write_addr, uint8_t* read_addr, int size_w,
               int size_r);
    /** Destroys the resources. */
    virtual ~_resources();
    /**
     * Out-of-Band memory and send management
     * These are not currently supported when using the Verbs interface, but are
     * declared here to allow compilation to succeed. Attempting to use any of
     * them will throw an exception.
     */

    /**
     * get the descriptor of the corresponding oob memory region
     * Important: it assumes shared lock on oob_mrs_mutex.
     * If iov does not fall into an oob memory region, it fails with nullptr.
     *
     * @param addr
     *
     * @return the descriptor of type void*, or nullptr in case of failure.
     * @throw   derecho::derecho_exception if not found.
     */
    static void* get_oob_mr_desc(void* addr);

    /**
     * Get the key of the corresponding oob memory region for remote access.
     *
     * @param addr      The address of registered oob memory
     *
     * @return  the remote access key,
     * @throw   derecho::derecho_exception if not found.
     */
    static uint64_t get_oob_mr_key(void* addr);

    /**
     * Register oob memory
     * @param addr  the address of the OOB memory
     * @param size  the size of the OOB memory
     * @param attr  the memory attribute
     *
     * @throws derecho_exception on failure.
     */
    static void register_oob_memory_ex(void* addr, size_t size, const memory_attribute_t& attr);

    /**
     * Deregister oob memory
     * @param addr the address of OOB memory
     *
     * @throws derecho_exception on failure.
     */
    static void deregister_oob_memory(void* addr);

    /**
     * Wait for a completion entries
     * @param num_entries   The number of entries to wait for
     * @param timeout_us    The number of microseconds to wait before throwing timeout
     *
     * @throws derecho_exception on failure.
     */
    void wait_for_thread_local_completion_entries(size_t num_entries, uint64_t timeout_us);

    /*
     * oob write
     * @param iov               The gather memory vector, the total size of the source should not go beyond 'size'.
     * @param iovcnt            The length of the vector.
     * @param remote_dest_addr  The remote address for receiving this message
     * @param rkey              The access key for the remote memory.
     * @param size              The size of the remote buffer
     *
     * @throws derecho_exception at failure.
     */
    void oob_remote_write(const struct iovec* iov, int iovcnt,
                          void* remote_dest_addr, uint64_t rkey, size_t size);

    /*
     * oob read
     * @param iov               The scatter memory vector, the total size of the source should not go beyond 'size'.
     * @param iovcnt            The length of the vector.
     * @param remote_src_addr   The remote address for receiving this message
     * @param rkey              The access key for the remote memory.
     * @param size              The size of the remote buffer
     *
     * @throws derecho_exception at failure.
     */
    void oob_remote_read(const struct iovec* iov, int iovcnt,
                         void* remote_src_addr, uint64_t rkey, size_t size);

    /*
     * oob send
     * @param iov               The gather memory vector.
     * @param iovcnt            The length of the vector.
     *
     * @throws derecho_exception at failure.
     */
    void oob_send(const struct iovec* iov, int iovcnt);

    /*
     * oob recv
     * @param iov               The gather memory vector.
     * @param iovcnt            The length of the vector.
     *
     * @throws derecho_exception at failure.
     */
    void oob_recv(const struct iovec* iov, int iovcnt);

#define OOB_OP_READ     0x0
#define OOB_OP_WRITE    0x1
#define OOB_OP_SEND     0x2
#define OOB_OP_RECV     0x3
    /*
     * Public callable wrapper around wait_for_thread_local_completion_entries()
     * @param op                The OOB operations, one of the following:
     *                          - OOB_OP_READ
     *                          - OOB_OP_WRITE
     *                          - OOB_OP_SEND
     *                          - OOB_OP_RECV
     *                          For most of the cases, we wait for only one completion. To allow an operation like
     *                          "exchange", which is to be implemented, we might need to write for two completions.
     * @param timeout_us        Timeout settings in microseconds.
     */
    void wait_for_oob_op(uint32_t op, uint64_t timeout_us);
};

class resources : public _resources {
public:
    resources(int r_id, uint8_t* write_addr, uint8_t* read_addr, int size_w,
              int size_r);
    /**
     * Report that the remote node this object is connected to has failed.
     * This will cause all future remote operations to be no-ops.
     */
    void report_failure();
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
    /** Post an RDMA write at the beginning address of remote memory, and also request a completion event for it. */
    void post_remote_write_with_completion(verbs_sender_ctxt* ce_ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory, and also request a completion event for it. */
    void post_remote_write_with_completion(verbs_sender_ctxt* ce_ctxt, const long long int offset, const long long int size);
};

class resources_two_sided : public _resources {
    int post_receive(verbs_sender_ctxt* ce_ctxt, const long long int offset, const long long int size);

public:
    resources_two_sided(int r_id, uint8_t* write_addr, uint8_t* read_addr, int size_w,
                        int size_r);
    /**
     * Report that the remote node this object is connected to has failed.
     * This will cause all future remote operations to be no-ops.
     */
    void report_failure();
    void post_two_sided_send(const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send(const long long int offset, long long int size);
    void post_two_sided_send_with_completion(verbs_sender_ctxt* ce_ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send_with_completion(verbs_sender_ctxt* ce_ctxt, const long long int offset, const long long int size);
    void post_two_sided_receive(verbs_sender_ctxt* ce_ctxt, const long long int size);
    void post_two_sided_receive(verbs_sender_ctxt* ce_ctxt, const long long int offset, const long long int size);
};

bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
bool add_external_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
bool remove_node(uint32_t node_id);

/*
 * Blocks the current rehad until both this node and a remote node reach this function.
 *
 * @param r_index
 *
 * @return
 */
bool sync(uint32_t r_index);

/**
 * Compares the set of external client connections to a list of known live nodes and
 * removes any connections to nodes not in that list. This is used to filter out
 * connections to nodes that were removed from the view.
 * @param live_nodes_list A list of node IDs whose connections should be retained;
 *        all other connections will be deleted.
 */
void filter_external_to(const std::vector<derecho::node_id_t>& live_nodes_list);

/** Initializes the global verbs resources. */
void verbs_initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_sst_ports,
                      const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_external_ports,
                      uint32_t node_id);
/** Polls for completion of a single posted remote write. */
std::pair<uint32_t, std::pair<int, int>> verbs_poll_completion();
void shutdown_polling_thread();
/** Destroys the global verbs resources. */
void verbs_destroy();

}  // namespace sst

#endif  // VERBS_HPP
