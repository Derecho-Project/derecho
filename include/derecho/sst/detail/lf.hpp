#ifndef LF_HPP
#define LF_HPP

/**
 * @file lf.h
 * Contains declarations needed for working with RDMA using LibFabric libraries,
 * including the Resources class and global setup functions.
 */

#include <iostream>
#include <map>
#include <rdma/fabric.h>
#include <rdma/fi_errno.h>
#include <thread>

#include <derecho/core/derecho_type_definitions.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/utils/logger.hpp>

#ifndef LF_VERSION
#define LF_VERSION FI_VERSION(1, 5)
#endif

namespace sst {

struct lf_sender_ctxt {
    uint32_t _ce_idx;     // index into the comepletion entry vector. - 0xFFFFFFFF for invalid
    uint32_t _remote_id;  // thread id of the sender
    // getters and setters
    uint32_t ce_idx() {return _ce_idx;}
    uint32_t remote_id() {return _remote_id;}
    void set_ce_idx(const uint32_t& idx) {_ce_idx = idx;}
    void set_remote_id(const uint32_t& rid) {_remote_id = rid;}
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
    int init_endpoint(struct fi_info* fi);

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
    int post_remote_send(struct lf_sender_ctxt* ctxt, const long long int offset, const long long int size,
                         const int op, const bool completion);

public:
    /** ID of the remote node. */
    int remote_id;
    /** tx/rx completion queue */
    // struct fid_cq *txcq, *rxcq; - moved to g_ctxt
    /** Handle for the LibFabric endpoint. */
    struct fid_ep* ep;
    /** memory region for remote writer */
    struct fid_mr* write_mr;
    /** memory region for remote writer */
    struct fid_mr* read_mr;
    /** Pointer to the memory buffer used for local writes.*/
    char* write_buf;
    /** Pointer to the memory buffer used for the results of RDMA remote reads. */
    char* read_buf;
    /** key for local read buffer */
    uint64_t mr_lrkey;
    /** key for local write buffer */
    uint64_t mr_lwkey;
    /** key for remote write buffer */
    uint64_t mr_rwkey;
    /** remote write memory address */
    fi_addr_t remote_fi_addr;
    /** the event queue */
    struct fid_eq* eq;

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
    _resources(int r_id, char* write_addr, char* read_addr, int size_w,
               int size_r, int is_lf_server);
    /** Destroys the resources. */
    virtual ~_resources();
};

/**
 * A public-facing version of the internal _resources class that extends it
 * with more convenient functions.
 */
class resources : public _resources {
public:
    /** Constructor: simply forwards to _resources::_resources */
    resources(int r_id, char* write_addr, char* read_addr, int size_w,
              int size_r, int is_lf_server) : _resources(r_id, write_addr, read_addr, size_w, size_r, is_lf_server) {
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
    void post_remote_write_with_completion(struct lf_sender_ctxt* ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_remote_write_with_completion(struct lf_sender_ctxt* ctxt, const long long int offset, const long long int size);
};

/**
 * A public-facing version of the internal _resources class that extends it
 * with functions that support two-sided sends and receives.
 */
class resources_two_sided : public _resources {
    int post_receive(struct lf_sender_ctxt* ctxt, const long long int offset, const long long int size);

public:
    /** constructor: simply forwards to _resources::_resources */
    resources_two_sided(int r_id, char* write_addr, char* read_addr, int size_w,
                        int size_r, int is_lf_server) : _resources(r_id, write_addr, read_addr, size_w, size_r, is_lf_server) {
    }

    void post_two_sided_send(const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send(const long long int offset, long long int size);
    void post_two_sided_send_with_completion(struct lf_sender_ctxt* ctxt, const long long int size);
    /** Post an RDMA write at an offset into remote memory. */
    void post_two_sided_send_with_completion(struct lf_sender_ctxt* ctxt, const long long int offset, const long long int size);
    void post_two_sided_receive(struct lf_sender_ctxt* ctxt, const long long int size);
    void post_two_sided_receive(struct lf_sender_ctxt* ctxt, const long long int offset, const long long int size);
};

/**
 * Adds a new node to the SST TCP connections set.
 */
bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
/**
 * Adds a new node to external client connections set.
 */
bool add_external_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
/**
 * Removes a node from the SST TCP connections set
 */
bool remove_node(uint32_t node_id);
/**
 * Blocks the current thread until both this node and a remote node reach this
 * function, which exchanges some trivial data over a TCP connection.
 * @param r_id - ID of the node to exchange data with.
 */
bool sync(uint32_t r_id);
/**
 * Compares the set of external client connections to a list of known live nodes and
 * removes any connections to nodes not in that list. This is used to
 * filter out connections to nodes that were removed from the view.
 * @param live_nodes_list A list of node IDs whose connections should be
 * retained; all other connections will be deleted.
 */
void filter_external_to(const std::vector<node_id_t>& live_nodes_list);
/** 
 * Initializes the global libfabric resources. Must be called before creating
 * or using any SST instance. 
 * 
 * @param internal_ip_addrs_and_ports A map from id to (IP address, port) pairs for internal group members
 * @param external_ip_addrs_and_ports A map from id to (IP address, port) pairs for external connections
 * @param node_id id of this node.
 */
void lf_initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& internal_ip_addrs_and_ports,
                   const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& external_ip_addrs_and_ports,
                   uint32_t node_id);
/** Polls for completion of a single posted remote write. */
std::pair<uint32_t, std::pair<int32_t, int32_t>> lf_poll_completion();
/** Shutdown the polling thread. */
void shutdown_polling_thread();
/** Destroys the global libfabric resources. */
void lf_destroy();

/* -------- Error-handling tools -------- */

/**
 * Internal-only enum describing what action error-handling functions should
 * take if a LibFabric function fails.
 */
enum NextOnFailure {
    REPORT_ON_FAILURE = 0,//!< REPORT_ON_FAILURE Print an error message, but continue
    CRASH_ON_FAILURE = 1  //!< CRASH_ON_FAILURE Print an error message, then exit the entire program
};

/**
 * Calls a LibFabrics function with any number of arguments forwarded via perfect
 * forwarding. If the function returns the FI_EAGAIN error code, keeps calling it
 * again with the same arguments. If the function returns any other error code,
 * prints an error message containing the "description" argument, then either
 * crashes the program or returns the error code depending on the value of the
 * "failure_mode" argument.
 *
 * @param description A description of the LibFabrics operation being performed,
 * for debugging purposes. Will be printed in the error message if this
 * operation fails.
 * @param failure_mode An enum value representing what to do if the LibFabric
 * function returns a failure other than EAGAIN
 * @param lf_function A pointer to the LibFabrics function to call
 * @param lf_args The arguments to call the LibFabrics function with
 * @return The same return value returned by the LibFabrics function
 */
template<typename FuncType, typename... ArgTypes>
inline int64_t fail_if_nonzero_retry_on_eagain(const std::string& description, const NextOnFailure& failure_mode,
                                               FuncType lf_function, ArgTypes&& ... lf_args) {
    //Some lf functions return int, others return ssize_t, but both will fit in an int64_t
    int64_t return_code;
    do {
        return_code = (*lf_function)(std::forward<ArgTypes>(lf_args)...);
    } while(return_code == -FI_EAGAIN);
    if(return_code != 0) {
        dbg_default_error("LibFabric error! Return code = {}. Operation description: {}", return_code, description);
        std::cerr << "LibFabric error! Ret=" << return_code << ", desc=" << description << std::endl;
        if(failure_mode == CRASH_ON_FAILURE) {
            dbg_default_flush();
            exit(-1);
        }
    }
    return return_code;
}

/**
 * Calls a C function that may return either a pointer or NULL, forwarding
 * all of its arguments by copy (because C functions don't understand
 * references). If the function succeeds, returns the pointer; if the function
 * returns NULL, prints an error message and crashes.
 *
 * @param description A description of the operation being performed, which
 * will be printed with the error message if it fails.
 * @param c_func A pointer to the C function to call
 * @param args The arguments to call the C function with
 * @return The pointer that the C function returns
 */
template<typename FuncType, typename... ArgTypes>
inline std::invoke_result_t<FuncType, ArgTypes...> crash_if_nullptr(const std::string& description,
                                                                    FuncType c_func, ArgTypes ... args) {
    std::invoke_result_t<FuncType, ArgTypes...> return_val = (*c_func)(args...);
    if(return_val == nullptr) {
        dbg_default_error("Null pointer error in lf.cpp! Description: {}", description);
        std::cerr << "Null pointer error in lf.cpp! Description: " << description << std::endl;
        dbg_default_flush();
        exit(-1);
    }
    return return_val;
}

}  // namespace sst

#endif  // LF_HPP
