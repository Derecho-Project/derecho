/**
 * @file verbs.cpp
 * Contains the implementation of the IB Verbs adapter layer of %SST.
 */
#include <arpa/inet.h>
#include <byteswap.h>
#include <cstring>
#include <endian.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <iostream>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include "derecho/connection_manager.h"
#include "poll_utils.h"
#include "verbs.h"

using std::cout;
using std::cerr;
using std::endl;
using std::map;
using std::string;

#define MSG "SEND operation      "
#define RDMAMSGR "RDMA read operation "
#define RDMAMSGW "RDMA write operation"
#define MSG_SIZE (strlen(MSG) + 1)
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither
__LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

template <class T>
void check_for_error(T var, string msg) {
    if(!var) {
        cerr << msg << endl;
    }
}

namespace sst {
/** Completion Queue poll timeout in millisec */
const int MAX_POLL_CQ_TIMEOUT = 2000;
/** IB device name. */
const char *dev_name = NULL;
/** Local IB port to work with. */
int ib_port = 1;
/** GID index to use. */
int gid_idx = 0;

static const int port = 22549;
tcp::tcp_connections *sst_connections;

//  unsigned int max_time_to_completion = 0;

/** Structure containing global system resources. */
struct global_resources {
    /** RDMA device attributes. */
    struct ibv_device_attr device_attr;
    /** IB port attributes. */
    struct ibv_port_attr port_attr;
    /** Device handle. */
    struct ibv_context *ib_ctx;
    /** PD handle. */
    struct ibv_pd *pd;
    /** Completion Queue handle. */
    struct ibv_cq *cq;
};
/** The single instance of global_resources for the %SST system */
struct global_resources *g_res;

std::thread polling_thread;
static bool shutdown = false;

/**
 * Initializes the resources. Registers write_addr and read_addr as the read
 * and write buffers and connects a queue pair with the specified remote node.
 *
 * @param r_index The node rank of the remote node to connect to.
 * @param write_addr A pointer to the memory to use as the write buffer. This
 * is where data should be written locally in order to send it in an RDMA write
 * to the remote node.
 * @param read_addr A pointer to the memory to use as the read buffer. This is
 * where the results of RDMA reads from the remote node will arrive.
 * @param size_w The size of the write buffer (in bytes).
 * @param size_r The size of the read buffer (in bytes).
 */
resources::resources(int r_index, char *write_addr, char *read_addr, int size_w,
                     int size_r) {
    // set the remote index
    remote_index = r_index;

    write_buf = write_addr;
    check_for_error(write_buf, "Write address is NULL");

    read_buf = read_addr;
    check_for_error(read_buf, "Read address is NULL");

    // register the memory buffer
    int mr_flags = 0;
    // allow access for only local writes and remote reads
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
               IBV_ACCESS_REMOTE_WRITE;
    // register memory with the protection domain and the buffer
    write_mr = ibv_reg_mr(g_res->pd, write_buf, size_w, mr_flags);
    read_mr = ibv_reg_mr(g_res->pd, read_buf, size_r, mr_flags);
    check_for_error(
        write_mr,
        "Could not register memory region : write_mr, error code is : " +
            std::to_string(errno));
    check_for_error(
        read_mr,
        "Could not register memory region : read_mr, error code is : " +
            std::to_string(errno));

    // set the queue pair up for creation
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    // same completion queue for both send and receive operations
    qp_init_attr.send_cq = g_res->cq;
    qp_init_attr.recv_cq = g_res->cq;
    // allow a lot of requests at a time
    qp_init_attr.cap.max_send_wr = 10;
    qp_init_attr.cap.max_recv_wr = 10;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    // create the queue pair
    qp = ibv_create_qp(g_res->pd, &qp_init_attr);

    check_for_error(qp, "Could not create queue pair, error code is : " +
                            std::to_string(errno));

    // connect the QPs
    connect_qp();
    cout << "Established RDMA connection with node " << r_index << endl;
}

/**
 * Cleans up all IB Verbs resources associated with this connection.
 */
resources::~resources() {
    int rc = 0;
    if(qp) {
        rc = ibv_destroy_qp(qp);
        check_for_error(qp, "Could not destroy queue pair, error code is " +
                                std::to_string(rc));
    }

    if(write_mr) {
        rc = ibv_dereg_mr(write_mr);
        check_for_error(
            !rc,
            "Could not de-register memory region : write_mr, error code is " +
                std::to_string(rc));
    }
    if(read_mr) {
        rc = ibv_dereg_mr(read_mr);
        check_for_error(
            !rc,
            "Could not de-register memory region : read_mr, error code is " +
                std::to_string(rc));
    }
}

/**
 * This transitions the queue pair to the init state.
 */
void resources::set_qp_initialized() {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    // the init state
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    // give access to local writes and remote reads
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE;
    flags =
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    // modify the queue pair to init state
    rc = ibv_modify_qp(qp, &attr, flags);
    check_for_error(
        !rc, "Failed to modify queue pair to init state, error code is " +
                 std::to_string(rc));
}

void resources::set_qp_ready_to_receive() {
    struct ibv_qp_attr attr;
    int flags, rc;
    memset(&attr, 0, sizeof(attr));
    // change the state to ready to receive
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    // set the queue pair number of the remote side
    attr.dest_qp_num = remote_props.qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    // set the local id of the remote side
    attr.ah_attr.dlid = remote_props.lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    // the infiniband port to associate with
    attr.ah_attr.port_num = ib_port;
    if(gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, remote_props.gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    check_for_error(!rc,
                    "Failed to modify queue pair to ready-to-receive state, "
                    "error code is " +
                        std::to_string(rc));
}

void resources::set_qp_ready_to_send() {
    struct ibv_qp_attr attr;
    int flags, rc;
    memset(&attr, 0, sizeof(attr));
    // set the state to ready to send
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 4;  // The timeout is 4.096x2^(timeout) microseconds
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    check_for_error(
        !rc,
        "Failed to modify queue pair to ready-to-send state, error code is " +
            std::to_string(rc));
}

/**
 * This method implements the entire setup of the queue pairs, calling all the
 * `modify_qp_*` methods in the process.
 */
void resources::connect_qp() {
    // local connection data
    struct cm_con_data_t local_con_data;
    // remote connection data. Obtained via TCP
    struct cm_con_data_t remote_con_data;
    // this is used to ensure that host byte order is correct at each node
    struct cm_con_data_t tmp_con_data;

    union ibv_gid my_gid;
    if(gid_idx >= 0) {
        int rc = ibv_query_gid(g_res->ib_ctx, ib_port, gid_idx, &my_gid);
        check_for_error(!rc, "ibv_query_gid failed, error code is " +
                                 std::to_string(errno));
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    // exchange using TCP sockets info required to connect QPs
    local_con_data.addr = htonll((uintptr_t)(char *)write_buf);
    local_con_data.rkey = htonl(write_mr->rkey);
    local_con_data.qp_num = htonl(qp->qp_num);
    local_con_data.lid = htons(g_res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);
    bool success =
        sst_connections->exchange(remote_index, local_con_data, tmp_con_data);
    check_for_error(success,
                    "Could not exchange qp data in connect_qp");
    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
    // save the remote side attributes, we will need it for the post SR
    remote_props = remote_con_data;

    // modify the QP to init
    set_qp_initialized();

    // modify the QP to RTR
    set_qp_ready_to_receive();

    // modify it to RTS
    set_qp_ready_to_send();

    // sync to make sure that both sides are in states that they can connect to
    // prevent packet loss
    // just send a dummy char back and forth
    success = sync(remote_index);
    check_for_error(
        success,
        "Could not sync in connect_qp after qp transition to RTS state");
}

/**
 * This is used for both reads and writes.
 *
 * @param offset The offset within the remote buffer to start the operation at.
 * @param size The number of bytes to read or write.
 * @param op The operation mode; 0 is for read, 1 is for write.
 * @return The return code of the IB Verbs post_send operation.
 */
int resources::post_remote_send(uint32_t id, long long int offset, long long int size,
                                int op) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    // don't care where the read buffer is saved
    sge.addr = (uintptr_t)(read_buf + offset);
    sge.length = size;
    sge.lkey = read_mr->lkey;
    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    // set the id for the work request, useful at the time of polling
    sr.wr_id = id;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    // set opcode depending on op parameter
    if(op == 0) {
        sr.opcode = IBV_WR_RDMA_READ;
    } else {
        sr.opcode = IBV_WR_RDMA_WRITE;
    }
    sr.send_flags = IBV_SEND_SIGNALED;

    // set the remote rkey and virtual address
    sr.wr.rdma.remote_addr = remote_props.addr + offset;
    sr.wr.rdma.rkey = remote_props.rkey;

    // there is a receive request in the responder side, so we won't get any
    // into
    // RNR flow
    int ret_code = ibv_post_send(qp, &sr, &bad_wr);
    return ret_code;
}

/**
 * @param size The number of bytes to read from remote memory.
 */
void resources::post_remote_read(uint32_t id, long long int size) {
    int rc = post_remote_send(id, 0, size, 0);
    check_for_error(
        !rc, "Could not post RDMA read, error code is " + std::to_string(rc));
}
/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start reading.
 * @param size The number of bytes to read from remote memory.
 */
void resources::post_remote_read(uint32_t id, long long int offset, long long int size) {
    int rc = post_remote_send(id, offset, size, 0);
    check_for_error(
        !rc, "Could not post RDMA read, error code is " + std::to_string(rc));
}
/**
 * @param size The number of bytes to write from the local buffer to remote
 * memory.
 */
void resources::post_remote_write(uint32_t id, long long int size) {
    int rc = post_remote_send(id, 0, size, 1);
    check_for_error(
        !rc, "Could not post RDMA write, error code is " + std::to_string(rc));
}

/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start writing.
 * @param size The number of bytes to write from the local buffer into remote
 * memory.
 */
void resources::post_remote_write(uint32_t id, long long int offset, long long int size) {
    int rc = post_remote_send(id, offset, size, 1);
    check_for_error(
        !rc, "Could not post RDMA write, error code is " + std::to_string(rc));
}

void polling_loop() {
    std::cout << "Polling thread starting" << std::endl;
    while(!shutdown) {
        auto ce = verbs_poll_completion();
        util::polling_data.insert_completion_entry(ce.first, ce.second);
    }
    std::cout << "Polling thread ending" << std::endl;
}

/**
 * @details
 * This blocks until a single entry in the completion queue has
 * completed
 * It is exclusively used by the polling thread
 * the thread can sleep while in this function, when it calls util::polling_data.wait_for_requests
 * @return pair(qp_num,result) The queue pair number associated with the
 * completed request and the result (1 for successful, -1 for unsuccessful)
 */
std::pair<uint32_t, std::pair<int, int>> verbs_poll_completion() {
    struct ibv_wc wc;
    int poll_result;

    while(true) {
        poll_result = 0;
        for(int i = 0; i < 50; ++i) {
            poll_result = ibv_poll_cq(g_res->cq, 1, &wc);
            if(poll_result) {
                break;
            }
        }
        if(poll_result) {
            break;
        }
        // util::polling_data.wait_for_requests();
    }
    // not sure what to do when we cannot read entries off the CQ
    // this means that something is wrong with the local node
    if(poll_result < 0) {
        check_for_error(false, "Poll completion failed");
        exit(-1);
    }
    // check the completion status (here we don't care about the completion
    // opcode)
    if(wc.status != IBV_WC_SUCCESS) {
        cout << "got bad completion with status: 0x%x, vendor syndrome: "
             << wc.status << ", " << wc.vendor_err;
        return {wc.wr_id, {wc.qp_num, -1}};
    }
    return {wc.wr_id, {wc.qp_num, 1}};
}

/** Allocates memory for global RDMA resources. */
void resources_init() {
    // initialize the global resources
    g_res = (global_resources *)malloc(sizeof(global_resources));
    memset(g_res, 0, sizeof *g_res);
}

/** Creates global RDMA resources. */
void resources_create() {
    struct ibv_device **dev_list = NULL;
    struct ibv_device *ib_dev = NULL;
    int i;
    int cq_size = 0;
    int num_devices;
    int rc = 0;

    // get device names in the system
    dev_list = ibv_get_device_list(&num_devices);
    check_for_error(dev_list,
                    "ibv_get_device_list failed; returned a NULL list");

    // if there isn't any IB device in host
    check_for_error(num_devices, "NO RDMA device present");
    // search for the specific device we want to work with
    for(i = 0; i < num_devices; i++) {
        if(!dev_name) {
            dev_name = strdup(ibv_get_device_name(dev_list[i]));
        }
        if(!strcmp(ibv_get_device_name(dev_list[i]), dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }
    // if the device wasn't found in host
    check_for_error(ib_dev, "No RDMA devices found in the host");
    // get device handle
    g_res->ib_ctx = ibv_open_device(ib_dev);
    check_for_error(g_res->ib_ctx, "Could not open RDMA device");
    // we are now done with device list, free it
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    // query port properties
    rc = ibv_query_port(g_res->ib_ctx, ib_port, &g_res->port_attr);
    check_for_error(!rc, "Could not query port properties, error code is " +
                             std::to_string(rc));

    // allocate Protection Domain
    g_res->pd = ibv_alloc_pd(g_res->ib_ctx);
    check_for_error(g_res->pd, "Could not allocate protection domain");

    // get the device attributes for the device
    ibv_query_device(g_res->ib_ctx, &g_res->device_attr);

    // set to 1000 entries, we actually don't need more than the number of nodes
    cq_size = 1000;
    g_res->cq = ibv_create_cq(g_res->ib_ctx, cq_size, NULL, NULL, 0);
    check_for_error(g_res->cq,
                    "Could not create completion queue, error code is " +
                        std::to_string(errno));

    // start the polling thread
    polling_thread = std::thread(polling_loop);
}

bool add_node(uint32_t new_id, const string new_ip_addr) {
    return sst_connections->add_node(new_id, new_ip_addr);
}

/**
*@param r_index The node rank of the node to exchange data with.
*/
bool sync(uint32_t r_index) {
    int s = 0, t = 0;
    return sst_connections->exchange(r_index, s, t);
}

/**
 * @details
 * This must be called before creating or using any SST instance.
 */
void verbs_initialize(const map<uint32_t, string> &ip_addrs, uint32_t node_rank) {
    sst_connections = new tcp::tcp_connections(node_rank, ip_addrs, port);

    // init all of the resources, so cleanup will be easy
    resources_init();
    // create resources before using them
    resources_create();

    cout << "Initialized global RDMA resources" << endl;
}

/**
 * @details
 * This cleans up all the global resources used by the SST system, so it should
 * only be called once all SST instances have been destroyed.
 */
void verbs_destroy() {
    std::cout << "Waiting for polling thread to exit" << std::endl;
    shutdown = true;
    // int rc;
    // if(g_res->cq) {
    //     rc = ibv_destroy_cq(g_res->cq);
    //     check_for_error(!rc, "Could not destroy completion queue");
    // }
    // if(g_res->pd) {
    //     rc = ibv_dealloc_pd(g_res->pd);
    //     check_for_error(!rc, "Could not deallocate protection domain");
    // }
    // if(g_res->ib_ctx) {
    //     rc = ibv_close_device(g_res->ib_ctx);
    //     check_for_error(!rc, "Could not close RDMA device");
    // }

    // if (polling_thread.joinable()) {
    // polling_thread.join();
    // }
    std::cout << "Shutting down" << std::endl;
}

}  // namespace sst
