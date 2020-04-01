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

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/sst/detail/verbs.hpp>
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/detail/sst_impl.hpp>
#include <derecho/tcp/tcp.hpp>
#include <derecho/utils/logger.hpp>

using std::cout;
using std::cerr;
using std::endl;

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

namespace sst {
/** Local IB port to work with. */
int ib_port = 1;
/** GID index to use. */
int gid_idx = 0;

tcp::tcp_connections* sst_connections;
tcp::tcp_connections* external_client_connections;

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
_resources::_resources(int r_index, char *write_addr, char *read_addr, int size_w,
                       int size_r) {
    // set the remote index
    remote_index = r_index;

    write_buf = write_addr;
    if(!write_buf) {
        cout << "Write address is NULL" << endl;
    }

    read_buf = read_addr;
    if(!read_buf) {
        cout << "Read address is NULL" << endl;
    }

    // register the memory buffer
    int mr_flags = 0;
    // allow access for only local writes and remote reads
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    // register memory with the protection domain and the buffer
    write_mr = ibv_reg_mr(g_res->pd, write_buf, size_w, mr_flags);
    read_mr = ibv_reg_mr(g_res->pd, read_buf, size_r, mr_flags);
    if(!write_mr) {
        cout << "Could not register memory region : write_mr, error code is: " << errno << endl;
    }
    if(!read_mr) {
        cout << "Could not register memory region : read_mr, error code is: " << errno << endl;
    }

    // set the queue pair up for creation
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    // same completion queue for both send and receive operations
    qp_init_attr.send_cq = g_res->cq;
    qp_init_attr.recv_cq = g_res->cq;
    // since we send the value first and the update the counter, we double the depth configurations.
    qp_init_attr.cap.max_send_wr = derecho::getConfUInt32(CONF_RDMA_TX_DEPTH)<<1;
    qp_init_attr.cap.max_recv_wr = derecho::getConfUInt32(CONF_RDMA_RX_DEPTH)<<1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    // create the queue pair
    qp = ibv_create_qp(g_res->pd, &qp_init_attr);

    if(!qp) {
        cout << "Could not create queue pair, error code is: " << errno << endl;
    }

    // connect the QPs
    connect_qp();

    // prepare the flow control counters
    without_completion_send_cnt = 0;
    // leave 20% queue pair space for ops with completion.
    // send signal every half of the capacity.
    without_completion_send_signal_interval = (derecho::getConfInt32(CONF_RDMA_TX_DEPTH)*4/5)/2;
    without_completion_send_capacity = without_completion_send_signal_interval*2;
    // sender context
    without_completion_sender_ctxt.type = verbs_sender_ctxt::INTERNAL_FLOW_CONTROL;
    without_completion_sender_ctxt.ctxt.res = this;

    cout << "Established RDMA connection with node " << r_index << endl;
}

/**
 * Cleans up all IB Verbs resources associated with this connection.
 */
_resources::~_resources() {
    int rc = 0;
    if(qp) {
        rc = ibv_destroy_qp(qp);
        if(!qp) {
            cout << "Could not destroy queue pair, error code is " << rc << endl;
        }
    }

    if(write_mr) {
        rc = ibv_dereg_mr(write_mr);
        if(rc) {
            cout << "Could not de-register memory region : write_mr, error code is " << rc << endl;
        }
    }
    if(read_mr) {
        rc = ibv_dereg_mr(read_mr);
        if(rc) {
            cout << "Could not de-register memory region : read_mr, error code is " << rc << endl;
        }
    }
}

/**
 * This transitions the queue pair to the init state.
 */
void _resources::set_qp_initialized() {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    // the init state
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    // give access to local writes and remote reads
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    // modify the queue pair to init state
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) {
        cout << "Failed to modify queue pair to init state, error code is " << rc << endl;
    }
}

void _resources::set_qp_ready_to_receive() {
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
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) {
        cout << "Failed to modify queue pair to ready-to-receive state, error code is " << rc << endl;
    }
}

void _resources::set_qp_ready_to_send() {
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
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) {
        cout << "Failed to modify queue pair to ready-to-send state, error code is " << rc << endl;
    }
}

/**
 * This method implements the entire setup of the queue pairs, calling all the
 * `modify_qp_*` methods in the process.
 */
void _resources::connect_qp() {
    // local connection data
    struct cm_con_data_t local_con_data;
    // remote connection data. Obtained via TCP
    struct cm_con_data_t remote_con_data;
    // this is used to ensure that host byte order is correct at each node
    struct cm_con_data_t tmp_con_data;

    union ibv_gid my_gid;
    if(gid_idx >= 0) {
        int rc = ibv_query_gid(g_res->ib_ctx, ib_port, gid_idx, &my_gid);
        if(rc) {
            cout << "ibv_query_gid failed, error code is " << errno << endl;
        }
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    // exchange using TCP sockets info required to connect QPs
    local_con_data.addr = htonll((uintptr_t)(char *)write_buf);
    local_con_data.rkey = htonl(write_mr->rkey);
    local_con_data.qp_num = htonl(qp->qp_num);
    local_con_data.lid = htons(g_res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);
    bool success = sst_connections->exchange(remote_index, local_con_data, tmp_con_data);
    if(!success) {
        cout << "Could not exchange qp data in connect_qp" << endl;
    }
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
    if(!success) {
        cout << "Could not sync in connect_qp after qp transition to RTS state" << endl;
    }
}

/**
 * This is used for both reads and writes.
 *
 * @param id
 * @param offset The offset within the remote buffer to start the operation at.
 * @param size The number of bytes to read or write.
 * @param op The operation mode; 0 is for read, 1 is for write.
 * @return The return code of the IB Verbs post_send operation.
 */
int _resources::post_remote_send(struct verbs_sender_ctxt* sctxt, const long long int offset, const long long int size,
                                 const int op, const bool completion) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;

    static std::atomic<long> counter = 0;

    // don't care where the read buffer is saved
    sge.addr = (uintptr_t)(read_buf + offset);
    sge.length = size;
    sge.lkey = read_mr->lkey;
    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    // set the id for the work request, useful at the time of polling
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    // set opcode depending on op parameter
    if(op == 0) {
        sr.opcode = IBV_WR_RDMA_READ;
    } else if(op == 1) {
        sr.opcode = IBV_WR_RDMA_WRITE;
    } else {
        sr.opcode = IBV_WR_SEND;
    }
    if(completion) {
        sr.send_flags = IBV_SEND_SIGNALED;
        if (sctxt == nullptr) {
            cerr << "post_remote_send(): sctxt cannot be nullptr for send with completion." << std::endl;
            return EINVAL;
        }
        sr.wr_id = reinterpret_cast<uint64_t>(sctxt);
    } else {
        // increase the counter.
        uint32_t my_slot = ++without_completion_send_cnt;
        if(my_slot > without_completion_send_capacity) {
            // spin on the counter until space released in queue pair
            while(without_completion_send_cnt >= without_completion_send_capacity);
        }
        // set signal flag if required.
        if ((my_slot+1)%without_completion_send_signal_interval == 0) {
            sr.send_flags = IBV_SEND_SIGNALED;
            sr.wr_id = reinterpret_cast<uint64_t>(&(this->without_completion_sender_ctxt));
        }
    }
    if(op == 0 || op == 1) {
        // set the remote rkey and virtual address
        sr.wr.rdma.remote_addr = remote_props.addr + offset;
        sr.wr.rdma.rkey = remote_props.rkey;
    }
    // there is a receive request in the responder side
    // , so we won't get any into RNR flow
    int ret;
    do {
        ret = ibv_post_send(qp, &sr, &bad_wr);
    } while(ret == ENOMEM);
    counter ++;
    return ret;
}

resources::resources(int r_index, char *write_addr, char *read_addr, int size_w,
                     int size_r) : _resources(r_index, write_addr, read_addr, size_w, size_r) {
}

/**
 * @param size The number of bytes to read from remote memory.
 */
void resources::post_remote_read(const long long int size) {
    int rc = post_remote_send(nullptr, 0, size, 0, false);
    if(rc) {
        cout << "Could not post RDMA read, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}
/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start reading.
 * @param size The number of bytes to read from remote memory.
 */
void resources::post_remote_read(const long long int offset, const long long int size) {
    int rc = post_remote_send(nullptr, offset, size, 0, false);
    if(rc) {
        cout << "Could not post RDMA read, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}
/**
 * @param size The number of bytes to write from the local buffer to remote
 * memory.
 */
void resources::post_remote_write(const long long int size) {
    int rc = post_remote_send(nullptr, 0, size, 1, false);
    if(rc) {
        cout << "Could not post RDMA write (with no offset), error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start writing.
 * @param size The number of bytes to write from the local buffer into remote
 * memory.
 */
void resources::post_remote_write(const long long int offset, const long long int size) {
    int rc = post_remote_send(nullptr, offset, size, 1, false);
    if(rc) {
        cout << "Could not post RDMA write with offset, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void resources::post_remote_write_with_completion(struct verbs_sender_ctxt* sctxt, const long long int size) {
    int rc = post_remote_send(sctxt, 0, size, 1, true);
    if(rc) {
        cout << "Could not post RDMA write (with no offset) with completion, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void resources::post_remote_write_with_completion(struct verbs_sender_ctxt* sctxt, const long long int offset, const long long int size) {
    int rc = post_remote_send(sctxt, offset, size, 1, true);
    if(rc) {
        cout << "Could not post RDMA write with offset and completion, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

resources_two_sided::resources_two_sided(int r_index, char *write_addr, char *read_addr, int size_w,
                                         int size_r) : _resources(r_index, write_addr, read_addr, size_w, size_r) {
}

/**
 * @param size The number of bytes to write from the local buffer to remote
 * memory.
 */
void resources_two_sided::post_two_sided_send(const long long int size) {
    int rc = post_remote_send(nullptr, 0, size, 2, false);
    if(rc) {
        cout << "Could not post RDMA two sided send (with no offset), error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start writing.
 * @param size The number of bytes to write from the local buffer into remote
 * memory.
 */
void resources_two_sided::post_two_sided_send(const long long int offset, const long long int size) {
    int rc = post_remote_send(nullptr, offset, size, 2, false);
    if(rc) {
        cout << "Could not post RDMA two sided send with offset, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void resources_two_sided::post_two_sided_send_with_completion(struct verbs_sender_ctxt* sctxt, const long long int size) {
    int rc = post_remote_send(sctxt, 0, size, 2, true);
    if(rc) {
        cout << "Could not post RDMA two sided send (with no offset) with completion, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void resources_two_sided::post_two_sided_send_with_completion(struct verbs_sender_ctxt* sctxt, const long long int offset, const long long int size) {
    int rc = post_remote_send(sctxt, offset, size, 2, true);
    if(rc) {
        cout << "Could not post RDMA two sided send with offset and completion, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

int resources_two_sided::post_receive(struct verbs_sender_ctxt* sctxt, const long long int offset, const long long int size) {
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;

    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(write_buf + offset);
    sge.length = size;
    sge.lkey = write_mr->lkey;
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = reinterpret_cast<uint64_t>(sctxt);
    rr.sg_list = &sge;
    rr.num_sge = 1;

    /* post the Receive Request to the RQ */
    auto ret = ibv_post_recv(qp, &rr, &bad_wr);
    return ret;
}

void resources_two_sided::post_two_sided_receive(struct verbs_sender_ctxt* sctxt, const long long int size) {
    int rc = post_receive(sctxt, 0, size);
    if(rc) {
        cout << "Could not post RDMA two sided receive (with no offset), error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void resources_two_sided::post_two_sided_receive(struct verbs_sender_ctxt* sctxt, const long long int offset, const long long int size) {
    int rc = post_receive(sctxt, offset, size);
    if(rc) {
        cout << "Could not post RDMA two sided receive with offset, error code is " << rc << ", remote_index is " << remote_index << endl;
    }
}

void polling_loop() {
    pthread_setname_np(pthread_self(), "sst_poll");
    cout << "Polling thread starting" << endl;
    while(!shutdown) {
        auto ce = verbs_poll_completion();
        util::polling_data.insert_completion_entry(ce.first, ce.second);
    }
    cout << "Polling thread ending" << endl;
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
    struct verbs_sender_ctxt* sctxt;

    while(!shutdown) {
        poll_result = 0;
        for(int i = 0; i < 50; ++i) {
            poll_result = ibv_poll_cq(g_res->cq, 1, &wc);
            if(poll_result) {
                break;
            }
        }
        if(poll_result) {
            // not sure what to do when we cannot read entries off the CQ
            // this means that something is wrong with the local node
            if(poll_result < 0) {
                cout << "Poll completion failed" << endl;
                exit(-1);
            }
            // check the completion status (here we don't care about the completion
            sctxt = reinterpret_cast<struct verbs_sender_ctxt*>(wc.wr_id);
            // opcode)
            if(wc.status != IBV_WC_SUCCESS) {
                cerr << "got bad completion with status: "
                     << wc.status << ", vendor syndrome: "
                     << wc.vendor_err << std::endl;
                if (sctxt == nullptr) {
                    cerr << "WARNING: unsignaled work request failed found in " << __func__ << "(). Ignoring..." << std::endl;
                } else if (sctxt->type == verbs_sender_ctxt::INTERNAL_FLOW_CONTROL) {
                    cerr << "WARNING: skip a bad completion for flow control of messages without completion."
                         << std::endl;
                    sctxt->ctxt.res->without_completion_send_cnt.fetch_sub(sctxt->ctxt.res->without_completion_send_signal_interval,std::memory_order_relaxed);
                    continue;
                } else if (sctxt->type == verbs_sender_ctxt::EXPLICIT_SEND_WITH_COMPLETION) {
                    return {sctxt->ce_idx(), {sctxt->remote_id(), -1}};
                }
            } else if (sctxt->type == verbs_sender_ctxt::INTERNAL_FLOW_CONTROL) {
                // internal flow control path, we continue to wait for the completion for explicit send with complition.
                sctxt->ctxt.res->without_completion_send_cnt.fetch_sub(sctxt->ctxt.res->without_completion_send_signal_interval,std::memory_order_relaxed);
                continue;
            } else if (sctxt->type == verbs_sender_ctxt::EXPLICIT_SEND_WITH_COMPLETION) {
                // normal path
                break;
            } else {
                // this should not happen.
                cerr << "WARNING: unknown sender context type:" << sctxt->type << "." << std::endl;
            }
        }
        // util::polling_data.wait_for_requests();
    }
    return {sctxt->ce_idx(), {sctxt->remote_id(), 1}};
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
    int num_devices;
    int rc = 0;

    // get device names in the system
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list) {
        cout << "ibv_get_device_list failed; returned a NULL list" << endl;
    }

    // if there isn't any IB device in host
    if(!num_devices) {
        cout << "NO RDMA device present" << endl;
    }
    // search for the specific device we want to work with
    char* dev_name = strdup(derecho::getConfString(CONF_RDMA_DOMAIN).c_str());
    for(i = 0; i < num_devices; i++) {
        if(!dev_name) {
            dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n",
                    dev_name);
        }
        if(!strcmp(ibv_get_device_name(dev_list[i]), dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }
    free(static_cast<void*>(dev_name));
    // if the device wasn't found in host
    if(!ib_dev) {
        cout << "No RDMA devices found in the host" << endl;
    }
    // get device handle
    g_res->ib_ctx = ibv_open_device(ib_dev);
    if(!g_res->ib_ctx) {
        cout << "Could not open RDMA device" << endl;
    }
    // we are now done with device list, free it
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    // query port properties
    rc = ibv_query_port(g_res->ib_ctx, ib_port, &g_res->port_attr);
    if(rc) {
        cout << "Could not query port properties, error code is " << rc << endl;
    }

    // allocate Protection Domain
    g_res->pd = ibv_alloc_pd(g_res->ib_ctx);
    if(!g_res->pd) {
        cout << "Could not allocate protection domain" << endl;
    }

    // get the device attributes for the device
    ibv_query_device(g_res->ib_ctx, &g_res->device_attr);

    // cout << "device_attr.max_qp_wr = " << g_res->device_attr.max_qp_wr << endl;
    // cout << "device_attr.max_cqe = " << g_res->device_attr.max_cqe << endl;

    // set to many entries
    int cq_size = 1000;
    g_res->cq = ibv_create_cq(g_res->ib_ctx, cq_size, NULL, NULL, 0);
    if(!g_res->cq) {
        cout << "Could not create completion queue, error code is " << errno << endl;
    }

    // start the polling thread
    polling_thread = std::thread(polling_loop);
    polling_thread.detach();
}

bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return sst_connections->add_node(new_id, new_ip_addr_and_port);
}

bool add_external_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return external_client_connections->add_node(new_id, new_ip_addr_and_port);
}

bool remove_node(uint32_t node_id) {
    if (sst_connections->contains_node(node_id)) {
        return sst_connections->delete_node(node_id);
    } else {
        return external_client_connections->delete_node(node_id);
    }
}

bool sync(uint32_t r_index) {
    int s = 0, t = 0;
    if (sst_connections->contains_node(r_index)) {
        return sst_connections->exchange(r_index, s, t);
    } else {
        return external_client_connections->exchange(r_index, s, t);
    }
}

void filter_external_to(const std::vector<node_id_t>& live_nodes_list) {
    external_client_connections->filter_to(live_nodes_list);
}

/**
 * @details
 * This must be called before creating or using any SST instance.
 */
void verbs_initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> &ip_addrs_and_sst_ports,
                        const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> &ip_addrs_and_external_ports, 
                        uint32_t node_id) {
    sst_connections = new tcp::tcp_connections(node_id, ip_addrs_and_sst_ports);
    external_client_connections = new tcp::tcp_connections(node_id, ip_addrs_and_external_ports);

    // init all of the resources, so cleanup will be easy
    resources_init();
    // create resources before using them
    resources_create();

    cout << "Initialized global RDMA resources" << endl;
}

void shutdown_polling_thread() {
    shutdown = true;
}

/**
 * @details
 * This cleans up all the global resources used by the SST system, so it should
 * only be called once all SST instances have been destroyed.
 */
void verbs_destroy() {
    shutdown = true;
    // int rc;
    // if(g_res->cq) {
    //     rc = ibv_destroy_cq(g_res->cq);
    //     if(rc) {
    //         cout << "Could not destroy completion queue" << endl;
    //     }
    // }
    // if(g_res->pd) {
    //     rc = ibv_dealloc_pd(g_res->pd);
    //     if(rc) {
    //         cout << "Could not deallocate protection domain" << endl;
    //     }
    // }
    // if(g_res->ib_ctx) {
    //     rc = ibv_close_device(g_res->ib_ctx);
    //     if(rc) {
    //         cout << "Could not close RDMA device" << endl;
    //     }
    // }
    delete sst_connections;
    cout << "SST Verbs shutting down" << endl;
}

}  // namespace sst
