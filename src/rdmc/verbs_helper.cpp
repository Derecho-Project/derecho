#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <mutex>
#include <poll.h>
#include <thread>
#include <vector>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/rdmc/detail/util.hpp>
#include <derecho/rdmc/detail/verbs_helper.hpp>
#include <derecho/tcp/tcp.hpp>
#include <derecho/utils/logger.hpp>

extern "C" {
#include <infiniband/verbs.h>
}

#ifdef INFINIBAND_VERBS_EXP_H
#define MELLANOX_EXPERIMENTAL_VERBS
#endif

using namespace std;

namespace rdma {

struct config_t {
    char* dev_name;   // IB device name
    int ib_port = 1;  // local IB port to work with
    int gid_idx = 0;  // gid index to use
    ~config_t() {
        if(dev_name) {
            free(dev_name);
        }
    }
};

// structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
} __attribute__((packed));

// sockets for each connection
tcp::tcp_connections* rdmc_connections;

// listener to detect new incoming connections
static unique_ptr<tcp::connection_listener> connection_listener;

static config_t local_config;

// structure of system resources
struct ibv_resources {
    ibv_device_attr device_attr;  // Device attributes
    ibv_port_attr port_attr;      // IB port attributes
    ibv_context* ib_ctx;          // device handle
    ibv_pd* pd;                   // PD handle
    ibv_cq* cq;                   // CQ handle
    ibv_comp_channel* cc;         // Completion channel
} verbs_resources;

struct completion_handler_set {
    completion_handler send;
    completion_handler recv;
    completion_handler write;
    string name;
};
static vector<completion_handler_set> completion_handlers;
static std::mutex completion_handlers_mutex;

static atomic<bool> interrupt_mode;
static atomic<bool> contiguous_memory_mode;

static feature_set supported_features;

static atomic<bool> polling_loop_shutdown_flag;
static void polling_loop() {
    pthread_setname_np(pthread_self(), "rdmc_poll");
    TRACE("Spawned main loop");

    const int max_work_completions = 1024;
    unique_ptr<ibv_wc[]> work_completions(new ibv_wc[max_work_completions]);

    while(true) {
        int num_completions = 0;
        while(num_completions == 0) {
            if(polling_loop_shutdown_flag) return;
            uint64_t poll_end = get_time() + (interrupt_mode ? 0L : 50000000L);
            do {
                if(polling_loop_shutdown_flag) return;
                num_completions = ibv_poll_cq(verbs_resources.cq, max_work_completions,
                                              work_completions.get());
            } while(num_completions == 0 && get_time() < poll_end);

            if(num_completions == 0) {
                if(ibv_req_notify_cq(verbs_resources.cq, 0))
                    throw rdma::exception();

                num_completions = ibv_poll_cq(verbs_resources.cq, max_work_completions,
                                              work_completions.get());

                if(num_completions == 0) {
                    pollfd file_descriptor;
                    file_descriptor.fd = verbs_resources.cc->fd;
                    file_descriptor.events = POLLIN;
                    file_descriptor.revents = 0;
                    int rc = 0;
                    while(rc == 0 && !polling_loop_shutdown_flag) {
                        if(polling_loop_shutdown_flag) return;
                        rc = poll(&file_descriptor, 1, 50);
                    }

                    if(rc > 0) {
                        ibv_cq* ev_cq;
                        void* ev_ctx;
                        ibv_get_cq_event(verbs_resources.cc, &ev_cq, &ev_ctx);
                        ibv_ack_cq_events(ev_cq, 1);
                    }
                }
            }
        }

        if(num_completions < 0) {  // Negative indicates an IBV error.
            fprintf(stderr, "Failed to poll completion queue.");
            continue;
        }

        std::lock_guard<std::mutex> l(completion_handlers_mutex);
        for(int i = 0; i < num_completions; i++) {
            ibv_wc& wc = work_completions[i];

            if(wc.status == 5) continue;  // Queue Flush
            if(wc.status != 0) {
                string opcode = "[unknown]";
                if(wc.opcode == IBV_WC_SEND) opcode = "IBV_WC_SEND";
                if(wc.opcode == IBV_WC_RECV) opcode = "IBV_WC_RECV";
                if(wc.opcode == IBV_WC_RDMA_WRITE) opcode = "IBV_WC_RDMA_WRITE";

                // Failed operation
                printf("wc.status = %d; wc.wr_id = 0x%llx; imm = 0x%x; "
                       "opcode = %s\n",
                       (int)wc.status, (long long)wc.wr_id,
                       (unsigned int)wc.imm_data, opcode.c_str());
                fflush(stdout);
            }

            message_type::tag_type type = wc.wr_id >> message_type::shift_bits;
            if(type == std::numeric_limits<message_type::tag_type>::max())
                continue;

            uint64_t masked_wr_id = wc.wr_id & 0x00ffffffffffffff;
            if(type >= completion_handlers.size()) {
                // Unrecognized message type
            } else if(wc.status != 0) {
                // Failed operation
            } else if(wc.opcode == IBV_WC_SEND) {
                completion_handlers[type].send(masked_wr_id, wc.imm_data,
                                               wc.byte_len);
            } else if(wc.opcode == IBV_WC_RECV) {
                completion_handlers[type].recv(masked_wr_id, wc.imm_data,
                                               wc.byte_len);
            } else if(wc.opcode == IBV_WC_RDMA_WRITE) {
                completion_handlers[type].write(masked_wr_id, wc.imm_data,
                                                wc.byte_len);
            } else {
                puts("Sent unrecognized completion type?!");
            }
        }
    }
}

static int modify_qp_to_init(struct ibv_qp* qp, int ib_port) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}

static int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn,
                            uint16_t dlid, uint8_t* dgid, int ib_port,
                            int gid_idx) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = verbs_resources.port_attr.active_mtu;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 16;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;
    if(gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 0xFF;
        attr.ah_attr.grh.sgid_index = gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTR\n");
    return rc;
}

static int modify_qp_to_rts(struct ibv_qp* qp) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 4;
    attr.retry_cnt = 6;
    attr.rnr_retry = 6;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTS. ERRNO=%d\n", rc);
    return rc;
}

namespace impl {
void verbs_destroy() {
    if(verbs_resources.cq && ibv_destroy_cq(verbs_resources.cq)) {
        fprintf(stderr, "failed to destroy CQ\n");
    }
    if(verbs_resources.cc && ibv_destroy_comp_channel(verbs_resources.cc)) {
        fprintf(stderr, "failed to destroy Completion Channel\n");
    }
    if(verbs_resources.pd && ibv_dealloc_pd(verbs_resources.pd)) {
        fprintf(stderr, "failed to deallocate PD\n");
    }
    if(verbs_resources.ib_ctx && ibv_close_device(verbs_resources.ib_ctx)) {
        fprintf(stderr, "failed to close device context\n");
    }
}

bool verbs_initialize(const map<uint32_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_ports,
                      uint32_t node_id) {
    rdmc_connections = new tcp::tcp_connections(node_id, ip_addrs_and_ports);
    memset(&verbs_resources, 0, sizeof(verbs_resources));
    auto res = &verbs_resources;

    ibv_device** dev_list = NULL;
    ibv_device* ib_dev = NULL;
    int i;
    int cq_size = 0;
    int num_devices = 0;

    fprintf(stdout, "searching for IB devices in host\n");
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list) {
        fprintf(stderr, "failed to get IB devices list\n");
        goto resources_create_exit;
    }
    /* if there isn't any IB device in host */
    if(!num_devices) {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        goto resources_create_exit;
    }

    local_config.dev_name = strdup(derecho::getConfString(CONF_RDMA_DOMAIN).c_str());
    fprintf(stdout, "found %d device(s)\n", num_devices);
    /* search for the specific device we want to work with */
    for(i = 0; i < num_devices; i++) {
        if(!local_config.dev_name) {
            local_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n",
                    local_config.dev_name);
        }
        if(!strcmp(ibv_get_device_name(dev_list[i]), local_config.dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }
    /* if the device wasn't found in host */
    if(!ib_dev) {
        fprintf(stderr, "IB device %s wasn't found\n", local_config.dev_name);
        goto resources_create_exit;
    }
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if(!res->ib_ctx) {
        fprintf(stderr, "failed to open device %s\n", local_config.dev_name);
        goto resources_create_exit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    /* query port properties  */
    if(ibv_query_port(res->ib_ctx, local_config.ib_port, &res->port_attr)) {
        fprintf(stderr, "ibv_query_port on port %u failed\n",
                local_config.ib_port);
        goto resources_create_exit;
    }
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if(!res->pd) {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        goto resources_create_exit;
    }

    res->cc = ibv_create_comp_channel(res->ib_ctx);
    if(!res->cc) {
        fprintf(stderr, "ibv_create_comp_channel failed\n");
        goto resources_create_exit;
    }

    if(fcntl(res->cc->fd, F_SETFL, fcntl(res->cc->fd, F_GETFL) | O_NONBLOCK)) {
        fprintf(stderr,
                "failed to change file descriptor for completion channel\n");
        goto resources_create_exit;
    }

    cq_size = 1024;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, res->cc, 0);
    if(!res->cq) {
        fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
        goto resources_create_exit;
    }

    set_interrupt_mode(false);
    set_contiguous_memory_mode(true);

    // Initialize the ignored message type.
    (void)message_type::ignored();

// Detect experimental features
#ifdef MELLANOX_EXPERIMENTAL_VERBS
    {
        supported_features.contiguous_memory = true;

        ibv_exp_device_attr attr;
        attr.comp_mask = 0;
        int ret = ibv_exp_query_device(res->ib_ctx, &attr);
        if(ret == 0) {
            supported_features.cross_channel = attr.exp_device_cap_flags & IBV_EXP_DEVICE_CROSS_CHANNEL;
        }
    }
#endif

    {
        thread t(polling_loop);
        t.detach();
    }

    TRACE("verbs_initialize() - SUCCESS");
    return true;
resources_create_exit:
    TRACE("verbs_initialize() - ERROR!!!!!!!!!!!!!!");
    if(res->cq) {
        ibv_destroy_cq(res->cq);
        res->cq = NULL;
    }
    if(res->cq) {
        ibv_destroy_comp_channel(res->cc);
        res->cc = NULL;
    }
    if(res->pd) {
        ibv_dealloc_pd(res->pd);
        res->pd = NULL;
    }
    if(res->ib_ctx) {
        ibv_close_device(res->ib_ctx);
        res->ib_ctx = NULL;
    }
    if(dev_list) {
        ibv_free_device_list(dev_list);
        dev_list = NULL;
    }
    return false;
}

/**
 * Adds a node to the group via tcp.
 */
bool verbs_add_connection(
        uint32_t new_id,
        const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return rdmc_connections->add_node(new_id, new_ip_addr_and_port);
}

bool verbs_remove_connection(uint32_t node_id) {
    return rdmc_connections->delete_node(node_id);
}
bool set_interrupt_mode(bool enabled) {
    interrupt_mode = enabled;
    return true;
}
bool set_contiguous_memory_mode(bool enabled) {
#ifdef MELLANOX_EXPERIMENTAL_VERBS
    contiguous_memory_mode = enabled;
    return true;
#else
    return false;
#endif
}
}  // namespace impl

using ibv_mr_unique_ptr = unique_ptr<ibv_mr, std::function<void(ibv_mr*)>>;
static ibv_mr_unique_ptr create_mr(char* buffer, size_t size) {
    if(!buffer || size == 0) throw rdma::invalid_args();

    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    ibv_mr_unique_ptr mr = ibv_mr_unique_ptr(
            ibv_reg_mr(verbs_resources.pd, (void*)buffer, size, mr_flags),
            [](ibv_mr* m) { ibv_dereg_mr(m); });

    if(!mr) {
        throw rdma::mr_creation_failure();
    }
    return mr;
}
#ifdef MELLANOX_EXPERIMENTAL_VERBS
static ibv_mr_unique_ptr create_contiguous_mr(size_t size) {
    if(size == 0) throw rdma::invalid_args();

    ibv_exp_reg_mr_in in;
    in.pd = verbs_resources.pd;
    in.addr = 0;
    in.length = size;
    in.exp_access = IBV_EXP_ACCESS_LOCAL_WRITE | IBV_EXP_ACCESS_REMOTE_READ | IBV_EXP_ACCESS_REMOTE_WRITE | IBV_EXP_ACCESS_ALLOCATE_MR;
    in.create_flags = IBV_EXP_REG_MR_CREATE_CONTIG;
    in.comp_mask = IBV_EXP_REG_MR_CREATE_FLAGS;
    ibv_mr_unique_ptr mr = ibv_mr_unique_ptr(ibv_exp_reg_mr(&in),
                                             [](ibv_mr* m) { ibv_dereg_mr(m); });
    if(!mr) {
        throw rdma::mr_creation_failure();
    }
    return mr;
}
memory_region::memory_region(size_t s, bool contiguous)
        : mr(contiguous ? create_contiguous_mr(s) : create_mr(new char[s], s)),
          buffer((char*)mr->addr),
          size(s) {
    if(contiguous) {
        memset(buffer, 0, size);
    } else {
        allocated_buffer.reset(buffer);
    }
}
#else
memory_region::memory_region(size_t s, bool contiguous) : memory_region(new char[s], s) {
    allocated_buffer.reset(buffer);
}
#endif

memory_region::memory_region(size_t s) : memory_region(s, contiguous_memory_mode) {}
memory_region::memory_region(char* buf, size_t s) : mr(create_mr(buf, s)), buffer(buf), size(s) {}

uint32_t memory_region::get_rkey() const { return mr->rkey; }

completion_queue::completion_queue(bool cross_channel) {
    ibv_cq* cq_ptr = nullptr;
    if(!cross_channel) {
        cq_ptr = ibv_create_cq(verbs_resources.ib_ctx, 1024, nullptr, nullptr, 0);
    } else {
#ifdef MELLANOX_EXPERIMENTAL_VERBS
        ibv_exp_cq_init_attr attr;
        attr.comp_mask = IBV_EXP_CQ_INIT_ATTR_FLAGS;
        attr.flags = IBV_EXP_CQ_CREATE_CROSS_CHANNEL;
        cq_ptr = ibv_exp_create_cq(verbs_resources.ib_ctx, 1024, nullptr,
                                   nullptr, 0, &attr);

        ibv_exp_cq_attr mod_attr;
        mod_attr.comp_mask = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS;
        mod_attr.cq_cap_flags = IBV_EXP_CQ_IGNORE_OVERRUN;
        ibv_exp_modify_cq(cq_ptr, &mod_attr, IBV_EXP_CQ_CAP_FLAGS);
#else
        throw invalid_args();
#endif
    }
    if(!cq_ptr) {
        throw cq_creation_failure();
    }

    cq = decltype(cq)(cq_ptr, [](ibv_cq* q) { ibv_destroy_cq(q); });
}

queue_pair::~queue_pair() {
    //    if(qp) cout << "Destroying Queue Pair..." << endl;
}
queue_pair::queue_pair(size_t remote_index)
        : queue_pair(remote_index, [](queue_pair*) {}) {}

// The post_recvs lambda will be called before queue_pair creation completes on
// either end of the connection. This enables the user to avoid race conditions
// between post_send() and post_recv().
queue_pair::queue_pair(size_t remote_index,
                       std::function<void(queue_pair*)> post_recvs) {
    ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = verbs_resources.cq;
    qp_init_attr.recv_cq = verbs_resources.cq;
    qp_init_attr.cap.max_send_wr = derecho::getConfUInt32(CONF_RDMA_TX_DEPTH);
    qp_init_attr.cap.max_recv_wr = derecho::getConfUInt32(CONF_RDMA_RX_DEPTH);
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    qp = unique_ptr<ibv_qp, std::function<void(ibv_qp*)>>(
            ibv_create_qp(verbs_resources.pd, &qp_init_attr),
            [](ibv_qp* q) { ibv_destroy_qp(q); });

    if(!qp) {
        fprintf(stderr, "failed to create QP\n");
        throw rdma::qp_creation_failure();
    }

    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    memset(&local_con_data, 0, sizeof(local_con_data));
    memset(&remote_con_data, 0, sizeof(remote_con_data));
    union ibv_gid my_gid;

    if(local_config.gid_idx >= 0) {
        int rc = ibv_query_gid(verbs_resources.ib_ctx, local_config.ib_port,
                               local_config.gid_idx, &my_gid);
        if(rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    local_config.ib_port, local_config.gid_idx);
            return;
        }
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.qp_num = qp->qp_num;
    local_con_data.lid = verbs_resources.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    // fprintf(stdout, "Local QP number  = 0x%x\n", qp->qp_num);
    // fprintf(stdout, "Local LID        = 0x%x\n",
    // verbs_resources.port_attr.lid);

    if(!rdmc_connections->exchange(remote_index, local_con_data, remote_con_data))
        throw rdma::qp_creation_failure();

    bool success = !modify_qp_to_init(qp.get(), local_config.ib_port) && !modify_qp_to_rtr(qp.get(), remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid, local_config.ib_port, local_config.gid_idx) && !modify_qp_to_rts(qp.get());

    if(!success) printf("Failed to initialize QP\n");

    post_recvs(this);

    /* sync to make sure that both sides are in states that they can connect to
   * prevent packet loss */
    /* just send a dummy char back and forth */
    int tmp = -1;
    if(!rdmc_connections->exchange(remote_index, 0, tmp) || tmp != 0) throw rdma::qp_creation_failure();
}
bool queue_pair::post_send(const memory_region& mr, size_t offset,
                           size_t length, uint64_t wr_id, uint32_t immediate,
                           const message_type& type) {
    if(mr.size < offset + length || wr_id >> type.shift_bits || !type.tag)
        throw invalid_args();

    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr* bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    sr.imm_data = immediate;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}
bool queue_pair::post_empty_send(uint64_t wr_id, uint32_t immediate,
                                 const message_type& type) {
    if(wr_id >> type.shift_bits || !type.tag) throw invalid_args();

    ibv_send_wr sr;
    ibv_send_wr* bad_wr = NULL;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    sr.imm_data = immediate;
    sr.sg_list = NULL;
    sr.num_sge = 0;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

bool queue_pair::post_recv(const memory_region& mr, size_t offset,
                           size_t length, uint64_t wr_id,
                           const message_type& type) {
    if(mr.size < offset + length || wr_id >> type.shift_bits || !type.tag)
        throw invalid_args();

    ibv_recv_wr rr;
    ibv_sge sge;
    ibv_recv_wr* bad_wr;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    rr.sg_list = &sge;
    rr.num_sge = 1;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_empty_recv(uint64_t wr_id, const message_type& type) {
    if(wr_id >> type.shift_bits || !type.tag) throw invalid_args();

    ibv_recv_wr rr;
    ibv_recv_wr* bad_wr;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    rr.sg_list = NULL;
    rr.num_sge = 0;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_write(const memory_region& mr, size_t offset,
                            size_t length, uint64_t wr_id,
                            remote_memory_region remote_mr,
                            size_t remote_offset, const message_type& type,
                            bool signaled, bool send_inline) {
    if(wr_id >> type.shift_bits || !type.tag) throw invalid_args();
    if(mr.size < offset + length || remote_mr.size < remote_offset + length) {
        cout << "mr.size = " << mr.size << " offset = " << offset
             << " length = " << length << " remote_mr.size = " << remote_mr.size
             << " remote_offset = " << remote_offset;
        return false;
    }

    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr* bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    sr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0) | (send_inline ? IBV_SEND_INLINE : 0);
    sr.wr.rdma.remote_addr = remote_mr.buffer + remote_offset;
    sr.wr.rdma.rkey = remote_mr.rkey;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

#ifdef MELLANOX_EXPERIMENTAL_VERBS
managed_queue_pair::managed_queue_pair(
        size_t remote_index, std::function<void(managed_queue_pair*)> post_recvs)
        : queue_pair(), scq(true), rcq(true) {
    ibv_exp_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = nullptr;
    attr.send_cq = scq.cq.get();
    attr.recv_cq = rcq.cq.get();
    attr.srq = nullptr;
    attr.cap.max_send_wr = 1024;
    attr.cap.max_recv_wr = 1024;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;
    attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
    attr.pd = verbs_resources.pd;
    attr.xrcd = nullptr;
    attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL | IBV_EXP_QP_CREATE_MANAGED_SEND;
    attr.max_inl_recv = 0;

    qp = decltype(qp)(ibv_exp_create_qp(verbs_resources.ib_ctx, &attr),
                      [](ibv_qp* q) { ibv_destroy_qp(q); });

    if(!qp) {
        fprintf(stderr, "failed to create QP, (errno = %s)\n", strerror(errno));
        throw rdma::qp_creation_failure();
    }

    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    memset(&local_con_data, 0, sizeof(local_con_data));
    memset(&remote_con_data, 0, sizeof(remote_con_data));
    union ibv_gid my_gid;

    if(local_config.gid_idx >= 0) {
        int rc = ibv_query_gid(verbs_resources.ib_ctx, local_config.ib_port,
                               local_config.gid_idx, &my_gid);
        if(rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    local_config.ib_port, local_config.gid_idx);
            return;
        }
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.qp_num = qp->qp_num;
    local_con_data.lid = verbs_resources.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    // fprintf(stdout, "Local QP number  = 0x%x\n", qp->qp_num);
    // fprintf(stdout, "Local LID        = 0x%x\n",
    // verbs_resources.port_attr.lid);

    if(!rdmc_connections->exchange(remote_index, local_con_data, remote_con_data))
        throw rdma::qp_creation_failure();

    bool success = !modify_qp_to_init(qp.get(), local_config.ib_port) && !modify_qp_to_rtr(qp.get(), remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid, local_config.ib_port, local_config.gid_idx) && !modify_qp_to_rts(qp.get());

    if(!success) throw rdma::qp_creation_failure();

    post_recvs(this);

    // Sync to make sure that both sides are in states that they can connect to
    // prevent packet loss.
    int tmp = -1;
    if(!rdmc_connections->exchange(remote_index, 0, tmp) || tmp != 0) throw rdma::qp_creation_failure();
}
manager_queue_pair::manager_queue_pair() : queue_pair() {
    ibv_exp_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = nullptr;
    attr.send_cq = verbs_resources.cq;
    attr.recv_cq = verbs_resources.cq;
    attr.srq = nullptr;
    attr.cap.max_send_wr = 1024;
    attr.cap.max_recv_wr = 0;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;
    attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
    attr.pd = verbs_resources.pd;
    attr.xrcd = nullptr;
    attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL;
    attr.max_inl_recv = 0;

    qp = decltype(qp)(ibv_exp_create_qp(verbs_resources.ib_ctx, &attr),
                      [](ibv_qp* q) { ibv_destroy_qp(q); });

    if(!qp) {
        fprintf(stderr, "failed to create QP, (errno = %s)\n", strerror(errno));
        throw rdma::qp_creation_failure();
    }

    bool success = !modify_qp_to_init(qp.get(), local_config.ib_port) && !modify_qp_to_rtr(qp.get(), qp->qp_num, 0, nullptr, local_config.ib_port, -1) && !modify_qp_to_rts(qp.get());

    if(!success) throw rdma::qp_creation_failure();
}

struct task::task_impl {
    struct wr_list {
        size_t start_index;
        size_t end_index;
    };

    std::list<ibv_sge> sges;
    vector<ibv_recv_wr> recv_wrs;
    vector<ibv_exp_send_wr> send_wrs;

    map<ibv_qp*, vector<size_t>> recv_list;
    map<ibv_qp*, vector<size_t>> send_list;
    vector<size_t> mqp_list;
    ibv_qp* mqp;

    task_impl(ibv_qp* mqp_ptr) : mqp(mqp_ptr) {}
};

task::task(std::shared_ptr<manager_queue_pair> manager_qp)
        : impl(new task_impl(manager_qp->qp.get())), mqp(manager_qp) {}
task::~task() {}

void task::append_wait(const completion_queue& cq, int count, bool signaled,
                       bool last, uint64_t wr_id, const message_type& type) {
    impl->send_wrs.emplace_back();
    auto& wr = impl->send_wrs.back();
    wr.wr_id = wr_id | ((uint64_t)*type.tag << type.shift_bits);
    wr.sg_list = nullptr;
    wr.num_sge = 0;
    wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
    wr.exp_send_flags = (signaled ? IBV_SEND_SIGNALED : 0) | (last ? IBV_EXP_SEND_WAIT_EN_LAST : 0);
    wr.ex.imm_data = 0;
    wr.task.cqe_wait.cq = cq.cq.get();
    wr.task.cqe_wait.cq_count = count;
    wr.comp_mask = 0;
    wr.next = nullptr;
    impl->mqp_list.push_back(impl->send_wrs.size() - 1);
}
void task::append_enable_send(const managed_queue_pair& qp, int count) {
    impl->send_wrs.emplace_back();
    auto& wr = impl->send_wrs.back();
    wr.wr_id = 0xfffffffff1f1f1f1;
    wr.sg_list = nullptr;
    wr.num_sge = 0;
    wr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
    wr.exp_send_flags = 0;
    wr.ex.imm_data = 0;
    wr.task.wqe_enable.qp = qp.qp.get();
    wr.task.wqe_enable.wqe_count = count;
    wr.comp_mask = 0;
    wr.next = nullptr;
    impl->mqp_list.push_back(impl->send_wrs.size() - 1);
}
void task::append_send(const managed_queue_pair& qp, const memory_region& mr,
                       size_t offset, size_t length, uint32_t immediate) {
    impl->sges.emplace_back();
    auto& sge = impl->sges.back();
    sge.addr = (uintptr_t)mr.buffer + offset;
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    impl->send_wrs.emplace_back();
    auto& wr = impl->send_wrs.back();
    wr.wr_id = 0xfffffffff2f2f2f2;
    wr.next = nullptr;
    wr.sg_list = &impl->sges.back();
    wr.num_sge = 1;
    wr.exp_opcode = IBV_EXP_WR_SEND;
    wr.exp_send_flags = 0;
    wr.ex.imm_data = immediate;
    wr.comp_mask = 0;
    impl->send_list[qp.qp.get()].push_back(impl->send_wrs.size() - 1);
}
void task::append_recv(const managed_queue_pair& qp, const memory_region& mr,
                       size_t offset, size_t length) {
    impl->sges.emplace_back();
    auto& sge = impl->sges.back();
    sge.addr = (uintptr_t)mr.buffer + offset;
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    impl->recv_wrs.emplace_back();
    auto& wr = impl->recv_wrs.back();
    wr.wr_id = 0xfffffffff3f3f3f3;
    wr.next = nullptr;
    wr.sg_list = &impl->sges.back();
    wr.num_sge = 1;
    impl->recv_list[qp.qp.get()].push_back(impl->recv_wrs.size() - 1);
}
bool task::post() {
    size_t num_tasks = 1 + impl->send_list.size() + impl->recv_list.size();
    auto tasks = make_unique<ibv_exp_task[]>(num_tasks);

    size_t index = 0;
    for(auto&& l : impl->recv_list) {
        for(size_t i = 0; i + 1 < l.second.size(); i++) {
            impl->recv_wrs[l.second[i]].next = &impl->recv_wrs[l.second[i + 1]];
        }

        tasks[index].item.qp = l.first;
        tasks[index].item.recv_wr = &impl->recv_wrs[l.second.front()];
        tasks[index].task_type = IBV_EXP_TASK_RECV;
        tasks[index].next = &tasks[index + 1];
        tasks[index].comp_mask = 0;
        ++index;
    }
    for(auto&& l : impl->send_list) {
        for(size_t i = 0; i + 1 < l.second.size(); i++) {
            impl->send_wrs[l.second[i]].next = &impl->send_wrs[l.second[i + 1]];
        }

        tasks[index].item.qp = l.first;
        tasks[index].item.send_wr = &impl->send_wrs[l.second.front()];
        tasks[index].task_type = IBV_EXP_TASK_SEND;
        tasks[index].next = &tasks[index + 1];
        tasks[index].comp_mask = 0;
        ++index;
    }

    for(size_t i = 0; i + 1 < impl->mqp_list.size(); i++) {
        impl->send_wrs[impl->mqp_list[i]].next = &impl->send_wrs[impl->mqp_list[i + 1]];
    }

    tasks[index].item.qp = impl->mqp;
    tasks[index].item.send_wr = &impl->send_wrs[impl->mqp_list.front()];
    tasks[index].task_type = IBV_EXP_TASK_SEND;
    tasks[index].next = nullptr;
    tasks[index].comp_mask = 0;

    ibv_exp_task* bad = nullptr;
    return !ibv_exp_post_task(verbs_resources.ib_ctx, &tasks[0], &bad);
}

#else
managed_queue_pair::managed_queue_pair(size_t remote_index,
                                       std::function<void(managed_queue_pair*)> post_recvs)
        : queue_pair(), scq(true), rcq(true) {
    throw rdma::qp_creation_failure();
}

manager_queue_pair::manager_queue_pair() : queue_pair() {
    throw rdma::qp_creation_failure();
}

struct task::task_impl {};
task::~task() {}
task::task(std::shared_ptr<manager_queue_pair> manager_qp) {
    throw unsupported_feature();
}
void task::append_wait(const completion_queue& cq, int count, bool signaled,
                       bool last, uint64_t wr_id, const message_type& type) {
    throw unsupported_feature();
}
void task::append_enable_send(const managed_queue_pair& qp, int count) {
    throw unsupported_feature();
}
void task::append_send(const managed_queue_pair& qp, const memory_region& mr,
                       size_t offset, size_t length, uint32_t immediate) {
    throw unsupported_feature();
}
void task::append_recv(const managed_queue_pair& qp, const memory_region& mr,
                       size_t offset, size_t length) {
    throw unsupported_feature();
}
bool task::post() {
    throw unsupported_feature();
}

#endif

message_type::message_type(const string& name, completion_handler send_handler,
                           completion_handler recv_handler,
                           completion_handler write_handler) {
    std::lock_guard<std::mutex> l(completion_handlers_mutex);

    if(completion_handlers.size() >= std::numeric_limits<tag_type>::max())
        throw message_types_exhausted();

    tag = completion_handlers.size();

    completion_handler_set set;
    set.send = send_handler;
    set.recv = recv_handler;
    set.write = write_handler;
    set.name = name;
    completion_handlers.push_back(set);
}

message_type message_type::ignored() {
    static message_type m(std::numeric_limits<tag_type>::max());
    return m;
}

feature_set get_supported_features() {
    return supported_features;
}

// int poll_for_completions(int num, ibv_wc *wcs, atomic<bool> &shutdown_flag) {
//     while(true) {
//         int poll_result = ibv_poll_cq(verbs_resources.cq, num, wcs);
//         if(poll_result != 0 || shutdown_flag) {
//             return poll_result;
//         }
//     }

//     // if(poll_result < 0) {
//     //     /* poll CQ failed */
//     //     fprintf(stderr, "poll CQ failed\n");
//     // } else {
//     //     return
//     //     /* CQE found */
//     //     fprintf(stdout, "completion was found in CQ with status 0x%x\n",
//     //             wc.status);
//     //     /* check the completion status (here we don't care about the
//     //     completion
//     //      * opcode */
//     //     if(wc.status != IBV_WC_SUCCESS) {
//     //         fprintf(
//     //             stderr,
//     //             "got bad completion with status: 0x%x, vendor syndrome:
//     //             0x%x\n",
//     //             wc.status, wc.vendor_err);
//     //     }
//     // }
// }
namespace impl {
map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
        const vector<uint32_t>& members, uint32_t node_rank,
        const memory_region& mr) {
    map<uint32_t, remote_memory_region> remote_mrs;
    for(uint32_t m : members) {
        if(m == node_rank) {
            continue;
        }

        uintptr_t buffer;
        size_t size;
        uint32_t rkey;

        bool still_connected = rdmc_connections->exchange(m, (uintptr_t)mr.buffer, buffer) && 
                               rdmc_connections->exchange(m, (size_t)mr.size, size) && 
                               rdmc_connections->exchange(m, (uint32_t)mr.get_rkey(), rkey);

        if(!still_connected) {
            fprintf(stderr, "WARNING: lost connection to node %u\n", m);
            throw rdma::connection_broken();
        }

        remote_mrs.emplace(m, remote_memory_region(buffer, size, rkey));
    }
    return remote_mrs;
}
ibv_cq* verbs_get_cq() { return verbs_resources.cq; }
ibv_comp_channel* verbs_get_completion_channel() { return verbs_resources.cc; }
}  // namespace impl
}  // namespace rdma
