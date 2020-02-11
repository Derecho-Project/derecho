#include <arpa/inet.h>
#include <atomic>
#include <byteswap.h>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <mutex>
#include <poll.h>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
#include <thread>
#include <vector>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/rdmc/detail/lf_helper.hpp>
#include <derecho/rdmc/detail/util.hpp>
#include <derecho/tcp/tcp.hpp>
#include <derecho/utils/logger.hpp>

/** From sst/verbs.cpp */
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

namespace rdma {

/**
 * Prints a formatted message to stderr (via fprintf), then crashes the program.
 * @param format_str A printf-style format string
 * @param ... Any number of arguments to the format string
 */
inline void crash_with_message(const char* format_str, ...) {
    va_list format_args;
    va_start(format_args, format_str);
    vfprintf(stderr, format_str, format_args);
    va_end(format_args);
    fflush(stderr);
    exit(-1);
}

/** 
 * Passive endpoint info to be exchange
 */
static constexpr size_t max_lf_addr_size = 128 - sizeof(uint32_t) - 2 * sizeof(uint64_t);
struct cm_con_data_t {
    uint32_t pep_addr_len;           /** local endpoint address length */
    char pep_addr[max_lf_addr_size]; /** local endpoint address */
} __attribute__((packed));

/** 
 * Object to hold the tcp connections for every node 
 */
tcp::tcp_connections* rdmc_connections;

/** 
 * Listener to detect new incoming connections 
 */
//static unique_ptr<tcp::connection_listener> connection_listener;

/**
 * Vector of completion handlers and a mutex for accessing it
 */
struct completion_handler_set {
    completion_handler send;
    completion_handler recv;
    completion_handler write;
    std::string name;
};
static std::vector<completion_handler_set> completion_handlers;
static std::mutex completion_handlers_mutex;

/** 
 * Global states 
 */
struct lf_ctxt {
    struct fi_info* hints;     /** hints */
    struct fi_info* fi;        /** fabric information */
    struct fid_fabric* fabric; /** fabric handle */
    struct fid_domain* domain; /** domain handle */
    struct fid_pep* pep;       /** passive endpoint for receiving connection */
    struct fid_eq* peq;        /** event queue for connection management */
    // struct fid_eq      * eq;              /** event queue for transmitting events */ : moved to resources.
    struct fid_cq* cq;               /** completion queue for all rma operations */
    size_t pep_addr_len;             /** length of local pep address */
    char pep_addr[max_lf_addr_size]; /** local pep address */
    struct fi_eq_attr eq_attr;       /** event queue attributes */
    struct fi_cq_attr cq_attr;       /** completion queue attributes */
};
/** The global context for libfabric */
struct lf_ctxt g_ctxt;

#define LF_USE_VADDR ((g_ctxt.fi->domain_attr->mr_mode) & (FI_MR_VIRT_ADDR | FI_MR_BASIC))
#define LF_CONFIG_FILE "rdma.cfg"

enum RDMAOps {
    RDMA_OP_SEND = 1,
    RDMA_OP_RECV,
    RDMA_OP_WRITE
};
#define OP_BITS_SHIFT (48)
#define OP_BITS_MASK (0x00ff000000000000ull)
#define EXTRACT_RDMA_OP_CODE(x) ((uint8_t)((((uint64_t)x) & OP_BITS_MASK) >> OP_BITS_SHIFT))

namespace impl {

/** 
 * Populate some of the global context with default valus 
 */
static void default_context() {
    memset((void*)&g_ctxt, 0, sizeof(struct lf_ctxt));

    /** Create a new empty fi_info structure */
    g_ctxt.hints = crash_if_nullptr("Fail to allocate fi hints", fi_allocinfo);
    /** Set the interface capabilities, see fi_getinfo(3) for details */
    g_ctxt.hints->caps = FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    /** Use connection-based endpoints */
    g_ctxt.hints->ep_attr->type = FI_EP_MSG;
    /** Enable all modes */
    g_ctxt.hints->mode = ~0;
    /** Set the completion format to contain additional context */
    g_ctxt.cq_attr.format = FI_CQ_FORMAT_DATA;
    /** Use a file descriptor as the wait object (see polling_loop)*/
    // g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC; //FI_WAIT_FD;
    g_ctxt.cq_attr.wait_obj = FI_WAIT_FD;
    /** Set the size of the local pep address */
    g_ctxt.pep_addr_len = max_lf_addr_size;

    /** Set the provider, can be verbs|psm|sockets|usnic */
    g_ctxt.hints->fabric_attr->prov_name = crash_if_nullptr("strdup provider name.",
                                                            strdup, derecho::getConfString(CONF_RDMA_PROVIDER).c_str());
    /** Set the domain */
    g_ctxt.hints->domain_attr->name = crash_if_nullptr("strdup domain name.",
                                                       strdup, derecho::getConfString(CONF_RDMA_DOMAIN).c_str());
    /** Set the memory region mode mode bits, see fi_mr(3) for details */
    if(strcmp(g_ctxt.hints->fabric_attr->prov_name, "sockets") == 0) {
        g_ctxt.hints->domain_attr->mr_mode = FI_MR_BASIC;
    } else {  // default
        /** Set the sizes of the tx and rx queues */
        g_ctxt.hints->tx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_TX_DEPTH);
        g_ctxt.hints->rx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_RX_DEPTH);
        if(g_ctxt.hints->tx_attr->size == 0 || g_ctxt.hints->rx_attr->size == 0) {
            dbg_default_error("Configuration error! RDMA TX and RX depth must be nonzero.");
            std::cerr << "Configuration error! RDMA TX and RX depth must be nonzero." << std::endl;
            dbg_default_flush();
            exit(-1);
        }
        g_ctxt.hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    }
}
}  // namespace impl

//Within the CPP file, the impl functions should be available
using namespace impl;

/**
 * Memory region constructors and member functions
 */

memory_region::memory_region(size_t s) : memory_region(new char[s], s) {
    allocated_buffer.reset(buffer);
}

memory_region::memory_region(char* buf, size_t s) : buffer(buf), size(s) {
    if(!buffer || size <= 0) throw rdma::invalid_args();

    const int mr_access = FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;

    /** Register the memory, use it to construct a smart pointer */
    fid_mr* raw_mr;
    fail_if_nonzero_retry_on_eagain(
            "Failed to register memory", CRASH_ON_FAILURE,
            fi_mr_reg, g_ctxt.domain, (void*)buffer, size, mr_access, 0, 0, 0, &raw_mr, nullptr);
    if(!raw_mr) {
        crash_with_message("Pointer to memory region is null");
    }

    mr = std::unique_ptr<fid_mr, std::function<void(fid_mr*)>>(
            raw_mr, [](fid_mr* mr) { fi_close(&mr->fid); });
}

uint64_t memory_region::get_key() const { return mr->key; }

/** 
 * Completion queue constructor
 */
completion_queue::completion_queue() {
    g_ctxt.cq_attr.size = g_ctxt.fi->tx_attr->size;
    fid_cq* raw_cq;
    fail_if_nonzero_retry_on_eagain(
            "failed to initialize tx completion queue", CRASH_ON_FAILURE,
            fi_cq_open, g_ctxt.domain, &(g_ctxt.cq_attr), &raw_cq, nullptr);
    if(!raw_cq) {
        crash_with_message("Pointer to completion queue is null");
    }

    cq = std::unique_ptr<fid_cq, std::function<void(fid_cq*)>>(
            raw_cq, [](fid_cq* cq) { fi_close(&cq->fid); });
}

endpoint::~endpoint() {}
endpoint::endpoint(size_t remote_index, bool is_lf_server)
        : endpoint(remote_index, is_lf_server, [](endpoint*) {}) {}
endpoint::endpoint(size_t remote_index, bool is_lf_server,
                   std::function<void(endpoint*)> post_recvs) {
    connect(remote_index, is_lf_server, post_recvs);
}

int endpoint::init(struct fi_info* fi) {
    int ret;
    /** Open an endpoint */
    fid_ep* raw_ep;
    ret = fail_if_nonzero_retry_on_eagain(
            "Failed to open endpoint", REPORT_ON_FAILURE,
            fi_endpoint, g_ctxt.domain, fi, &raw_ep, nullptr);
    if(ret) return ret;
    dbg_default_trace("{}:{} created rdmc endpoint: {}", __FILE__, __func__, (void*)&raw_ep->fid);
    dbg_default_flush();
    /** Construct the smart pointer to manage the endpoint */
    ep = std::unique_ptr<fid_ep, std::function<void(fid_ep*)>>(
            raw_ep,
            [](fid_ep* ep) {
                fi_close(&ep->fid);
            });

    /** Create an event queue */
    fid_eq* raw_eq;
    ret = fail_if_nonzero_retry_on_eagain(
            "Failed to open event queue", REPORT_ON_FAILURE,
            fi_eq_open, g_ctxt.fabric, &g_ctxt.eq_attr, &raw_eq, nullptr);
    if(ret) return ret;
    /** Construct the smart pointer to manage the event queue */
    eq = std::unique_ptr<fid_eq, std::function<void(fid_eq*)>>(
            raw_eq, [](fid_eq* eq) { fi_close(&eq->fid); });

    /** Bind endpoint to event queue and completion queue */
    ret = fail_if_nonzero_retry_on_eagain(
            "Failed to bind endpoint and event queue", REPORT_ON_FAILURE,
            fi_ep_bind, raw_ep, &(raw_eq)->fid, 0);
    if(ret) return ret;
    const uint64_t ep_flags = FI_RECV | FI_TRANSMIT | FI_SELECTIVE_COMPLETION;
    ret = fail_if_nonzero_retry_on_eagain(
            "Failed to bind endpoint and tx completion queue", REPORT_ON_FAILURE,
            fi_ep_bind, raw_ep, &(g_ctxt.cq)->fid, ep_flags);
    if(ret) return ret;
    ret = fail_if_nonzero_retry_on_eagain(
            "Failed to enable endpoint", REPORT_ON_FAILURE,
            fi_enable, raw_ep);
    return ret;
}

bool sync(uint32_t r_id) {
    int s = 0, t = 0;

    return rdmc_connections->exchange(r_id, s, t);
}

void endpoint::connect(size_t remote_index, bool is_lf_server,
                       std::function<void(endpoint*)> post_recvs) {
    struct cm_con_data_t local_cm_data, remote_cm_data;
    memset(&local_cm_data, 0, sizeof(local_cm_data));
    memset(&remote_cm_data, 0, sizeof(remote_cm_data));

    /** Populate local cm struct and exchange cm info */
    local_cm_data.pep_addr_len = (uint32_t)htonl((uint32_t)g_ctxt.pep_addr_len);
    memcpy((void*)&local_cm_data.pep_addr, &g_ctxt.pep_addr, g_ctxt.pep_addr_len);

    if(!rdmc_connections->exchange(remote_index, local_cm_data, remote_cm_data)) {
        crash_with_message("RDMC failed to exchange cm info\n");
    }

    remote_cm_data.pep_addr_len = (uint32_t)ntohl(remote_cm_data.pep_addr_len);

    /** Connect to remote node */
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    if(is_lf_server) {
        /** Synchronously read from the passive event queue, init the server ep */
        nRead = fi_eq_sread(g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            crash_with_message("Failed to get connection from remote. nRead=%ld\n", nRead);
        }
        if(init(entry.info)) {
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            crash_with_message("Failed to initialize server endpoint.\n");
        }
        if(fi_accept(ep.get(), NULL, 0)) {
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            crash_with_message("Failed to accept connection.\n");
        }
        fi_freeinfo(entry.info);
    } else {
        struct fi_info* client_hints = fi_dupinfo(g_ctxt.hints);
        struct fi_info* client_info = NULL;

        /** TODO document this */
        client_hints->dest_addr = crash_if_nullptr("Failed to malloc address space for server pep.",
                                                   malloc, remote_cm_data.pep_addr_len);
        memcpy((void*)client_hints->dest_addr,
               (void*)remote_cm_data.pep_addr,
               (size_t)remote_cm_data.pep_addr_len);
        client_hints->dest_addrlen = remote_cm_data.pep_addr_len;
        fail_if_nonzero_retry_on_eagain(
                "fi_getinfo() failed.", CRASH_ON_FAILURE,
                fi_getinfo, LF_VERSION, nullptr, nullptr, 0, client_hints, &client_info);

        /** TODO document this */
        if(init(client_info)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            crash_with_message("failed to initialize client endpoint.\n");
        }
        fail_if_nonzero_retry_on_eagain(
                "fi_connect() failed", CRASH_ON_FAILURE,
                fi_connect, ep.get(), remote_cm_data.pep_addr, nullptr, 0);

        /** TODO document this */
        nRead = fi_eq_sread(this->eq.get(), &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            crash_with_message("failed to connect remote. nRead=%ld.\n", nRead);
        }
        if(event != FI_CONNECTED || entry.fid != &(ep->fid)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            crash_with_message("RDMC Unexpected CM event: %d.\n", event);
        }
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
    }

    post_recvs(this);
    int tmp = -1;
    if(!rdmc_connections->exchange(remote_index, 0, tmp) || tmp != 0) {
        crash_with_message("Failed to sync after endpoint creation");
    }
}

bool endpoint::post_send(const memory_region& mr, size_t offset, size_t size,
                         uint64_t wr_id, uint32_t immediate,
                         const message_type& type) {
    struct iovec msg_iov;
    struct fi_msg msg;

    msg_iov.iov_base = mr.buffer + offset;
    msg_iov.iov_len = size;

    msg.msg_iov = &msg_iov;
    msg.desc = (void**)&mr.mr->key;
    msg.iov_count = 1;
    msg.addr = 0;
    msg.context = (void*)(wr_id | ((uint64_t)*type.tag << type.shift_bits) | ((uint64_t)RDMA_OP_SEND) << OP_BITS_SHIFT);
    msg.data = immediate;

    fail_if_nonzero_retry_on_eagain(
            "fi_sendmsg() failed", REPORT_ON_FAILURE,
            fi_sendmsg, ep.get(), &msg, FI_COMPLETION | FI_REMOTE_CQ_DATA);
    return true;
}

bool endpoint::post_recv(const memory_region& mr, size_t offset, size_t size,
                         uint64_t wr_id, const message_type& type) {
    struct iovec msg_iov;
    struct fi_msg msg;

    msg_iov.iov_base = mr.buffer + offset;
    msg_iov.iov_len = size;

    msg.msg_iov = &msg_iov;
    msg.desc = (void**)&mr.mr->key;
    msg.iov_count = 1;
    msg.addr = 0;
    msg.context = (void*)(wr_id | ((uint64_t)*type.tag << type.shift_bits) | ((uint64_t)RDMA_OP_RECV) << OP_BITS_SHIFT);

    fail_if_nonzero_retry_on_eagain(
            "fi_recvmsg() failed", REPORT_ON_FAILURE,
            fi_recvmsg, ep.get(), &msg, FI_COMPLETION);
    return true;
}

bool endpoint::post_empty_send(uint64_t wr_id, uint32_t immediate,
                               const message_type& type) {
    struct fi_msg msg;

    memset(&msg, 0, sizeof(msg));
    msg.context = (void*)(wr_id | ((uint64_t)*type.tag << type.shift_bits) | ((uint64_t)RDMA_OP_SEND) << OP_BITS_SHIFT);
    msg.data = immediate;

    fail_if_nonzero_retry_on_eagain(
            "fi_sendmsg() failed", REPORT_ON_FAILURE,
            fi_sendmsg, ep.get(), &msg, FI_COMPLETION | FI_REMOTE_CQ_DATA);
    return true;
}

bool endpoint::post_empty_recv(uint64_t wr_id, const message_type& type) {
    struct fi_msg msg;

    memset(&msg, 0, sizeof(msg));
    msg.context = (void*)(wr_id | ((uint64_t)*type.tag << type.shift_bits) | ((uint64_t)RDMA_OP_RECV) << OP_BITS_SHIFT);

    fail_if_nonzero_retry_on_eagain(
            "fi_recvmsg() failed", REPORT_ON_FAILURE,
            fi_recvmsg, ep.get(), &msg, FI_COMPLETION);
    return true;
}

bool endpoint::post_write(const memory_region& mr, size_t offset, size_t size,
                          uint64_t wr_id, remote_memory_region remote_mr,
                          size_t remote_offset, const message_type& type,
                          bool signaled, bool send_inline) {
    if(wr_id >> type.shift_bits || !type.tag) throw invalid_args();
    if(mr.size < offset + size || remote_mr.size < remote_offset + size) {
        std::cout << "mr.size = " << mr.size << " offset = " << offset
                  << " length = " << size << " remote_mr.size = " << remote_mr.size
                  << " remote_offset = " << remote_offset << std::endl;
        return false;
    }

    struct iovec msg_iov;
    struct fi_rma_iov rma_iov;
    struct fi_msg_rma msg;

    msg_iov.iov_base = mr.buffer + offset;
    msg_iov.iov_len = size;

    rma_iov.addr = ((LF_USE_VADDR) ? remote_mr.buffer : 0) + remote_offset;
    rma_iov.len = size;
    rma_iov.key = remote_mr.rkey;

    msg.msg_iov = &msg_iov;
    msg.desc = (void**)&mr.mr->key;
    msg.iov_count = 1;
    msg.addr = 0;
    msg.rma_iov = &rma_iov;
    msg.rma_iov_count = 1;
    msg.context = (void*)(wr_id | ((uint64_t)*type.tag << type.shift_bits) | ((uint64_t)RDMA_OP_WRITE) << OP_BITS_SHIFT);
    // msg.data          = RDMA_OP_WRITE;

    fail_if_nonzero_retry_on_eagain(
            "fi_writemsg() failed", REPORT_ON_FAILURE,
            fi_writemsg, ep.get(), &msg, FI_COMPLETION);

    return true;
}

message_type::message_type(const std::string& name, completion_handler send_handler,
                           completion_handler recv_handler,
                           completion_handler write_handler) {
    std::lock_guard<std::mutex> l(completion_handlers_mutex);

    //if(completion_handlers.size() >= std::numeric_limits<tag_type>::max())
    //    throw message_types_exhausted();

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

struct task::task_impl {
    int dummy;
};

task::task(std::shared_ptr<manager_endpoint> manager_ep) {
    return;
}

task::~task() {}

void task::append_wait(const completion_queue& cq, int count, bool signaled,
                       bool last, uint64_t wr_id, const message_type& type) {
    throw unsupported_feature();
}

void task::append_enable_send(const managed_endpoint& ep, int count) {
    throw unsupported_feature();
}

void task::append_send(const managed_endpoint& ep, const memory_region& mr,
                       size_t offset, size_t length, uint32_t immediate) {
    throw unsupported_feature();
}
void task::append_recv(const managed_endpoint& ep, const memory_region& mr,
                       size_t offset, size_t length) {
    throw unsupported_feature();
}

bool task::post() {
    throw unsupported_feature();
}

namespace impl {
/**
 * Adds a node to the group via tcp
 */
bool lf_add_connection(
        uint32_t new_id,
        const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return rdmc_connections->add_node(new_id, new_ip_addr_and_port);
}

/**
 * Removes a node's TCP connection, presumably because it has failed.
 */
bool lf_remove_connection(uint32_t node_id) {
    return rdmc_connections->delete_node(node_id);
}

static std::atomic<bool> interrupt_mode;
static std::atomic<bool> polling_loop_shutdown_flag;
static void polling_loop() {
    pthread_setname_np(pthread_self(), "rdmc_poll");

    const int max_cq_entries = 1024;
    std::unique_ptr<fi_cq_data_entry[]> cq_entries(new fi_cq_data_entry[max_cq_entries]);

    while(true) {
        int num_completions = 0;
        while(num_completions == 0 || num_completions == -FI_EAGAIN) {
            if(polling_loop_shutdown_flag) return;
            uint64_t poll_end = get_time() + (interrupt_mode ? 0L : 50000000L);
            do {
                if(polling_loop_shutdown_flag) return;
                num_completions = fi_cq_read(g_ctxt.cq, cq_entries.get(), max_cq_entries);
            } while((num_completions == 0 || num_completions == -FI_EAGAIN) && get_time() < poll_end);

            if(num_completions == 0 || num_completions == -FI_EAGAIN) {
                /** Need ibv_req_notify_cq equivalent here? */

                num_completions = fi_cq_read(g_ctxt.cq, cq_entries.get(), max_cq_entries);

                if(num_completions == 0 || num_completions == -FI_EAGAIN) {
                    pollfd file_descriptor;
                    fi_control(&g_ctxt.cq->fid, FI_GETWAIT, &file_descriptor);
                    int rc = 0;
                    while(rc == 0 && !polling_loop_shutdown_flag) {
                        if(polling_loop_shutdown_flag) return;
                        file_descriptor.events = POLLIN | POLLERR | POLLHUP;
                        file_descriptor.revents = 0;
                        rc = poll(&file_descriptor, 1, 50);
                    }

                    if(rc > 0) {
                        num_completions = fi_cq_read(g_ctxt.cq, cq_entries.get(), max_cq_entries);
                    }
                }
            }
        }

        if(num_completions < 0) {
            std::cout << "Failed to read from completion queue, fi_cq_read returned "
                      << num_completions << std::endl;
        }

        std::lock_guard<std::mutex> l(completion_handlers_mutex);
        for(int i = 0; i < num_completions; i++) {
            fi_cq_data_entry& cq_entry = cq_entries[i];

            message_type::tag_type type = (uint64_t)cq_entry.op_context >> message_type::shift_bits;
            if(type == std::numeric_limits<message_type::tag_type>::max())
                continue;

            uint64_t masked_wr_id = (uint64_t)cq_entry.op_context & 0x0000ffffffffffffull;
            uint32_t opcode = (uint32_t)EXTRACT_RDMA_OP_CODE(cq_entry.op_context);
            uint32_t immediate = cq_entry.data;
            if(type >= completion_handlers.size()) {
                // Unrecognized message type
            } else if(opcode == RDMA_OP_SEND) {
                completion_handlers[type].send(masked_wr_id, immediate,
                                               cq_entry.len);
            } else if(opcode == RDMA_OP_RECV) {
                completion_handlers[type].recv(masked_wr_id, immediate,
                                               cq_entry.len);
            } else if(opcode == RDMA_OP_WRITE) {
                completion_handlers[type].write(masked_wr_id, immediate,
                                                cq_entry.len);
            } else {
                puts("Sent unrecognized completion type?!");
            }
        }
    }
}

/**
 * Initialize the global context 
 */
bool lf_initialize(const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_ports,
                   uint32_t node_rank) {
    /** Initialize the connection listener on the rdmc tcp port */
    // connection_listener =
    // make_unique<tcp::connection_listener>(derecho::rdmc_tcp_port);

    /** Initialize the tcp connections, also connects all the nodes together */
    rdmc_connections = new tcp::tcp_connections(node_rank, ip_addrs_and_ports);

    /** Set the context to defaults to start with */
    default_context();
    // load_configuration();

    dbg_default_debug(fi_tostr(g_ctxt.hints, FI_TYPE_INFO));
    /** Initialize the fabric, domain and completion queue */
    fail_if_nonzero_retry_on_eagain(
            "fi_getinfo() failed", CRASH_ON_FAILURE,
            fi_getinfo, LF_VERSION, nullptr, nullptr, 0, g_ctxt.hints, &(g_ctxt.fi));
    fail_if_nonzero_retry_on_eagain(
            "fi_fabric() failed", CRASH_ON_FAILURE,
            fi_fabric, g_ctxt.fi->fabric_attr, &(g_ctxt.fabric), nullptr);
    fail_if_nonzero_retry_on_eagain(
            "fi_domain() failed", CRASH_ON_FAILURE,
            fi_domain, g_ctxt.fabric, g_ctxt.fi, &(g_ctxt.domain), nullptr);
    fail_if_nonzero_retry_on_eagain(
            "failed to initialize tx completion queue", CRASH_ON_FAILURE,
            fi_cq_open, g_ctxt.domain, &(g_ctxt.cq_attr), &(g_ctxt.cq), nullptr);

    if(!g_ctxt.cq) {
        crash_with_message("Pointer to completion queue is null\n");
    }

    /** Initialize the event queue, initialize and configure pep  */
    fail_if_nonzero_retry_on_eagain(
            "failed to open the event queue for passive endpoint", CRASH_ON_FAILURE,
            fi_eq_open, g_ctxt.fabric, &g_ctxt.eq_attr, &g_ctxt.peq, nullptr);
    fail_if_nonzero_retry_on_eagain(
            "failed to open a local passive endpoint", CRASH_ON_FAILURE,
            fi_passive_ep, g_ctxt.fabric, g_ctxt.fi, &g_ctxt.pep, nullptr);
    fail_if_nonzero_retry_on_eagain(
            "failed to bind event queue to passive endpoint", CRASH_ON_FAILURE,
            fi_pep_bind, g_ctxt.pep, &g_ctxt.peq->fid, 0);
    fail_if_nonzero_retry_on_eagain(
            "failed to prepare passive endpoint for incoming connections", CRASH_ON_FAILURE,
            fi_listen, g_ctxt.pep);
    fail_if_nonzero_retry_on_eagain(
            "failed to get the local PEP address", CRASH_ON_FAILURE,
            fi_getname, &g_ctxt.pep->fid, g_ctxt.pep_addr, &g_ctxt.pep_addr_len);
    if(g_ctxt.pep_addr_len > max_lf_addr_size) {
        crash_with_message("local name is too big to fit in local buffer\n");
    }
    //  event queue moved to endpoint.
    //  FAIL_IF_NONZERO_RETRY_EAGAIN(
    //      fi_eq_open(g_ctxt.fabric, &g_ctxt.eq_attr, &g_ctxt.eq, NULL),
    //      "failed to open the event queue for rdma transmission.",
    //      CRASH_ON_FAILURE
    //  );

    /** Start a polling thread and run in the background */
    std::thread polling_thread(polling_loop);
    polling_thread.detach();

    return true;
}

bool lf_destroy() {
    return false;
}

std::map<uint32_t, remote_memory_region> lf_exchange_memory_regions(
        const std::vector<uint32_t>& members, uint32_t node_rank,
        const memory_region& mr) {
    /** Maps a node's ID to a memory region on that node */
    std::map<uint32_t, remote_memory_region> remote_mrs;
    for(uint32_t m : members) {
        if(m == node_rank) {
            continue;
        }

        uint64_t buffer;
        size_t size;
        uint64_t rkey;

        if(!rdmc_connections->exchange(m, (uint64_t)mr.buffer, buffer) || !rdmc_connections->exchange(m, mr.size, size) || !rdmc_connections->exchange(m, mr.get_key(), rkey)) {
            fprintf(stderr, "WARNING: lost connection to node %u\n", m);
            throw rdma::connection_broken();
        }
        remote_mrs.emplace(m, remote_memory_region(buffer, size, rkey));
    }
    return remote_mrs;
}

bool set_interrupt_mode(bool enabled) {
    interrupt_mode = enabled;
    return true;
}

} /* namespace impl */
} /* namespace rdma */
