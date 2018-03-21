#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <mutex>
#include <poll.h>
#include <thread>
#include <vector>
#include <GetPot>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_domain.h>

#include "derecho/connection_manager.h"
#include "derecho/derecho_ports.h"
#include "lf_helper.h"
#include "tcp/tcp.h"
#include "util.h"

#ifdef _DEBUG
#include <spdlog/spdlog.h>
#endif

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

using namespace std;

namespace rdma {

/** Debugging tools from Weijia's sst code */  
#ifdef _DEBUG
    inline auto dbgConsole() {
        static auto console = spdlog::stdout_color_mt("console");
        return console;
    }
    #define dbg_trace(...) dbgConsole()->trace(__VA_ARGS__)
    #define dbg_debug(...) dbgConsole()->debug(__VA_ARGS__)
    #define dbg_info(...) dbgConsole()->info(__VA_ARGS__)
    #define dbg_warn(...) dbgConsole()->warn(__VA_ARGS__)
    #define dbg_error(...) dbgConsole()->error(__VA_ARGS__)
    #define dbg_crit(...) dbgConsole()->critical(__VA_ARGS__)
#else
    #define dbg_trace(...)
    #define dbg_debug(...)
    #define dbg_info(...)
    #define dbg_warn(...)
    #define dbg_error(...)
    #define dbg_crit(...)
#endif//_DEBUG
#define CRASH_WITH_MESSAGE(...) \
do { \
    fprintf(stderr,__VA_ARGS__); \
    fflush(stderr); \
    exit(-1); \
} while (0);

/** Testing tools from Weijia's sst code */
enum NextOnFailure{
    REPORT_ON_FAILURE = 0,
    CRASH_ON_FAILURE = 1
};
#define FAIL_IF_NONZERO(x,desc,next) \
    do { \
        int64_t _int64_r_ = (int64_t)(x); \
        if (_int64_r_ != 0) { \
            dbg_error("{}:{},ret={},{}",__FILE__,__LINE__,_int64_r_,desc); \
            fprintf(stderr,"%s:%d,ret=%ld,%s\n",__FILE__,__LINE__,_int64_r_,desc); \
            if (next == CRASH_ON_FAILURE) { \
                fflush(stderr); \
                exit(-1); \
            } \
        } \
    } while (0)
#define FAIL_IF_ZERO(x,desc,next) \
    do { \
        int64_t _int64_r_ = (int64_t)(x); \
        if (_int64_r_ == 0) { \
            dbg_error("{}:{},{}",__FILE__,__LINE__,desc); \
            fprintf(stderr,"%s:%d,%s\n",__FILE__,__LINE__,desc); \
            if (next == CRASH_ON_FAILURE) { \
                fflush(stderr); \
                exit(-1); \
            } \
        } \
    } while (0)

/** 
 * Passive endpoint info to be exchange
 */
struct cm_con_data_t {
  #define MAX_LF_ADDR_SIZE    ((128)-sizeof(uint32_t)-2*sizeof(uint64_t))
  uint32_t           pep_addr_len;               /** local endpoint address length */
  char               pep_addr[MAX_LF_ADDR_SIZE]; /** local endpoint address */
  uint64_t           mr_key;                     /** local memory key */
  uint64_t           vaddr;                      /** virtual addr */
} __attribute__((packed));

/** 
 * Object to hold the tcp connections for every node 
 */
tcp::tcp_connections *rdmc_connections;

/** 
 * Listener to detect new incoming connections 
 */
static unique_ptr<tcp::connection_listener> connection_listener;

/**
 * Vector of completion handlers and a mutex for accessing it
 */
static vector<completion_handler_set> completion_handlers;
static std::mutex completion_handlers_mutex;

/** 
 * Global states 
 */
struct lf_ctxt {
    struct fi_info     * hints;           /** hints */
    struct fi_info     * fi;              /** fabric information */
    struct fid_fabric  * fabric;          /** fabric handle */
    struct fid_domain  * domain;          /** domain handle */
    struct fid_pep     * pep;             /** passive endpoint for receiving connection */
    struct fid_eq      * peq;             /** event queue for connection management */
    struct fid_eq      * eq;              /** event queue for transmitting events */
    struct fid_cq      * cq;              /** completion queue for all rma operations */
    size_t             pep_addr_len;      /** length of local pep address */
    char               pep_addr[MAX_LF_ADDR_SIZE]; /** local pep address */
    struct fi_eq_attr  eq_attr;           /** event queue attributes */
    struct fi_cq_attr  cq_attr;           /** completion queue attributes */
};
/** The global context for libfabric */
struct lf_ctxt g_ctxt;

#define LF_USE_VADDR ((g_ctxt.fi->domain_attr->mr_mode) & FI_MR_VIRT_ADDR)
#define LF_CONFIG_FILE "rdma.cfg";

namespace impl {

/** 
 * Populate some of the global context with default valus 
 */
static void default_context() {
    memset((void*)&g_ctxt, 0, sizeof(struct lf_ctxt));
    /** Create a new empty fi_info structure */
    g_ctxt.hints = fi_allocinfo();
    /** Set the interface capabilities, see fi_getinfo(3) for details */
    g_ctxt.hints->caps = FI_MSG|FI_RMA|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE;
    /** Use connection-based endpoints */
    g_ctxt.hints->ep_attr->type = FI_EP_MSG;
    /** Enable all modes */
    g_ctxt.hints->mode = ~0;
    /** Set the completion format to be user-specifed */ 
    if (g_ctxt.cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
        g_ctxt.cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    }
    /** Says the user will only wait on the CQ using libfabric calls */
    g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC;
    /** Set the size of the local pep address */
    g_ctxt.pep_addr_len = MAX_LF_ADDR_SIZE;
}

/** 
 * Load the global context from a configuration file
 */
static void load_configuration() {
    #define DEFAULT_PROVIDER "sockets"; /** Can be one of verbs|psm|sockets|usnic */
    #define DEFAULT_DOMAIN   "eth0";    /** Default domain depends on system */
    #define DEFAULT_TX_DEPTH  4096;     /** Tx queue depth */
    #define DEFAULT_RX_DEPTH  4096;     /** Rx queue depth */
    GetPot cfg(LF_CONFIG_FILE);         /** Load the configuration file */
    
    FAIL_IF_ZERO(g_ctxt.hints, "FI hints not allocated",  CRASH_ON_FAILURE);
    THROW_IF_ZERO(                      /** Load the provider from config */
        g_ctxt.hints->fabric_attr->prov_name = strdup(cfg("provider", DEFAULT_PROVIDER)),
        "Failed to load the provider from config file", CRASH_ON_FAILURE
    );
    THROW_IF_ZERO(                      /** Load the domain from config */
        g_ctxt.hints->domain_attr->name = strdup(cfg("domain", DEFAULT_DOMAIN));
        "Failed to load the domain from config file", CRASH_ON_FAILURE
    );
    /** Set the memory region mode mode bits, see fi_mr(3) for details */
    g_ctxt.hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | 
                                         FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    /** Set the tx and rx queue sizes, see fi_endpoint(3) for details */
    g_ctxt.hints->tx_attr->size = cfg("tx_depth", DEFAULT_TX_DEPTH);
    g_ctxt.hints->rx_attr->size = cfg("rx_depth", DEFAULT_RX_DEPTH);
}
}
static void polling_loop() {

}

/**
 * Memory region constructors and member functions
 */

memory_region::memory_region(size_t s, uint32_t node_rank) 
    : memory_region(new char[s], s, node_rank) {}

memory_region::memory_region(char *buf, size_t s, uint32_t node_rank) 
    : buffer(buf), size(s), node_rank(node_rank) { register_mr(); }

#define LF_RMR_KEY(rid) (((uint64_t)0xf0000000)<<32 | (uint64_t)(rid))
#define LF_WMR_KEY(rid) (((uint64_t)0xf8000000)<<32 | (uint64_t)(rid))
void memory_region::register_mr() {
    //if (!buffer || size <= 0) throw rdma::invalidargs();

    const int mr_access = FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
  
    /** Register the memory, then manage it w/ our smart pointer */  
    FAIL_IF_NONZERO(
        fi_mr_reg(g_ctxt.domain, (void *)buffer, size, mr_access, 0,
                  LF_RMR_KEY(node_rank), 0, &mr, nullptr),
        "Failed to register memory", CRASH_ON_FAILURE
    );
    FAIL_IF_ZERO(mr, "Pointer to memory region is null", CRASH_ON_FAILURE);
}

/** TODO: Check if mr->key is right, or if the correct key is in fi_mr_attr */
uint32_t memory_region::get_rkey() const { return mr->key; }

//fid_mr* memory_region::get_mr() const {return mr.get(); }

/** 
 * Completion queue constructor
 */
completion_queue::completion_queue() {
    g_ctxt.cq_attr.size = g_ctxt.fi->tx_attr->size;
    FAIL_IF_NONZERO(
        fi_cq_open(g_ctxt.domain, &(g_ctxt.cq_attr), &cq, NULL),
        "failed to initialize tx completion queue", CRASH_ON_FAILURE
    );

    FAIL_IF_ZERO(cq, "Pointer to completion queue is null", CRASH_ON_FAILURE);
}

endpoint::~endpoint() {}
endpoint::endpoint(size_t remote_index)
    : endpoint(remote_index, [](endpoint*){}) {}
endpoint::endpoint(size_t remote_index, bool is_lf_server,
                   std::function<void(endpoint *)> post_recvs) { 
    connect(remote_index, is_lf_server, post_recvs); 
}

int endpoint::init(struct fi_info *fi) {
    /** Open an endpoint */
    FAIL_IF_NONZERO(
        ret = fi_endpoint(g_ctxt.domain, fi, &ep, NULL), 
        "Failed to open endpoint", REPORT_ON_FAILURE
    );
    if(ret) return ret;
 
    /** Bind endpoint to event queue and completion queue */
    FAIL_IF_NONZERO(
        ret = fi_ep_bind(ep, &(g_ctxt.eq)->fid, 0), 
        "Failed to bind endpoint and event queue", REPORT_ON_FAILURE
    );
    if(ret) return ret;
    const int ep_flags = FI_RECV | FI_TRANSMIT | FI_SELECTIVE_COMPLETION;
    FAIL_IF_NONZERO(
        ret = fi_ep_bind(ep, &(g_ctxt.cq)->fid, ep_flags), 
        "Failed to bind endpoint and tx completion queue", REPORT_ON_FAILURE
    );
    if(ret) return ret;
    FAIL_IF_NONZERO(
        ret = fi_enable(ep), 
        "Failed to enable endpoint", REPORT_ON_FAILURE
    );
    return ret;
}

bool sync(uint32_t r_id) {
    int s = 0, t = 0;
    return sst_connections->exchange(r_id, s, t);
}

void endpoint::connect(size_t remote_index, bool is_lf_server, 
                       std::function<void(endpoint *)> post_recvs) {
    struct cm_con_data_t local_cm_data, remote_cm_data;
    memset(&local_con_data, 0, sizeof(local_con_data));
    memset(&remote_con_data, 0, sizeof(remote_con_data));
    
    /** Populate local cm struct and exchange cm info */    
    local_cm_data.pep_addr_len  = (uint32_t)htonl((uint32_t)g_ctxt.pep_addr_len);
    memcpy((void*)&local_cm_data.pep_addr, &g_ctxt.pep_addr, g_ctxt.pep_addr_len);
    /** TODO Check if this needs to be rkey, lkey, etc */
    local_cm_data.mr_key        = (uint64_t)htonll(mr->key);
    local_cm_data.vaddr         = (uint64_t)htonll((uint64_t)buffer);

    FAIL_IF_ZERO(
        rdmc_connections->exchange(remote_index, local_cm_data, remote_cm_data),
        "Failed to exchange cm info", CRASH_ON_FAILURE
    );

    remote_cm_data.pep_addr_len = (uint32_t)ntohl(remote_cm_data.pep_addr_len);
    /** TODO Check if this needs to be rkey, lkey, etc */
    mr->key                     = (uint64_t)ntohll(remote_cm_data.mr_key);
    remote_fi_addr              = (fi_addr_t)ntohll(remote_cm_data.vaddr);

    /** Connect to remote node */
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    if (is_lf_server) {
        /** Synchronously read from the passive event queue, init the server ep */ 
        nRead = fi_eq_sread(g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            CRASH_WITH_MESSAGE("Failed to get connection from remote. nRead=%ld\n",nRead);
        }
        if (init(entry.info)){
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            CRASH_WITH_MESSAGE("Failed to initialize server endpoint.\n");
        }
        if (fi_accept(ep, NULL, 0)){
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            CRASH_WITH_MESSAGE("Failed to accept connection.\n");
        }
        fi_freeinfo(entry.info);
    } else {
        struct fi_info * client_hints = fi_dupinfo(g_ctxt.hints);
        struct fi_info * client_info = NULL;

        /** TODO document this */    
        FAIL_IF_ZERO(
            client_hints->dest_addr = malloc(remote_cm_data.pep_addr_len),
            "Failed to malloc address space for server pep.", CRASH_ON_FAILURE
        );
        memcpy((void*)client_hints->dest_addr,
               (void*)remote_cm_data.pep_addr,
               (size_t)remote_cm_data.pep_addr_len);
        client_hints->dest_addrlen = remote_cm_data.pep_addr_len;
        FAIL_IF_NONZERO(
            fi_getinfo(LF_VERSION, NULL, NULL, 0, client_hints, &client_info),
            "fi_getinfo() failed.", CRASH_ON_FAILURE
        );

        /** TODO document this */
        if (init(client_info)){
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            CRASH_WITH_MESSAGE("failed to initialize client endpoint.\n");
        }
        FAIL_IF_NONZERO(
            fi_connect(ep, remote_cm_data.pep_addr, NULL, 0),
            "fi_connect() failed", CRASH_ON_FAILURE
        );
       
        /** TODO document this */
         nRead = fi_eq_sread(g_ctxt.eq, &event, &entry, sizeof(entry), -1, 0);
        if (nRead != sizeof(entry)) {
            CRASH_WITH_MESSAGE("failed to connect remote. nRead=%ld.\n",nRead);
        }
        if (event != FI_CONNECTED || entry.fid != &(ep->fid)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            CRASH_WITH_MESSAGE("Unexpected CM event: %d.\n", event);
        }
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
    }

    post_recvs(this);
    int tmp = -1;
    if (!sst_connections->exchange(remote_index, 0, tmp) || tmp != 0)
        CRASH_WITH_MESSAGE("Failed to sync after endpoint creation");
}

bool endpoint::post_send(const memory_region& mr, size_t offset, size_t size,
                         uint64_t wr_id, uint32_t immediate, 
                         const message_type& type) {
    int ret = 0;
    struct iovec iov;
    struct fi_msg msg;
    
    iov.iov_base  = mr.buffer + offset;
    iov.iov_len   = size;

    msg.msg_iov   = &msg_iov;
    msg.desc      = &fi_mr_desc(mr);
    msg.iov_count = 1;
    msg.addr      = 0;           /** Not used for connected ep */
    msg.context   =              /** Used to store the tag for a block */ 
        (uintptr_t)(wr_id | ((uint64_t)*type.tag << type.shift_bits)); 
    msg.data      = immediate;   /** Used to store the immdiate for a block */

    const int flags = 0;         /** TODO Check if I should be passing FI_COMPLETION */
 
    FAIL_IF_NONZERO(
        ret = fi_sendmsg(ep, &msg, flags);
        "fi_sendmsg() failed", REPORT_ON_FAILURE
    );
    return ret; 
}

bool post_recv(const memory_region& mr, size_t offset, size_t size,
               uint64_t wr_id, const message_type& type) {
    return false;
}

bool post_empty_send(uint64_t wr_id, uint32_t immediate,
                     const message_type& type) {
    return false;
}

bool post_empty_recv(uint64_t wr_id, const message_type& type) {
    return false;
}

bool post_write(const memory_region& mr, size_t offset, size_t size,
                uint64_t wr_id, remote_memory_region remote_mr,
                size_t remote_offset, const message_type& type,
                bool signaled = false, bool send_inline = false) {
    return false;

    /** TODO: Update this. Original was my first attempt at post_send */
    int ret = 0;
    struct iovec iov;
    struct fi_rma_iov rma_iov;
    struct fi_msg_rma msg;
    
    iov.iov_base  = buffer + offset;
    iov.iov_len   = size;
   
    rma_iov.addr  = ((LF_USE_VADDR) ? remote_fi_addr : 0) + offset;
    rma_iov.len   = size;
    rma_iov.key   = mr.get_rkey();

    msg.msg_iov   = &msg_iov;
    msg.desc      = &fi_mr_desc(mr);
    msg.iov_count = 1;
    msg.addr      = 0;           /** Not used for connected ep */
    msg.rma_iov   = &rma_iov;
    msg.rma_iov_count = 1;
    msg.context   =              /** Used to store the tag for a block */ 
        (uintptr_t)(wr_id | ((uint64_t)*type.tag << type.shift_bits)); 
    msg.data      = immediate;   /** Used to store the immdiate for a block */

    const int flags = 0;         /** TODO Check if I should be passing FI_COMPLETION */
 
    FAIL_IF_NONZERO(
        ret = fi_writemsg(ep, &msg, flags),
        "fi_send() failed", REPORT_ON_FAILURE
    );

    return ret; 

}

message_type::message_type(const std::string& name, completion_handler send_handler,
                           completion_handler recv_handler,
                 completin_handler write_handler = nullptr) {

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

task::task(std::shared_ptr<manager_endpoint> manager_ep) {

}

void append_wait(const completion_queue& cq, int count, bool signaled,
                 bool last, uint64_t wr_id, const message_type& type) {

}

void append_enable_send(const managed_endpoint& ep, int count) {

}

void append_send(const managed_endpoint& ep, const memory_region& mr, 
                 size_t offset, size_t length, uint32_t immediate) {

}
void append_recv(const managed_endpoint& ep, const memory_region& mr, 
                 size_t offset, size_t length) {

}

bool post() __attribute__((warn_unused_result)) {
    return false;
}

namespace impl {
/**
 * Adds a node to the group via tcp
 */
bool rdmc_add_node(uint32_t new_id, const std::string new_ip_addr) {
   return rdmc_connections->add_node(new_id, new_ip_addr);
}

/**
 * Initialize the global context 
 */
void lf_initialize( 
    const std::map<uint32_t, std::string> &node_addrs, uint32_t node_rank) {
   
    /** Initialize the connection listener on the rdmc tcp port */     
    connection_listener = make_unique<tcp::connection_listener>(derecho::rdmc_tcp_port);
    
    /** Initialize the tcp connections, also connects all the nodes together */
    sst_connections = new tcp::tcp_connections(node_rank, node_addrs, derecho::rdmc_tcp_port); 
    
    /** Set the context to defaults to start with */
    default_context();
    load_configuration();  
   
    dbg_info(fi_tostr(g_ctxt.hints, FI_TYPE_INFO)); 

    /** Initialize the fabric, domain and completion queue */ 
    FAIL_IF_NONZERO(
        fi_getinfo(LF_VERSION, NULL, NULL, 0, g_ctxt.hints, &(g_ctxt.fi)),
        "fi_getinfo() failed", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_fabric(g_ctxt.fi->fabric_attr, &(g_ctxt.fabric), NULL),
        "fi_fabric() failed", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_domain(g_ctxt.fabric, g_ctxt.fi, &(g_ctxt.domain), NULL),
        "fi_domain() failed", CRASH_ON_FAILURE
    );

    /** Initialize the event queue, initialize and configure pep  */
    FAIL_IF_NONZERO(
        fi_eq_open(g_ctxt.fabric, &g_ctxt.eq_attr, &g_ctxt.peq, NULL),
        "failed to open the event queue for passive endpoint",CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_passive_ep(g_ctxt.fabric, g_ctxt.fi, &g_ctxt.pep, NULL),
        "failed to open a local passive endpoint", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_pep_bind(g_ctxt.pep, &g_ctxt.peq->fid, 0),
        "failed to bind event queue to passive endpoint", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_listen(g_ctxt.pep), 
        "failed to prepare passive endpoint for incoming connections", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_getname(&g_ctxt.pep->fid, g_ctxt.pep_addr, &g_ctxt.pep_addr_len),
        "failed to get the local PEP address", CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        (g_ctxt.pep_addr_len > MAX_LF_ADDR_SIZE),
        "local name is too big to fit in local buffer",CRASH_ON_FAILURE
    );
    FAIL_IF_NONZERO(
        fi_eq_open(g_ctxt.fabric, &g_ctxt.eq_attr, &g_ctxt.eq, NULL),
        "failed to open the event queue for rdma transmission.", CRASH_ON_FAILURE
    );

    /** Start a polling thread and run in the background */
    polling_thread = std::thread(polling_loop);
    polling_thread.detach();
}

void lf_destroy {

}

std::map<uint32_t, remote_memory_region> lf_exchange_memory_regions(
         const std::vector<uint32_t>& members, uint32_t node_rank,
         const memory_region& mr);

bool set_interrupt_mode(bool enabled) {
    return false;
}

}/* namespace impl */
}/* namespace rdma */
