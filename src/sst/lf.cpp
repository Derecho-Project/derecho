
/**
 * @file lf.cpp
 * Implementation of RDMA interface defined in lf.h.
 */
#include <arpa/inet.h>
#include <byteswap.h>
#include <errno.h>
#include <iostream>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/sst/detail/lf.hpp>
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/detail/sst_impl.hpp>
#include <derecho/tcp/tcp.hpp>
#include <derecho/utils/logger.hpp>

using std::cout;
using std::endl;

//from verbs.cpp
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

#ifndef NDEBUG
#define RED "\x1B[31m"
#define GRN "\x1B[32m"
#define YEL "\x1B[33m"
#define BLU "\x1B[34m"
#define MAG "\x1B[35m"
#define CYN "\x1B[36m"
#define WHT "\x1B[37m"
#define RESET "\x1B[0m"
#endif  //NDEBUG

namespace sst {

static constexpr size_t max_lf_addr_size = 128 - sizeof(uint32_t) - 2 * sizeof(uint64_t);

/**
 * passive endpoint info to be exchanged.
 */
struct cm_con_data_t {
    uint32_t pep_addr_len;  // local endpoint address length
    char pep_addr[max_lf_addr_size];
    // local endpoint address
    uint64_t mr_key;  // local memory key
    uint64_t vaddr;   // virtual addr
} __attribute__((packed));

/**
 * Global States
 */
class lf_ctxt {
public:
    // libfabric resources
    struct fi_info* hints;            // hints
    struct fi_info* fi;               // fabric information
    struct fid_fabric* fabric;        // fabric handle
    struct fid_domain* domain;        // domain handle
    struct fid_pep* pep;              // passive endpoint for receiving connection
    struct fid_eq* peq;               // event queue for connection management
    struct fid_cq* cq;                // completion queue for all rma operations
    size_t pep_addr_len;              // length of local pep address
    char pep_addr[max_lf_addr_size];  // local pep address

    // configuration resources
    struct fi_eq_attr eq_attr;  // event queue attributes
    struct fi_cq_attr cq_attr;  // completion queue attributes
    virtual ~lf_ctxt() {
        lf_destroy();
    }
};
#define LF_CONFIG_FILE "rdma.cfg"
#define LF_USE_VADDR ((g_ctxt.fi->domain_attr->mr_mode) & (FI_MR_VIRT_ADDR | FI_MR_BASIC))
static bool shutdown = false;
std::thread polling_thread;
tcp::tcp_connections* sst_connections;
tcp::tcp_connections* external_client_connections;
// singleton: global states
lf_ctxt g_ctxt;

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

/** initialize the context with default value */
static void default_context() {
    memset((void*)&g_ctxt, 0, sizeof(lf_ctxt));
    g_ctxt.hints = crash_if_nullptr("Fail to allocate fi hints", fi_allocinfo);
    //defaults the hints:
    g_ctxt.hints->caps = FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    g_ctxt.hints->ep_attr->type = FI_EP_MSG;  // use connection based endpoint by default.
    g_ctxt.hints->mode = ~0;                  // all modes

    if(g_ctxt.cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
        g_ctxt.cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    }
    g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC;

    g_ctxt.pep_addr_len = max_lf_addr_size;
}

/** load RDMA device configurations from file */
static void load_configuration() {
    if(!g_ctxt.hints) {
        dbg_default_error("lf.cpp: load_configuration error: hints is not initialized.");
        std::cerr << "lf.cpp: load_configuration error: hints is not initialized." << std::endl;
        dbg_default_flush();
        exit(-1);
    }

    // provider:
    g_ctxt.hints->fabric_attr->prov_name = crash_if_nullptr("strdup provider name.",
                                                            strdup, derecho::getConfString(CONF_RDMA_PROVIDER).c_str());
    // domain:
    g_ctxt.hints->domain_attr->name = crash_if_nullptr("strdup domain name.",
                                                       strdup, derecho::getConfString(CONF_RDMA_DOMAIN).c_str());
    if(strcmp(g_ctxt.hints->fabric_attr->prov_name, "sockets") == 0) {
        g_ctxt.hints->domain_attr->mr_mode = FI_MR_BASIC;
    } else {  // default
        g_ctxt.hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    }

    // scatter/gather batch size
    // g_ctxt.sge_bat_size = DEFAULT_SGE_BATCH_SIZE;
    // send pipeline depth
    // g_ctxt.tx_depth = DEFAULT_TX_DEPTH;
    // recv pipeline depth
    // g_ctxt.rx_depth = DEFAULT_RX_DEPTH;

    // tx_depth
    g_ctxt.hints->tx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_TX_DEPTH);
    g_ctxt.hints->rx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_RX_DEPTH);
}

int _resources::init_endpoint(struct fi_info* fi) {
    int ret = 0;

    // 1 - open completion queue - use unified cq

    // 2 - open endpoint
    ret = fail_if_nonzero_retry_on_eagain("open endpoint.", REPORT_ON_FAILURE,
                                          fi_endpoint, g_ctxt.domain, fi, &(this->ep), nullptr);

    if(ret) return ret;
    dbg_default_debug("{}:{} init_endpoint:ep->fid={}", __FILE__, __func__, (void*)&this->ep->fid);

    // 2.5 - open an event queue.
    fail_if_nonzero_retry_on_eagain("open the event queue for rdma transmission.", CRASH_ON_FAILURE,
                                    fi_eq_open, g_ctxt.fabric, &g_ctxt.eq_attr, &this->eq, nullptr);
    dbg_default_debug("{}:{} event_queue opened={}", __FILE__, __func__, (void*)&this->eq->fid);

    // 3 - bind them and global event queue together
    ret = fail_if_nonzero_retry_on_eagain("bind endpoint and event queue", REPORT_ON_FAILURE,
                                          fi_ep_bind, this->ep, &(this->eq)->fid, 0);
    if(ret) return ret;
    ret = fail_if_nonzero_retry_on_eagain("bind endpoint and tx completion queue", REPORT_ON_FAILURE,
                                          fi_ep_bind, this->ep, &(g_ctxt.cq)->fid, FI_RECV | FI_TRANSMIT | FI_SELECTIVE_COMPLETION);
    if(ret) return ret;
    ret = fail_if_nonzero_retry_on_eagain("enable endpoint", REPORT_ON_FAILURE,
                                          fi_enable, this->ep);
    return ret;
}

void _resources::connect_endpoint(bool is_lf_server) {
    dbg_default_trace("preparing connection to remote node(id=%d)...\n", this->remote_id);
    struct cm_con_data_t local_cm_data, remote_cm_data;

    // STEP 1 exchange CM info
    dbg_default_trace("Exchanging connection management info.");
    local_cm_data.pep_addr_len = (uint32_t)htonl((uint32_t)g_ctxt.pep_addr_len);
    memcpy((void*)&local_cm_data.pep_addr, &g_ctxt.pep_addr, g_ctxt.pep_addr_len);
    local_cm_data.mr_key = (uint64_t)htonll(this->mr_lwkey);
    local_cm_data.vaddr = (uint64_t)htonll((uint64_t)this->write_buf);  // for pull mode

    if(sst_connections->contains_node(this->remote_id)) {
        if(!sst_connections->exchange(this->remote_id, local_cm_data, remote_cm_data)) {
            dbg_default_error("Failed to exchange connection management info with node {}", this->remote_id);
            crash_with_message("Failed to exchange connection management info with node %d\n", this->remote_id);
        }
    } else {
        if(!external_client_connections->exchange(this->remote_id, local_cm_data, remote_cm_data)) {
            dbg_default_error("Failed to exchange connection management info with node {}", this->remote_id);
            crash_with_message("Failed to exchange connection management info with node %d\n", this->remote_id);
        }
    }

    remote_cm_data.pep_addr_len = (uint32_t)ntohl(remote_cm_data.pep_addr_len);
    this->mr_rwkey = (uint64_t)ntohll(remote_cm_data.mr_key);
    this->remote_fi_addr = (fi_addr_t)ntohll(remote_cm_data.vaddr);
    dbg_default_trace("Exchanging connection management info succeeds.");

    // STEP 2 connect to remote
    dbg_default_trace("connect to remote node.");
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    if(is_lf_server) {
        dbg_default_trace("connecting as a server.");
        dbg_default_trace("waiting for connection.");

        nRead = fi_eq_sread(g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            dbg_default_error("failed to get connection from remote.");
            crash_with_message("failed to get connection from remote. nRead=%ld\n", nRead);
        }
        if(init_endpoint(entry.info)) {
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            crash_with_message("failed to initialize server endpoint.\n");
        }
        if(fi_accept(this->ep, NULL, 0)) {
            fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            crash_with_message("failed to accept connection.\n");
        }
        fi_freeinfo(entry.info);
    } else {
        // libfabric connection client
        dbg_default_trace("connecting as a client.\n");
        dbg_default_trace("initiating a connection.\n");

        struct fi_info* client_hints = fi_dupinfo(g_ctxt.hints);
        struct fi_info* client_info = NULL;

        client_hints->dest_addr = crash_if_nullptr("failed to malloc address space for server pep.",
                                                   malloc, remote_cm_data.pep_addr_len);
        memcpy((void*)client_hints->dest_addr, (void*)remote_cm_data.pep_addr, (size_t)remote_cm_data.pep_addr_len);
        client_hints->dest_addrlen = remote_cm_data.pep_addr_len;
        fail_if_nonzero_retry_on_eagain("fi_getinfo() failed.", CRASH_ON_FAILURE,
                                        fi_getinfo, LF_VERSION, nullptr, nullptr, 0, client_hints, &client_info);
        if(init_endpoint(client_info)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            crash_with_message("failed to initialize client endpoint.\n");
        }

        fail_if_nonzero_retry_on_eagain("fi_connect()", CRASH_ON_FAILURE,
                                        fi_connect, this->ep, remote_cm_data.pep_addr, nullptr, 0);

        nRead = fi_eq_sread(this->eq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            dbg_default_error("failed to connect remote.");
            crash_with_message("failed to connect remote. nRead=%ld.\n", nRead);
        }
        dbg_default_debug("{}:{} entry.fid={},this->ep->fid={}", __FILE__, __func__, (void*)entry.fid, (void*)&(this->ep->fid));
        if(event != FI_CONNECTED || entry.fid != &(this->ep->fid)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            dbg_default_flush();
            crash_with_message("SST: Unexpected CM event: %d.\n", event);
        }

        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
    }
    sync(remote_id);
}

/**
 * Implementation for Public APIs
 */
_resources::_resources(
        int r_id,
        char* write_addr,
        char* read_addr,
        int size_w,
        int size_r,
        int is_lf_server)
        : remote_failed(false),
          remote_id(r_id),
          write_buf(write_addr),
          read_buf(read_addr) {
    dbg_default_trace("resources constructor: this={}", (void*)this);

    if(!write_addr) {
        dbg_default_warn("{}:{} called with NULL write_addr!", __FILE__, __func__);
    }

    if(!read_addr) {
        dbg_default_warn("{}:{} called with NULL read_addr!", __FILE__, __func__);
    }

#define LF_RMR_KEY(rid) (((uint64_t)0xf0000000) << 32 | (uint64_t)(rid))
#define LF_WMR_KEY(rid) (((uint64_t)0xf8000000) << 32 | (uint64_t)(rid))
    // register the write buffer
    fail_if_nonzero_retry_on_eagain("register memory buffer for write", CRASH_ON_FAILURE,
                                    fi_mr_reg, g_ctxt.domain, write_buf, size_w,
                                    FI_SEND | FI_RECV | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE,
                                    0, 0, 0, &this->write_mr, nullptr);
    dbg_default_trace("{}:{} registered memory for remote write: {}:{}", __FILE__, __func__, (void*)write_addr, size_w);
    // register the read buffer
    fail_if_nonzero_retry_on_eagain("register memory buffer for read", CRASH_ON_FAILURE,
                                    fi_mr_reg, g_ctxt.domain, read_buf, size_r,
                                    FI_SEND | FI_RECV | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE,
                                    0, 0, 0, &this->read_mr, nullptr);
    dbg_default_trace("{}:{} registered memory for remote read: {}:{}", __FILE__, __func__, (void*)read_addr, size_r);

    this->mr_lrkey = fi_mr_key(this->read_mr);
    if(this->mr_lrkey == FI_KEY_NOTAVAIL) {
        crash_with_message("fail to get read memory key.");
    }
    this->mr_lwkey = fi_mr_key(this->write_mr);
    dbg_default_trace("{}:{} local write key:{}, local read key:{}", __FILE__, __func__, (uint64_t)this->mr_lwkey, (uint64_t)this->mr_lrkey);
    if(this->mr_lwkey == FI_KEY_NOTAVAIL) {
        crash_with_message("fail to get write memory key.");
    }
    // set up the endpoint
    connect_endpoint(is_lf_server);
}

_resources::~_resources() {
    dbg_default_trace("resources destructor:this={}", (void*)this);
    if(this->ep) {
        fail_if_nonzero_retry_on_eagain("close endpoint", REPORT_ON_FAILURE,
                                        fi_close, &this->ep->fid);
    }
    if(this->eq) {
        fail_if_nonzero_retry_on_eagain("close event", REPORT_ON_FAILURE,
                                        fi_close, &this->eq->fid);
    }
    if(this->write_mr)
        fail_if_nonzero_retry_on_eagain("unregister write mr", REPORT_ON_FAILURE,
                                        fi_close, &this->write_mr->fid);
    if(this->read_mr)
        fail_if_nonzero_retry_on_eagain("unregister read mr", REPORT_ON_FAILURE,
                                        fi_close, &this->read_mr->fid);
}

int _resources::post_remote_send(
        lf_sender_ctxt* ctxt,
        const long long int offset,
        const long long int size,
        const int op,
        const bool completion) {
    // dbg_default_trace("resources::post_remote_send(),this={}",(void*)this);
    // #ifdef !NDEBUG
    // printf(YEL "resources::post_remote_send(),this=%p\n" RESET, this);
    // fflush(stdout);
    // #endif
    // dbg_default_trace("resources::post_remote_send(ctxt=({},{}),offset={},size={},op={},completion={})",ctxt?ctxt->ce_idx:0,ctxt?ctxt->remote_id:0,offset,size,op,completion);

    if(remote_failed) {
        dbg_default_warn("lf.cpp: remote has failed, post_remote_send() does nothing.");
        return -EFAULT;
    }
    int ret = 0;

    if(op == 2) {  // two sided send
        struct fi_msg msg;
        struct iovec msg_iov;

        msg_iov.iov_base = read_buf + offset;
        msg_iov.iov_len = size;

        msg.msg_iov = &msg_iov;
        msg.desc = (void**)&this->mr_lrkey;
        msg.iov_count = 1;
        msg.addr = 0;
        msg.context = (void*)ctxt;
        msg.data = 0l;  // not used

        ret = fail_if_nonzero_retry_on_eagain("fi_sendmsg failed.", REPORT_ON_FAILURE,
                                              fi_sendmsg, this->ep, &msg,
                                              (completion) ? (FI_COMPLETION | FI_REMOTE_CQ_DATA) : (FI_REMOTE_CQ_DATA));
    } else {  // one sided send or receive
        struct iovec msg_iov;
        struct fi_rma_iov rma_iov;
        struct fi_msg_rma msg;

        msg_iov.iov_base = read_buf + offset;
        msg_iov.iov_len = size;

        rma_iov.addr = ((LF_USE_VADDR) ? remote_fi_addr : 0) + offset;
        rma_iov.len = size;
        rma_iov.key = this->mr_rwkey;

        msg.msg_iov = &msg_iov;
        msg.desc = (void**)&this->mr_lrkey;
        msg.iov_count = 1;
        msg.addr = 0;  // not used for a connection endpoint
        msg.rma_iov = &rma_iov;
        msg.rma_iov_count = 1;
        msg.context = (void*)ctxt;
        msg.data = 0l;  // not used

        // dbg_default_trace("{}:{} calling fi_writemsg/fi_readmsg with",__FILE__,__func__);
        // dbg_default_trace("remote addr = {} len = {} key = {}",(void*)rma_iov.addr,rma_iov.len,(uint64_t)this->mr_rwkey);
        // dbg_default_trace("local addr = {} len = {} key = {}",(void*)msg_iov.iov_base,msg_iov.iov_len,(uint64_t)this->mr_lrkey);
        // dbg_default_flush();

        auto remote_has_failed = [this]() { return remote_failed.load(); };
        if(op == 1) {  //write
            ret = retry_on_eagain_unless("fi_writemsg failed.", remote_has_failed,
                                         fi_writemsg, this->ep, &msg, (completion) ? FI_COMPLETION : 0);
        } else {  // read op==0
            ret = retry_on_eagain_unless("fi_readmsg failed.", remote_has_failed,
                                         fi_readmsg, this->ep, &msg, (completion) ? FI_COMPLETION : 0);
        }
    }
    // dbg_default_trace("post_remote_send return with ret={}",ret);
    // dbg_default_flush();
    // #ifdef !NDEBUG
    // printf(YEL "resources::post_remote_send return with ret=%d\n" RESET, ret);
    // fflush(stdout);
    // #endif//!NDEBUG
    return ret;
}

void resources::report_failure() {
    remote_failed = true;
}

void resources::post_remote_read(const long long int size) {
    int return_code = post_remote_send(NULL, 0, size, 0, false);
    if(return_code != 0) {
        dbg_default_error("post_remote_read(1) failed with return code {}", return_code);
        std::cerr << "post_remote_read(1) failed with return code " << return_code << std::endl;
    }
}

void resources::post_remote_read(const long long int offset, const long long int size) {
    int return_code = post_remote_send(NULL, offset, size, 0, false);
    if(return_code != 0) {
        dbg_default_error("post_remote_read(2) failed with return code {}", return_code);
        std::cerr << "post_remote_read(2) failed with return code " << return_code << std::endl;
    }
}

void resources::post_remote_write(const long long int size) {
    int return_code = post_remote_send(NULL, 0, size, 1, false);
    if(return_code != 0) {
        dbg_default_error("post_remote_write(1) failed with return code {}", return_code);
        std::cerr << "post_remote_write(1) failed with return code " << return_code << std::endl;
    }
}

void resources::post_remote_write(const long long int offset, long long int size) {
    int return_code = post_remote_send(NULL, offset, size, 1, false);
    if(return_code != 0) {
        dbg_default_error("post_remote_write(2) failed with return code {}", return_code);
        std::cerr << "post_remote_write(2) failed with return code " << return_code << std::endl;
    }
}

void resources::post_remote_write_with_completion(lf_sender_ctxt* ctxt, const long long int size) {
    int return_code = post_remote_send(ctxt, 0, size, 1, true);
    if(return_code != 0) {
        dbg_default_error("post_remote_write(3) failed with return code {}", return_code);
        std::cerr << "post_remote_write(3) failed with return code " << return_code << std::endl;
    }
}

void resources::post_remote_write_with_completion(lf_sender_ctxt* ctxt, const long long int offset, const long long int size) {
    int return_code = post_remote_send(ctxt, offset, size, 1, true);
    if(return_code != 0) {
        dbg_default_error("post_remote_write(4) failed with return code {}", return_code);
        std::cerr << "post_remote_write(4) failed with return code " << return_code << std::endl;
    }
}

void resources_two_sided::report_failure() {
    remote_failed = true;
}

/**
 * @param size The number of bytes to write from the local buffer to remote
 * memory.
 */
void resources_two_sided::post_two_sided_send(const long long int size) {
    int rc = post_remote_send(NULL, 0, size, 2, false);
    if(rc) {
        cout << "Could not post RDMA two sided send (with no offset), error code is " << rc << endl;
    }
}

/**
 * @param offset The offset, in bytes, of the remote memory buffer at which to
 * start writing.
 * @param size The number of bytes to write from the local buffer into remote
 * memory.
 */
void resources_two_sided::post_two_sided_send(const long long int offset, const long long int size) {
    int rc = post_remote_send(NULL, offset, size, 2, false);
    if(rc) {
        cout << "Could not post RDMA two sided send with offset, error code is " << rc << endl;
    }
}

void resources_two_sided::post_two_sided_send_with_completion(lf_sender_ctxt* ctxt, const long long int size) {
    int rc = post_remote_send(ctxt, 0, size, 2, true);
    if(rc) {
        cout << "Could not post RDMA two sided send (with no offset) with completion, error code is " << rc << endl;
    }
}

void resources_two_sided::post_two_sided_send_with_completion(lf_sender_ctxt* ctxt, const long long int offset, const long long int size) {
    int rc = post_remote_send(ctxt, offset, size, 2, true);
    if(rc) {
        cout << "Could not post RDMA two sided send with offset and completion, error code is " << rc << ", remote_id is" << ctxt->remote_id() << endl;
    }
}

void resources_two_sided::post_two_sided_receive(lf_sender_ctxt* ctxt, const long long int size) {
    int rc = post_receive(ctxt, 0, size);
    if(rc) {
        cout << "Could not post RDMA two sided receive (with no offset), error code is " << rc << ", remote_id is " << ctxt->remote_id() << endl;
    }
}

void resources_two_sided::post_two_sided_receive(lf_sender_ctxt* ctxt, const long long int offset, const long long int size) {
    int rc = post_receive(ctxt, offset, size);
    if(rc) {
        cout << "Could not post RDMA two sided receive with offset, error code is " << rc << ", remote_id is " << ctxt->remote_id() << endl;
    }
}

int resources_two_sided::post_receive(lf_sender_ctxt* ctxt, const long long int offset, const long long int size) {
    struct iovec msg_iov;
    struct fi_msg msg;
    int ret;

    msg_iov.iov_base = write_buf + offset;
    msg_iov.iov_len = size;

    msg.msg_iov = &msg_iov;
    msg.desc = (void**)&this->mr_lwkey;
    msg.iov_count = 1;
    msg.addr = 0;  // not used
    msg.context = (void*)ctxt;
    ret = fail_if_nonzero_retry_on_eagain("fi_recvmsg", REPORT_ON_FAILURE,
                                          fi_recvmsg, this->ep, &msg, FI_COMPLETION | FI_REMOTE_CQ_DATA);
    return ret;
}

bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return sst_connections->add_node(new_id, new_ip_addr_and_port);
}

bool add_external_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return external_client_connections->add_node(new_id, new_ip_addr_and_port);
}

bool remove_node(uint32_t node_id) {
    if(sst_connections->contains_node(node_id)) {
        return sst_connections->delete_node(node_id);
    } else {
        return external_client_connections->delete_node(node_id);
    }
}

bool sync(uint32_t r_id) {
    int s = 0, t = 0;
    if(sst_connections->contains_node(r_id)) {
        return sst_connections->exchange(r_id, s, t);
    } else {
        return external_client_connections->exchange(r_id, s, t);
    }
}

void filter_external_to(const std::vector<node_id_t>& live_nodes_list) {
    external_client_connections->filter_to(live_nodes_list);
}

void polling_loop() {
    pthread_setname_np(pthread_self(), "sst_poll");
    dbg_default_trace("Polling thread starting.");

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    while(!shutdown) {
        auto ce = lf_poll_completion();
        if(shutdown) {
            break;
        }
        if(ce.first != 0xFFFFFFFF) {
            util::polling_data.insert_completion_entry(ce.first, ce.second);

            // update last time
            clock_gettime(CLOCK_REALTIME, &last_time);
        } else {
            clock_gettime(CLOCK_REALTIME, &cur_time);
            // check if the system has been inactive for enough time to induce sleep
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                        + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            if(time_elapsed_in_ms > 1) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
            }
        }
    }
    dbg_default_trace("Polling thread ending.");
}

/**
 * @details
 * This blocks until a single entry in the completion queue has
 * completed
 * It is exclusively used by the polling thread
 * the thread can sleep while in this function, when it calls util::polling_data.wait_for_requests
 * @return pair(remote_id,result) The queue pair number associated with the
 * completed request and the result (1 for successful, -1 for unsuccessful)
 */
std::pair<uint32_t, std::pair<int32_t, int32_t>> lf_poll_completion() {
    struct fi_cq_entry entry;
    int poll_result = 0;

    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

    while(!shutdown) {
        clock_gettime(CLOCK_REALTIME, &cur_time);
        // check if the system has been inactive for enough time to induce sleep
        double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                    + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
        if(time_elapsed_in_ms > 1) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        }

        poll_result = 0;
        for(int i = 0; i < 50; ++i) {
            poll_result = fi_cq_read(g_ctxt.cq, &entry, 1);
            if(poll_result && (poll_result != -FI_EAGAIN)) {
                break;
            }
        }
        if(poll_result && (poll_result != -FI_EAGAIN)) {
            break;
        }
        // util::polling_data.wait_for_requests();
    }
    // not sure what to do when we cannot read entries off the CQ
    // this means that something is wrong with the local node
    if((poll_result < 0) && (poll_result != -FI_EAGAIN)) {
        struct fi_cq_err_entry eentry;
        fi_cq_readerr(g_ctxt.cq, &eentry, 0);

        dbg_default_error("fi_cq_readerr() read the following error entry:");
        if(eentry.op_context == NULL) {
            dbg_default_error("\top_context:NULL");
        } else {
#ifndef NOLOG
            lf_sender_ctxt* sctxt = (lf_sender_ctxt*)eentry.op_context;
#endif
            dbg_default_error("\top_context:ce_idx={},remote_id={}", sctxt->ce_idx(), sctxt->remote_id());
        }
#ifdef DEBUG_FOR_RELEASE
        printf("\tflags=%x\n", eentry.flags);
        printf("\tlen=%x\n", eentry.len);
        printf("\tbuf=%p\n", eentry.buf);
        printf("\tdata=0x%x\n", eentry.data);
        printf("\ttag=0x%x\n", eentry.tag);
        printf("\tolen=0x%x\n", eentry.olen);
        printf("\terr=0x%x\n", eentry.err);
#endif  //DEBUG_FOR_RELEASE
        dbg_default_error("\tflags={}", eentry.flags);
        dbg_default_error("\tlen={}", eentry.len);
        dbg_default_error("\tbuf={}", eentry.buf);
        dbg_default_error("\tdata={}", eentry.data);
        dbg_default_error("\ttag={}", eentry.tag);
        dbg_default_error("\tolen={}", eentry.olen);
        dbg_default_error("\terr={}", eentry.err);
#ifndef NOLOG
        char errbuf[1024];
#endif
        dbg_default_error("\tprov_errno={}:{}", eentry.prov_errno,
                          fi_cq_strerror(g_ctxt.cq, eentry.prov_errno, eentry.err_data, errbuf, 1024));
#ifdef DEBUG_FOR_RELEASE
        printf("\tproverr=0x%x,%s\n", eentry.prov_errno,
               fi_cq_strerror(g_ctxt.cq, eentry.prov_errno, eentry.err_data, errbuf, 1024));
#endif  //DEBUG_FOR_RELEASE
        dbg_default_error("\terr_data={}", eentry.err_data);
        dbg_default_error("\terr_data_size={}", eentry.err_data_size);
#ifdef DEBUG_FOR_RELEASE
        printf("\terr_data_size=%d\n", eentry.err_data_size);
#endif  //DEBUG_FOR_RELEASE
        /**
         * Since eentry.op_context is unreliable, we ignore it with returning a generic error.
        if(eentry.op_context != NULL) {
            lf_sender_ctxt* sctxt = (lf_sender_ctxt*)eentry.op_context;
            return {sctxt->ce_idx(), {sctxt->remote_id(), -1}};
        } else {
            dbg_default_error("\tFailed polling the completion queue");
            fprintf(stderr, "Failed polling the completion queue");
            return {(uint32_t)0xFFFFFFFF, {0, -1}};  // we don't know who sent the message.
        }*/
        dbg_default_error("\tFailed polling the completion queue");
        return {(uint32_t)0xFFFFFFFF, {0, -1}};  // we don't know who sent the message.
    }
    if(!shutdown) {
        lf_sender_ctxt* sctxt = (lf_sender_ctxt*)entry.op_context;
        if(sctxt == NULL) {
            dbg_default_debug("WEIRD: we get an entry with op_context = NULL.");
            return {0xFFFFFFFFu, {0, 0}};  // return a bad entry: weird!!!!
        } else {
            //dbg_default_trace("Normal: we get an entry with op_context = {}.",(long long unsigned)sctxt);
            return {sctxt->ce_idx(), {sctxt->remote_id(), 1}};
        }
    } else {  // shutdown return a bad entry
        return {0, {0, 0}};
    }
}

void lf_initialize(const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>& internal_ip_addrs_and_ports,
                   const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>& external_ip_addrs_and_ports,
                   uint32_t node_id) {
    // initialize derecho connection manager: This is derived from Sagar's code.
    // May there be a better desgin?
    sst_connections = new tcp::tcp_connections(node_id, internal_ip_addrs_and_ports);
    external_client_connections = new tcp::tcp_connections(node_id, external_ip_addrs_and_ports);

    // initialize global resources:
    // STEP 1: initialize with configuration.
    default_context();     // default the context
    load_configuration();  // load configuration

    //dbg_default_info(fi_tostr(g_ctxt.hints,FI_TYPE_INFO));
    // STEP 2: initialize fabric, domain, and completion queue
    fail_if_nonzero_retry_on_eagain("fi_getinfo()", CRASH_ON_FAILURE,
                                    fi_getinfo, LF_VERSION, nullptr, nullptr, 0, g_ctxt.hints, &(g_ctxt.fi));
    dbg_default_trace("going to use virtual address?{}", LF_USE_VADDR);
    fail_if_nonzero_retry_on_eagain("fi_fabric()", CRASH_ON_FAILURE,
                                    fi_fabric, g_ctxt.fi->fabric_attr, &(g_ctxt.fabric), nullptr);
    fail_if_nonzero_retry_on_eagain("fi_domain()", CRASH_ON_FAILURE,
                                    fi_domain, g_ctxt.fabric, g_ctxt.fi, &(g_ctxt.domain), nullptr);
    // Note: we don't have a way to throttle the sender based on the completion queue size.
    // Therefore we just use a very large completion queue size to buffer cq entries as much as possible.
    // ibv_query_device() in verbs API reports max_cqe, which is "Maximum number of entries in each CQ supported
    // by this device". The number is 4194303 (2^22-1) with Mellanox connectx-4 VPI. We hard lift the setting to
    // >=2097151(2^21-1), hoping it works for as many RDMA devices as possible. TODO: find a better approach
    // to determining completion queue size.
    size_t max_cqe = g_ctxt.fi->tx_attr->size * internal_ip_addrs_and_ports.size()
                     + g_ctxt.fi->tx_attr->size * external_ip_addrs_and_ports.size();
    g_ctxt.cq_attr.size = (max_cqe > 2097152)? max_cqe:2097152;
    fail_if_nonzero_retry_on_eagain("initialize tx completion queue.", REPORT_ON_FAILURE,
                                    fi_cq_open, g_ctxt.domain, &(g_ctxt.cq_attr), &(g_ctxt.cq), nullptr);

    // STEP 3: prepare local PEP
    fail_if_nonzero_retry_on_eagain("open the event queue for passive endpoint", CRASH_ON_FAILURE,
                                    fi_eq_open, g_ctxt.fabric, &g_ctxt.eq_attr, &g_ctxt.peq, nullptr);
    fail_if_nonzero_retry_on_eagain("open a local passive endpoint", CRASH_ON_FAILURE,
                                    fi_passive_ep, g_ctxt.fabric, g_ctxt.fi, &g_ctxt.pep, nullptr);
    fail_if_nonzero_retry_on_eagain("binding event queue to passive endpoint", CRASH_ON_FAILURE,
                                    fi_pep_bind, g_ctxt.pep, &g_ctxt.peq->fid, 0);
    fail_if_nonzero_retry_on_eagain("preparing passive endpoint for incoming connections", CRASH_ON_FAILURE,
                                    fi_listen, g_ctxt.pep);
    fail_if_nonzero_retry_on_eagain("get the local PEP address", CRASH_ON_FAILURE,
                                    fi_getname, &g_ctxt.pep->fid, g_ctxt.pep_addr, &g_ctxt.pep_addr_len);
    if(g_ctxt.pep_addr_len > max_lf_addr_size) {
        crash_with_message("LibFabric error! local name is too big to fit in local buffer");
    }

    // STEP 4: start polling thread.
    polling_thread = std::thread(polling_loop);
}

void shutdown_polling_thread() {
    shutdown = true;
    if(polling_thread.joinable()) {
        polling_thread.join();
    }
}

void lf_destroy() {
    shutdown_polling_thread();
    // TODO: make sure all resources are destroyed first.
    if(g_ctxt.pep) {
        fail_if_nonzero_retry_on_eagain("close passive endpoint", REPORT_ON_FAILURE,
                                        fi_close, &g_ctxt.pep->fid);
    }
    if(g_ctxt.peq) {
        fail_if_nonzero_retry_on_eagain("close event queue for passive endpoint", REPORT_ON_FAILURE,
                                        fi_close, &g_ctxt.peq->fid);
    }
    if(g_ctxt.cq) {
        fail_if_nonzero_retry_on_eagain("close completion queue", REPORT_ON_FAILURE,
                                        fi_close, &g_ctxt.cq->fid);
    }
    if(g_ctxt.domain) {
        fail_if_nonzero_retry_on_eagain("close domain", REPORT_ON_FAILURE,
                                        fi_close, &g_ctxt.domain->fid);
    }
    if(g_ctxt.fabric) {
        fail_if_nonzero_retry_on_eagain("close fabric", REPORT_ON_FAILURE,
                                        fi_close, &g_ctxt.fabric->fid);
    }
    if(g_ctxt.fi) {
        fi_freeinfo(g_ctxt.fi);
        g_ctxt.hints = nullptr;
    }
    if(g_ctxt.hints) {
        fi_freeinfo(g_ctxt.hints);
        g_ctxt.hints = nullptr;
    }
}
}  // namespace sst
