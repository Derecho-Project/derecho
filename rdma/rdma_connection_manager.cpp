#include "rdma_connection_manager.hpp"

#include <cassert>

#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include "conf/conf.hpp"

namespace rdma {
static tcp::TCPConnectionManager* connections;
failure_upcall_t RDMAConnectionManager::failure_upcall;
lf_ctxt RDMAConnectionManager::g_ctxt;

/**
 * Internal Tools
 */
#define CRASH_WITH_MESSAGE(...)       \
    do {                              \
        fprintf(stderr, __VA_ARGS__); \
        fflush(stderr);               \
        exit(-1);                     \
    } while(0);
// Test tools
enum NextOnFailure {
    REPORT_ON_FAILURE = 0,
    CRASH_ON_FAILURE = 1
};
#define FAIL_IF_NONZERO_RETRY_EAGAIN(x, desc, next)                                     \
    do {                                                                                \
        int64_t _int64_r_;                                                              \
        do {                                                                            \
            _int64_r_ = (int64_t)(x);                                                   \
        } while(_int64_r_ == -FI_EAGAIN);                                               \
        if(_int64_r_ != 0) {                                                            \
            fprintf(stderr, "%s:%d,ret=%ld,%s\n", __FILE__, __LINE__, _int64_r_, desc); \
            if(next == CRASH_ON_FAILURE) {                                              \
                fflush(stderr);                                                         \
                exit(-1);                                                               \
            }                                                                           \
        }                                                                               \
    } while(0)
#define FAIL_IF_ZERO(x, desc, next)                                  \
    do {                                                             \
        int64_t _int64_r_ = (int64_t)(x);                            \
        if(_int64_r_ == 0) {                                         \
            fprintf(stderr, "%s:%d,%s\n", __FILE__, __LINE__, desc); \
            if(next == CRASH_ON_FAILURE) {                           \
                fflush(stderr);                                      \
                exit(-1);                                            \
            }                                                        \
        }                                                            \
    } while(0)

/** Initialize the libfabric context with default values */
static void default_context() {
    memset((void*)&RDMAConnectionManager::g_ctxt, 0, sizeof(lf_ctxt));
    FAIL_IF_ZERO(RDMAConnectionManager::g_ctxt.hints = fi_allocinfo(), "Fail to allocate fi hints", CRASH_ON_FAILURE);
    //defaults the hints:
    RDMAConnectionManager::g_ctxt.hints->caps = FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    RDMAConnectionManager::g_ctxt.hints->ep_attr->type = FI_EP_MSG;  // use connection based endpoint by default.
    RDMAConnectionManager::g_ctxt.hints->mode = ~0;                  // all modes

    if(RDMAConnectionManager::g_ctxt.cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
        RDMAConnectionManager::g_ctxt.cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    }
    RDMAConnectionManager::g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC;

    RDMAConnectionManager::g_ctxt.pep_addr_len = MAX_LF_ADDR_SIZE;
}

/** load RDMA device configurations from file */
static void load_configuration() {
    FAIL_IF_ZERO(RDMAConnectionManager::g_ctxt.hints, "hints is not initialized.", CRASH_ON_FAILURE);

    // provider:
    FAIL_IF_ZERO(RDMAConnectionManager::g_ctxt.hints->fabric_attr->prov_name = strdup(derecho::getConfString(CONF_RDMA_PROVIDER).c_str()),
                 "strdup provider name.", CRASH_ON_FAILURE);
    // domain:
    FAIL_IF_ZERO(RDMAConnectionManager::g_ctxt.hints->domain_attr->name = strdup(derecho::getConfString(CONF_RDMA_DOMAIN).c_str()),
                 "strdup domain name.", CRASH_ON_FAILURE);
    if(strcmp(RDMAConnectionManager::g_ctxt.hints->fabric_attr->prov_name, "sockets") == 0) {
        RDMAConnectionManager::g_ctxt.hints->domain_attr->mr_mode = FI_MR_BASIC;
    } else {  // default
        RDMAConnectionManager::g_ctxt.hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    }

    // tx_depth
    RDMAConnectionManager::g_ctxt.hints->tx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_TX_DEPTH);
    RDMAConnectionManager::g_ctxt.hints->rx_attr->size = derecho::Conf::get()->getInt32(CONF_RDMA_RX_DEPTH);
}

void initialize(node_id_t my_id, const std::map<node_id_t, std::pair<tcp::ip_addr_t, tcp::port_t>>& ip_addrs_and_ports, const failure_upcall_t& failure_upcall) {
    connections = new tcp::TCPConnectionManager(my_id, ip_addrs_and_ports);

    // Initialize RDMAConnectionManager libfabric resources

    // STEP 1: initialize with configuration.
    default_context();     // default the context
    load_configuration();  // load configuration

    // STEP 2: initialize fabric, domain, and completion queue
    FAIL_IF_NONZERO_RETRY_EAGAIN(
            fi_getinfo(LF_VERSION, NULL, NULL, 0, RDMAConnectionManager::g_ctxt.hints, &(RDMAConnectionManager::g_ctxt.fi)),
            "fi_getinfo()",
            CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(
            fi_fabric(RDMAConnectionManager::g_ctxt.fi->fabric_attr, &(RDMAConnectionManager::g_ctxt.fabric), NULL),
            "fi_fabric()",
            CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(
            fi_domain(RDMAConnectionManager::g_ctxt.fabric, RDMAConnectionManager::g_ctxt.fi, &(RDMAConnectionManager::g_ctxt.domain), NULL),
            "fi_domain()",
            CRASH_ON_FAILURE);
    RDMAConnectionManager::g_ctxt.cq_attr.size = RDMAConnectionManager::g_ctxt.fi->tx_attr->size;
    FAIL_IF_NONZERO_RETRY_EAGAIN(
            fi_cq_open(RDMAConnectionManager::g_ctxt.domain, &(RDMAConnectionManager::g_ctxt.cq_attr), &(RDMAConnectionManager::g_ctxt.cq), NULL),
            "initialize tx completion queue.",
            REPORT_ON_FAILURE);

    // STEP 3: prepare local PEP
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_eq_open(RDMAConnectionManager::g_ctxt.fabric, &RDMAConnectionManager::g_ctxt.eq_attr, &RDMAConnectionManager::g_ctxt.peq, NULL), "open the event queue for passive endpoint", CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_passive_ep(RDMAConnectionManager::g_ctxt.fabric, RDMAConnectionManager::g_ctxt.fi, &RDMAConnectionManager::g_ctxt.pep, NULL), "open a local passive endpoint", CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_pep_bind(RDMAConnectionManager::g_ctxt.pep, &RDMAConnectionManager::g_ctxt.peq->fid, 0), "binding event queue to passive endpoint", CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_listen(RDMAConnectionManager::g_ctxt.pep), "preparing passive endpoint for incoming connections", CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_getname(&RDMAConnectionManager::g_ctxt.pep->fid, RDMAConnectionManager::g_ctxt.pep_addr, &RDMAConnectionManager::g_ctxt.pep_addr_len), "get the local PEP address", CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN((RDMAConnectionManager::g_ctxt.pep_addr_len > MAX_LF_ADDR_SIZE), "local name is too big to fit in local buffer", CRASH_ON_FAILURE);

    // TODO: Polling thread

    RDMAConnectionManager::failure_upcall = failure_upcall;
    for(auto p : ip_addrs_and_ports) {
        if(my_id != p.first) {
            RDMAConnectionManager::add(my_id, p.first);
        }
    }
}

  void lf_destroy(){
    // TODO: make sure all resources are destroyed first.
    if (RDMAConnectionManager::g_ctxt.pep) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&RDMAConnectionManager::g_ctxt.pep->fid),"close passive endpoint",REPORT_ON_FAILURE);
    }
    if (RDMAConnectionManager::g_ctxt.peq) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&RDMAConnectionManager::g_ctxt.peq->fid),"close event queue for passive endpoint",REPORT_ON_FAILURE);
    }
    if (RDMAConnectionManager::g_ctxt.cq) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&RDMAConnectionManager::g_ctxt.cq->fid),"close completion queue",REPORT_ON_FAILURE);
    }
    // g_ctxt.eq has been moved to resources
    // if (g_ctxt.eq) {
    //  FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.eq->fid),"close event queue",REPORT_ON_FAILURE);
    // }
    if (RDMAConnectionManager::g_ctxt.domain) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&RDMAConnectionManager::g_ctxt.domain->fid),"close domain",REPORT_ON_FAILURE);
    }
    if (RDMAConnectionManager::g_ctxt.fabric) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&RDMAConnectionManager::g_ctxt.fabric->fid),"close fabric",REPORT_ON_FAILURE);
    }
    if (RDMAConnectionManager::g_ctxt.fi) {
      fi_freeinfo(RDMAConnectionManager::g_ctxt.fi);
      RDMAConnectionManager::g_ctxt.hints = nullptr;
    }
    if (RDMAConnectionManager::g_ctxt.hints) {
      fi_freeinfo(RDMAConnectionManager::g_ctxt.hints);
      RDMAConnectionManager::g_ctxt.hints = nullptr;
    }
  }

tcp::TCPConnectionManager* get_connections() {
    return connections;
}

int RDMAConnection::init_endpoint(struct fi_info* fi) {
    int ret = 0;

    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_endpoint(RDMAConnectionManager::g_ctxt.domain, fi, &(this->ep), NULL), "open endpoint.", REPORT_ON_FAILURE);
    if(ret) return ret;

    // 2.5 - open an event queue.
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_eq_open(RDMAConnectionManager::g_ctxt.fabric, &RDMAConnectionManager::g_ctxt.eq_attr, &this->eq, NULL), "open the event queue for rdma transmission.", CRASH_ON_FAILURE);

    // 3 - bind them and global event queue together
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_ep_bind(this->ep, &(this->eq)->fid, 0), "bind endpoint and event queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_ep_bind(this->ep, &(RDMAConnectionManager::g_ctxt.cq)->fid, FI_RECV | FI_TRANSMIT | FI_SELECTIVE_COMPLETION), "bind endpoint and tx completion queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_enable(this->ep), "enable endpoint", REPORT_ON_FAILURE);
    return ret;
}

RDMAConnection::RDMAConnection(node_id_t my_id, node_id_t remote_id) : my_id(my_id),
                                                                       remote_id(remote_id) {
    // first create the connection data to exchange
    RDMAConnectionData local_connection_data;
    RDMAConnectionData remote_connection_data;

    // STEP 1 exchange CM info
    local_connection_data.pep_addr_len = (uint32_t)htonl((uint32_t)RDMAConnectionManager::g_ctxt.pep_addr_len);
    memcpy((void*)&local_connection_data.pep_addr,&RDMAConnectionManager::g_ctxt.pep_addr,RDMAConnectionManager::g_ctxt.pep_addr_len);

    FAIL_IF_ZERO(connections->exchange(remote_id, local_connection_data, remote_connection_data),"exchange connection management info.",CRASH_ON_FAILURE);

    remote_connection_data.pep_addr_len = (uint32_t)ntohl(remote_connection_data.pep_addr_len);

    // proceed with the rest of the connection - create endpoints etc.
    // STEP 2 connect to remote
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    bool is_lf_server = (my_id < remote_id);
    if(is_lf_server) {
        nRead = fi_eq_sread(RDMAConnectionManager::g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            CRASH_WITH_MESSAGE("failed to get connection from remote. nRead=%ld\n", nRead);
        }
        if(init_endpoint(entry.info)) {
            fi_reject(RDMAConnectionManager::g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            CRASH_WITH_MESSAGE("failed to initialize server endpoint.\n");
        }
        if(fi_accept(this->ep, NULL, 0)) {
            fi_reject(RDMAConnectionManager::g_ctxt.pep, entry.info->handle, NULL, 0);
            fi_freeinfo(entry.info);
            CRASH_WITH_MESSAGE("failed to accept connection.\n");
        }
        fi_freeinfo(entry.info);
    } else {
        // libfabric connection client
        struct fi_info* client_hints = fi_dupinfo(RDMAConnectionManager::g_ctxt.hints);
        struct fi_info* client_info = NULL;

        FAIL_IF_ZERO(client_hints->dest_addr = malloc(remote_connection_data.pep_addr_len), "failed to malloc address space for server pep.", CRASH_ON_FAILURE);
        memcpy((void*)client_hints->dest_addr, (void*)remote_connection_data.pep_addr, (size_t)remote_connection_data.pep_addr_len);
        client_hints->dest_addrlen = remote_connection_data.pep_addr_len;
        FAIL_IF_NONZERO_RETRY_EAGAIN(fi_getinfo(LF_VERSION, NULL, NULL, 0, client_hints, &client_info), "fi_getinfo() failed.", CRASH_ON_FAILURE);
        if(init_endpoint(client_info)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            CRASH_WITH_MESSAGE("failed to initialize client endpoint.\n");
        }

        FAIL_IF_NONZERO_RETRY_EAGAIN(fi_connect(this->ep, remote_connection_data.pep_addr, NULL, 0), "fi_connect()", CRASH_ON_FAILURE);

        nRead = fi_eq_sread(this->eq, &event, &entry, sizeof(entry), -1, 0);
        if(nRead != sizeof(entry)) {
            CRASH_WITH_MESSAGE("failed to connect remote. nRead=%ld.\n", nRead);
        }
        if(event != FI_CONNECTED || entry.fid != &(this->ep->fid)) {
            fi_freeinfo(client_hints);
            fi_freeinfo(client_info);
            CRASH_WITH_MESSAGE("SST: Unexpected CM event: %d.\n", event);
        }

        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
    }
}

// not complete - will need to provide local/remote mr_key etc.
bool RDMAConnection::write_remote(char* local_addr, char* remote_addr, size_t size, bool with_completion, uint64_t remote_write_key, uint64_t local_read_key) {
    if(!is_broken) {
        // post a remote write to the NIC
        int ret = 0;

        struct iovec msg_iov;
        struct fi_rma_iov rma_iov;
        struct fi_msg_rma msg;

        msg_iov.iov_base = local_addr;
        msg_iov.iov_len = size;

        rma_iov.addr = (uint64_t) remote_addr;
        rma_iov.len = size;
        rma_iov.key = remote_write_key;

        msg.msg_iov = &msg_iov;
        msg.desc = (void**)&local_read_key;
        msg.iov_count = 1;
        msg.addr = 0;  // not used for a connection endpoint
        msg.rma_iov = &rma_iov;
        msg.rma_iov_count = 1;
        // msg.context = (void*)ctxt; // questionable change
        msg.context = NULL;
        msg.data = 0l;  // not used

        FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_writemsg(this->ep, &msg, (with_completion) ? FI_COMPLETION : 0),
                                     "fi_writemsg failed.",
                                     REPORT_ON_FAILURE);

        /* if a failure happens*/
        if(ret) {
            // call the failure upcall only once
            if(!is_broken.exchange(true)) {
                if(RDMAConnectionManager::failure_upcall) {
                    RDMAConnectionManager::failure_upcall(remote_id);
                }
            }
        }
    }
    // if the connection is broken, ignore
    return false;
}

bool RDMAConnection::sync() const {
    return false;
}

std::map<node_id_t, std::shared_ptr<RDMAConnection>> RDMAConnectionManager::rdma_connections;
std::mutex RDMAConnectionManager::rdma_connections_mutex;

void RDMAConnectionManager::add(node_id_t my_id, node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    assert(rdma_connections.find(remote_id) == rdma_connections.end());
    rdma_connections[remote_id] = std::shared_ptr<RDMAConnection>(new RDMAConnection(my_id, remote_id));
}

void RDMAConnectionManager::remove(node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    rdma_connections.erase(remote_id);
}

std::shared_ptr<RDMAConnection> RDMAConnectionManager::get(node_id_t remote_id) {
    std::unique_lock<std::mutex> lock(rdma_connections_mutex);
    return rdma_connections.at(remote_id);
}
}  // namespace rdma
