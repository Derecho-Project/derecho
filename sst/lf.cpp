/**
 * @file lf.cpp
 * Implementation of RDMA interface defined in lf.h.
 */
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

#ifdef _DEBUG
#include <spdlog/spdlog.h>
#endif//_DEBUG

#include "derecho/connection_manager.h"
#include "derecho/derecho_ports.h"
#include "poll_utils.h"
#include "tcp/tcp.h"
#include "lf.h"


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



namespace sst{
  /**
   * passive endpoint info to be exchanged.
   */
  struct cm_con_data_t {
    #define MAX_LF_ADDR_SIZE    ((128)-sizeof(uint32_t)-2*sizeof(uint64_t))
    uint32_t           pep_addr_len; // local endpoint address length
    char               pep_addr[MAX_LF_ADDR_SIZE];
                                          // local endpoint address
    uint64_t           mr_key; // local memory key
    uint64_t           vaddr;  // virtual addr
  } __attribute__((packed));

  /**
   * Global States
   */
  struct lf_ctxt {
    // libfabric resources
    struct fi_info     * hints;           // hints
    struct fi_info     * fi;              // fabric information
    struct fid_fabric  * fabric;          // fabric handle
    struct fid_domain  * domain;          // domain handle
    struct fid_pep     * pep;             // passive endpoint for receiving connection
    struct fid_eq      * peq;             // event queue for connection management
    struct fid_eq      * eq;              // event queue for transmitting events
    struct fid_cq      * cq;              // completion queue for all rma operations
    size_t             pep_addr_len;      // length of local pep address
    char               pep_addr[MAX_LF_ADDR_SIZE];
                                          // local pep address
    // configuration resources
    struct fi_eq_attr  eq_attr;           // event queue attributes
    struct fi_cq_attr  cq_attr;           // completion queue attributes
    #define DEFAULT_TX_DEPTH            (4096)
    uint32_t           tx_depth;          // transfer depth
    #define DEFAULT_RX_DEPTH            (4096)
    uint32_t           rx_depth;          // transfer depth
    #define DEFAULT_SGE_BATCH_SIZE      (8)
    uint32_t           sge_bat_size;      // maximum scatter/gather batch size
  };
  // singlton: global states
  struct lf_ctxt g_ctxt;
  #define LF_USE_VADDR ((g_ctxt.fi->domain_attr->mr_mode) & FI_MR_VIRT_ADDR)
  static bool shutdown = false;
  std::thread polling_thread;
  tcp::tcp_connections *sst_connections;
  /**
   * Internal Tools
   */
  // Debug tools
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
  // Test tools
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

  /** initialize the context with default value */
  static void default_context() {
    memset((void*)&g_ctxt,0,sizeof(struct lf_ctxt));
    FAIL_IF_ZERO(g_ctxt.hints = fi_allocinfo(),"Fail to allocate fi hints",CRASH_ON_FAILURE);
    //defaults the hints:
    g_ctxt.hints->caps = FI_MSG|FI_RMA|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE;
    g_ctxt.hints->ep_attr->type = FI_EP_MSG; // use connection based endpoint by default.
    g_ctxt.hints->mode = ~0; // all modes

    g_ctxt.hints->tx_attr->rma_iov_limit = DEFAULT_SGE_BATCH_SIZE; // 
    g_ctxt.hints->tx_attr->iov_limit = DEFAULT_SGE_BATCH_SIZE;
    g_ctxt.hints->rx_attr->iov_limit = DEFAULT_SGE_BATCH_SIZE;

    if (g_ctxt.cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
      g_ctxt.cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    }
    g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC;

    g_ctxt.pep_addr_len = MAX_LF_ADDR_SIZE;
  }

  /** load RDMA device configurations from file */
  static void load_configuration() {
    FAIL_IF_ZERO(g_ctxt.hints,"hints is not initialized.", CRASH_ON_FAILURE);

    // possible providers: verbs|psm|sockets|usnic
    #define DEFAULT_PROVIDER    NULL
    // possible domain is decided by the system
    #define DEFAULT_DOMAIN      "mlx5_0"

    if (false) {
      // TODO:load configuration from file...
    } else { // use default values
      dbg_info("No RDMA conf file, use the default values.");
      // provider:
      if(DEFAULT_PROVIDER) {
        FAIL_IF_ZERO(g_ctxt.hints->fabric_attr->prov_name = strdup(DEFAULT_PROVIDER?DEFAULT_PROVIDER:""),
          "strdup provider name.", CRASH_ON_FAILURE);
      }
      // domain:
      FAIL_IF_ZERO(g_ctxt.hints->domain_attr->name = strdup(DEFAULT_DOMAIN),
        "strdup domain name.", CRASH_ON_FAILURE);
      g_ctxt.hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
      // scatter/gather batch size
      g_ctxt.sge_bat_size = DEFAULT_SGE_BATCH_SIZE;
      // send pipeline depth
      g_ctxt.tx_depth = DEFAULT_TX_DEPTH;
      // recv pipeline depth
      g_ctxt.rx_depth = DEFAULT_RX_DEPTH;
    }
  }

  int resources::init_endpoint(struct fi_info *fi) {
    int ret = 0;
    // struct fi_cq_attr cq_attr = g_ctxt.cq_attr;

    // 1 - open completion queue - use unified cq
    // cq_attr.size = fi->tx_attr->size;
    // FAIL_IF_NONZERO(ret = fi_cq_open(g_ctxt.domain, &cq_attr, &(this->txcq), NULL)"initialize tx completion queue.",REPORT_ON_FAILURE);
    // if(ret) return ret;
    // FAIL_IF_NONZERO(ret = fi_cq_open(g_ctxt.domain, &cq_attr, &(this->rxcq), NULL)"initialize rx completion queue.",REPORT_ON_FAILURE);
    // if(ret) return ret;
    // 2 - open endpoint
    FAIL_IF_NONZERO(ret = fi_endpoint(g_ctxt.domain, fi, &(this->ep), NULL), "open endpoint.", REPORT_ON_FAILURE);
    if(ret) return ret;
    // 3 - bind them and global event queue together
    FAIL_IF_NONZERO(ret = fi_ep_bind(this->ep, &(g_ctxt.eq)->fid, 0), "bind endpoint and event queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO(ret = fi_ep_bind(this->ep, &(g_ctxt.cq)->fid, FI_TRANSMIT | FI_SELECTIVE_COMPLETION), "bind endpoint and tx completion queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO(ret = fi_ep_bind(this->ep, &(g_ctxt.cq)->fid, FI_RECV | FI_SELECTIVE_COMPLETION), "bind endpoint and rx completion queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO(ret = fi_enable(this->ep), "enable endpoint", REPORT_ON_FAILURE);
    return ret;
  }

  void resources::connect_endpoint(bool is_lf_server) {
    dbg_trace("preparing connection to remote node(id=%d)...\n",this->remote_id);
    struct cm_con_data_t local_cm_data,remote_cm_data;

    // STEP 1 exchange CM info
    dbg_trace("Exchanging connection management info.");
    local_cm_data.pep_addr_len = (uint32_t)htonl((uint32_t)g_ctxt.pep_addr_len);
    memcpy((void*)&local_cm_data.pep_addr,&g_ctxt.pep_addr,g_ctxt.pep_addr_len);
    local_cm_data.mr_key = (uint64_t)htonll(this->mr_lkey);
    local_cm_data.vaddr = (uint64_t)htonll((uint64_t)this->write_buf); // for pull mode

    FAIL_IF_ZERO(sst_connections->exchange(this->remote_id,local_cm_data,remote_cm_data),"exchange connection management info.",CRASH_ON_FAILURE);

    remote_cm_data.pep_addr_len = (uint32_t)ntohl(remote_cm_data.pep_addr_len);
    this->mr_rkey = (uint64_t)ntohll(remote_cm_data.mr_key);
    this->remote_fi_addr = (fi_addr_t)ntohll(remote_cm_data.vaddr);
    dbg_trace("Exchanging connection management info succeeds.");

    // STEP 2 connect to remote
    dbg_trace("connect to remote node.");
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    if (is_lf_server) {
      dbg_trace("connecting as a server.");
      dbg_trace("waiting for connection.");

      nRead = fi_eq_sread(g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
      if(nRead != sizeof(entry)) {
        dbg_error("failed to get connection from remote.");
        CRASH_WITH_MESSAGE("failed to get connection from remote. nRead=%ld\n",nRead);
      }
      if(init_endpoint(entry.info)){
        fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
        fi_freeinfo(entry.info);
        CRASH_WITH_MESSAGE("failed to initialize server endpoint.\n");
      }
      if(fi_accept(this->ep, NULL, 0)){
        fi_reject(g_ctxt.pep, entry.info->handle, NULL, 0);
        fi_freeinfo(entry.info);
        CRASH_WITH_MESSAGE("failed to accept connection.\n");
      }
      fi_freeinfo(entry.info);
    } else {
      // libfabric connection client
      dbg_trace("connecting as a client.\n");
      dbg_trace("initiating a connection.\n");

      struct fi_info * client_hints = fi_dupinfo(g_ctxt.hints);
      struct fi_info * client_info = NULL;

      FAIL_IF_ZERO(client_hints->dest_addr = malloc(remote_cm_data.pep_addr_len),"failed to malloc address space for server pep.",CRASH_ON_FAILURE);
      memcpy((void*)client_hints->dest_addr,(void*)remote_cm_data.pep_addr,(size_t)remote_cm_data.pep_addr_len);
      FAIL_IF_NONZERO(fi_getinfo(LF_VERSION,NULL,NULL,0,client_hints,&client_info),"fi_getinfo() failed.",CRASH_ON_FAILURE);
      if(init_endpoint(client_info)){
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
        CRASH_WITH_MESSAGE("failed to initialize client endpoint.\n");
      }
      FAIL_IF_NONZERO(fi_connect(this->ep, remote_cm_data.pep_addr, NULL, 0),"fi_connect()",CRASH_ON_FAILURE);
      nRead = fi_eq_sread(g_ctxt.eq, &event, &entry, sizeof(entry), -1, 0);
      if (nRead != sizeof(entry)) {
        dbg_error("failed to connect remote.");
        CRASH_WITH_MESSAGE("failed to connect remote. nRead=%ld.\n",nRead);
      }
      if (event != FI_CONNECTED || entry.fid != &(this->ep->fid)) {
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
        CRASH_WITH_MESSAGE("Unexpected CM event: %d.\n", event);
      }
      fi_freeinfo(client_hints);
      fi_freeinfo(client_info);
    }
  }

  /**
   * Implementation for Public APIs
   */
  resources::resources(
    int r_id,
    char *write_addr,
    char *read_addr,
    int size_w,
    int size_r,
    int is_lf_server) {

    // set remote id
    this->remote_id = r_id;

    // set write and read buffer
    this->write_buf = write_addr;
    if (!write_addr) {
      dbg_warn("%{}:%{} called with NULL write_addr!",__FILE__,__func__);
    }
    this->read_buf = read_addr;
    if (!write_addr) {
      dbg_warn("%{}:%{} called with NULL read_addr!",__FILE__,__func__);
    }

#define LF_RMR_KEY(rid) (((uint64_t)0xf0000000)<<32 | (uint64_t)(rid))
#define LF_WMR_KEY(rid) (((uint64_t)0xf8000000)<<32 | (uint64_t)(rid))
    // register the memory buffers
    FAIL_IF_NONZERO(
      fi_mr_reg(
        g_ctxt.domain,write_buf,size_w,FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE,
        0, LF_WMR_KEY(r_id), 0, &this->write_mr, NULL),
      "register memory buffer for write",
      CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(
      fi_mr_reg(
        g_ctxt.domain,read_buf,size_w,FI_READ|FI_WRITE|FI_REMOTE_READ,
        0, LF_RMR_KEY(r_id), 0, &this->read_mr, NULL),
      "register memory buffer for read",
      CRASH_ON_FAILURE);
    this->mr_lkey = fi_mr_key(this->write_mr);
    if (this->mr_lkey == FI_KEY_NOTAVAIL) {
      CRASH_WITH_MESSAGE("fail to get memory key.");
    }
    // set up the endpoint
    connect_endpoint(is_lf_server);
  }

  resources::~resources(){
    dbg_trace("resources destructor.");
    // if(this->txcq) 
    //  FAIL_IF_NONZERO(fi_close(&this->txcq->fid),"close txcq",REPORT_ON_FAILURE);
    // if(this->rxcq) 
    //  FAIL_IF_NONZERO(fi_close(&this->rxcq->fid),"close rxcq",REPORT_ON_FAILURE);
    if(this->ep) 
      FAIL_IF_NONZERO(fi_close(&this->ep->fid),"close endpoint",REPORT_ON_FAILURE);
    if(this->write_mr)
      FAIL_IF_NONZERO(fi_close(&this->write_mr->fid),"unregister write mr",REPORT_ON_FAILURE);
    if(this->read_mr)
      FAIL_IF_NONZERO(fi_close(&this->read_mr->fid),"unregister read mr",REPORT_ON_FAILURE);
  }

  int resources::post_remote_send(
    struct lf_sender_ctxt *ctxt,
    const long long int offset,
    const long long int size,
    const int op,
    const bool completion) {
    dbg_trace("resources::post_remote_send(ctxt={{},{}},offset={},size={},op={},completion={}).",ctxt?0:ctxt->ce_idx,ctxt?0:ctxt->remote_id,offset,size,op,completion);

    int ret = 0;
    struct iovec msg_iov;
    struct fi_rma_iov rma_iov;
    struct fi_msg_rma msg;

    msg_iov.iov_base = read_buf + offset;
    msg_iov.iov_len = size;

    rma_iov.addr = ((LF_USE_VADDR)?remote_fi_addr:0) + offset;
    rma_iov.len = size;
    rma_iov.key = this->mr_rkey;

    msg.msg_iov = &msg_iov;
    msg.desc = (void**)&this->mr_lkey;
    msg.iov_count = 1;
    msg.addr = 0; // not used for a connection endpoint
    msg.rma_iov = &rma_iov;
    msg.context = (void*)ctxt;
    msg.data = 0l; // not used

    if(op) { //write
      FAIL_IF_NONZERO(ret = fi_writemsg(this->ep,&msg,(completion)?FI_COMPLETION:0),
        "fi_writemsg failed.",
        REPORT_ON_FAILURE);
    } else { // read
      FAIL_IF_NONZERO(ret = fi_readmsg(this->ep,&msg,(completion)?FI_COMPLETION:0),
        "fi_readmsg failed.",
        REPORT_ON_FAILURE);
    }
    dbg_trace("post_remote_send return with ret=%d",ret);
    return ret;
  }

  void resources::post_remote_read(const long long int size){
    FAIL_IF_NONZERO(post_remote_send(NULL,0,size,0,false),"post_remote_read(1) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_read(const long long int offset, const long long int size){
    FAIL_IF_NONZERO(post_remote_send(NULL,offset,size,0,false),"post_remote_read(2) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write(const long long int size){
    FAIL_IF_NONZERO(post_remote_send(NULL,0,size,1,false),"post_remote_write(1) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write(const long long int offset, long long int size){
    FAIL_IF_NONZERO(post_remote_send(NULL,offset,size,1,false),"post_remote_write(2) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int size){
    FAIL_IF_NONZERO(post_remote_send(ctxt,0,size,1,true),"post_remote_write(3) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size){
    FAIL_IF_NONZERO(post_remote_send(ctxt,offset,size,1,true),"post_remote_write(4) failed.",REPORT_ON_FAILURE);
  }

  bool add_node(uint32_t new_id, const std::string new_ip_addr) {
    return sst_connections->add_node(new_id, new_ip_addr);
  }

  bool sync(uint32_t r_id) {
    int s = 0, t = 0;
    return sst_connections->exchange(r_id, s, t);
  }

  void polling_loop() {
    pthread_setname_np(pthread_self(), "sst_poll");
    std::cout << "Polling thread starting" << std::endl;
    while(shutdown) {
        auto ce = lf_poll_completion();
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
   * @return pair(,result) The queue pair number associated with the
   * completed request and the result (1 for successful, -1 for unsuccessful)
   */
  std::pair<uint32_t, std::pair<int32_t, int32_t>> lf_poll_completion() {
    struct fi_cq_entry entry;
    int poll_result;

    while(shutdown) {
        poll_result = 0;
        for(int i = 0; i < 50; ++i) {
            poll_result = fi_cq_read(g_ctxt.cq, &entry, 1);
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
      struct fi_cq_err_entry eentry;
      fi_cq_readerr(g_ctxt.cq, &eentry, 0);
      
      dbg_error("fi_cq_readerr() read the following error entry:");
      if (eentry.op_context == NULL) {
        dbg_error("\top_context:NULL");
      } else {
        struct lf_sender_ctxt *sctxt = (struct lf_sender_ctxt *)eentry.op_context;
        dbg_error("\top_context:ce_idx={},remote_id={}",sctxt->ce_idx,sctxt->remote_id);
      }
      dbg_error("\tflags={}",eentry.flags);
      dbg_error("\tlen={}",eentry.len);
      dbg_error("\tbuf={}",eentry.buf);
      dbg_error("\tdata={}",eentry.data);
      dbg_error("\ttag={}",eentry.tag);
      dbg_error("\tolen={}",eentry.olen);
      dbg_error("\terr={}",eentry.err);
      dbg_error("\tprov_errno={}",eentry.prov_errno);
      dbg_error("\terr_data={}",eentry.err_data);
      dbg_error("\terr_data_size={}",eentry.err_data_size);

      if (eentry.op_context!=NULL){
        struct lf_sender_ctxt * sctxt = (struct lf_sender_ctxt *)eentry.op_context;
        return {sctxt->ce_idx, {sctxt->remote_id, -1}};
      } else {
        CRASH_WITH_MESSAGE("faile to poll the completion queue");
      }
    }
    struct lf_sender_ctxt * sctxt = (struct lf_sender_ctxt *)entry.op_context;
    return {sctxt->ce_idx, {sctxt->remote_id, 1}};
  }


  void lf_initialize(
    const std::map<uint32_t, std::string> &ip_addrs,
    uint32_t node_rank){
    // initialize derecho connection manager: This is derived from Sagar's code.
    // May there be a better desgin?
    sst_connections = new tcp::tcp_connections(node_rank, ip_addrs, derecho::sst_tcp_port);

    // initialize global resources:
    // STEP 1: initialize with configuration.
    default_context(); // default the context
    load_configuration(); // load configuration

    dbg_info(fi_tostr(g_ctxt.hints,FI_TYPE_INFO));
    // STEP 2: initialize fabric, domain, and completion queue
    FAIL_IF_NONZERO(fi_getinfo(LF_VERSION,NULL,NULL,0,g_ctxt.hints,&(g_ctxt.fi)),"fi_getinfo()",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_fabric(g_ctxt.fi->fabric_attr, &(g_ctxt.fabric), NULL),"fi_fabric()",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_domain(g_ctxt.fabric, g_ctxt.fi, &(g_ctxt.domain), NULL),"fi_domain()",CRASH_ON_FAILURE);
    g_ctxt.cq_attr.size = g_ctxt.fi->tx_attr->size;
    FAIL_IF_NONZERO(fi_cq_open(g_ctxt.domain, &(g_ctxt.cq_attr), &(g_ctxt.cq), NULL),"initialize tx completion queue.",REPORT_ON_FAILURE);

    // STEP 3: prepare local PEP
    FAIL_IF_NONZERO(fi_eq_open(g_ctxt.fabric,&g_ctxt.eq_attr,&g_ctxt.peq,NULL),"open the event queue for passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_passive_ep(g_ctxt.fabric,g_ctxt.hints,&g_ctxt.pep,NULL),"open a local passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_pep_bind(g_ctxt.pep,&g_ctxt.peq->fid,0),"binding event queue to passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_listen(g_ctxt.pep),"preparing passive endpoint for incoming connections",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO(fi_getname(&g_ctxt.pep->fid, g_ctxt.pep_addr, &g_ctxt.pep_addr_len),"get the local PEP address",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO((g_ctxt.pep_addr_len > MAX_LF_ADDR_SIZE),"local name is too big to fit in local buffer",CRASH_ON_FAILURE);

    FAIL_IF_NONZERO(fi_eq_open(g_ctxt.fabric,&g_ctxt.eq_attr,&g_ctxt.eq,NULL),"open the event queue for rdma transmission.", CRASH_ON_FAILURE);
    
    // STEP 4: start polling thread.
    polling_thread = std::thread(polling_loop);
    polling_thread.detach();
  }

  void shutdown_polling_thread(){
    shutdown = true;
  }

  void lf_destroy(){
    shutdown_polling_thread();
    // TODO: make sure all resources are destroyed first.
    if (g_ctxt.pep) {
      FAIL_IF_NONZERO(fi_close(&g_ctxt.pep->fid),"close passive endpoint",REPORT_ON_FAILURE);
    }
    if (g_ctxt.peq) {
      FAIL_IF_NONZERO(fi_close(&g_ctxt.peq->fid),"close event queue for passive endpoint",REPORT_ON_FAILURE);
    }
    if (g_ctxt.eq) {
      FAIL_IF_NONZERO(fi_close(&g_ctxt.eq->fid),"close event queue",REPORT_ON_FAILURE);
    }
    if (g_ctxt.domain) {
      FAIL_IF_NONZERO(fi_close(&g_ctxt.domain->fid),"close domain",REPORT_ON_FAILURE);
    }
    if (g_ctxt.fabric) {
      FAIL_IF_NONZERO(fi_close(&g_ctxt.fabric->fid),"close fabric",REPORT_ON_FAILURE);
    }
    if (g_ctxt.fi) {
      fi_freeinfo(g_ctxt.fi);
      g_ctxt.hints = nullptr;
    }
    if (g_ctxt.hints) {
      fi_freeinfo(g_ctxt.hints);
      g_ctxt.hints = nullptr;
    }
  }
}
