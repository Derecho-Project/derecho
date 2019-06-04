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

#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>
#include <derecho/core/detail/connection_manager.hpp>
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/tcp/tcp.hpp>
#include <derecho/sst/detail/lf.hpp>

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
 #define RED   "\x1B[31m"
 #define GRN   "\x1B[32m"
 #define YEL   "\x1B[33m"
 #define BLU   "\x1B[34m"
 #define MAG   "\x1B[35m"
 #define CYN   "\x1B[36m"
 #define WHT   "\x1B[37m"
 #define RESET "\x1B[0m"
#endif//NDEBUG

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
  class lf_ctxt {
  public:
    // libfabric resources
    struct fi_info     * hints;           // hints
    struct fi_info     * fi;              // fabric information
    struct fid_fabric  * fabric;          // fabric handle
    struct fid_domain  * domain;          // domain handle
    struct fid_pep     * pep;             // passive endpoint for receiving connection
    struct fid_eq      * peq;             // event queue for connection management
    // struct fid_eq      * eq;           // event queue for transmitting events --> now move to resources.
    struct fid_cq      * cq;              // completion queue for all rma operations
    size_t             pep_addr_len;      // length of local pep address
    char               pep_addr[MAX_LF_ADDR_SIZE];
                                          // local pep address
    // configuration resources
    struct fi_eq_attr  eq_attr;           // event queue attributes
    struct fi_cq_attr  cq_attr;           // completion queue attributes
    // #define DEFAULT_TX_DEPTH            (4096)
    // uint32_t           tx_depth;          // transfer depth
    // #define DEFAULT_RX_DEPTH            (4096)
    // uint32_t           rx_depth;          // transfer depth
    // #define DEFAULT_SGE_BATCH_SIZE      (8)
    // uint32_t           sge_bat_size;      // maximum scatter/gather batch size
    virtual ~lf_ctxt() {
      lf_destroy();
    }
  };
  #define LF_CONFIG_FILE "rdma.cfg"
  #define LF_USE_VADDR ((g_ctxt.fi->domain_attr->mr_mode) & (FI_MR_VIRT_ADDR|FI_MR_BASIC))
  static bool shutdown = false;
  std::thread polling_thread;
  tcp::tcp_connections *sst_connections;
  // singlton: global states
  lf_ctxt g_ctxt;

  /**
   * Internal Tools
   */
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
  #define FAIL_IF_NONZERO_RETRY_EAGAIN(x,desc,next) \
    do { \
      int64_t _int64_r_; \
      do { \
        _int64_r_ = (int64_t)(x); \
      } while ( _int64_r_ == -FI_EAGAIN ); \
      if (_int64_r_ != 0) { \
        dbg_default_error("{}:{},ret={},{}",__FILE__,__LINE__,_int64_r_,desc); \
        fprintf(stderr,"%s:%d,ret=%ld,%s\n",__FILE__,__LINE__,_int64_r_,desc); \
        if (next == CRASH_ON_FAILURE) { \
          fflush(stderr); \
          dbg_default_flush(); \
          exit(-1); \
        } \
      } \
    } while (0)
  #define FAIL_IF_ZERO(x,desc,next) \
    do { \
      int64_t _int64_r_ = (int64_t)(x); \
      if (_int64_r_ == 0) { \
        dbg_default_error("{}:{},{}",__FILE__,__LINE__,desc); \
        fprintf(stderr,"%s:%d,%s\n",__FILE__,__LINE__,desc); \
        if (next == CRASH_ON_FAILURE) { \
          fflush(stderr); \
          dbg_default_flush(); \
          exit(-1); \
        } \
      } \
    } while (0)

  /** initialize the context with default value */
  static void default_context() {
    memset((void*)&g_ctxt,0,sizeof(lf_ctxt));
    FAIL_IF_ZERO(g_ctxt.hints = fi_allocinfo(),"Fail to allocate fi hints",CRASH_ON_FAILURE);
    //defaults the hints:
    g_ctxt.hints->caps = FI_MSG|FI_RMA|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE;
    g_ctxt.hints->ep_attr->type = FI_EP_MSG; // use connection based endpoint by default.
    g_ctxt.hints->mode = ~0; // all modes

    // g_ctxt.hints->tx_attr->rma_iov_limit = DEFAULT_SGE_BATCH_SIZE; // 
    // g_ctxt.hints->tx_attr->iov_limit = DEFAULT_SGE_BATCH_SIZE;
    // g_ctxt.hints->rx_attr->iov_limit = DEFAULT_SGE_BATCH_SIZE;

    if (g_ctxt.cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
      g_ctxt.cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    }
    g_ctxt.cq_attr.wait_obj = FI_WAIT_UNSPEC;

    g_ctxt.pep_addr_len = MAX_LF_ADDR_SIZE;
  }

  /** load RDMA device configurations from file */
  static void load_configuration() {
    FAIL_IF_ZERO(g_ctxt.hints,"hints is not initialized.", CRASH_ON_FAILURE);

    // dbg_default_info("No RDMA conf file, use the default values.");
    // provider:
    FAIL_IF_ZERO(g_ctxt.hints->fabric_attr->prov_name = strdup(derecho::getConfString(CONF_RDMA_PROVIDER).c_str()),
          "strdup provider name.", CRASH_ON_FAILURE);
    // domain:
    FAIL_IF_ZERO(g_ctxt.hints->domain_attr->name = strdup(derecho::getConfString(CONF_RDMA_DOMAIN).c_str()),
        "strdup domain name.", CRASH_ON_FAILURE);
    if (strcmp(g_ctxt.hints->fabric_attr->prov_name,"sockets")==0) {
      g_ctxt.hints->domain_attr->mr_mode = FI_MR_BASIC;
    } else { // default
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

  int _resources::init_endpoint(struct fi_info *fi) {
    int ret = 0;
    // struct fi_cq_attr cq_attr = g_ctxt.cq_attr;

    // 1 - open completion queue - use unified cq
    // cq_attr.size = fi->tx_attr->size;
    // FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_cq_open(g_ctxt.domain, &cq_attr, &(this->txcq), NULL)"initialize tx completion queue.",REPORT_ON_FAILURE);
    // if(ret) return ret;
    // FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_cq_open(g_ctxt.domain, &cq_attr, &(this->rxcq), NULL)"initialize rx completion queue.",REPORT_ON_FAILURE);
    // if(ret) return ret;
    // 2 - open endpoint
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_endpoint(g_ctxt.domain, fi, &(this->ep), NULL), "open endpoint.", REPORT_ON_FAILURE);
    if(ret) return ret;
    dbg_default_debug("{}:{} init_endpoint:ep->fid={}",__FILE__,__func__,(void*)&this->ep->fid);

    // 2.5 - open an event queue.
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_eq_open(g_ctxt.fabric,&g_ctxt.eq_attr,&this->eq,NULL),"open the event queue for rdma transmission.", CRASH_ON_FAILURE);
    dbg_default_debug("{}:{} event_queue opened={}",__FILE__,__func__,(void*)&this->eq->fid);

    // 3 - bind them and global event queue together
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_ep_bind(this->ep, &(this->eq)->fid, 0), "bind endpoint and event queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_ep_bind(this->ep, &(g_ctxt.cq)->fid, FI_RECV | FI_TRANSMIT | FI_SELECTIVE_COMPLETION), "bind endpoint and tx completion queue", REPORT_ON_FAILURE);
    if(ret) return ret;
    FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_enable(this->ep), "enable endpoint", REPORT_ON_FAILURE);
    return ret;
  }

  void _resources::connect_endpoint(bool is_lf_server) {
    dbg_default_trace("preparing connection to remote node(id=%d)...\n",this->remote_id);
    struct cm_con_data_t local_cm_data,remote_cm_data;

    // STEP 1 exchange CM info
    dbg_default_trace("Exchanging connection management info.");
    local_cm_data.pep_addr_len = (uint32_t)htonl((uint32_t)g_ctxt.pep_addr_len);
    memcpy((void*)&local_cm_data.pep_addr,&g_ctxt.pep_addr,g_ctxt.pep_addr_len);
    local_cm_data.mr_key = (uint64_t)htonll(this->mr_lwkey);
    local_cm_data.vaddr = (uint64_t)htonll((uint64_t)this->write_buf); // for pull mode

    FAIL_IF_ZERO(sst_connections->exchange(this->remote_id,local_cm_data,remote_cm_data),"exchange connection management info.",CRASH_ON_FAILURE);

    remote_cm_data.pep_addr_len = (uint32_t)ntohl(remote_cm_data.pep_addr_len);
    this->mr_rwkey = (uint64_t)ntohll(remote_cm_data.mr_key);
    this->remote_fi_addr = (fi_addr_t)ntohll(remote_cm_data.vaddr);
    dbg_default_trace("Exchanging connection management info succeeds.");

    // STEP 2 connect to remote
    dbg_default_trace("connect to remote node.");
    ssize_t nRead;
    struct fi_eq_cm_entry entry;
    uint32_t event;

    if (is_lf_server) {
      dbg_default_trace("connecting as a server.");
      dbg_default_trace("waiting for connection.");

      nRead = fi_eq_sread(g_ctxt.peq, &event, &entry, sizeof(entry), -1, 0);
      if(nRead != sizeof(entry)) {
        dbg_default_error("failed to get connection from remote.");
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
      dbg_default_trace("connecting as a client.\n");
      dbg_default_trace("initiating a connection.\n");

      struct fi_info * client_hints = fi_dupinfo(g_ctxt.hints);
      struct fi_info * client_info = NULL;

      FAIL_IF_ZERO(client_hints->dest_addr = malloc(remote_cm_data.pep_addr_len),"failed to malloc address space for server pep.",CRASH_ON_FAILURE);
      memcpy((void*)client_hints->dest_addr,(void*)remote_cm_data.pep_addr,(size_t)remote_cm_data.pep_addr_len);
      client_hints->dest_addrlen = remote_cm_data.pep_addr_len;
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_getinfo(LF_VERSION,NULL,NULL,0,client_hints,&client_info),"fi_getinfo() failed.",CRASH_ON_FAILURE);
      if(init_endpoint(client_info)){
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
        CRASH_WITH_MESSAGE("failed to initialize client endpoint.\n");
      }

      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_connect(this->ep, remote_cm_data.pep_addr, NULL, 0),"fi_connect()",CRASH_ON_FAILURE);

      nRead = fi_eq_sread(this->eq, &event, &entry, sizeof(entry), -1, 0);
      if (nRead != sizeof(entry)) {
        dbg_default_error("failed to connect remote.");
        CRASH_WITH_MESSAGE("failed to connect remote. nRead=%ld.\n",nRead);
      }
      dbg_default_debug("{}:{} entry.fid={},this->ep->fid={}",__FILE__,__func__,(void*)entry.fid,(void*)&(this->ep->fid));
      if (event != FI_CONNECTED || entry.fid != &(this->ep->fid)) {
        fi_freeinfo(client_hints);
        fi_freeinfo(client_info);
        dbg_default_flush();
        CRASH_WITH_MESSAGE("SST: Unexpected CM event: %d.\n", event);
      }

      fi_freeinfo(client_hints);
      fi_freeinfo(client_info);
    }
  }

  /**
   * Implementation for Public APIs
   */
  _resources::_resources(
    int r_id,
    char *write_addr,
    char *read_addr,
    int size_w,
    int size_r,
    int is_lf_server) {

    dbg_default_trace("resources constructor: this={}",(void*)this);

    // set remote id
    this->remote_id = r_id;

    // set write and read buffer
    this->write_buf = write_addr;
    if (!write_addr) {
      dbg_default_warn("{}:{} called with NULL write_addr!",__FILE__,__func__);
    }
    this->read_buf = read_addr;
    if (!read_addr) {
      dbg_default_warn("{}:{} called with NULL read_addr!",__FILE__,__func__);
    }

#define LF_RMR_KEY(rid) (((uint64_t)0xf0000000)<<32 | (uint64_t)(rid))
#define LF_WMR_KEY(rid) (((uint64_t)0xf8000000)<<32 | (uint64_t)(rid))
    // register the write buffer
    FAIL_IF_NONZERO_RETRY_EAGAIN(
      fi_mr_reg(
        g_ctxt.domain,write_buf,size_w,FI_SEND|FI_RECV|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE,
        0, 0, 0, &this->write_mr, NULL),
        // 0, LF_WMR_KEY(r_id), 0, &this->write_mr, NULL),
      "register memory buffer for write",
      CRASH_ON_FAILURE);
    dbg_default_trace("{}:{} registered memory for remote write: {}:{}",__FILE__,__func__,(void*)write_addr,size_w);
    // register the read buffer
    FAIL_IF_NONZERO_RETRY_EAGAIN(
      fi_mr_reg(
        g_ctxt.domain,read_buf,size_r,FI_SEND|FI_RECV|FI_READ|FI_WRITE|FI_REMOTE_READ|FI_REMOTE_WRITE,
        0, 0, 0, &this->read_mr, NULL),
        //0, LF_RMR_KEY(r_id), 0, &this->read_mr, NULL),
      "register memory buffer for read",
      CRASH_ON_FAILURE);
    dbg_default_trace("{}:{} registered memory for remote read: {}:{}",__FILE__,__func__,(void*)read_addr,size_r);

    this->mr_lrkey = fi_mr_key(this->read_mr);
    if (this->mr_lrkey == FI_KEY_NOTAVAIL) {
      CRASH_WITH_MESSAGE("fail to get read memory key.");
    }
    this->mr_lwkey = fi_mr_key(this->write_mr);
    dbg_default_trace("{}:{} local write key:{}, local read key:{}",__FILE__,__func__,(uint64_t)this->mr_lwkey,(uint64_t)this->mr_lrkey);
    if (this->mr_lwkey == FI_KEY_NOTAVAIL) {
      CRASH_WITH_MESSAGE("fail to get write memory key.");
    }
    // set up the endpoint
    connect_endpoint(is_lf_server);
  }

  _resources::~_resources(){
    dbg_default_trace("resources destructor:this={}",(void*)this);
    // if(this->txcq) 
    //  FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->txcq->fid),"close txcq",REPORT_ON_FAILURE);
    // if(this->rxcq) 
    //  FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->rxcq->fid),"close rxcq",REPORT_ON_FAILURE);
    if(this->ep) 
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->ep->fid),"close endpoint",REPORT_ON_FAILURE);
    if(this->eq)
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->eq->fid),"close event",REPORT_ON_FAILURE);
    if(this->write_mr)
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->write_mr->fid),"unregister write mr",REPORT_ON_FAILURE);
    if(this->read_mr)
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&this->read_mr->fid),"unregister read mr",REPORT_ON_FAILURE);
  }

  int _resources::post_remote_send(
    struct lf_sender_ctxt *ctxt,
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

    int ret = 0;

    if (op == 2) { // two sided send
      struct fi_msg msg;
      struct iovec msg_iov;

      msg_iov.iov_base = read_buf + offset;
      msg_iov.iov_len = size;

      msg.msg_iov = &msg_iov;
      msg.desc = (void**)&this->mr_lrkey;
      msg.iov_count = 1;
      msg.addr = 0;
      msg.context = (void*)ctxt;
      msg.data = 0l; // not used

      FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_sendmsg(this->ep,&msg,(completion)?(FI_COMPLETION|FI_REMOTE_CQ_DATA):(FI_REMOTE_CQ_DATA)),
        "fi_sendmsg failed.",
        REPORT_ON_FAILURE);
    } else { // one sided send or receive
      struct iovec msg_iov;
      struct fi_rma_iov rma_iov;
      struct fi_msg_rma msg;
  
      msg_iov.iov_base = read_buf + offset;
      msg_iov.iov_len = size;
  
      rma_iov.addr = ((LF_USE_VADDR)?remote_fi_addr:0) + offset;
      rma_iov.len = size;
      rma_iov.key = this->mr_rwkey;
  
      msg.msg_iov = &msg_iov;
      msg.desc = (void**)&this->mr_lrkey;
      msg.iov_count = 1;
      msg.addr = 0; // not used for a connection endpoint
      msg.rma_iov = &rma_iov;
      msg.rma_iov_count = 1;
      msg.context = (void*)ctxt;
      msg.data = 0l; // not used
  
      // dbg_default_trace("{}:{} calling fi_writemsg/fi_readmsg with",__FILE__,__func__);
      // dbg_default_trace("remote addr = {} len = {} key = {}",(void*)rma_iov.addr,rma_iov.len,(uint64_t)this->mr_rwkey);
      // dbg_default_trace("local addr = {} len = {} key = {}",(void*)msg_iov.iov_base,msg_iov.iov_len,(uint64_t)this->mr_lrkey);
      // dbg_default_flush();
  
      if(op == 1) { //write
        FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_writemsg(this->ep,&msg,(completion)?FI_COMPLETION:0),
          "fi_writemsg failed.",
          REPORT_ON_FAILURE);
      } else { // read op==0
        FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_readmsg(this->ep,&msg,(completion)?FI_COMPLETION:0),
          "fi_readmsg failed.",
          REPORT_ON_FAILURE);
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

  void resources::post_remote_read(const long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(NULL,0,size,0,false),"post_remote_read(1) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_read(const long long int offset, const long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(NULL,offset,size,0,false),"post_remote_read(2) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write(const long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(NULL,0,size,1,false),"post_remote_write(1) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write(const long long int offset, long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(NULL,offset,size,1,false),"post_remote_write(2) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(ctxt,0,size,1,true),"post_remote_write(3) failed.",REPORT_ON_FAILURE);
  }

  void resources::post_remote_write_with_completion(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size){
    FAIL_IF_NONZERO_RETRY_EAGAIN(post_remote_send(ctxt,offset,size,1,true),"post_remote_write(4) failed.",REPORT_ON_FAILURE);
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
  
  void resources_two_sided::post_two_sided_send_with_completion(struct lf_sender_ctxt *ctxt, const long long int size) {
      int rc = post_remote_send(ctxt, 0, size, 2, true);
      if(rc) {
          cout << "Could not post RDMA two sided send (with no offset) with completion, error code is " << rc << endl;
      }
  }
  
  void resources_two_sided::post_two_sided_send_with_completion(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size) {
      int rc = post_remote_send(ctxt, offset, size, 2, true);
      if(rc) {
          cout << "Could not post RDMA two sided send with offset and completion, error code is " << rc << ", remote_id is " << ctxt->remote_id << endl;
      }
  }

  void resources_two_sided::post_two_sided_receive(struct lf_sender_ctxt *ctxt, const long long int size) {
      int rc = post_receive(ctxt, 0, size);
      if(rc) {
          cout << "Could not post RDMA two sided receive (with no offset), error code is " << rc << ", remote_id is " << ctxt->remote_id << endl;
      }
  }
  
  void resources_two_sided::post_two_sided_receive(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size) {
      int rc = post_receive(ctxt, offset, size);
      if(rc) {
          cout << "Could not post RDMA two sided receive with offset, error code is " << rc << ", remote_id is " << ctxt->remote_id << endl;
      }
  }
  
  int resources_two_sided::post_receive(struct lf_sender_ctxt *ctxt, const long long int offset, const long long int size) {
      struct iovec msg_iov;
      struct fi_msg msg;
      int ret;
  
      msg_iov.iov_base = write_buf + offset;
      msg_iov.iov_len = size;
  
      msg.msg_iov    = &msg_iov;
      msg.desc       = (void**)&this->mr_lwkey;
      msg.iov_count  = 1;
      msg.addr       = 0; // not used
      msg.context    = (void*)ctxt;
      FAIL_IF_NONZERO_RETRY_EAGAIN(ret = fi_recvmsg(this->ep, &msg, FI_COMPLETION|FI_REMOTE_CQ_DATA),
          "fi_recvmsg",
          REPORT_ON_FAILURE);
      return ret;
  }

  bool add_node(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port) {
    return sst_connections->add_node(new_id, new_ip_addr_and_port);
  }

  bool remove_node(uint32_t node_id) {
      return sst_connections->delete_node(node_id);
  }

  bool sync(uint32_t r_id) {
    int s = 0, t = 0;
    return sst_connections->exchange(r_id, s, t);
  }

  void polling_loop() {
    pthread_setname_np(pthread_self(), "sst_poll");
    dbg_default_trace("Polling thread starting.");
    while(!shutdown) {
        auto ce = lf_poll_completion();
        if (shutdown) {
          break;
        }
        if (ce.first != 0xFFFFFFFF) {
          util::polling_data.insert_completion_entry(ce.first, ce.second);
        } // else we don't know who sent the message, so just drop it.
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

    while(!shutdown) {
        poll_result = 0;
        for(int i = 0; i < 50; ++i) {
            poll_result = fi_cq_read(g_ctxt.cq, &entry, 1);
            if(poll_result && (poll_result!=-FI_EAGAIN)) {
                break;
            }
        }
        if(poll_result && (poll_result!=-FI_EAGAIN)) {
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
      if (eentry.op_context == NULL) {
        dbg_default_error("\top_context:NULL");
      } else {
#ifndef NDEBUG
        struct lf_sender_ctxt *sctxt = (struct lf_sender_ctxt *)eentry.op_context;
#endif
        dbg_default_error("\top_context:ce_idx={},remote_id={}",sctxt->ce_idx,sctxt->remote_id);
      }
#ifdef DEBUG_FOR_RELEASE
      printf("\tflags=%x\n",eentry.flags);
      printf("\tlen=%x\n",eentry.len);
      printf("\tbuf=%p\n",eentry.buf);
      printf("\tdata=0x%x\n",eentry.data);
      printf("\ttag=0x%x\n",eentry.tag);
      printf("\tolen=0x%x\n",eentry.olen);
      printf("\terr=0x%x\n",eentry.err);
#endif//DEBUG_FOR_RELEASE
      dbg_default_error("\tflags={}",eentry.flags);
      dbg_default_error("\tlen={}",eentry.len);
      dbg_default_error("\tbuf={}",eentry.buf);
      dbg_default_error("\tdata={}",eentry.data);
      dbg_default_error("\ttag={}",eentry.tag);
      dbg_default_error("\tolen={}",eentry.olen);
      dbg_default_error("\terr={}",eentry.err);
#ifndef NDEBUG
      char errbuf[1024];
#endif
      dbg_default_error("\tprov_errno={}:{}",eentry.prov_errno,
        fi_cq_strerror(g_ctxt.cq,eentry.prov_errno,eentry.err_data,errbuf,1024));
#ifdef DEBUG_FOR_RELEASE
      printf("\tproverr=0x%x,%s\n",eentry.prov_errno,
        fi_cq_strerror(g_ctxt.cq,eentry.prov_errno,eentry.err_data,errbuf,1024));
#endif//DEBUG_FOR_RELEASE
      dbg_default_error("\terr_data={}",eentry.err_data);
      dbg_default_error("\terr_data_size={}",eentry.err_data_size);
#ifdef DEBUG_FOR_RELEASE
      printf("\terr_data_size=%d\n",eentry.err_data_size);
#endif//DEBUG_FOR_RELEASE
      if (eentry.op_context!=NULL){
        struct lf_sender_ctxt * sctxt = (struct lf_sender_ctxt *)eentry.op_context;
        return {sctxt->ce_idx, {sctxt->remote_id, -1}};
      } else {
          dbg_default_error("\tFailed polling the completion queue");
          fprintf(stderr,"Failed polling the completion queue");
          return {(uint32_t)0xFFFFFFFF,{0,-1}}; // we don't know who sent the message.
          // CRASH_WITH_MESSAGE("failed polling the completion queue");
      }
    }
    if (!shutdown) {
      struct lf_sender_ctxt * sctxt = (struct lf_sender_ctxt *)entry.op_context;
      if (sctxt == NULL) {
        dbg_default_debug("WEIRD: we get an entry with op_context = NULL.");
        return {0xFFFFFFFFu,{0,0}}; // return a bad entry: weird!!!!
      } else {
//        dbg_default_trace("Normal: we get an entry with op_context = {}.",(long long unsigned)sctxt);
        return {sctxt->ce_idx, {sctxt->remote_id, 1}};
      }
    } else { // shutdown return a bad entry
      return {0,{0,0}};
    }
  }

  void lf_initialize(const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>
                         &ip_addrs_and_ports,
                     uint32_t node_rank) {
    // initialize derecho connection manager: This is derived from Sagar's code.
    // May there be a better desgin?
    sst_connections = new tcp::tcp_connections(node_rank, ip_addrs_and_ports);

    // initialize global resources:
    // STEP 1: initialize with configuration.
    default_context(); // default the context
    load_configuration(); // load configuration

    //dbg_default_info(fi_tostr(g_ctxt.hints,FI_TYPE_INFO));
    // STEP 2: initialize fabric, domain, and completion queue
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_getinfo(LF_VERSION,NULL,NULL,0,g_ctxt.hints,&(g_ctxt.fi)),"fi_getinfo()",CRASH_ON_FAILURE);
    dbg_default_trace("going to use virtual address?{}",LF_USE_VADDR);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_fabric(g_ctxt.fi->fabric_attr, &(g_ctxt.fabric), NULL),"fi_fabric()",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_domain(g_ctxt.fabric, g_ctxt.fi, &(g_ctxt.domain), NULL),"fi_domain()",CRASH_ON_FAILURE);
    g_ctxt.cq_attr.size = g_ctxt.fi->tx_attr->size;
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_cq_open(g_ctxt.domain, &(g_ctxt.cq_attr), &(g_ctxt.cq), NULL),"initialize tx completion queue.",REPORT_ON_FAILURE);

    // STEP 3: prepare local PEP
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_eq_open(g_ctxt.fabric,&g_ctxt.eq_attr,&g_ctxt.peq,NULL),"open the event queue for passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_passive_ep(g_ctxt.fabric,g_ctxt.fi,&g_ctxt.pep,NULL),"open a local passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_pep_bind(g_ctxt.pep,&g_ctxt.peq->fid,0),"binding event queue to passive endpoint",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_listen(g_ctxt.pep),"preparing passive endpoint for incoming connections",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN(fi_getname(&g_ctxt.pep->fid, g_ctxt.pep_addr, &g_ctxt.pep_addr_len),"get the local PEP address",CRASH_ON_FAILURE);
    FAIL_IF_NONZERO_RETRY_EAGAIN((g_ctxt.pep_addr_len > MAX_LF_ADDR_SIZE),"local name is too big to fit in local buffer",CRASH_ON_FAILURE);
    // FAIL_IF_NONZERO_RETRY_EAGAIN(fi_eq_open(g_ctxt.fabric,&g_ctxt.eq_attr,&g_ctxt.eq,NULL),"open the event queue for rdma transmission.", CRASH_ON_FAILURE);
    
    // STEP 4: start polling thread.
    polling_thread = std::thread(polling_loop);
    // polling_thread.detach();
  }

  void shutdown_polling_thread(){
    shutdown = true;
    if(polling_thread.joinable()) {
      polling_thread.join();
    }
  }

  void lf_destroy(){
    shutdown_polling_thread();
    // TODO: make sure all resources are destroyed first.
    if (g_ctxt.pep) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.pep->fid),"close passive endpoint",REPORT_ON_FAILURE);
    }
    if (g_ctxt.peq) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.peq->fid),"close event queue for passive endpoint",REPORT_ON_FAILURE);
    }
    if (g_ctxt.cq) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.cq->fid),"close completion queue",REPORT_ON_FAILURE);
    }
    // g_ctxt.eq has been moved to resources
    // if (g_ctxt.eq) {
    //  FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.eq->fid),"close event queue",REPORT_ON_FAILURE);
    // }
    if (g_ctxt.domain) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.domain->fid),"close domain",REPORT_ON_FAILURE);
    }
    if (g_ctxt.fabric) {
      FAIL_IF_NONZERO_RETRY_EAGAIN(fi_close(&g_ctxt.fabric->fid),"close fabric",REPORT_ON_FAILURE);
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
