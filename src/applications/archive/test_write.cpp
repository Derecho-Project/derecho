#include <iostream>
#include <map>
#include <thread>

#include <derecho/sst/detail/poll_utils.hpp>
#ifdef USE_VERBS_API
    #include <derecho/sst/detail/verbs.hpp>
#else
    #include <derecho/sst/detail/lf.hpp>
#endif

#ifndef NDEBUG
    #include <spdlog/spdlog.h>
#endif

using namespace std;
using namespace sst;

// #define ROWSIZE (31893)
#define ROWSIZE (2048)

int main() {
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    // input number of nodes and the local node rank
    std::cout << "Enter node_rank and num_nodes" << std::endl;
    int node_rank, num_nodes;
    cin >> node_rank;
    cin >> num_nodes;

    std::cout << "Input the IP addresses" << std::endl;
    uint16_t port = 32567;
    // input the ip addresses
    map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(int i = 0; i < num_nodes; ++i) {
      std::string ip;
      cin >> ip;
      ip_addrs_and_ports[i] = {ip, port};
    }
    std::cout << "Using the default port value of " << port << std::endl;

    // create all tcp connections and initialize global rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, {}, node_rank);
#else
    lf_initialize(ip_addrs_and_ports, {}, node_rank);
#endif
    // create read and write buffers
//  char *write_buf = (char *)malloc(ROWSIZE);
//  char *read_buf = (char *)malloc(ROWSIZE);
    char *write_buf = nullptr,*read_buf = nullptr;
    if(posix_memalign((void**)&write_buf,4096l,ROWSIZE) || posix_memalign((void**)&read_buf,4096l,ROWSIZE)){
      cerr << "failed to memalign SST ROWs." << endl;
      return -1;
    }

    // write message (in a way that distinguishes nodes)
    for(int i = 0; i < ROWSIZE; ++i) {
        write_buf[i] = '0' + i + node_rank % 10;
    }
    write_buf[9] = 0;

    cout << "write buffer is " << write_buf << endl;

    int r_index = num_nodes - 1 - node_rank;

    // create the rdma struct for exchanging data
#ifdef USE_VERBS_API
    resources *res = new resources(r_index, read_buf, write_buf, ROWSIZE, ROWSIZE);
#else
    resources *res = new resources(r_index, read_buf, write_buf, ROWSIZE, ROWSIZE, node_rank < r_index);
#endif

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);

    // remotely write data from the write_buf
#ifdef USE_VERBS_API
    struct verbs_sender_ctxt sctxt;
#else
    struct lf_sender_ctxt sctxt;
#endif
    sctxt.set_remote_id(r_index);
    sctxt.set_ce_idx(id);
    res->post_remote_write_with_completion(&sctxt, ROWSIZE);
    // poll for completion
    while(true)
    {
      auto ce =  util::polling_data.get_completion_entry(tid);
      if (ce) break;
    }
    sync(r_index);

    cout << "Buffer written by remote side is : " << read_buf << endl;

    for(int i = 0; i < 10; ++i) {
        write_buf[i] = '0' + i + node_rank % 10;
    }
    write_buf[9] = 0;

    cout << "write buffer is " << write_buf << endl;

    sync(r_index);
    cout << "Buffer written by remote side is : " << read_buf << endl;

    // // destroy resources
    // delete(res);

    // // destroy global resources
    // verbs_destroy();

    return 0;
}
