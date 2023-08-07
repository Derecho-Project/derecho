#include <iostream>

#include <sys/time.h>
#include <derecho/sst/detail/poll_utils.hpp>
#ifdef USE_VERBS_API
  #include <derecho/sst/detail/verbs.hpp>
#else
  #include <derecho/sst/detail/lf.hpp>
#endif
#include <derecho/tcp/tcp.hpp>

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::string;

using namespace sst;
using namespace tcp;

void initialize(int node_rank, const map<uint32_t, std::pair<string, uint16_t>> &ip_addrs_and_ports) {
    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, {}, node_rank);
#else
    lf_initialize(ip_addrs_and_ports, {}, node_rank);
#endif
}

void wait_for_completion(std::thread::id tid,int32_t nid) {
    std::optional<int32_t> cr;

    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;

    // wait for completion for a while before giving up of doing it ..
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

    while(true) {
        // check if polling result is available
        cr = util::polling_data.get_completion_entry(tid,nid);
        if(cr) {
            break;
        }
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
        if((cur_time_msec - start_time_msec) >= 2000) {
            break;
        }
    }
    // if waiting for a completion entry timed out
    if(!cr) {
        std::cerr << "Failed to get recv completion" << std::endl;
    }
}

int main() {
    srand(time(NULL));
    // input number of nodes and the local node id
    std::cout << "Enter node_rank and num_nodes" << std::endl;
    int node_rank, num_nodes;
    cin >> node_rank;
    cin >> num_nodes;

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
    initialize(node_rank, ip_addrs_and_ports);

    int a;
    volatile int b;
    a = b = 0;
    // create read and write buffers
    uint8_t *write_buf = (uint8_t *)&a;
    uint8_t *read_buf = (uint8_t *)&b;

    int r_index = num_nodes - 1 - node_rank;

    // create the rdma struct for exchanging data
#ifdef USE_VERBS_API
    resources_two_sided *res = new resources_two_sided(r_index, read_buf, write_buf, sizeof(int), sizeof(int));
#else
    resources_two_sided *res = new resources_two_sided(r_index, read_buf, write_buf, sizeof(int), sizeof(int), r_index);
#endif

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);

    util::polling_data.set_waiting(tid);
#ifdef USE_VERBS_API
    struct verbs_sender_ctxt ce_ctxt;
#else
    lf_completion_entry_ctxt ce_ctxt;
#endif
    ce_ctxt.set_remote_id(r_index);
    ce_ctxt.set_ce_idx(id);

    if(node_rank == 0) {
        // wait for random time
        volatile long long int wait_time = (long long int)5e5;
        for(long long int i = 0; i < wait_time; ++i) {
        }
        cout << "Wait finished" << endl;

        a = 1;
        res->post_two_sided_send(sizeof(int));
        util::polling_data.set_waiting(tid);
        res->post_two_sided_receive(&ce_ctxt, sizeof(int));

        cout << "Receive buffer posted" << endl;
        wait_for_completion(tid,r_index);
        util::polling_data.reset_waiting(tid);
        cout << "Data received" << endl;

        while(b == 0) {
        }
    }

    else {
        util::polling_data.set_waiting(tid);
        res->post_two_sided_receive(&ce_ctxt, sizeof(int));
        cout << "Receive buffer posted" << endl;
        wait_for_completion(tid,r_index);
        util::polling_data.reset_waiting(tid);
        cout << "Data received" << endl;
        while(b == 0) {
        }
        a = 1;
        cout << "Sending" << endl;
        res->post_two_sided_send(sizeof(int));
    }

    sync(r_index);
    return 0;
}
