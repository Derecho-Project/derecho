#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>

#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/sst.hpp>
//Since all SST instances are named sst, we can use this convenient hack
#define LOCAL sst.get_local_index()

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_rank) : SST<mySST>(this, SSTParams{_members, my_rank}) {
        SSTInit(a, heartbeat);
    }
    SSTField<int> a;
    SSTField<bool> heartbeat;
};

int main() {
    // input number of nodes and the local node rank
    std::cout << "Enter node_rank and num_nodes" << std::endl;
    uint32_t node_rank, num_nodes;
    cin >> node_rank >> num_nodes;

    std::cout << "Input the IP addresses" << std::endl;
    uint16_t port = 32567;
    // input the ip addresses
    map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(uint i = 0; i < num_nodes; ++i) {
      std::string ip;
      cin >> ip;
      ip_addrs_and_ports[i] = {ip, port};
    }
    std::cout << "Using the default port value of " << port << std::endl;

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, {}, node_rank);
#else
    lf_initialize(ip_addrs_and_ports, {}, node_rank);
#endif

    // form a group with a subset of all the nodes
    vector<uint32_t> members(num_nodes);
    for(unsigned int i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    // create a new shared state table with all the members
    mySST sst(members, node_rank);
    sst.a[node_rank] = 0;
    sst.put((char*)std::addressof(sst.a[0]) - sst.getBaseAddress(), sizeof(int));

    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            sst.put_with_completion((char*)std::addressof(sst.heartbeat[0]) - sst.getBaseAddress(), sizeof(bool));
        }
    };

    std::thread failures_thread = std::thread(check_failures_loop);

    bool if_exit = false;
    // wait till all a's are 0
    while(if_exit == false) {
        if_exit = true;
        for(unsigned int i = 0; i < num_nodes; ++i) {
            if(sst.a[i] != 0) {
                if_exit = false;
            }
        }
    }

    for(unsigned int i = 0; i < num_nodes; ++i) {
        if(i == node_rank) {
            continue;
        }
        sync(i);
    }

    struct timespec start_time;

    // the predicate
    auto f = [num_nodes](const mySST& sst) {
        for(unsigned int i = 0; i < num_nodes; ++i) {
            if(sst.a[i] < sst.a[LOCAL]) {
                return false;
            }
        }
        return true;
    };

    // trigger. Increments self value
    auto g = [&start_time](mySST& sst) {
        ++(sst.a[LOCAL]);
        sst.put((char*)std::addressof(sst.a[0]) - sst.getBaseAddress(), sizeof(int));
        if(sst.a[LOCAL] == 1000000) {
            // end timer
            struct timespec end_time;
            clock_gettime(CLOCK_REALTIME, &end_time);
            // my_time is time taken to count
            double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec)) / 1e9;
            int node_rank = sst.get_local_index();
            // node 0 finds the average by reading all the times taken by remote nodes
            // Anyway, the values will be quite close as the counting is synchronous
            if(node_rank == 0) {
                int num_nodes = sst.get_num_rows();
                resources* res;
                double times[num_nodes];
                const auto tid = std::this_thread::get_id();
                // get id first
                uint32_t id = util::polling_data.get_index(tid);
                util::polling_data.set_waiting(tid);

                // read the other nodes' time
                for(int i = 0; i < num_nodes; ++i) {
                    if(i == node_rank) {
                        times[i] = my_time;
                    } else {
#ifdef USE_VERBS_API
                        res = new resources(i, (char*)&my_time, (char*)&times[i], sizeof(double), sizeof(double));
#else
                        res = new resources(i, (char*)&my_time, (char*)&times[i], sizeof(double), sizeof(double), node_rank < i);
#endif
                        res->post_remote_read(id, sizeof(double));
                        free(res);
                    }
                }
                for(int i = 0; i < num_nodes; ++i) {
                    util::polling_data.get_completion_entry(tid);
                }
                util::polling_data.reset_waiting(tid);

                double sum = 0.0;
                // compute the average
                for(int i = 0; i < num_nodes; ++i) {
                    sum += times[i];
                }
                ofstream fout;
                fout.open("data_count_write", ofstream::app);
                fout << num_nodes << " " << sum / num_nodes << endl;
                fout.close();

                // sync to tell other nodes to exit
                for(int i = 0; i < num_nodes; ++i) {
                    if(i == node_rank) {
                        continue;
                    }
                    sync(i);
                }
            } else {
                resources* res;
                double no_need;
#ifdef USE_VERBS_API
                res = new resources(0, (char*)&my_time, (char*)&no_need, sizeof(double), sizeof(double));
#else
                res = new resources(0, (char*)&my_time, (char*)&no_need, sizeof(double), sizeof(double), 0);
#endif
                sync(0);
                free(res);
            }
#ifdef USE_VERBS_API
            verbs_destroy();
#else
            lf_destroy();
#endif
            exit(0);
        }
    };

    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);

    // register as a recurring predicate
    sst.predicates.insert(f, g, PredicateType::RECURRENT);

    while(true) {
    }
    return 0;
}
