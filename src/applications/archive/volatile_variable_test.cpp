#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <thread>
#include <time.h>
#include <vector>

#include "sst/verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

using namespace sst;

int main() {
    srand(time(NULL));
    // test with 2 number of nodes
    int num_nodes = 2, node_id;
    // node_id should be 0 or 1
    cin >> node_id;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // create all tcp connections and initialize global rdma resources
    verbs_initialize(ip_addrs, node_id);

    int a;
    int b;
    a = b = 0;
    // create read and write buffers
    char *write_buf = (char *)&a;
    char *read_buf = (char *)&b;

    int remote_id = num_nodes - 1 - node_id;

    // create the rdma struct for exchanging data
    resources *res = new resources(remote_id, read_buf, write_buf, sizeof(int), sizeof(int));

    if(node_id == 0) {
        a = 1;
        asm volatile("mfence"
                     :
                     :
                     : "memory");
        res->post_remote_write(0, sizeof(int));
        while(b == 0) {
        }
    }

    else {
        while(b == 0) {
        }
        a = 1;
        asm volatile("mfence"
                     :
                     :
                     : "memory");
        res->post_remote_write(0, sizeof(int));
    }

    sync(remote_id);
    return 0;
}
