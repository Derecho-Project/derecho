#include <iostream>
#include <map>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>

#include "sst/tcp.h"
#include "sst/verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

using namespace sst;
using namespace sst::tcp;

int num_reads = 10;
long long int size = 1024 * 1024 * 1024;

// resources class
resources *res;

void *post_reads(void *nothing) {
    for(int i = 0; i < num_reads; ++i) {
        res->post_remote_read(35, 50);
        cout << "Posted read number " << i << endl;
    }
    return NULL;
}

void *poll_reads(void *nothing) {
    for(int i = 0; i < num_reads; ++i) {
        verbs_poll_completion();
        cout << "Polled read number " << i << endl;
    }
    return NULL;
}

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize tcp connections
    tcp_initialize(node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();
}

int main() {
    srand(time(NULL));

    int num_nodes;
    int node_rank;
    cin >> num_nodes >> node_rank;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // create all tcp connections and initialize global rdma resources
    initialize(node_rank, ip_addrs);

    uint8_t *write_buf, *read_buf;
    write_buf = (uint8_t *)malloc(size);
    read_buf = (uint8_t *)malloc(50);
    for(int i = 35; i < 35 + 50; ++i) {
        char ch = (rand() % 26) + 'a';
        write_buf[i] = ch;
    }
    cout << "Done initializing buffer for remote read" << endl;
    res = new resources(num_nodes - node_rank - 1, write_buf, read_buf, size, 50);

    pthread_t post_thread, poll_thread;
    pthread_attr_t attr;
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    pthread_create(&post_thread, NULL, post_reads, (void *)NULL);
    pthread_create(&poll_thread, NULL, poll_reads, (void *)NULL);

    void *status;
    pthread_join(post_thread, &status);
    pthread_join(poll_thread, &status);

    for(int i = 35; i < 35 + 50; ++i) {
        cout << write_buf[i] << " ";
    }
    cout << endl;
    for(int i = 0; i < 50; ++i) {
        cout << read_buf[i] << " ";
    }
    cout << endl;

    // sync to notify the remote node
    char temp_char;
    char tQ[2] = {'Q', 0};
    // wait for node 0 to finish read
    sock_sync_data(get_socket(num_nodes - node_rank - 1), 1, tQ, &temp_char);

    return 0;
}
