/*
  FARM like experiment for RDMA read throughput
  Had to adjust size of completion queues and queue pairs to allow for large number of pending requests since we post all the reads without waiting for their completion
*/

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <string.h>
#include <thread>
#include <time.h>
#include <vector>

#include "sst/tcp.h"
#include "sst/verbs.h"

using std::cout;
using std::vector;
using std::map;
using std::endl;
using std::cin;
using std::string;

using namespace sst;
using namespace sst::tcp;

// number of threads to spawn for posting reads
int num_threads = 16;
// threads set this to indicate main that they are ready to post reads
vector<bool> done_initializing;
// main sets this to set off the threads
bool send = false;

// number of nodes
int num_nodes;
// rank of the current node
int node_rank;

// size of data transfer
long long int read_size = 16;
// size of the entire buffer. Its size shouldn't matter for performance. CHECK THIS!)
long long int buf_size = 1024 * 1024;
// number of reads per thread
int num_reads = 10000;

// resources for the nodes
vector<resources *> res_vec;

void *post_reads(void *tid) {
    int thread_id = (long)tid;

    // we want to separate calls to random number generator from read requests
    int node[num_reads];
    long long int offset[num_reads];
    for(int i = 0; i < num_reads; ++i) {
        // pick a random node
        node[i] = rand() % (num_nodes - 1);
        // but not the local node!
        if(node[i] >= node_rank) {
            node[i] += 1;
        }
        // pick a random offset to read
        offset[i] = read_size * (rand() % (buf_size / read_size));
    }
    cout << "Done with initializing : thread " << thread_id << endl;
    // indicate to main thread, readiness to post reads
    done_initializing[thread_id] = true;

    // wait for the signal to start reading
    while(send == false) {
    }
    // read randomly many times
    for(int i = 0; i < num_reads; ++i) {
        // post the read
        res_vec[node[i]]->post_remote_read(offset[i], read_size);
    }

    pthread_exit(NULL);
}

void *poll_reads(void *tid) {
    for(int i = 0; i < num_reads; ++i) {
        verbs_poll_completion();
    }

    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    cout << end_time.tv_sec * 1000000000 + end_time.tv_nsec << endl;

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

    pthread_t threads[num_threads], poll_threads[num_threads];
    pthread_attr_t attr;
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    cin >> num_nodes;
    cin >> node_rank;
    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // create all tcp connections and initialize global rdma resources
    initialize(node_rank, ip_addrs);

    // create memory for RDMA operations (with all nodes, we share the same buffer, which should not cause contention. CHECK THIS!)
    char *write_buf, *read_buf;
    write_buf = (char *)malloc(buf_size);
    read_buf = (char *)malloc(read_size);
    // create the resources
    res_vec.resize(num_nodes);
    for(int i = 0; i < num_nodes; ++i) {
        res_vec[i] = new resources(num_nodes - node_rank - 1, write_buf, read_buf, buf_size, read_size);
    }

    done_initializing.resize(num_threads, false);
    // create the threads
    for(long i = 0; i < num_threads; ++i) {
        pthread_create(&threads[i], NULL, post_reads, (void *)i);
    }
    // check to see that all threads have finished random number calls
    bool ready = false;
    while(ready == false) {
        ready = true;
        for(int i = 0; i < num_threads; ++i) {
            if(done_initializing[i] == false) {
                ready = false;
            }
        }
    }

    // // create the polling threads
    // for (long i = 0; i < num_threads; ++i) {
    //   pthread_create(&poll_threads[i], NULL, poll_reads, (void *) i);
    // }

    cout << "Starting experiments" << endl;

    // create structures for storing time
    struct timespec start_time;
    struct timespec end_time;
    long long int nanoseconds_elapsed;

    // note the current time
    clock_gettime(CLOCK_REALTIME, &start_time);
    // set the threads off
    send = true;

    // cout << start_time.tv_sec*1000000000 + start_time.tv_nsec <<endl;

    // main thread polls for completion. I think creating multiple threads to poll should not improve performance by much. CHECK THIS!
    for(long long int i = 0; i < num_threads * num_reads; ++i) {
        verbs_poll_completion();
    }

    // note the number of years elapsed
    clock_gettime(CLOCK_REALTIME, &end_time);
    nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);

    cout << "Number of operations : " << num_threads * num_reads << endl;
    cout << "Time taken (in ns) : " << nanoseconds_elapsed << endl;

    // wait for threads to exit
    void *status;
    // pthread_attr_destroy (&attr);
    for(int i = 0; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], &status);
        if(rc) {
            cout << "Error in thread join " << endl;
        }
    }
    // for (int i = 0; i < num_threads; ++i) {
    //   int rc = pthread_join(poll_threads[i], &status);
    //   if (rc) {
    //     cout << "Error in thread join " << endl;
    //   }
    // }
    cout << "All threads terminated. Exiting!!" << endl;

    // sync with every other node for grace exit
    for(int i = 0; i < num_nodes; ++i) {
        if(i == node_rank) {
            continue;
        }
        sync(i);
    }

    // free malloc()ed area
    free(write_buf);
    free(read_buf);
    // destroy the resources
    for(int i = 0; i < num_nodes; ++i) {
        delete(res_vec[i]);
    }
    verbs_destroy();

    return 0;
}
