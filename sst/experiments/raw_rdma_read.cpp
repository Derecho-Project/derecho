#include <iostream>
#include <map>
#include <thread>
#include <chrono>
#include <time.h>
#include <cstdlib>
#include <algorithm>
#include <numeric>

#include "sst/verbs.h"
#include "sst/tcp.h"
#include "statistics.h"

using namespace std;
using namespace sst;
using namespace sst::tcp;

void initialize(int node_rank, const map <uint32_t, string> & ip_addrs) {
  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
}

int main () {
  srand (time (NULL));
  // input number of nodes and the local node id
  int num_nodes, node_rank;
  cin >> num_nodes;
  cin >> node_rank;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // create all tcp connections and initialize global rdma resources
  initialize(node_rank, ip_addrs);

  int a;
  volatile int b;
  a=b=0;
  // create read and write buffers
  char *write_buf = (char*)&a;
  char *read_buf = (char*)&b;

  int r_index = num_nodes-1-node_rank;
  
  // create the rdma struct for exchanging data
  resources *res = new resources (r_index, write_buf, read_buf, sizeof(int), sizeof(int));

  int num_times = 10000;
  vector <long long int> start_times (num_times), end_times (num_times);

  for (int rep = 0; rep < num_times; ++rep) {
    if (node_rank == 0) {
      // wait for random time
      long long int rand_time = (long long int) 2e5 + 1 + rand() % (long long int) 6e5;
      for (long long int i = 0; i < rand_time; ++i) {
	
      }
      
      struct timespec start_time, end_time;
      // start timer
      clock_gettime(CLOCK_REALTIME, &start_time);
      start_times[rep] = start_time.tv_sec*(long long int) 1e9 + start_time.tv_nsec;
      a = 1;
      while (b == 0) {
	res->post_remote_read (sizeof(int));
	verbs_poll_completion ();
      }
      clock_gettime(CLOCK_REALTIME, &end_time);
      end_times[rep] = end_time.tv_sec*(long long int) 1e9 + end_time.tv_nsec;
      sync(r_index);
      a = b = 0;
    }
    
    else {
      while (b == 0) {
	res->post_remote_read (sizeof(int));
	verbs_poll_completion ();
      }
      a = 1;
      sync(r_index);
      a = b = 0;
    }
  }

  if (node_rank == 0) {
    experiments::print_statistics (start_times, end_times, 2);
  }

  delete (res);
  verbs_destroy ();
  
  return 0;
}
