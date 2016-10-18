#include <iostream>
#include <map>
#include <fstream>

#include "../verbs.h"
#include "../tcp.h"

using namespace std;
using namespace sst;
using namespace sst::tcp;

// size of buffer
int size = 16;

// number of reruns
long long int num_reruns = 10000;

void initialize(int node_rank, const map <uint32_t, string> & ip_addrs) {
  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
}

int main () {
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
  
  // create an array of resources
  resources *res[num_nodes];
  // create buffer for write and read
  char *write_buf[num_nodes], *read_buf[num_nodes];
  for (int r_index = 0; r_index < num_nodes; ++r_index) {
    if (r_index == node_rank) {
      continue;
    }
    
    write_buf[r_index] = (char *) malloc (size);
    read_buf[r_index] = (char *) malloc (size);
    res[r_index]  = new resources (r_index, write_buf[r_index], read_buf[r_index], size, size);
  }

  // start the timing experiment for reads
  struct timespec start_time;
  struct timespec end_time;
  long long int nanoseconds_elapsed1, nanoseconds_elapsed2;

  clock_gettime(CLOCK_REALTIME, &start_time);

  for (int i = 0; i < num_reruns; ++i) {
    // start sequential reads
    for (int r_index = 0; r_index < num_nodes; ++r_index) {
      if (r_index == node_rank) {
	continue;
      }
      
      res[r_index]->post_remote_read (size);
    }
      
    // poll for completion simultaneously
    for (int r_index = 1; r_index < num_nodes; ++r_index) {
      verbs_poll_completion();
    }
  }

  clock_gettime(CLOCK_REALTIME, &end_time);
  nanoseconds_elapsed1 = (end_time.tv_sec-start_time.tv_sec)*1000000000 + (end_time.tv_nsec-start_time.tv_nsec);
  
  // sync before starting the writes
  for (int r_index = 0; r_index < num_nodes; ++r_index) {
    if (r_index == node_rank) {
      continue;
    }
    sync (r_index);
  }

  // start the experiments for the writes
  
  clock_gettime(CLOCK_REALTIME, &start_time);

  for (int i = 0; i < num_reruns; ++i) {
    // start sequential reads
    for (int r_index = 0; r_index < num_nodes; ++r_index) {
      if (r_index == node_rank) {
	continue;
      }
      
      res[r_index]->post_remote_write (size);
    }
      
    // poll for completion simultaneously
    for (int r_index = 1; r_index < num_nodes; ++r_index) {
      verbs_poll_completion();
    }
  }
    
  clock_gettime(CLOCK_REALTIME, &end_time);
  nanoseconds_elapsed2 = (end_time.tv_sec-start_time.tv_sec)*1000000000 + (end_time.tv_nsec-start_time.tv_nsec);
  
  // sync for final measurements
  for (int r_index = 0; r_index < num_nodes; ++r_index) {
    if (r_index == node_rank) {
      continue;
    }
    sync (r_index);
  }

  double my_time1 = (nanoseconds_elapsed1+0.0)/(1000*num_reruns);
  double my_time2 = (nanoseconds_elapsed2+0.0)/(1000*num_reruns);
  
  // node 0 finds the average by reading all the times taken by remote nodes
  if (node_rank == 0) {
    resources *res1, *res2;
    double times1[num_nodes], times2[num_nodes];
    // read the other nodes' time
    for (int i = 0; i < num_nodes; ++i) {
      if (i == node_rank) {
	times1[i] = my_time1;
	times2[i] = my_time2;	
      }
      else {
	res1 = new resources (i, (char *)&my_time1, (char *)&times1[i], sizeof(double), sizeof(double));
	res2 = new resources (i, (char *)&my_time2, (char *)&times2[i], sizeof(double), sizeof(double));
	res1->post_remote_read (sizeof(double));
	res2->post_remote_read (sizeof(double));
	verbs_poll_completion();
	verbs_poll_completion();
	free(res1);
	free(res2);	
      }
    }

    // sync to tell other nodes to exit
    for (int i = 0; i < num_nodes; ++i) {
      if (i == node_rank) {
	continue;
      }
      sync (i);
    }

    double sum = 0.0;
    // compute the average
    for (int i = 0; i < num_nodes; ++i) {
      sum += times1[i];
    }
    ofstream fout;
    fout.open ("data_sequential_remote_read", ofstream::app);
    fout << num_nodes << " " << sum/num_nodes << endl;
    fout.close();

    sum = 0.0;
    // compute the average
    for (int i = 0; i < num_nodes; ++i) {
      sum += times2[i];
    }
    fout.open ("data_sequential_remote_write", ofstream::app);
    fout << num_nodes << " " << sum/num_nodes << endl;
    fout.close();
  }
  
  else {
    resources *res1, *res2;
    double no_need;
    res1 = new resources (0, (char *)&my_time1, (char *)&no_need, sizeof(double), sizeof(double));
    res2 = new resources (0, (char *)&my_time2, (char *)&no_need, sizeof(double), sizeof(double));    
    sync(0);
    delete(res1);
    delete(res2);
  }

  return 0;
}
