#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <ctime>
#include <cstdlib>

#include "sst/sst.h"
//Since all SST instances are named sst, we can use this convenient hack
#define LOCAL sst.get_local_index()

using std::vector;
using std::map;
using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::ofstream;

using namespace sst;
struct TestRow {
  int a;
};

int main () {
  // input number of nodes and the local node id
  uint32_t num_nodes, node_rank;
  cin >> num_nodes >> node_rank;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (size_t i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }
  
  // initialize the rdma resources
  verbs_initialize(node_rank, ip_addrs);
  
  // form a group with a subset of all the nodes
  vector <uint32_t> members (num_nodes);
  for (uint32_t i = 0; i < num_nodes; ++i) {
    members[i] = i;
  }
  
  // create a new shared state table with all the members
  SST<TestRow, Mode::Reads> *sst = new SST<TestRow, Mode::Reads> (members, node_rank);
  (*sst)[sst->get_local_index()].a = 0;

  bool if_exit = false;
  // wait till all a's are 0
  while (if_exit == false) {
    if_exit = true;
    for (unsigned int i = 0; i < num_nodes; ++i) {
      if ((*sst)[i].a != 0) {
	if_exit = false;
      }
    }
  }

  for (size_t i = 0; i < num_nodes; ++i) {
    if (i == node_rank) {
      continue;
    }
    sync (i);
  }
  
  struct timespec start_time;
  
  // the predicate
  auto f = [num_nodes] (const SST<TestRow, Mode::Reads>& sst) {
    for (size_t i = 0; i < num_nodes; ++i) {
      if (sst[i].a < sst[LOCAL].a) {
	return false;
      }
    }
    return true;
  };

  // trigger. Increments self value
  auto g = [&start_time] (SST<TestRow, Mode::Reads>& sst) {
    ++(sst[LOCAL].a);
    if (sst[LOCAL].a == 1000000) {
      // end timer
      struct timespec end_time;
      clock_gettime(CLOCK_REALTIME, &end_time);
      // my_time is time taken to count
      double my_time = ((end_time.tv_sec*1e9 + end_time.tv_nsec)- (start_time.tv_sec*1e9 + start_time.tv_nsec))/1e9;
      int node_rank = sst.get_local_index();
      // node 0 finds the average by reading all the times taken by remote nodes
      // Anyway, the values will be quite close as the counting is synchronous
      if (node_rank == 0) {
	int num_nodes = sst.get_num_rows();
	resources *res;
	double times[num_nodes];
	// read the other nodes' time
	for (int i = 0; i < num_nodes; ++i) {
	  if (i == node_rank) {
	    times[i] = my_time;
	  }
	  else {
	    res = new resources (i, (char *)&my_time, (char *)&times[i], sizeof(double), sizeof(double));
	    res->post_remote_read (sizeof(double));
	    free(res);
	  }
	}

	double sum = 0.0;
	// compute the average
	for (int i = 0; i < num_nodes; ++i) {
	  sum += times[i];
	}
	ofstream fout;
	fout.open ("data_count_read", ofstream::app);
	fout << num_nodes << " " << sum/num_nodes << endl;
	fout.close();

	// sync to tell other nodes to exit
	for (int i = 0; i < num_nodes; ++i) {
	  if (i == node_rank) {
	    continue;
	  }
	  sync (i);
	}
      }
      else {
	resources *res;
	double no_need;
	res = new resources (0, (char *)&my_time, (char *)&no_need, sizeof(double), sizeof(double));
	sync(0);
	free(res);
      }
      
      exit (0);
    }
  };

  // start timer
  clock_gettime(CLOCK_REALTIME, &start_time);

  // register as a recurring predicate, 1 is the type
  sst->predicates.insert (f, g, PredicateType::RECURRENT);

  while (true) {
    
  }
  return 0;
}
