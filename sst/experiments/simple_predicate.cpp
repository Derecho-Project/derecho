#include <iostream>
#include <map>
#include <vector>
#include <thread>
#include <chrono>
#include <time.h>
#include <cstdlib>

#include "../sst.h"
#include "../tcp.h"
#include "statistics.h"
#include "timing.h"

using std::cin;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::string;

using namespace sst;
using namespace sst::tcp;

struct SimpleRow {
  int a;
};

int main () {
  srand (time (NULL));
  // input number of nodes and the local node id
  uint32_t num_nodes, node_rank;
  cin >> num_nodes >> node_rank;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (unsigned int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
  
  // form a group with a subset of all the nodes
  vector <uint32_t> members (num_nodes);
  for (uint32_t i = 0; i < num_nodes; ++i) {
    members[i] = i;
  }
  
  // create a new shared state table with all the members
  enum class Name {name, newname};
  using namespace predicate_builder;
  
  auto second_pred_pre = as_row_pred([](volatile const SimpleRow&) -> int {return 7;});
  auto second_pred = name_predicate<Name,Name::newname>(Min(second_pred_pre));
  
  auto test_pred_pre = as_row_pred([](volatile const SimpleRow&) -> bool {return true;});
  auto test_pred = name_predicate<Name,Name::name>(E(E(E(E(test_pred_pre)))));
  
  using PredicateTemplateArgs =
	  NamedRowPredicates<rowpred_template_arg(test_pred),
						 rowpred_template_arg(second_pred)>;
  using this_SST = SST<SimpleRow, Mode::Writes, Name, PredicateTemplateArgs>;
  this_SST *sst = new this_SST (members,node_rank,nullptr,true,test_pred,second_pred);
  const int local = sst->get_local_index();
  sst->call_named_predicate<Name::name>(local);
  sst->call_named_predicate<Name::newname>(local);

  // there are only 2 nodes; r_index is the index of the remote node
  int r_index = num_nodes-node_rank-1;
  (*sst)[local].a = 0;
  sst->put();
  // sync to make sure sst->a is 0 for both nodes
  sync (r_index);

  int num_times = 10000;
  vector <long long int> start_times (num_times), end_times (num_times);
  
  // start the experiment
  for (int i = 0; i < num_times; ++i) {
    // the predicate. Detects if the remote entry is greater than 0
    auto f = [r_index] (const this_SST& sst) {return sst[r_index].a > 0;};
    
    // the initiator node
    if (node_rank == 0) {
	  // the trigger for the predicate. outputs time.
	  auto g = [&end_times, i] (this_SST& sst) {
		  end_times[i] = experiments::get_realtime_clock();
	  };

      // register the predicate and the trigger
      sst->predicates.insert (f, g, PredicateType::ONE_TIME);

      // wait for random time
      long long int rand_time = (long long int) 2e6 + 1 + rand() % (long long int) 6e5;
	  experiments::busy_wait_for(rand_time);

      // start timer
      start_times[i] = experiments::get_realtime_clock();
      // set the integer
      (*sst)[local].a = 1;
      // update all nodes
      sst->put();
    }

    // the helper node
    else {
      // the trigger for the predicate. sets own entry in response
      auto g = [] (this_SST& sst) {
		  sst[sst.get_local_index()].a = 1;
		  sst.put();
      };

      // register the predicate and the trigger
      sst->predicates.insert (f, g);
    }

    // allow some time for detection
	experiments::busy_wait_for(1000000);

    sync (r_index);
    (*sst)[local].a =0;
    sst->put();
    // sync to make sure that both nodes are at this point
    sync (r_index);
  }

  if (node_rank == 0) {
    experiments::print_statistics (start_times, end_times, 2);
  }

  delete(sst);
  verbs_destroy();
  return 0;
}
