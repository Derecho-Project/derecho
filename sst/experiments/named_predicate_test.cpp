#include <iostream>
#include <map>
#include <thread>
#include <chrono>
#include <time.h>
#include <cstdlib>

#include "sst/sst.h"
#include "sst/tcp.h"
#include "statistics.h"
#include "timing.h"

using std::map;
using std::cin;
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
  int num_nodes, node_rank;
  cin >> num_nodes >> node_rank;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
  
  // form a group with a subset of all the nodes
  vector <unsigned int> members (num_nodes);
  for (int i = 0; i < num_nodes; ++i) {
    members[i] = i;
  }
  
  // create a new shared state table with all the members
  enum class Name {outer_name, inner_name};
  using namespace predicate_builder;
  auto test_pred_pre = name_predicate<Name,Name::inner_name>(
	  as_row_pred(
		  [](volatile const SimpleRow&) -> bool
		  {
			  cout << "here" << endl;
			  return true;
		  }));
  auto test_pred = name_predicate<Name,Name::outer_name>(E(test_pred_pre));
  static_assert(decltype(test_pred)::num_updater_functions::value >= 1,"why aren't there any?");
  using this_SST = SST<SimpleRow, Mode::Writes, Name, decltype(test_pred) >;
  this_SST *sst = new this_SST (members, node_rank,nullptr,true,test_pred);
  const int local = sst->get_local_index();
  using namespace std::chrono;
  std::this_thread::sleep_for (seconds(3));
  cout << "Calling named predicate" << endl;
  bool ret = sst->call_named_predicate<Name::inner_name>(local);
  cout << "Return value is " << ret << endl;

  delete(sst);
  verbs_destroy();
  return 0;
}
