#include <iostream>
#include <map>
#include <vector>
#include <string>

#include "../sst.h"
#include "../tcp.h"

using std::cin;
using std::string;
using std::vector;
using std::map;
using std::cout;
using std::endl;

using namespace sst;
using sst::tcp::tcp_initialize;
using sst::tcp::sync;

struct TokenRow {
  int token_num;
};

int main () {
  
  // input number of nodes and the local node id
  int num_nodes, node_rank;
  cin >> num_nodes >> node_rank;

  if (num_nodes < 3) {
    cout << "Number of nodes must be 3 or greater for this experiment" << endl;
    return 0;
  }
  
  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
  
  // form a group with only successor and predecessor
  vector <uint32_t> members (3);
  // having first row for predecessor and third for successor creates an infinite wait loop
  // instead we should have the members in increasing order of node_rank
  int predecessor, successor, pred_index, succ_index;
  if (node_rank == 0) {
    members[0] = 0;
    members[1] = 1;
    members[2] = num_nodes-1;
    pred_index = 2;
    succ_index = 1;
  }
  // third row is for the successor
  else if (node_rank == num_nodes-1) {
    members[0] = 0;
    members[1] = num_nodes-2;
    members[2] = num_nodes-1;
    pred_index = 1;
    succ_index = 2;
  }
  else {
    members[0] = node_rank-1;
    members[1] = node_rank;
    members[2] = node_rank+1;
    pred_index = 0;
    succ_index = 2;
  }

  int pred_rank = members[pred_index];
  int succ_rank = members[succ_index];
  // create a new shared state table with all the members
  SST<TokenRow, Mode::Writes> *sst = new SST<TokenRow, Mode::Writes> (members, node_rank);
  (*sst)[sst->get_local_index()].token_num = 0;
  sst->put ();
  // sync before registering
  if (node_rank == 0) {
    sync (members[1]);
    sync (members[2]);
  }
  else if (node_rank == num_nodes-1) {
    sync (members[0]);
    sync (members[1]);
  }
  else {
    sync (members[0]);
    sync (members[2]);
  }
  
  cout << "Started Token Passing" << endl;

  // trigger is common to all nodes
  // transfers the token by increases its token_num
  auto g = [node_rank, members, num_nodes] (SST<TokenRow, Mode::Writes> & sst) {
      const int local = sst.get_local_index();
      // release the token
          sst[local].token_num++;
          sst.put();
          if (sst[local].token_num == 1000) {
              cout << "Done" << endl;
              // sync before exiting
              if (node_rank == 0) {
                  sync (members[1]);
                  sync (members[2]);
              }
              else if (node_rank == num_nodes-1) {
                  sync (members[0]);
                  sync (members[1]);
              }
              else {
                  sync (members[0]);
                  sync (members[2]);
              }
              exit (0);
          }
      };
  
  if (node_rank == 0) {
    // node 0 detects if last round of token passing is complete and if so, in the trigger passes the next token
    auto f = [pred_rank, pred_index, node_rank] (const SST<TokenRow, Mode::Writes>& sst) {
      cout << "predecessor's token value" << sst[pred_index].token_num << endl;
      // checks if the predecssor has released the token
      return sst[pred_index].token_num == sst[sst.get_local_index()].token_num;
    };

    // register as a recurring predicate
    sst->predicates.insert (f, g, PredicateType::RECURRENT);
  }
  else {
    // the predicate, checks if it can grab the token
    auto f = [pred_rank, pred_index, node_rank] (const SST<TokenRow, Mode::Writes>& sst) {
      cout << "predecessor's token value" << sst[pred_index].token_num << endl;
      // checks if the predecssor has released the token
      return sst[pred_index].token_num > sst[sst.get_local_index()].token_num;
    };

    // register as a recurring predicate
    sst->predicates.insert (f, g, PredicateType::RECURRENT);
  }

  // wait. trigger will exit after 1000 rounds of token passing
  while (true) {
    
  }
  return 0;
}
