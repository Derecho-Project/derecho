#include <iostream>
#include <map>

#include "sst/verbs.h"
#include "sst/tcp.h"

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
  // input number of nodes and the local node id
  int num_nodes, node_rank;
  cin >> node_rank;
  cin >> num_nodes;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // create all tcp connections and initialize global rdma resources
  initialize(node_rank, ip_addrs);

  // create read and write buffers
  char *write_buf = (char*) malloc (10);
  char *read_buf = (char*) malloc (10);

  // write message (in a way that distinguishes nodes)
  for (int i = 0; i < 9; ++i) {
    write_buf[i] = '0'+node_rank%10;
  }
  write_buf[9] = 0;

  cout << "write buffer is " << write_buf << endl;

  int r_index = num_nodes-1-node_rank;
  
  // create the rdma struct for exchanging data
  resources *res = new resources (r_index, read_buf, write_buf, 10, 10);
  
  // remotely write data from the write_buf
  res->post_remote_write (10);
  // poll for completion
  verbs_poll_completion();

  // sync before destroying resources
  char  temp_char; 
  char tQ[2] = {'Q', 0};
  sock_sync_data(get_socket (r_index), 1, tQ, &temp_char);

  cout << "Buffer written by remote side is : " << read_buf << endl;
  
  for (int i = 0; i < 7; ++i) {
    write_buf[i] = '5'+node_rank%10;
  }
  for (int i = 7; i < 9; ++i) {
    write_buf[i] = '1'+node_rank%10;
  }
  write_buf[9] = 0;

  cout << "write buffer is " << write_buf << endl;

  // remotely write data from the write_buf
  res->post_remote_write (5, 3);
  // poll for completion
  verbs_poll_completion();
  
  sock_sync_data(get_socket (r_index), 1, tQ, &temp_char);

  cout << "Buffer written by remote side is : " << read_buf << endl;

  // destroy resources
  delete(res);

  // destroy global resources
  verbs_destroy();

  return 0;
}
