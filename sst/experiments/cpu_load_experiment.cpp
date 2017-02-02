#include <iostream>
#include <chrono>
#include <thread>

#include "derecho/experiments/aggregate_bandwidth.h"
#include "derecho/experiments/log_results.h"
#include "multicast.h"

using namespace std;
using namespace sst;

volatile bool done = false;

void cpuNow(pthread_t t, struct timespec start_time) {
  struct timespec ts;
  clockid_t cid;

  pthread_getcpuclockid(t, &cid);
  clock_gettime(cid, &ts);
  // printf("%4ld.%03ld\n", ts.tv_sec, ts.tv_nsec / 1000000);
  double my_time =  ts.tv_sec + ts.tv_nsec/1e9;
  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME, &end_time);
  double total_time = ((end_time.tv_sec*1e9 + end_time.tv_nsec)- (start_time.tv_sec*1e9 + start_time.tv_nsec))/1e9;

  cout << "CPU utilization: " << (my_time * 100)/total_time << "%" << endl;
}

int main() {
    constexpr uint max_msg_size = 10, window_size = 1000;
    unsigned int last_message_index = -1;
    bool done = false;
    // input number of nodes and the local node id
    uint32_t node_id, num_nodes;
    cin >> node_id >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(unsigned int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    verbs_initialize(ip_addrs, node_id);

    std::vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }


    struct timespec start_time, end_time;
    group<max_msg_size> g(
        members, node_id, window_size, [&start_time, &last_message_index, &done, &node_id, &num_nodes](
                                           uint32_t sender_rank, uint64_t index,
                                           volatile char* msg, uint32_t size) {
	  // cout << "Sender rank = " << sender_rank << ", index = " << index << ", last message index = " << last_message_index << endl;
	  if(sender_rank == node_id && index >= last_message_index) {
	    cpuNow(pthread_self(), start_time);
	    done = true;
	  }
        });
    auto send = [&]() {
      volatile char* buf;
      while((buf = g.get_buffer(max_msg_size)) == NULL) {
      }
      g.send();
    };
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    uint64_t next_index = 0;
    for(uint i = 0; i < 6; ++i) {
      struct timespec round_start_time, cur_time;
      clock_gettime(CLOCK_REALTIME, &round_start_time);
      while (true) {
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double time_in_sec = ((cur_time.tv_sec*1e9 + cur_time.tv_nsec)- (round_start_time.tv_sec*1e9 + round_start_time.tv_nsec))/1e9;
	if (time_in_sec > 10) {
	  break;
	}
	send();
	next_index++;
      }
      while (true) {
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double time_in_sec = ((cur_time.tv_sec*1e9 + cur_time.tv_nsec)- (round_start_time.tv_sec*1e9 + round_start_time.tv_nsec))/1e9;
	if (time_in_sec > 20) {
	  break;
	}
	send();
	next_index++;
	std::this_thread::sleep_for(1ms);
      }
      cout << "Round " << i << " complete" << endl;
    }
    last_message_index = next_index-1;
    send();
    // cout << "last message index is: " << last_message_index << endl;
    cout << "Done sending" << endl;
    while(!done) {
    }
    // end timer
    clock_gettime(CLOCK_REALTIME, &end_time);
    double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) -
                      (start_time.tv_sec * 1e9 + start_time.tv_nsec));
    double message_rate = (next_index * 1e9) / my_time;
    
    double sum_message_rate =
        aggregate_bandwidth(members, node_id, message_rate * num_nodes);
    log_results(num_nodes, max_msg_size, sum_message_rate, "data_multicast");
    sync (1-node_id);
}
