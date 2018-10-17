#include <fstream>
#include <iostream>
#include <sstream>

#include "sst/multicast.h"
#include "sst/multicast_sst.h"

using namespace std;
using namespace sst;

volatile bool done = false;

int main() {
    constexpr uint max_msg_size = 1, window_size = 1000;
    const unsigned int num_messages = 1000000;
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

    vector<vector<int64_t>> recv_times(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        recv_times[i].resize(num_messages);
    }

    vector<int64_t> send_times(num_messages);

    uint num_finished = 0;

    std::shared_ptr<multicast_sst> sst = make_shared<multicast_sst>(
            sst::SSTParams(members, node_id),
            window_size);

    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(chrono::microseconds(100));
            if(sst) {
                sst->put_with_completion((char*)std::addressof(sst->heartbeat[0]) - sst->getBaseAddress(), sizeof(bool));
            }
        }
    };

    thread failures_thread = std::thread(check_failures_loop);

    struct timespec recv_time, send_time;
    auto sst_receive_handler = [&recv_times, &recv_time, &num_finished, &num_nodes, &num_messages](
                                       uint32_t sender_rank, uint64_t index, volatile char* msg,
                                       uint32_t size) {
        // start timer
        clock_gettime(CLOCK_REALTIME, &recv_time);
        recv_times[sender_rank][index] = recv_time.tv_sec * 1e9 + recv_time.tv_nsec;
        if(index == num_messages - 1) {
            num_finished++;
        }
        if(num_finished == num_nodes) {
            done = true;
        }
    };
    auto receiver_pred = [](const multicast_sst&) {
        return true;
    };
    auto num_times = window_size / num_nodes;
    if(!num_times) {
        num_times = 1;
    }
    auto receiver_trig = [num_times, num_nodes, node_id, sst_receive_handler](multicast_sst& sst) {
        bool update_sst = false;
        for(uint i = 0; i < num_times; ++i) {
            for(uint j = 0; j < num_nodes; ++j) {
                uint32_t slot = sst.num_received_sst[node_id][j] % window_size;
                if((int64_t)sst.slots[j][slot].next_seq == (sst.num_received_sst[node_id][j]) / window_size + 1) {
                    sst_receive_handler(j, sst.num_received_sst[node_id][j],
                                        sst.slots[j][slot].buf,
                                        sst.slots[j][slot].size);
                    sst.num_received_sst[node_id][j]++;
                    update_sst = true;
                }
            }
        }
        if(update_sst) {
            sst.put(sst.num_received_sst.get_base() - sst.getBaseAddress(),
                    sizeof(sst.num_received_sst[0][0]) * num_nodes);
        }
    };
    sst->predicates.insert(receiver_pred, receiver_trig,
                           sst::PredicateType::RECURRENT);
    vector<uint32_t> indices;
    iota(indices.begin(), indices.end(), 0);
    multicast_group<multicast_sst> g(
            sst, indices, window_size);
    for(uint i = 0; i < num_messages; ++i) {
        volatile char* buf;
        while((buf = g.get_buffer(max_msg_size)) == NULL) {
        }
        clock_gettime(CLOCK_REALTIME, &send_time);
        send_times[i] = send_time.tv_sec * 1e9 + send_time.tv_nsec;
        g.send();
    }
    while(!done) {
    }

    for(uint i = 0; i < num_nodes; ++i) {
        stringstream ss;
        ss << "ml_" << num_nodes << "_" << i;
        ofstream fout;
        fout.open(ss.str());
        for(uint j = 0; j < num_messages; ++j) {
            fout << recv_times[i][j] << endl;
        }
        fout.close();
    }

    stringstream ss;
    ss << "ml_" << num_nodes;
    ofstream fout;
    fout.open(ss.str());
    for(uint i = 0; i < num_messages; ++i) {
        fout << send_times[i] << endl;
    }
    fout.close();

    for(uint i = 0; i < num_nodes; ++i) {
        if(i == node_id) {
            continue;
        }
        sync(i);
    }
}
