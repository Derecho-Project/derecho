/**
 * @file sst_multicast_throughput.cpp
 *
 * This test sets up an SST Multicast group independently of Derecho and tests
 * its throughput. The delivery logic is manually copied and pasted from
 * multicast_group.cpp, where it is normally interleaved with the RDMC delivery
 * logic, so care must be taken to ensure the multicast tested in this test is
 * reflective of the one actually used in Derecho.
 */

#include <chrono>
#include <derecho/sst/multicast_sst.hpp>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "aggregate_bandwidth.hpp"
#include "initialize.hpp"
#include "log_results.hpp"

volatile bool done = false;

struct exp_results {
    uint32_t num_nodes;
    int num_senders_selector;
    uint max_msg_size;
    double sum_message_rate;
    void print(std::ofstream& fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << max_msg_size << " " << sum_message_rate << std::endl;
    }
};

#ifndef NDEBUG
#define DEBUG_MSG(str)                 \
    do {                               \
        std::cout << str << std::endl; \
    } while(false)
#else
#define DEBUG_MSG(str) \
    do {               \
    } while(false)
#endif

int main(int argc, char* argv[]) {
    constexpr uint max_msg_size = 1;
    const unsigned int num_messages = 1000000;
    if(argc < 3) {
        std::cout << "Insufficient number of command line arguments" << std::endl;
        std::cout << "Usage: " << argv[0] << " <num_nodes> <num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender)>" << std::endl;
        std::cout << "Thank you" << std::endl;
        std::exit(1);
    }
    uint32_t num_nodes = atoi(argv[1]);
    int num_senders_selector = atoi(argv[2]);
    uint32_t window_size = derecho::getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE);
    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);

    // initialize the rdma resources
#ifdef USE_VERBS_API
    sst::verbs_initialize(ip_addrs_and_ports, {}, node_id);
#else
    sst::lf_initialize(ip_addrs_and_ports, {}, node_id);
#endif

    std::vector<uint32_t> members;
    for(const auto& p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    uint32_t num_senders = num_nodes, row_offset = 0;
    if(num_senders_selector == 0) {
    } else if(num_senders_selector == 1) {
        num_senders = num_nodes / 2;
        row_offset = (num_nodes + 1) / 2;
    } else {
        num_senders = 1;
        row_offset = num_nodes - 1;
    }

    std::shared_ptr<sst::multicast_sst> sst = std::make_shared<sst::multicast_sst>(
            sst::SSTParams(members, node_id),
            window_size,
            num_senders, max_msg_size);

    uint32_t node_rank = sst->get_local_index();

    // Set up the failure-checking thread, in the same way Derecho would
    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            if(sst) {
                sst->put_with_completion((char*)std::addressof(sst->heartbeat[0]) - sst->getBaseAddress(), sizeof(bool));
            }
        }
    };

    std::thread failures_thread = std::thread(check_failures_loop);

    // Create a "receive handler" that just counts received messages, instead of delivering them to an application
    std::vector<bool> completed(num_senders, false);
    uint num_finished = 0;
    auto sst_receive_handler = [&num_finished, num_senders_selector, &num_nodes, &num_messages, &completed](
                                       uint32_t sender_rank, uint64_t index,
                                       volatile char* msg, uint32_t size) {
        if(index == num_messages - 1) {
            completed[sender_rank] = true;
            num_finished++;
        }
        if(num_finished == num_nodes || (num_senders_selector == 1 && num_finished == num_nodes / 2) || (num_senders_selector == 2 && num_finished == 1)) {
            done = true;
        }
    };

    // Set up a predicate and trigger for the SST Multicast receiver function, similar to what MulticastGroup would do
    auto receiver_pred = [&](const sst::multicast_sst& sst) {
        return true;
    };
    std::vector<int64_t> last_max_num_received(num_senders, -1);
    auto receiver_trig = [&completed, last_max_num_received, window_size, num_nodes, node_rank, sst_receive_handler,
                          row_offset, num_senders](sst::multicast_sst& sst) mutable {
        while(true) {
            for(uint j = 0; j < num_senders; ++j) {
                auto num_received = sst.num_received_sst[node_rank][j] + 1;
                uint32_t slot = num_received % window_size;
                if((int64_t&)sst.slots[row_offset + j][(max_msg_size + 2 * sizeof(uint64_t)) * (slot + 1) - sizeof(uint64_t)] == (num_received / window_size + 1)) {
                    sst_receive_handler(j, num_received,
                                        &sst.slots[row_offset + j][(max_msg_size + 2 * sizeof(uint64_t)) * slot],
                                        sst.slots[row_offset + j][(max_msg_size + 2 * sizeof(uint64_t)) * (slot + 1) - 2 * sizeof(uint64_t)]);
                    sst.num_received_sst[node_rank][j]++;
                }
            }
            bool time_to_push = true;
            for(uint j = 0; completed.size() > 0 && j < num_senders; ++j) {
                if(completed[j]) {
                    continue;
                }
                if(sst.num_received_sst[node_rank][j] - last_max_num_received[j] <= window_size / 2) {
                    time_to_push = false;
                }
            }
            if(time_to_push) {
                break;
            }
            if(completed.size() > 0) {
                break;
            }
        }
        sst.put(sst.num_received_sst.get_base() - sst.getBaseAddress(),
                sizeof(sst.num_received_sst[0][0]) * num_senders);
        for(uint j = 0; j < num_senders; ++j) {
            last_max_num_received[j] = sst.num_received_sst[node_rank][j];
        }
    };
    // insert the predicate later

    // Configure the senders based on the user's input
    std::vector<uint32_t> indices(num_nodes);
    std::iota(indices.begin(), indices.end(), 0);
    std::vector<int> is_sender(num_nodes, 1);
    if(num_senders_selector == 0) {
    } else if(num_senders_selector == 1) {
        for(uint i = 0; i <= (num_nodes - 1) / 2; ++i) {
            is_sender[i] = 0;
        }
    } else {
        for(uint i = 0; i < num_nodes - 1; ++i) {
            is_sender[i] = 0;
        }
    }

    //Construct an SST Multicast group
    sst::multicast_group<sst::multicast_sst> group(sst, indices, window_size, max_msg_size, is_sender);

    DEBUG_MSG("Group created");

    // now insert the predicate
    sst->predicates.insert(receiver_pred, receiver_trig,
                           sst::PredicateType::RECURRENT);
    // uint count = 0;
    struct timespec start_time, end_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    if(node_rank == num_nodes - 1 || num_senders_selector == 0
       || (node_rank > (num_nodes - 1) / 2 && num_senders_selector == 1)) {
        for(uint i = 0; i < num_messages; ++i) {
            // Get the send buffer
            volatile char* buf;
            while((buf = group.get_buffer(max_msg_size)) == NULL) {
                // ++count;
            }
            // Send a nonsense message
            // for(uint i = 0; i < size; ++i) {
            //     buf[i] = 'a' + rand() % 26;
            // }
            group.send(group.commit_send());
        }
    }
    // cout << "Done sending" << endl;
    while(!done) {
    }
    // end timer
    clock_gettime(CLOCK_REALTIME, &end_time);
    double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec));
    double message_rate = (num_messages * 1e9) / my_time;
    ;
    if(num_senders_selector == 0) {
        message_rate *= num_nodes;
    } else if(num_senders_selector == 1) {
        message_rate *= num_nodes / 2;
    }

    double sum_message_rate = aggregate_bandwidth(members, node_rank, message_rate);
    log_results(exp_results{num_nodes, num_senders_selector, max_msg_size, sum_message_rate},
                "data_multicast");
    sst->sync_with_members();
}
