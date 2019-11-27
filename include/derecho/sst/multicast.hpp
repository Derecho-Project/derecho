#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "sst.hpp"

namespace sst {
template <typename sstType>
class multicast_group {
    // number of messages for which get_buffer has been called
    long long int queued_num = -1;
    // current index of sent messages
    uint64_t current_sent_index = 0;
    // the number of messages acknowledged by all the nodes
    long long int finished_multicasts_num = -1;
    // row of the node in the sst
    const uint32_t my_row;
    // rank of the node in the members list
    uint32_t my_member_index;
    // rank of node in the senders list
    int32_t my_sender_index;
    // only one send at a time
    std::mutex msg_send_mutex;

    // SST
    std::shared_ptr<sstType> sst;

    // rows indices
    const std::vector<uint32_t> row_indices;
    const std::vector<int> is_sender;

    // start indexes for sst fields it uses
    // need to know the range it can operate on
    const uint32_t num_received_offset;
    const uint32_t slots_offset;

    // number of members
    const uint32_t num_members;
    // number of senders
    uint32_t num_senders;
    // window size
    const uint32_t window_size;
    // maximum size that the SST can send
    const uint64_t max_msg_size;

    std::thread timeout_thread;

    //sender thread
    std::atomic<bool> thread_shutdown;
    std::thread sender_thread;

    //statistic - for multicast_throughput.cpp
    std::vector<struct timespec> requested_send_times;
    std::vector<std::pair<bool, struct timespec>> actual_send_msg_and_times;

    void initialize() {
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                sst->num_received_sst[i][j] = -1;
            }
            sst->index[i] = 0;
            for(uint j = 0; j < window_size; ++j) {
                sst->slots[i][slots_offset + max_msg_size * j] = 0;
            }
        }
        sender_thread = std::thread(&multicast_group::sender_function, this);

        requested_send_times = std::vector<struct timespec>(1000000, {0});
        actual_send_msg_and_times = std::vector<std::pair<bool, struct timespec>>(1000000, {false, {0}});

        sst->sync_with_members(row_indices);
    }

    void sender_function() {
        pthread_setname_np(pthread_self(), "multicast_group_sender");

        struct timespec last_time, cur_time;
        clock_gettime(CLOCK_REALTIME, &last_time);
        bool msg_sent;

        uint64_t old_sent_index = 0;
        uint64_t num_to_be_sent = 0;
        uint32_t my_index = sst->get_local_index();
        uint32_t first_slot;

        while(!thread_shutdown) {
            msg_sent = false;
            sst->index[my_index] = current_sent_index;

            if(sst->index[my_index] > old_sent_index) {
                num_to_be_sent = sst->index[my_index] - old_sent_index;
                first_slot = old_sent_index % window_size;

                //slots are contiguous
                //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 2 ][ 3 ].
                if(first_slot + num_to_be_sent <= window_size) {
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * num_to_be_sent);
                } else {
                    //slots are not contiguous
                    //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 4 ][ 1 ].
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * (window_size - first_slot));
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                            max_msg_size * (first_slot + num_to_be_sent - window_size));
                }

                sst->put(sst->index);

                // //DEBUG
                // std::cout << "Sent " << num_to_be_sent << " message(s) together" << std::endl;
                actual_send_msg_and_times[sst->index[my_index]].first = true;
                clock_gettime(CLOCK_REALTIME, &actual_send_msg_and_times[sst->index[my_index]].second);

                msg_sent = true;
                old_sent_index = sst->index[my_row];
            }

            if(msg_sent) {
                // update last time
                clock_gettime(CLOCK_REALTIME, &last_time);
            } else {
                clock_gettime(CLOCK_REALTIME, &cur_time);
                // check if the system has been inactive for enough time to induce sleep
                double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                            + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
                if(time_elapsed_in_ms > 1) {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(1ms);
                }
            }
        }
    }

public:
    multicast_group(std::shared_ptr<sstType> sst,
                    std::vector<uint32_t> row_indices,
                    uint32_t window_size,
                    uint64_t max_msg_size,
                    std::vector<int> is_sender = {},
                    uint32_t num_received_offset = 0,
                    uint32_t slots_offset = 0)
            : my_row(sst->get_local_index()),
              sst(sst),
              row_indices(row_indices),
              is_sender([is_sender, row_indices]() {
                  if(is_sender.size() == 0) {
                      return std::vector<int32_t>(row_indices.size(), 1);
                  } else {
                      return is_sender;
                  }
              }()),
              num_received_offset(num_received_offset),
              slots_offset(slots_offset),
              num_members(row_indices.size()),
              window_size(window_size),
              max_msg_size(max_msg_size + 1 * sizeof(uint64_t)),
              thread_shutdown(false) {
        // find my_member_index
        for(uint i = 0; i < num_members; ++i) {
            if(row_indices[i] == my_row) {
                my_member_index = i;
            }
        }
        int j = 0;
        for(uint i = 0; i < num_members; ++i) {
            if(i == my_member_index) {
                my_sender_index = j;
            }
            if(this->is_sender[i]) {
                j++;
            }
        }
        num_senders = j;

        if(!this->is_sender[my_member_index]) {
            my_sender_index = -1;
        }

        initialize();
    }

    volatile char* get_buffer(uint64_t msg_size) {
        assert(my_sender_index >= 0);
        std::lock_guard<std::mutex> lock(msg_send_mutex);
        assert(msg_size <= max_msg_size);
        while(true) {
            if(queued_num - finished_multicasts_num < window_size) {
                queued_num++;
                uint32_t slot = queued_num % window_size;
                // set size appropriately
                (uint64_t&)sst->slots[my_row][slots_offset + (max_msg_size * (slot + 1)) - sizeof(uint64_t)] = msg_size;
                return &sst->slots[my_row][slots_offset + (max_msg_size * slot)];
            } else {
                long long int min_multicast_num = sst->num_received_sst[my_row][num_received_offset + my_sender_index];
                for(auto i : row_indices) {
                    long long int num_received_sst_copy = sst->num_received_sst[i][num_received_offset + my_sender_index];
                    min_multicast_num = std::min(min_multicast_num, num_received_sst_copy);
                }
                if(finished_multicasts_num == min_multicast_num) {
                    return nullptr;
                } else {
                    finished_multicasts_num = min_multicast_num;
                }
            }
        }
    }

    void send() {
        current_sent_index++;
        clock_gettime(CLOCK_REALTIME, &requested_send_times[current_sent_index]);
    }

    ~multicast_group() {
        thread_shutdown = true;
        sender_thread.join();
    }

    void debug_print() {
    //     using std::cout;
    //     using std::endl;
    //     cout << "Printing slots::next_seq" << endl;
    //     for(auto i : row_indices) {
    //         for(uint j = 0; j < window_size; ++j) {
    //             cout << (uint64_t&)sst->slots[i][slots_offset + (max_msg_size * (j + 1))] << " ";
    //         }
    //         cout << endl;
    //     }
    //     cout << "Printing num_received_sst" << endl;
    //     for(auto i : row_indices) {
    //         for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
    //             cout << sst->num_received_sst[i][j] << " ";
    //         }
    //         cout << endl;
    //     }
    //     cout << endl;

            ofstream fbatches("batches");
            ofstream fdelays("delays");
            uint64_t request_time, actual_time;
            int64_t last_sent = -1;
            for(uint64_t i = 0; i < 1000000; i++) {
                if(actual_send_msg_and_times[i].first) {
                    fbatches << i - last_sent << std::endl;
                    for(uint64_t j = last_sent + 1; j <= i; j++) {
                        request_time = actual_send_msg_and_times[j].second.tv_sec * (uint64_t)1e9 + actual_send_msg_and_times[j].second.tv_nsec;
                        actual_time = actual_send_msg_and_times[i].second.tv_sec * (uint64_t)1e9 + actual_send_msg_and_times[i].second.tv_nsec;
                        fdelays << actual_time - request_time << std::endl;
                    }
                    last_sent = i;
                }
            }
    }
};
}  // namespace sst
