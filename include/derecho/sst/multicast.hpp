#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "sst.hpp"

//#define WATERMARK

namespace sst {
template <typename sstType>
class multicast_group {
    // number of messages for which get_buffer has been called
    long long int queued_num = -1;
    // current index of sent messages
    int32_t current_sent_index = -1;
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
    const uint32_t index_field_index;
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
    // std::vector<struct timespec> requested_send_times;
    // std::vector<std::pair<bool, struct timespec>> actual_send_msg_and_times;

    // std::vector<struct timespec> loop_times;
    // uint64_t loop_count;

    void initialize() {
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                sst->num_received_sst[i][j] = -1;
            }
            sst->index[i][index_field_index] = -1;
            for(uint j = 0; j < window_size; ++j) {
                sst->slots[i][slots_offset + max_msg_size * j] = 0;
            }
        }

        // requested_send_times = std::vector<struct timespec>(1000001, {0});
        // actual_send_msg_and_times = std::vector<std::pair<bool, struct timespec>>(1000001, {false, {0}});
        // loop_times = std::vector<struct timespec>(5000000, {0});
        // loop_count = 0;

#ifdef WATERMARK
        sender_thread = std::thread(&multicast_group::sender_function_watermark, this);
#else
        sender_thread = std::thread(&multicast_group::sender_function_opportunistic, this);
#endif
        sst->sync_with_members(row_indices);
    }

    void sender_function_opportunistic() {
        pthread_setname_np(pthread_self(), "send_looper");

        struct timespec last_time, cur_time;
        clock_gettime(CLOCK_REALTIME, &last_time);
        bool msg_sent;

        int32_t old_sent_index = -1;
        uint64_t ready_to_be_sent = 0;
        uint32_t my_sst_index = sst->get_local_index();
        uint32_t first_slot;

        while(!thread_shutdown) {
            // clock_gettime(CLOCK_REALTIME, &loop_times[loop_count]);
            // loop_count++;

            msg_sent = false;
            sst->index[my_sst_index][index_field_index] = current_sent_index;

            if(sst->index[my_sst_index][index_field_index] > old_sent_index) {
                ready_to_be_sent = sst->index[my_sst_index][index_field_index] - old_sent_index;
                first_slot = (old_sent_index+1) % window_size;

                //slots are contiguous
                //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 2 ][ 3 ].
                if(first_slot + ready_to_be_sent <= window_size) {
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * ready_to_be_sent);
                } else {
                    //slots are not contiguous
                    //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 4 ][ 1 ].
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * (window_size - first_slot));
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                            max_msg_size * (first_slot + ready_to_be_sent - window_size));
                }

                sst->put(sst->index, index_field_index);

                msg_sent = true;
                old_sent_index = sst->index[my_sst_index][index_field_index];
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

    void sender_function_watermark() {
        pthread_setname_np(pthread_self(), "send_looper");

        // struct timespec last_time, cur_time;
        // clock_gettime(CLOCK_REALTIME, &last_time);
        // bool msg_sent;

        int32_t old_sent_index = -1;
        uint64_t ready_to_be_send = 0;
        uint32_t my_sst_index = sst->get_local_index();
        uint32_t first_slot;

        uint64_t queued_now = 0, pending_msgs = 0, sender_loop_counter = 0;
        bool buffered_mode = false;
        const uint64_t higher_watermark = ((double)6 / 10) * window_size;
        const uint64_t lower_watermark = ((double)3 / 10) * window_size;
        const uint64_t max_loop_iterations_waiting = 10;
        const uint64_t batch_size = ((double)1 / 2) * window_size;

        while(!thread_shutdown) {
            // clock_gettime(CLOCK_REALTIME, &loop_times[loop_count]);
            // loop_count++;

            // msg_sent = false;
            pending_msgs = current_sent_index - old_sent_index;
            queued_now = queued_num - finished_multicasts_num;
            first_slot = (old_sent_index+1) % window_size;

            /* BUFFERED MODE */
            if(buffered_mode) {
                if(pending_msgs + queued_now < lower_watermark) {
                    buffered_mode = false;
                    continue;
                }
                if(pending_msgs == 0 || (pending_msgs < batch_size && sender_loop_counter < max_loop_iterations_waiting)) {
                    sender_loop_counter++;
                    continue;
                }

                //send batch_size messages (not more than that)
                ready_to_be_send = pending_msgs < batch_size ? pending_msgs : batch_size;
                sst->index[my_sst_index][index_field_index] += ready_to_be_send;

                //case 1: slots are contiguous
                //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 2 ][ 3 ].
                if(first_slot + ready_to_be_send <= window_size) {
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * ready_to_be_send);
                } else {
                    //slots are not contiguous
                    //E.g. [ 1 ][ 2 ][ 3 ][ 4 ] and I have to send [ 4 ][ 1 ].
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * (window_size - first_slot));
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                            max_msg_size * (first_slot + ready_to_be_send - window_size));
                }

                sst->put(sst->index, index_field_index);
                old_sent_index = sst->index[my_sst_index][index_field_index];
                sender_loop_counter = 0;
                // msg_sent = true;

                /* NON - BUFFERED MODE */
            } else {
                if(pending_msgs + queued_now > higher_watermark) {
                    buffered_mode = true;
                    continue;
                }

                if(pending_msgs > 0) {
                    sst->index[my_sst_index][index_field_index]++;
                    sst->put((char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(), max_msg_size);
                    sst->put(sst->index);
                    old_sent_index = sst->index[my_row][index_field_index];
                    // msg_sent = true;
                }
            }

            // if(msg_sent) {
            //     // update last time
            //     clock_gettime(CLOCK_REALTIME, &last_time);
            // } else {
            //     clock_gettime(CLOCK_REALTIME, &cur_time);
            //     // check if the system has been inactive for enough time to induce sleep
            //     double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
            //                                 + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            //     if(time_elapsed_in_ms > 1) {
            //         using namespace std::chrono_literals;
            //         std::this_thread::sleep_for(1ms);
            //     }
            // }
        }
    }

public:
    multicast_group(std::shared_ptr<sstType> sst,
                    std::vector<uint32_t> row_indices,
                    uint32_t window_size,
                    uint64_t max_msg_size,
                    std::vector<int> is_sender = {},
                    uint32_t num_received_offset = 0,
                    uint32_t slots_offset = 0,
                    int32_t index_field_index = 0)
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
              index_field_index(index_field_index),
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
        // clock_gettime(CLOCK_REALTIME, &requested_send_times[current_sent_index]);
    }

    ~multicast_group() {
        thread_shutdown = true;
        sender_thread.join();
    }

    void debug_print() {
        /* This measures the time between a send issued and that send done 
     *  plus the number of messages batched each time
     */
        // std::ofstream fbatches("batches");
        // std::ofstream fdelays("delays");
        // uint64_t elapsed_time;
        // int64_t last_sent = 0;
        // for(uint64_t i = 1; i <= 1000000; i++) {
        //     if(actual_send_msg_and_times[i].first) {
        //         fbatches << i - last_sent << std::endl;
        //         for(uint64_t j = last_sent + 1; j <= i; j++) {
        //             elapsed_time = (actual_send_msg_and_times[i].second.tv_sec - requested_send_times[j].tv_sec)  * (uint64_t)1e9
        //                 + (actual_send_msg_and_times[i].second.tv_nsec - requested_send_times[j].tv_nsec);

        //             fdelays << elapsed_time << std::endl;
        //         }
        //         last_sent = i;
        //     }
        // }

        /* This measures the moment of time each message was actually  sent, starting from  
     * the first request to issue a message
     */
        // std::ofstream ftimes("send_times_batching");
        // uint64_t elapsed_time;
        // struct timespec first_time = requested_send_times[1];
        // int64_t last_sent = 0;
        // for(uint64_t i = 1; i <= 1000000; i++) {
        //     if(actual_send_msg_and_times[i].first) {
        //         elapsed_time = (actual_send_msg_and_times[i].second.tv_sec - first_time.tv_sec)  * (uint64_t)1e9
        //                 + (actual_send_msg_and_times[i].second.tv_nsec - first_time.tv_nsec);
        //         for(uint64_t j = last_sent + 1; j <= i; j++) {
        //             ftimes << j << " " << elapsed_time << std::endl;
        //         }
        //         last_sent = i;
        //     }
        // }

        // /* This measures how much it takes a single loop of the sender thread to send a msg */
        // std::ofstream floop("send_loop_times");
        // auto start_time = loop_times[0];
        // for(uint64_t i = 1; i < loop_count; i++) {
        //     floop << (loop_times[i].tv_sec - start_time.tv_sec)*(uint64_t)1e09 + (loop_times[i].tv_nsec - start_time.tv_nsec) << std::endl;
        //     start_time = loop_times[i];
        // }
    }
};
}  // namespace sst
