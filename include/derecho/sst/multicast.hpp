#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include "sst.hpp"

namespace sst {
template <typename sstType>
class multicast_group {
    // number of messages for which get_buffer has been called
    long long int queued_num = -1;
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
    const uint32_t index_offset;
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

    void initialize() {
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                sst->num_received_sst[i][j] = -1;
            }
            sst->index[i][index_offset] = -1;
            for(uint j = 0; j < window_size; ++j) {
                sst->slots[i][slots_offset + max_msg_size * j] = 0;
                (uint64_t&)sst->slots[i][slots_offset + (max_msg_size * (j + 1)) - sizeof(uint64_t)] = 0;
            }
        }
        sst->sync_with_members(row_indices);
    }

public:
    multicast_group(std::shared_ptr<sstType> sst,
                    std::vector<uint32_t> row_indices,
                    uint32_t window_size,
                    uint64_t max_msg_size,
                    std::vector<int> is_sender = {},
                    uint32_t num_received_offset = 0,
                    uint32_t slots_offset = 0,
                    int32_t index_offset = 0)
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
              index_offset(index_offset),
              num_received_offset(num_received_offset),
              slots_offset(slots_offset),
              num_members(row_indices.size()),
              window_size(window_size),
              max_msg_size(max_msg_size + sizeof(uint64_t)) {
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

    // In ORDERED MODE, we should hold the lock on msg_state_mtx
    uint32_t commit_send(uint32_t ready_to_be_sent = 1) {
        return sst->index[my_row][index_offset] += ready_to_be_sent;
    }

    // This function invocation should be always preceded by the commit_send,
    // that returns the first parameter (committed index) to be used here.
    void send(uint32_t committed_index, uint32_t ready_to_be_sent = 1, uint32_t num_nulls_queued = 0, int32_t first_null_index = -1, size_t header_size = 0) {
        uint32_t old_index = committed_index - ready_to_be_sent;
        uint32_t first_slot = (old_index + 1) % window_size;

        if(num_nulls_queued == 0) {  // NO NULLS: Regular send
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
        } else {  // NULLS: send only the header of the first null, skip the others
            assert(first_null_index >= 0);
            // Here we send three batches of messages:
            // 1) application messages
            // 2) nulls, of which we actually send only the header of the first
            // 3) application messages (optional)
            // Each of the batches can wrap around the ring buffer, so we should consider all these cases.

            uint32_t first_message_batch_size = first_null_index - 1 - old_index;
            uint32_t last_message_batch_size = ready_to_be_sent - num_nulls_queued - first_message_batch_size;
                       
            uint32_t first_null_slot = first_null_index % window_size;
            uint32_t last_batch_first_slot = (first_null_index + num_nulls_queued) % window_size;
            uint32_t last_batch_last_slot = committed_index % window_size;

            if(first_null_slot >= first_slot) {  // (1) and (2) have all contiguous messages
                //Send (1) and (2)
                sst->put(
                        (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                        (max_msg_size * first_message_batch_size) + header_size);
                if(last_message_batch_size > 0) {
                    if(last_batch_first_slot <= last_batch_last_slot) {  // (3) has contiguous messages
                        sst->put(
                                (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * last_batch_first_slot]) - sst->getBaseAddress(),
                                max_msg_size * last_message_batch_size);
                    } else {  // (3) has wrapping messages
                        sst->put(
                                (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * last_batch_first_slot]) - sst->getBaseAddress(),
                                max_msg_size * (window_size - last_batch_first_slot));
                        sst->put(
                                (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                                max_msg_size * (last_batch_first_slot + last_message_batch_size - window_size));
                    }
                }
            } else {                        // (1) and (2) wrap
                if(first_null_slot == 0) {  // (1) has contiguous messages, (2) wraps
                    //Send (1)
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * first_message_batch_size);
                    //Send (2)
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                            header_size);
                } else {  // (1) wraps
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * first_slot]) - sst->getBaseAddress(),
                            max_msg_size * (window_size - first_slot));
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset]) - sst->getBaseAddress(),
                            (max_msg_size * (first_slot + first_message_batch_size - window_size)) + header_size);
                }
                if(last_message_batch_size > 0) {
                    // (3) has contiguous messages
                    sst->put(
                            (char*)std::addressof(sst->slots[0][slots_offset + max_msg_size * last_batch_first_slot]) - sst->getBaseAddress(),
                            max_msg_size * last_message_batch_size);
                }
            }
        }
        // Push the index
        sst->put(sst->index, index_offset);
    }

    void debug_print() {
        using std::cout;
        using std::endl;
        cout << "Printing slots::next_seq" << endl;
        for(auto i : row_indices) {
            for(uint j = 0; j < window_size; ++j) {
                cout << (uint64_t&)sst->slots[i][slots_offset + (max_msg_size * (j + 1)) - sizeof(uint64_t)] << " ";
            }
            cout << endl;
        }
        cout << "Printing num_received_sst" << endl;
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                cout << sst->num_received_sst[i][j] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
};
}  // namespace sst
