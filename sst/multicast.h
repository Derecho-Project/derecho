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

#include "sst/multicast_msg.h"
#include "sst/sst.h"

namespace sst {
template <typename sstType>
class multicast_group {
    // number of messages for which get_buffer has been called
    long long int queued_num = -1;
    // number of messages for which RDMA write is complete
    uint64_t num_sent = 0;
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

    std::thread timeout_thread;

    void initialize() {
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                sst->num_received_sst[i][j] = -1;
            }
            for(uint j = slots_offset; j < slots_offset + window_size; ++j) {
                sst->slots[i][j].buf[0] = 0;
                sst->slots[i][j].next_seq = 0;
            }
        }
        sst->sync_with_members(row_indices);
        std::cout << "Initialization complete" << std::endl;
    }

public:
    multicast_group(std::shared_ptr<sstType> sst,
                    std::vector<uint32_t> row_indices,
                    uint32_t window_size,
                    std::vector<int> is_sender = {},
                    uint32_t num_received_offset = 0,
                    uint32_t slots_offset = 0)
            : my_row(sst->get_local_index()),
              sst(sst),
              row_indices(row_indices),
              is_sender([is_sender, row_indices]() {
                  if(is_sender.size() == 0) {
		    return std::vector<int32_t>(row_indices.size(), 1);
                  }
		  else {
		    return is_sender;
                  }
              }()),
              num_received_offset(num_received_offset),
              slots_offset(slots_offset),
              num_members(row_indices.size()),
              window_size(window_size) {
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
            if(is_sender[i]) {
                j++;
            }
        }
        num_senders = j;

        if(!is_sender[my_member_index]) {
            my_sender_index = -1;
        }
        initialize();
    }

    volatile char* get_buffer(uint32_t msg_size) {
        assert(my_sender_index >= 0);
        std::lock_guard<std::mutex> lock(msg_send_mutex);
        assert(msg_size <= max_msg_size);
        while(true) {
            if(queued_num - finished_multicasts_num < window_size) {
                queued_num++;
                uint32_t slot = queued_num % window_size;
                // std::cout << "queued_num " << queued_num << std::endl;
                // std::cout << "Giving slot " << slot << std::endl;
                // set size appropriately
                sst->slots[my_row][slots_offset + slot].size = msg_size;
                return sst->slots[my_row][slots_offset + slot].buf;
            } else {
	      long long int min_multicast_num = sst->num_received_sst[my_row][num_received_offset + my_sender_index];
                for(auto i : row_indices) {
		  if(sst->num_received_sst[i][num_received_offset + my_sender_index] < min_multicast_num) {
		    min_multicast_num = sst->num_received_sst[i][num_received_offset + my_sender_index];
                    }
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
        // std::cout << "In send: " << std::endl;
        uint32_t slot = num_sent % window_size;
        // std::cout << "slot = " << slot << std::endl;
        // std::cout << "slots_offset = " << slots_offset << std::endl;
        num_sent++;
        sst->slots[my_row][slots_offset + slot].next_seq++;
        sst->put(
                (char*)std::addressof(sst->slots[0][slots_offset + slot]) - sst->getBaseAddress(),
                sizeof(Message));
    }

    void debug_print() {
        using std::cout;
        using std::endl;
        for(auto i : row_indices) {
            cout << "Printing slots::next_seq" << endl;
            for(uint j = slots_offset; j < slots_offset + window_size; ++j) {
                cout << sst->slots[i][j].next_seq << " ";
            }
            cout << endl;
            cout << "Printing num_received_sst" << endl;
            for(uint j = num_received_offset; j < num_received_offset + num_senders; ++j) {
                cout << sst->num_received_sst[i][j] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
};
}
