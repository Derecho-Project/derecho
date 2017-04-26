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

#include "sst/sst.h"
#include "sst/multicast_msg.h"

namespace sst {
template <typename sstType>
class multicast_group {
    // number of messages for which get_buffer has been called
    uint64_t num_queued = 0;
    // number of messages for which RDMA write is complete
    uint64_t num_sent = 0;
    // the number of messages acknowledged by all the nodes
    uint64_t num_multicasts_finished = 0;
    // rank of the node in the sst
    const uint32_t my_rank;
    // only one send at a time
    std::mutex msg_send_mutex;

    // SST
    std::shared_ptr<sstType> sst;

    // rows indices
    const std::vector<uint32_t> row_indices;

    // start indexes for sst fields it uses
    // need to know the range it can operate on
    const uint32_t num_received_offset;
    const uint32_t slots_offset;

    // number of members
    const uint32_t num_members;
    // window size
    const uint32_t window_size;

    std::thread timeout_thread;

    void initialize() {
        for(auto i : row_indices) {
            for(uint j = num_received_offset; j < num_received_offset + num_members; ++j) {
                sst->num_received_sst[i][j] = 0;
            }
            for(uint j = slots_offset; j < slots_offset + window_size; ++j) {
                sst->slots[i][j].buf[0] = 0;
                sst->slots[i][j].next_seq = 0;
            }
        }
        sst->sync_with_members();
        std::cout << "Initialization complete" << std::endl;
    }

public:
    multicast_group(std::shared_ptr<sstType> sst,
		    std::vector<uint32_t> row_indices,
		    uint32_t num_received_offset,
		    uint32_t slots_offset,
                    uint32_t window_size)
            : my_rank(sst->get_local_index()),
              sst(sst), 
	      row_indices(row_indices),
	      num_received_offset(num_received_offset),
	      slots_offset(slots_offset),
              num_members(row_indices.size()),
              window_size(window_size) {
        initialize();
    }

    volatile char* get_buffer(uint32_t msg_size) {
        std::lock_guard<std::mutex> lock(msg_send_mutex);
        assert(msg_size <= max_msg_size);
        while(true) {
            if(num_queued - num_multicasts_finished < window_size) {
                uint32_t slot = num_queued % window_size;
                num_queued++;
                // set size appropriately
                sst->slots[my_rank][slots_offset + slot].size = msg_size;
                return sst->slots[my_rank][slots_offset + slot].buf;
            } else {
                uint64_t min_multicast_num = sst->num_received_sst[row_indices[0]][num_received_offset + my_rank];
                for(auto i : row_indices) {
                    if(sst->num_received_sst[i][num_received_offset + my_rank] < min_multicast_num) {
                        min_multicast_num = sst->num_received_sst[i][num_received_offset + my_rank];
                    }
                }
                if(num_multicasts_finished == min_multicast_num) {
                    return nullptr;
                } else {
                    num_multicasts_finished = min_multicast_num;
                }
            }
        }
    }

    void send() {
        uint32_t slot = num_sent % window_size;
        num_sent++;
        sst->slots[my_rank][slots_offset + slot].next_seq++;
        sst->put(
                (char*)std::addressof(sst->slots[0][slots_offset + slot]) - sst->getBaseAddress(),
                sizeof(Message));
    }

    void debug_print() {
        using namespace std;
        cout << "Printing slots::next_seq" << endl;
        for(auto i : row_indices) {
            for(uint j = slots_offset; j < slots_offset + window_size; ++j) {
                cout << sst->slots[i][j].next_seq << " ";
            }
            cout << endl;
	    for(uint j = num_received_offset; j < num_received_offset + num_members; ++j) {
                cout << sst->num_received_sst[i][j] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
};
}
