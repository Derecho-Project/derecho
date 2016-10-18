#include <cassert>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

#include "../sst.h"
#include "../tcp.h"

template <uint32_t max_msg_size>
struct Message {
    char buf[max_msg_size];
    uint32_t size;
    uint64_t next_seq;
};

template <uint32_t window_size, uint32_t max_msg_size, uint32_t max_members>
struct Row {
    Message<max_msg_size> slots[window_size];
    uint64_t num_received[max_members];
};

typedef std::function<void(uint32_t, uint64_t, volatile char*, uint32_t size)>
    receiver_callback_t;

template <uint32_t window_size, uint32_t max_msg_size, uint32_t max_members>
class group {
    // number of messages for which get_buffer has been called
    uint64_t num_queued = 0;
    // number of messages for which RDMA write is complete
    uint64_t num_sent = 0;
    // the number of messages acknowledged by all the nodes
    uint64_t num_multicasts_finished = 0;
    // rank of the node in the sst
    uint32_t my_rank;
    // only one send at a time
    std::mutex msg_send_mutex;

    // number of members
    uint32_t num_members;

    receiver_callback_t receiver_callback;

    using SST_type = sst::SST<Row<window_size, max_msg_size, max_members>>;

    // SST
    std::unique_ptr<SST_type> multicastSST;

    void initialize() {
        for(uint i = 0; i < num_members; ++i) {
            for(uint j = 0; j < num_members; ++j) {
                (*multicastSST)[i].num_received[j] = 0;
            }
            for(uint j = 0; j < window_size; ++j) {
                (*multicastSST)[i].slots[j].buf[0] = 0;
                (*multicastSST)[i].slots[j].next_seq = 0;
            }
        }
        multicastSST->sync_with_members();
        std::cout << "Initialization complete" << std::endl;
    }

    void register_predicates() {
        auto receiver_pred = [this](const SST_type& sst) { return true; };
        auto receiver_trig = [this](SST_type& sst) {
            for(uint i = 0; i < window_size / 2; ++i) {
                for(uint j = 0; j < num_members; ++j) {
                    uint32_t slot = sst[my_rank].num_received[j] % window_size;
                    if(sst[j].slots[slot].next_seq ==
                       (sst[my_rank].num_received[j]) / window_size + 1) {
                        this->receiver_callback(j, sst[my_rank].num_received[j],
                                                sst[j].slots[slot].buf,
                                                sst[j].slots[slot].size);
                        sst[my_rank].num_received[j]++;
                    }
                }
            }
            sst.put((char*)std::addressof(sst[0].num_received[0]) -
                        (char*)std::addressof(sst[0]),
                    sizeof(sst[0].num_received[0]) * num_members);
        };
        multicastSST->predicates.insert(receiver_pred, receiver_trig,
                                        sst::PredicateType::RECURRENT);
    }

public:
    group(std::vector<uint> members, uint32_t my_id,
          receiver_callback_t receiver_callback)
        : receiver_callback(receiver_callback) {
        num_members = members.size();
        assert(num_members <= max_members);
        for(uint32_t i = 0; i < num_members; ++i) {
            if(members[i] == my_id) {
                my_rank = i;
                break;
            }
        }
        multicastSST = std::make_unique<SST_type>(members, my_rank);
        initialize();
        register_predicates();
    }

    volatile char* get_buffer(uint32_t msg_size) {
        std::lock_guard<std::mutex> lock(msg_send_mutex);
        assert(msg_size <= max_msg_size);
        if(num_queued - num_multicasts_finished < window_size) {
            uint32_t slot = num_queued % window_size;
            num_queued++;
            // set size appropriately
            (*multicastSST)[my_rank].slots[slot].size = msg_size;
            return (*multicastSST)[my_rank].slots[slot].buf;
        } else {
            uint64_t min_multicast_num =
                (*multicastSST)[0].num_received[my_rank];
            for(uint32_t i = 1; i < num_members; ++i) {
                if((*multicastSST)[i].num_received[my_rank] <
                   min_multicast_num) {
                    min_multicast_num =
                        (*multicastSST)[i].num_received[my_rank];
                }
            }
            num_multicasts_finished = min_multicast_num;
        }
        return nullptr;
    }

    void send() {
        uint32_t slot = num_sent % window_size;
        num_sent++;
        (*multicastSST)[my_rank].slots[slot].next_seq++;
        multicastSST->put(
            (char*)std::addressof((*multicastSST)[0].slots[slot]) -
                (char*)std::addressof((*multicastSST)[0]),
            sizeof(Message<max_msg_size>));
    }
};
