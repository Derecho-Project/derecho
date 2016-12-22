#include <cassert>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

#include "sst/sst.h"

template <uint32_t max_msg_size>
struct Message {
    char buf[max_msg_size];
    uint32_t size;
    uint64_t next_seq;
};

template <uint32_t max_msg_size>
class multicastSST : public sst::SST<multicastSST<max_msg_size>> {
public:
    multicastSST(const std::vector<uint32_t>& _members, uint32_t my_id, uint32_t window_size)
            : sst::SST<multicastSST<max_msg_size>>(this, sst::SSTParams{_members, my_id}),
              slots(window_size),
              num_received(_members.size()) {
        this->SSTInit(slots, num_received);
    }
    sst::SSTFieldVector<Message<max_msg_size>> slots;
    sst::SSTFieldVector<uint64_t> num_received;
};

template <uint32_t window_size, uint32_t max_msg_size, uint32_t max_members>
struct Row {
    Message<max_msg_size> slots[window_size];
    uint64_t num_received[max_members];
};

typedef std::function<void(uint32_t, uint64_t, volatile char*, uint32_t size)>
    receiver_callback_t;

template <uint32_t max_msg_size>
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
    // window size
    uint32_t window_size;

    receiver_callback_t receiver_callback;

    // SST
    multicastSST<max_msg_size> sst;

    void initialize() {
        for(uint i = 0; i < num_members; ++i) {
            for(uint j = 0; j < num_members; ++j) {
                sst.num_received[i][j] = 0;
            }
            for(uint j = 0; j < window_size; ++j) {
                sst.slots[i][j].buf[0] = 0;
                sst.slots[i][j].next_seq = 0;
            }
        }
        sst.sync_with_members();
        std::cout << "Initialization complete" << std::endl;
    }

    void register_predicates() {
        auto receiver_pred = [this](const multicastSST<max_msg_size>& sst) {
	    for(uint i = 0; i < window_size / 2; ++i) {
                for(uint j = 0; j < num_members; ++j) {
                    uint32_t slot = sst.num_received[my_rank][j] % window_size;
                    if(sst.slots[j][slot].next_seq ==
                       (sst.num_received[my_rank][j]) / window_size + 1) {
		      return true;
                    }
                }
            }
	  return false;
        };
        auto receiver_trig = [this](multicastSST<max_msg_size>& sst) {
            for(uint i = 0; i < window_size / 2; ++i) {
                for(uint j = 0; j < num_members; ++j) {
                    uint32_t slot = sst.num_received[my_rank][j] % window_size;
                    if(sst.slots[j][slot].next_seq ==
                       (sst.num_received[my_rank][j]) / window_size + 1) {
                        this->receiver_callback(j, sst.num_received[my_rank][j],
                                                sst.slots[j][slot].buf,
                                                sst.slots[j][slot].size);
                        sst.num_received[my_rank][j]++;
                    }
                }
            }
            sst.put((char*)std::addressof(sst.num_received[0][0]) -
                        sst.getBaseAddress(),
                    sizeof(sst.num_received[0][0]) * num_members);
        };
        sst.predicates.insert(receiver_pred, receiver_trig,
                              sst::PredicateType::RECURRENT);
    }

public:
    group(std::vector<uint> members, uint32_t my_id,
          uint32_t window_size, receiver_callback_t receiver_callback)
            : num_members(members.size()),
              window_size(window_size),
              receiver_callback(receiver_callback),
              sst(members, my_id, window_size) {
        for(uint32_t i = 0; i < num_members; ++i) {
            if(members[i] == my_id) {
                my_rank = i;
                break;
            }
        }
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
            sst.slots[my_rank][slot].size = msg_size;
            return sst.slots[my_rank][slot].buf;
        } else {
            uint64_t min_multicast_num =
                sst.num_received[0][my_rank];
            for(uint32_t i = 1; i < num_members; ++i) {
                if(sst.num_received[i][my_rank] <
                   min_multicast_num) {
                    min_multicast_num =
                        sst.num_received[i][my_rank];
                }
            }
            num_multicasts_finished = min_multicast_num;
        }
        return nullptr;
    }

    void send() {
        uint32_t slot = num_sent % window_size;
        num_sent++;
        sst.slots[my_rank][slot].next_seq++;
        sst.put(
            (char*)std::addressof(sst.slots[0][slot]) -
                sst.getBaseAddress(),
            sizeof(Message<max_msg_size>));
    }
};
