#pragma once

#include "msg/msg.hpp"
#include "node/node_collection_with_senders.hpp"
#include "sst/sst.hpp"

class MulticastSST : public SST<MulticastSST> {
    SSTFieldVector<char> slots;
    SSTFieldVector<msg::msg_id_t> received_nums;
    size_t window_size;
    size_t max_msg_size;
    msg::msg_id_t initiated_num = -1;
    msg::msg_id_t completed_num = -1;
    // only one send at a time
    std::mutex msg_send_mutex;
public:
    MulticastSST(const node::NodeCollectionWithSenders& members_with_senders_info, size_t window_size, size_t max_msg_size);
    bool send(size_t msg_size, const std::function<void(char* buf)>& msg_generator);
};
