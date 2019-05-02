#pragma once

#include "msg/msg.hpp"
#include "multicast_sst.hpp"
#include "node/node_collection_with_senders.hpp"

namespace smc {

struct __attribute__((__packed__)) header {
    size_t header_size;
    msg::msg_id_t index;
    size_t msg_size;
};

class SMCGroup {
    size_t max_msg_size;
    size_t window_size;

    const node::NodeCollectionWithSenders members_with_senders_info;
    MulticastSST sst;

    msg::msg_id_t initiated_num = -1;
    msg::msg_id_t completed_num = -1;

    // only one send at a time
    std::mutex msg_send_mutex;

    const msg::recv_upcall_t receive_upcall;
    void receive_message();
    void update_completed_num();

public:
    SMCGroup(const node::NodeCollectionWithSenders& members_with_senders_info,
             size_t max_payload_size, size_t window_size, const msg::recv_upcall_t& receive_upcall);

    bool send(size_t msg_size, const msg::msg_generator_t& msg_generator);
};
}  // namespace smc
