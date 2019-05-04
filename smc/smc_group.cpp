#include "smc_group.hpp"
#include "exception/msg_exceptions.hpp"

namespace smc {

SMCGroup::SMCGroup(const node::NodeCollectionWithSenders& members_with_senders_info,
                   size_t max_payload_size, size_t window_size,
                   const msg::recv_upcall_t& receive_upcall)
        : max_msg_size(max_payload_size + sizeof(header)),
          window_size(window_size),
          members_with_senders_info(members_with_senders_info),
          sst(members_with_senders_info, max_msg_size * window_size),
          receive_upcall(receive_upcall) {
    auto always_true = [](const MulticastSST& sst) {
        return true;
    };
    auto receiver_trig = [this](MulticastSST& sst) {
        receive_message();
    };
    sst.predicates.insert(always_true, receiver_trig);

    if(members_with_senders_info.my_sender_rank >= 0) {
        auto update_completed_num_trig = [this](MulticastSST& sst) {
            update_completed_num();
        };
        sst.predicates.insert(always_true, update_completed_num_trig);
    }
}

bool SMCGroup::send(size_t msg_size, const msg::msg_generator_t& msg_generator) {
    std::lock_guard<std::mutex> lock(msg_send_mutex);
    if(msg_size + sizeof(header) > max_msg_size) {
        throw msg::MsgSizeViolation("SMC cannot send a message of size " + std::to_string(msg_size) + ". The maximum message size is only " + std::to_string(max_msg_size));
    }
    if(members_with_senders_info.my_sender_rank < 0) {
        throw msg::NotASender("This node, with id " + std::to_string(members_with_senders_info.my_id) + " is not a sender in this SMC group");
    }

    if(initiated_num - completed_num == (int64_t)window_size) {
        return false;
    }
    initiated_num++;
    size_t slot = initiated_num % window_size;
    char* buf = const_cast<char*>(&sst.slots[sst.get_my_index()][slot * max_msg_size]);
    header* h = (header*)(buf);
    buf += h->header_size;
    h->header_size = sizeof(header);
    h->index = initiated_num;
    h->msg_size = msg_size;
    msg_generator(buf, msg_size);

    // push the contents of the message
    sst.update_remote_rows(buf - sst.get_base_address(sst.get_my_index()), msg_size);
    // then the header
    sst.update_remote_rows((char*)h - sst.get_base_address(sst.get_my_index()), h->header_size);

    return true;
}

void SMCGroup::receive_message() {
    bool received_nums_updated = false;
    for(uint32_t sender_rank = 0; sender_rank < members_with_senders_info.num_senders; ++sender_rank) {
        for(auto i = 0u; i < window_size; ++i) {
            const msg::msg_id_t received_num = sst.received_nums[members_with_senders_info.my_rank][sender_rank];
            const size_t slot = (received_num + 1) % window_size;
            const uint32_t rank_of_sender = members_with_senders_info.ranks_of_senders[sender_rank];
            char* buf = const_cast<char*>(&sst.slots[rank_of_sender][slot * max_msg_size]);
            header* h = (header*)(buf);
            if(h->index == received_num + 1) {
                buf += h->header_size;
                receive_upcall(members_with_senders_info[rank_of_sender],
                               h->index, buf, h->msg_size);
                sst.received_nums[members_with_senders_info.my_rank][sender_rank]++;
                received_nums_updated = true;
            } else {
                break;
            }
        }
    }
    if(received_nums_updated) {
        sst.update_remote_rows(sst.received_nums.get_base_address() - sst.get_base_address(),
                               sizeof(sst.received_nums[0][0]) * sst.received_nums.size());
    }
}

void SMCGroup::update_completed_num() {
    const uint32_t& my_sender_rank = members_with_senders_info.my_sender_rank;
    msg::msg_id_t min_received_num = sst.received_nums[0][my_sender_rank];
    for(uint32_t row_index = 0; row_index < members_with_senders_info.num_nodes; ++row_index) {
        msg::msg_id_t received_num_copy = sst.received_nums[row_index][my_sender_rank];
        if(received_num_copy < min_received_num) {
            min_received_num = received_num_copy;
        }
    }
    completed_num = min_received_num;
}
}  // namespace smc
