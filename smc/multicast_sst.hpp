#pragma once

#include "msg/msg.hpp"
#include "node/node_collection_with_senders.hpp"
#include "sst/sst.hpp"

namespace smc {
using sst::SSTField;
using sst::SSTFieldVector;

class MulticastSST : public sst::SST<MulticastSST> {
public:
    SSTFieldVector<char> slots;
    SSTFieldVector<msg::msg_id_t> received_nums;

    MulticastSST(const node::NodeCollectionWithSenders& members_with_senders_info, size_t slots_size);
};
}  // namespace smc
