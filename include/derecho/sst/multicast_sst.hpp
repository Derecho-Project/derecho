#include "sst.hpp"

namespace sst {
class multicast_sst : public SST<multicast_sst> {
public:
    SSTFieldVector<char> slots;
    SSTFieldVector<int32_t> index;
    SSTFieldVector<int64_t> num_received_sst;
    SSTField<bool> heartbeat;
    multicast_sst(const SSTParams& parameters, uint32_t window_size, uint32_t num_senders, uint64_t max_msg_size)
            : SST<multicast_sst>(this, parameters),
              slots((max_msg_size + sizeof(uint64_t)) * window_size),
              index(1),
              num_received_sst(num_senders) {
        SSTInit(slots, index, num_received_sst, heartbeat);
    }
};
}  // namespace sst
