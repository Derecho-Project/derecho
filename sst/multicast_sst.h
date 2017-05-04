#include "max_msg_size.h"
#include "sst.h"
#include "multicast_msg.h"

namespace sst {
class multicast_sst : public SST<multicast_sst> {
public:
    SSTFieldVector<Message> slots;
    SSTFieldVector<uint64_t> num_received_sst;
    SSTField<bool> heartbeat;
    multicast_sst(const SSTParams& parameters, uint32_t window_size)
            : SST<multicast_sst>(this, parameters),
              slots(window_size),
              num_received_sst(parameters.members.size()) {
        SSTInit(slots, num_received_sst, heartbeat);
    }
};
}
