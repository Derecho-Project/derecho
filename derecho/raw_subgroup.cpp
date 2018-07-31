/**
 * @file raw_subgroup.cpp
 *
 * @date Feb 17, 2017
 */

#include "raw_subgroup.h"

namespace derecho {

char* RawSubgroup::get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns, bool null_send) {
    if(is_valid()) {
        return group_view_manager.get_sendbuffer_ptr(subgroup_id, payload_size, pause_sending_turns, false, null_send);
    } else {
        throw derecho::empty_reference_exception{"Attempted to use an empty RawSubgroup"};
    }
}

void RawSubgroup::send() {
    if(is_valid()) {
        group_view_manager.send(subgroup_id);
    } else {
        throw derecho::empty_reference_exception{"Attempted to use an empty RawSubgroup"};
    }
}

uint64_t RawSubgroup::compute_global_stability_frontier() {
    if(is_valid()) {
        return group_view_manager.compute_global_stability_frontier(subgroup_id);
    } else {
        throw derecho::empty_reference_exception{"Attempted to use an empty RawSubgroup"};
    }
}
}  // namespace derecho
