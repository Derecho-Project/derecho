/**
 * @file raw_subgroup.cpp
 *
 * @date Feb 17, 2017
 */

#include "raw_subgroup.h"

namespace derecho {
void RawSubgroup::send(unsigned long long int payload_size,
                       const std::function<void(char* buf)>& msg_generator) {
    if(is_valid()) {
        group_view_manager.send(subgroup_id, payload_size, msg_generator);
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
