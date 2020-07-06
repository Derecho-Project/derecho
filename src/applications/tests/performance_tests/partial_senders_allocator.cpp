#include <memory>
#include <typeindex>
#include <vector>

#include "partial_senders_allocator.hpp"

//Stupid redundant definition of class constants
const int PartialSendersAllocator::TRUE = 1;
const int PartialSendersAllocator::FALSE = 0;

derecho::subgroup_allocation_map_t PartialSendersAllocator::operator()(const std::vector<std::type_index>& subgroup_type_order,
                                                                       const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
    if(curr_view.num_members < min_size) {
        throw derecho::subgroup_provisioning_exception();
    }
    derecho::subgroup_shard_layout_t subgroup_vector(1);
    if(senders_option == PartialSendMode::ALL_SENDERS) {
        // a call to make_subview without the sender information defaults to all members sending
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, ordering_mode));
    } else {
        std::vector<int> is_sender(curr_view.num_members, TRUE);
        if(senders_option == PartialSendMode::HALF_SENDERS) {
            // mark members ranked 0 to num_members/2 as non-senders
            for(int32_t i = 0; i <= (curr_view.num_members - 1) / 2; ++i) {
                is_sender[i] = FALSE;
            }
        } else {
            // mark all members except the last ranked one as non-senders
            for(int32_t i = 0; i < curr_view.num_members - 1; ++i) {
                is_sender[i] = FALSE;
            }
        }
        // provide the sender information in a call to make_subview
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, ordering_mode, is_sender));
    }
    curr_view.next_unassigned_rank = curr_view.members.size();
    //Since we know there is only one subgroup type, just put a single entry in the map
    derecho::subgroup_allocation_map_t subgroup_allocation;
    subgroup_allocation.emplace(subgroup_type_order[0], std::move(subgroup_vector));
    return subgroup_allocation;
}