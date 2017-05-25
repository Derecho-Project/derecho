/**
 * @file subgroup_functions.cpp
 *
 * @date Feb 28, 2017
 */

#include <vector>

#include "derecho_modes.h"
#include "subgroup_functions.h"
#include "view.h"

namespace derecho {

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
    next_unassigned_rank = curr_view.members.size();
    return subgroup_vector;
}
subgroup_shard_layout_t one_subgroup_entire_view_raw(const View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, Mode::RAW));
    next_unassigned_rank = curr_view.members.size();
    return subgroup_vector;
}

subgroup_shard_layout_t DefaultSubgroupAllocator::operator()(const View& curr_view,
                                                             int& next_unassigned_rank,
                                                             bool previous_was_successful) {
    if(previous_was_successful) {
        //Save the previous assignment since it was successful
        last_good_assignment = deep_pointer_copy(previous_assignment);
    } else {
        //Overwrite previous_assignment with the one before that, if it exists
        previous_assignment = deep_pointer_copy(last_good_assignment);
    }
    if(previous_assignment) {
        for(int shard_num = 0; shard_num < policy.num_shards; ++shard_num) {
            //Check each member of the shard in the previous assignment
            for(std::size_t shard_rank = 0;
                    shard_rank < (*previous_assignment)[0][shard_num].members.size();
                    ++shard_rank) {
                if(curr_view.rank_of((*previous_assignment)[0][shard_num].members[shard_rank]) == -1) {
                    //This node is not in the current view, so take the next available one
                    if(next_unassigned_rank >= (int) curr_view.members.size()) {
                        throw subgroup_provisioning_exception();
                    }
                    node_id_t new_member = curr_view.members[next_unassigned_rank];
                    next_unassigned_rank++;
                    (*previous_assignment)[0][shard_num].members[shard_rank] = new_member;
                    (*previous_assignment)[0][shard_num].member_ips[shard_rank] =
                            curr_view.member_ips[curr_view.rank_of(new_member)];
                } else {
                }
            }
            //These will be initialized from scratch by the calling ViewManager
            (*previous_assignment)[0][shard_num].joined.clear();
            (*previous_assignment)[0][shard_num].departed.clear();
        }
    } else {
        //Right now we only handle types that create a single subgroup
        //TODO: Add policy options for multiple subgroups of the same type
        previous_assignment = std::make_unique<subgroup_shard_layout_t>(1);
        if(policy.even_shards) {
            if(static_cast<int>(curr_view.members.size()) - next_unassigned_rank
                    < policy.num_shards * policy.nodes_per_shard) {
                throw subgroup_provisioning_exception();
            }
        }
        for(int shard_num = 0; shard_num < policy.num_shards; ++shard_num) {
            if(!policy.even_shards &&
                    next_unassigned_rank + policy.num_nodes_by_shard[shard_num]
                    >= (int) curr_view.members.size()) {
                throw subgroup_provisioning_exception();
            }
            int nodes_needed = policy.even_shards ? policy.nodes_per_shard : policy.num_nodes_by_shard[shard_num];
            std::vector<node_id_t> desired_nodes(&curr_view.members[next_unassigned_rank],
                                                 &curr_view.members[next_unassigned_rank + nodes_needed]);
            next_unassigned_rank += nodes_needed;
            (*previous_assignment)[0].emplace_back(curr_view.make_subview(desired_nodes));
        }
    }
    return *previous_assignment;
}


}
