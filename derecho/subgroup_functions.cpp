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

/**
 * Allocates members to a single subgroup, using that subgroup's
 * ShardAllocationPolicy, and pushes the resulting vector of SubViews onto the
 * back of previous_assignment's outer vector. This should be called
 * num_subgroups times, in order of subgroup number.
 * @param curr_view A reference to the same curr_view passed to operator()
 * @param next_unassigned_rank A reference to the same next_unassigned_rank
 * "cursor" passed to operator()
 * @param subgroup_policy The ShardAllocationPolicy to use for this subgroup
 */
void DefaultSubgroupAllocator::assign_subgroup(const View& curr_view, int& next_unassigned_rank, const ShardAllocationPolicy& subgroup_policy) {
    if(subgroup_policy.even_shards) {
        if(static_cast<int>(curr_view.members.size()) - next_unassigned_rank
           < subgroup_policy.num_shards * subgroup_policy.nodes_per_shard) {
            throw subgroup_provisioning_exception();
        }
    }
    previous_assignment->emplace_back(std::vector<SubView>());
    for(int shard_num = 0; shard_num < subgroup_policy.num_shards; ++shard_num) {
        if(!subgroup_policy.even_shards && next_unassigned_rank + subgroup_policy.num_nodes_by_shard[shard_num] >= (int)curr_view.members.size()) {
            throw subgroup_provisioning_exception();
        }
        int nodes_needed = subgroup_policy.even_shards ? subgroup_policy.nodes_per_shard : subgroup_policy.num_nodes_by_shard[shard_num];
        std::vector<node_id_t> desired_nodes(&curr_view.members[next_unassigned_rank],
                                             &curr_view.members[next_unassigned_rank + nodes_needed]);
        next_unassigned_rank += nodes_needed;
        (*previous_assignment).back().emplace_back(curr_view.make_subview(desired_nodes));
    }
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
        for(int subgroup_num = 0; subgroup_num < policy.num_subgroups; ++subgroup_num) {
            int num_shards_in_subgroup;
            if(policy.identical_subgroups) {
                num_shards_in_subgroup = policy.shard_policy[0].num_shards;
            } else {
                num_shards_in_subgroup = policy.shard_policy[subgroup_num].num_shards;
            }
            for(int shard_num = 0; shard_num < num_shards_in_subgroup; ++shard_num) {
                //Check each member of the shard in the previous assignment
                for(std::size_t shard_rank = 0;
                    shard_rank < (*previous_assignment)[subgroup_num][shard_num].members.size();
                    ++shard_rank) {
                    if(curr_view.rank_of((*previous_assignment)[subgroup_num][shard_num].members[shard_rank]) == -1) {
                        //This node is not in the current view, so take the next available one
                        if(next_unassigned_rank >= (int)curr_view.members.size()) {
                            throw subgroup_provisioning_exception();
                        }
                        (*previous_assignment)[subgroup_num][shard_num].members[shard_rank] = curr_view.members[next_unassigned_rank];
                        (*previous_assignment)[subgroup_num][shard_num].member_ips[shard_rank] = curr_view.member_ips[next_unassigned_rank];
                        next_unassigned_rank++;
                    } else {
                    }
                }
                //These will be initialized from scratch by the calling ViewManager
                (*previous_assignment)[subgroup_num][shard_num].joined.clear();
                (*previous_assignment)[subgroup_num][shard_num].departed.clear();
            }
        }
    } else {
        previous_assignment = std::make_unique<subgroup_shard_layout_t>();
        for(int subgroup_num = 0; subgroup_num < policy.num_subgroups; ++subgroup_num) {
            if(policy.identical_subgroups) {
                assign_subgroup(curr_view, next_unassigned_rank, policy.shard_policy[0]);
            } else {
                assign_subgroup(curr_view, next_unassigned_rank, policy.shard_policy[subgroup_num]);
            }
        }
    }
    return *previous_assignment;
}
}
