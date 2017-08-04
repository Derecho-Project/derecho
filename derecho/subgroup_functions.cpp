/**
 * @file subgroup_functions.cpp
 *
 * @date Feb 28, 2017
 */

#include <vector>

#include "derecho_internal.h"
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

ShardAllocationPolicy even_sharding_policy(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, Mode::ORDERED, {}, {}};
}

ShardAllocationPolicy raw_even_sharding_policy(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, Mode::RAW, {}, {}};
}

ShardAllocationPolicy custom_shards_policy(const std::vector<int>& num_nodes_by_shard,
                                           const std::vector<Mode>& delivery_modes_by_shard) {
    return ShardAllocationPolicy{static_cast<int>(num_nodes_by_shard.size()), false, -1, Mode::ORDERED,
                                 num_nodes_by_shard, delivery_modes_by_shard};
}

SubgroupAllocationPolicy one_subgroup_policy(const ShardAllocationPolicy& policy) {
    return SubgroupAllocationPolicy{1, true, {policy}};
}

SubgroupAllocationPolicy identical_subgroups_policy(int num_subgroups, const ShardAllocationPolicy& subgroup_policy) {
    return SubgroupAllocationPolicy{num_subgroups, true, {subgroup_policy}};
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
        Mode delivery_mode = subgroup_policy.even_shards ? subgroup_policy.shards_mode : subgroup_policy.modes_by_shard[shard_num];
        (*previous_assignment).back().emplace_back(curr_view.make_subview(desired_nodes, delivery_mode));
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
                num_shards_in_subgroup = policy.shard_policy_by_subgroup[0].num_shards;
            } else {
                num_shards_in_subgroup = policy.shard_policy_by_subgroup[subgroup_num].num_shards;
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
                assign_subgroup(curr_view, next_unassigned_rank, policy.shard_policy_by_subgroup[0]);
            } else {
                assign_subgroup(curr_view, next_unassigned_rank, policy.shard_policy_by_subgroup[subgroup_num]);
            }
        }
    }
    return *previous_assignment;
}

subgroup_shard_layout_t CrossProductAllocator::operator()(const View& curr_view,
                                                          int& next_unassigned_rank,
                                                          bool previous_was_successful) {
    /* Ignore previous_was_successful and next_unassigned_rank, because this subgroup's assignment
     * is based entirely on the source and target subgroups, and doesn't provision any new nodes. */
    subgroup_id_t source_subgroup_id = curr_view.subgroup_ids_by_type.at(policy.source_subgroup.first)
                                               .at(policy.source_subgroup.second);
    subgroup_id_t target_subgroup_id = curr_view.subgroup_ids_by_type.at(policy.target_subgroup.first)
                                               .at(policy.target_subgroup.second);
    int num_source_members = 0;
    for(const auto& shard_view : curr_view.subgroup_shard_views[source_subgroup_id]) {
        num_source_members += shard_view.members.size();
    }
    int num_target_shards = curr_view.subgroup_shard_views[target_subgroup_id].size();
    //Each subgroup will have only one shard, since they'll all overlap, so there are source * target subgroups
    subgroup_shard_layout_t assignment(num_source_members * num_target_shards);
    //I want a list of all members of the source subgroup, "flattened" out of shards, but we don't have that
    //Instead, iterate through the source's shards in order and keep a consistent index
    int source_member_index = 0;
    for(std::size_t source_shard = 0; source_shard < curr_view.subgroup_shard_views[source_subgroup_id].size(); ++source_shard) {
        for(const auto& source_node : curr_view.subgroup_shard_views[source_subgroup_id][source_shard].members) {
            for(int target_shard = 0; target_shard < num_target_shards; ++target_shard) {
                const SubView& target_shard_view = curr_view.subgroup_shard_views[target_subgroup_id][target_shard];
                std::vector<node_id_t> desired_nodes(target_shard_view.members.size() + 1);
                desired_nodes[0] = source_node;
                std::copy(target_shard_view.members.begin(),
                          target_shard_view.members.end(),
                          desired_nodes.begin() + 1);
                std::vector<int> sender_flags(desired_nodes.size(), false);
                sender_flags[0] = true;
                //The vector at this subgroup's index will be default initialized, so push_back a single shard
                assignment[source_member_index * num_target_shards + target_shard].push_back(
                        curr_view.make_subview(desired_nodes, Mode::ORDERED, sender_flags));
                //Now, to send from source_member_index to target_shard, we can use the subgroup at
                //source_member_index * num_target_shards + target_shard
            }
            source_member_index++;
        }
    }
    return assignment;
}
}
