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

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view, int& highest_assigned_rank, bool previous_was_successful) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
    highest_assigned_rank = curr_view.members.size() - 1;
    return subgroup_vector;
}
subgroup_shard_layout_t one_subgroup_entire_view_raw(const View& curr_view, int& highest_assigned_rank, bool previous_was_successful) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, Mode::RAW));
    highest_assigned_rank = curr_view.members.size() - 1;
    return subgroup_vector;
}

subgroup_shard_layout_t DefaultSubgroupAllocator::operator()(const View& curr_view,
                                                             int& highest_assigned_rank,
                                                             bool previous_was_successful) {
    if(previous_was_successful) {
        //Save the previous assignment since it was successful
        last_good_assignment = deep_pointer_copy(previous_assignment);
    } else {
        //Overwrite previous_assignment with the one before that, if it exists
        previous_assignment = deep_pointer_copy(last_good_assignment);
    }
    if(previous_assignment) {
        std::cout << "Subgroup had a previous assignment" << std::endl;
        for(std::size_t shard_num = 0; shard_num < policy.num_shards; ++shard_num) {
            //Check each member of the shard in the previous assignment
            for(std::size_t shard_rank = 0;
                    shard_rank < (*previous_assignment)[0][shard_num].members.size();
                    ++shard_rank) {
                if(curr_view.rank_of((*previous_assignment)[0][shard_num].members[shard_rank]) == -1) {
                    //This node is not in the current view, so take the next available one
                    if(highest_assigned_rank >= curr_view.members.size()) {
                        throw subgroup_provisioning_exception();
                    }
                    node_id_t new_member = curr_view.members[highest_assigned_rank + 1];
                    highest_assigned_rank++;
                    std::cout << "Shard " << shard_num << ": replacing failed member " << (*previous_assignment)[0][shard_num].members[shard_rank] << " with node " << new_member << " in rank " << shard_rank << std::endl;
                    (*previous_assignment)[0][shard_num].members[shard_rank] = new_member;
                    (*previous_assignment)[0][shard_num].member_ips[shard_rank] =
                            curr_view.member_ips[curr_view.rank_of(new_member)];
                } else {
                    std::cout << "Shard " << shard_num << ": keeping node " << (*previous_assignment)[0][shard_num].members[shard_rank] << " in rank " << shard_rank << std::endl;
                }
            }
            //These will be initialized from scratch by the calling ViewManager
            (*previous_assignment)[0][shard_num].joined.clear();
            (*previous_assignment)[0][shard_num].departed.clear();
        }
    } else {
        std::cout << "Subgroup creating a new assignment..." << std::endl;
        //Right now we only handle types that create a single subgroup
        //TODO: Add policy options for multiple subgroups of the same type
        previous_assignment = std::make_unique<subgroup_shard_layout_t>(1);
        if(policy.even_shards) {
            if(curr_view.members.size() - (highest_assigned_rank + 1)
                    < policy.num_shards * policy.nodes_per_shard) {
                throw subgroup_provisioning_exception();
            }
        }
        for(std::size_t shard_num = 0; shard_num < policy.num_shards; ++shard_num) {
            if(!policy.even_shards &&
                    highest_assigned_rank + policy.num_nodes_by_shard[shard_num] + 1
                    > curr_view.members.size()) {
                throw subgroup_provisioning_exception();
            }
            int nodes_needed = policy.even_shards ? policy.nodes_per_shard : policy.num_nodes_by_shard[shard_num];
            std::vector<node_id_t> desired_nodes(&curr_view.members[highest_assigned_rank + 1],
                                                 &curr_view.members[highest_assigned_rank + nodes_needed + 1]);
            highest_assigned_rank += nodes_needed;
            (*previous_assignment)[0].emplace_back(curr_view.make_subview(desired_nodes));
            std::cout << "Assigned nodes " << desired_nodes << " to shard " << shard_num << std::endl;
        }
    }
    return *previous_assignment;
}


}
