/**
 * @file subgroup_functions.cpp
 *
 * @date Feb 28, 2017
 */

#include <vector>

#include "container_template_functions.h"
#include "derecho_internal.h"
#include "derecho_modes.h"
#include "subgroup_functions.h"
#include "view.h"

namespace derecho {

subgroup_shard_layout_t one_subgroup_entire_view(const std::type_index& subgroup_type,
                                                 const std::unique_ptr<View>& prev_view, View& curr_view) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
    curr_view.next_unassigned_rank = curr_view.members.size();
    return subgroup_vector;
}
subgroup_shard_layout_t one_subgroup_entire_view_raw(const std::type_index& subgroup_type,
                                                     const std::unique_ptr<View>& prev_view, View& curr_view) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, Mode::UNORDERED));
    curr_view.next_unassigned_rank = curr_view.members.size();
    return subgroup_vector;
}

ShardAllocationPolicy even_sharding_policy(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, Mode::ORDERED, {}, {}};
}

ShardAllocationPolicy raw_even_sharding_policy(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, Mode::UNORDERED, {}, {}};
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
 * ShardAllocationPolicy, and returns the resulting vector of SubViews.
 * This should be called num_subgroups times, in order of subgroup number.
 * @param prev_view A reference to the same prev_view pointer passed to operator()
 * @param curr_view A reference to the same curr_view passed to operator()
 * @param subgroup_policy The ShardAllocationPolicy to use for this subgroup
 * @return The SubViews containing the members of each shard of this subgroup
 * @throws subgroup_provisioning_exception if the View ran out of nodes and
 * the subgroup could not be fully populated.
 */
std::vector<SubView> DefaultSubgroupAllocator::assign_subgroup(const std::unique_ptr<View>& prev_view,
                                                               View& curr_view,
                                                               const ShardAllocationPolicy& subgroup_policy) const {
    if(subgroup_policy.even_shards) {
        if(static_cast<int>(curr_view.members.size()) - curr_view.next_unassigned_rank
           < subgroup_policy.num_shards * subgroup_policy.nodes_per_shard) {
            throw subgroup_provisioning_exception();
        }
    }
    std::vector<SubView> subgroup_allocation;
    for(int shard_num = 0; shard_num < subgroup_policy.num_shards; ++shard_num) {
        if(!subgroup_policy.even_shards
           && curr_view.next_unassigned_rank + subgroup_policy.num_nodes_by_shard[shard_num]
                      >= (int)curr_view.members.size()) {
            throw subgroup_provisioning_exception();
        }
        int nodes_needed = subgroup_policy.even_shards ? subgroup_policy.nodes_per_shard : subgroup_policy.num_nodes_by_shard[shard_num];
        std::vector<node_id_t> desired_nodes(&curr_view.members[curr_view.next_unassigned_rank],
                                             &curr_view.members[curr_view.next_unassigned_rank + nodes_needed]);
        curr_view.next_unassigned_rank += nodes_needed;
        Mode delivery_mode = subgroup_policy.even_shards ? subgroup_policy.shards_mode : subgroup_policy.modes_by_shard[shard_num];
        subgroup_allocation.emplace_back(curr_view.make_subview(desired_nodes, delivery_mode));
    }
    return subgroup_allocation;
}

subgroup_shard_layout_t DefaultSubgroupAllocator::compute_standard_membership(const std::type_index& subgroup_type,
                                                                              const std::unique_ptr<View>& prev_view,
                                                                              View& curr_view) const {
    const SubgroupAllocationPolicy& subgroup_policy = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));
    if(prev_view) {
        const uint32_t subgroup_type_id = index_of(prev_view->subgroup_type_order, subgroup_type);
        /* Subgroups of the same type will have contiguous IDs because they were created in order.
         * So the previous assignment is the slice of the previous subgroup_shard_views vector
         * starting at the first subgroup's ID, and extending for num_subgroups entries.
         */
        const std::size_t previous_assignment_offset = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
        subgroup_shard_layout_t next_assignment(&prev_view->subgroup_shard_views[previous_assignment_offset],
                                                &prev_view->subgroup_shard_views[previous_assignment_offset
                                                                                 + subgroup_policy.num_subgroups]);
        for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
            int num_shards_in_subgroup;
            if(subgroup_policy.identical_subgroups) {
                num_shards_in_subgroup = subgroup_policy.shard_policy_by_subgroup[0].num_shards;
            } else {
                num_shards_in_subgroup = subgroup_policy.shard_policy_by_subgroup[subgroup_num].num_shards;
            }
            for(int shard_num = 0; shard_num < num_shards_in_subgroup; ++shard_num) {
                //Check each member of the shard in the previous assignment
                const std::vector<node_id_t>& previous_shard_members = prev_view->subgroup_shard_views[previous_assignment_offset + subgroup_num][shard_num].members;
                for(std::size_t shard_rank = 0; shard_rank < previous_shard_members.size(); ++shard_rank) {
                    if(curr_view.rank_of(previous_shard_members[shard_rank]) == -1) {
                        //That node is not in the current view, so take the next available one
                        if(curr_view.next_unassigned_rank >= static_cast<int>(curr_view.members.size())) {
                            throw subgroup_provisioning_exception();
                        }
                        next_assignment[subgroup_num][shard_num].members[shard_rank] = curr_view.members[curr_view.next_unassigned_rank];
                        next_assignment[subgroup_num][shard_num].member_ips_and_ports[shard_rank] = curr_view.member_ips_and_ports[curr_view.next_unassigned_rank];
                        curr_view.next_unassigned_rank++;
                    }
                }
                //These will be initialized from scratch by the calling ViewManager, so don't keep the copied values
                next_assignment[subgroup_num][shard_num].joined.clear();
                next_assignment[subgroup_num][shard_num].departed.clear();
            }
        }
        return next_assignment;
    } else {
        subgroup_shard_layout_t next_assignment;
        for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
            if(subgroup_policy.identical_subgroups) {
                next_assignment.push_back(assign_subgroup(prev_view, curr_view,
                                                          subgroup_policy.shard_policy_by_subgroup[0]));
            } else {
                next_assignment.push_back(assign_subgroup(prev_view, curr_view,
                                                          subgroup_policy.shard_policy_by_subgroup[subgroup_num]));
            }
        }
        return next_assignment;
    }
}

subgroup_shard_layout_t DefaultSubgroupAllocator::compute_cross_product_membership(const std::type_index& subgroup_type,
                                                                                   const std::unique_ptr<View>& prev_view,
                                                                                   View& curr_view) const {
    const CrossProductPolicy& cross_product_policy = std::get<CrossProductPolicy>(policies.at(subgroup_type));
    /* Ignore prev_view and next_unassigned_rank, because this subgroup's assignment is based
     * entirely on the source and target subgroups, and doesn't provision any new nodes. */
    subgroup_type_id_t source_subgroup_type = index_of(curr_view.subgroup_type_order,
                                                       cross_product_policy.source_subgroup.first);
    subgroup_type_id_t target_subgroup_type = index_of(curr_view.subgroup_type_order,
                                                       cross_product_policy.target_subgroup.first);
    subgroup_id_t source_subgroup_id = curr_view.subgroup_ids_by_type_id.at(source_subgroup_type)
                                               .at(cross_product_policy.source_subgroup.second);
    subgroup_id_t target_subgroup_id = curr_view.subgroup_ids_by_type_id.at(target_subgroup_type)
                                               .at(cross_product_policy.target_subgroup.second);
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
    for(std::size_t source_shard = 0;
        source_shard < curr_view.subgroup_shard_views[source_subgroup_id].size();
        ++source_shard) {
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
subgroup_shard_layout_t DefaultSubgroupAllocator::operator()(const std::type_index& subgroup_type,
                                                             const std::unique_ptr<View>& prev_view, View& curr_view) const {
    if(std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
        return compute_standard_membership(subgroup_type, prev_view, curr_view);
    } else {
        return compute_cross_product_membership(subgroup_type, prev_view, curr_view);
    }
}

}  // namespace derecho
