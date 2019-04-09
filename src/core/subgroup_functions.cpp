/**
 * @file subgroup_functions.cpp
 *
 * @date Feb 28, 2017
 */

#include <vector>

#include <derecho/core/container_template_functions.hpp>
#include <derecho/core/derecho_internal.hpp>
#include <derecho/core/derecho_modes.hpp>
#include <derecho/core/subgroup_functions.hpp>
#include <derecho/core/view.hpp>

namespace derecho {

subgroup_allocation_map_t one_subgroup_entire_view(const std::vector<std::type_index>& subgroup_type_order,
                                                   const std::unique_ptr<View>& prev_view, View& curr_view) {
    subgroup_allocation_map_t subgroup_layouts;
    //There should really be only one subgroup type, but this could work
    //if you want every subgroup type to overlap and contain the entire view
    for(const auto& subgroup_type : subgroup_type_order) {
        subgroup_layouts.emplace(subgroup_type, subgroup_shard_layout_t{1});
        subgroup_layouts[subgroup_type][0].emplace_back(curr_view.make_subview(curr_view.members));
        curr_view.next_unassigned_rank = curr_view.members.size();
    }
    return subgroup_layouts;
}
subgroup_allocation_map_t one_subgroup_entire_view_raw(const std::vector<std::type_index>& subgroup_type_order,
                                                       const std::unique_ptr<View>& prev_view, View& curr_view) {
    subgroup_allocation_map_t subgroup_layouts;
    for(const auto& subgroup_type : subgroup_type_order) {
        subgroup_layouts.emplace(subgroup_type, subgroup_shard_layout_t{1});
        subgroup_layouts[subgroup_type][0].emplace_back(curr_view.make_subview(curr_view.members, Mode::UNORDERED));
        curr_view.next_unassigned_rank = curr_view.members.size();
    }
    return subgroup_layouts;
}

ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard) {
    return ShardAllocationPolicy{
            num_shards, true, min_nodes_per_shard, max_nodes_per_shard,
            Mode::ORDERED, {}, {}, {}};
}

ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{
            num_shards, true, nodes_per_shard, nodes_per_shard,
            Mode::ORDERED, {}, {}, {}};
}

ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{
        num_shards, true, nodes_per_shard, nodes_per_shard,
        Mode::UNORDERED, {}, {}, {}};
}

ShardAllocationPolicy custom_shards_policy(const std::vector<int>& min_nodes_by_shard,
                                           const std::vector<int>& max_nodes_by_shard,
                                           const std::vector<Mode>& delivery_modes_by_shard) {
    return ShardAllocationPolicy{static_cast<int>(min_nodes_by_shard.size()), false, -1, -1,
                                 Mode::ORDERED, min_nodes_by_shard, max_nodes_by_shard,
                                 delivery_modes_by_shard};
}

SubgroupAllocationPolicy one_subgroup_policy(const ShardAllocationPolicy& policy) {
    return SubgroupAllocationPolicy{1, true, {policy}};
}

SubgroupAllocationPolicy identical_subgroups_policy(int num_subgroups, const ShardAllocationPolicy& subgroup_policy) {
    return SubgroupAllocationPolicy{num_subgroups, true, {subgroup_policy}};
}

void DefaultSubgroupAllocator::compute_standard_memberships(
        const std::vector<std::type_index>& subgroup_type_order,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        subgroup_allocation_map_t& subgroup_layouts) const {
    //First, determine how many nodes each shard can have based on their policies
    std::map<std::type_index, std::vector<std::vector<uint32_t>>> shard_sizes
            = compute_standard_shard_sizes(subgroup_type_order, curr_view);
    //Now we can go through and actually allocate nodes to each shard,
    //knowing exactly how many nodes they will get
    if(!prev_view) {
        //The very first allocation is the easy case
        for(const auto& subgroup_type : subgroup_type_order) {
            //Ignore cross-product-allocated types
            if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
                continue;
            }
            subgroup_layouts[subgroup_type] = std::move(
                    allocate_standard_subgroup_type(subgroup_type, curr_view, shard_sizes));
        }
    } else {
        //First determine which nodes will need to get reassigned from one shard to another
        //(based on their previous shard getting assigned a smaller size)
        std::list<node_id_t> free_reassigned_nodes = find_reassigned_nodes(
                subgroup_type_order, prev_view, curr_view, shard_sizes);
        //Now we can go through and update the subgroups in order, already knowing
        //whether an earlier subgroup will need to "take" a member from a later one
        for(uint32_t subgroup_type_id = 0; subgroup_type_id < subgroup_type_order.size();
            ++subgroup_type_id) {
            //We need to both iterate through this vector and keep the counter in order to know the type IDs
            const std::type_index& subgroup_type = subgroup_type_order[subgroup_type_id];
            if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
                continue;
            }
            subgroup_layouts[subgroup_type] = std::move(
                    update_standard_subgroup_type(subgroup_type, subgroup_type_id,
                                                  prev_view, curr_view, shard_sizes,
                                                  free_reassigned_nodes));
        }
    }
}

std::map<std::type_index, std::vector<std::vector<uint32_t>>>
DefaultSubgroupAllocator::compute_standard_shard_sizes(
        const std::vector<std::type_index>& subgroup_type_order,
        const View& curr_view) const {
    //First, determine how many nodes we will need for a minimal allocation
    int nodes_needed = 0;
    std::map<std::type_index, std::vector<std::vector<uint32_t>>> shard_sizes;
    for(const auto& type_and_policy : policies) {
        if(!std::holds_alternative<SubgroupAllocationPolicy>(type_and_policy.second)) {
            continue;
        }
        const SubgroupAllocationPolicy& subgroup_type_policy
                = std::get<SubgroupAllocationPolicy>(type_and_policy.second);
        shard_sizes.emplace(type_and_policy.first,
                            std::vector<std::vector<uint32_t>>(subgroup_type_policy.num_subgroups));
        for(int subgroup_num = 0; subgroup_num < subgroup_type_policy.num_subgroups; ++subgroup_num) {
            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];
            shard_sizes[type_and_policy.first][subgroup_num].resize(sharding_policy.num_shards);
            for(int shard_num = 0; shard_num < sharding_policy.num_shards; ++shard_num) {
                int min_shard_size = sharding_policy.even_shards ? sharding_policy.min_nodes_per_shard
                                                                 : sharding_policy.min_num_nodes_by_shard[shard_num];
                shard_sizes[type_and_policy.first][subgroup_num][shard_num] = min_shard_size;
                nodes_needed += min_shard_size;
            }
        }
    }
    //At this point we know whether the View has enough members,
    //so throw the exception if it will be inadequate
    if(nodes_needed > curr_view.num_members) {
        throw subgroup_provisioning_exception();
    }
    //Now go back and add one node to each shard evenly, until either they reach max size
    //or we run out of members in curr_view
    bool done_adding = false;
    while(!done_adding) {
        //This starts at true, but if any shard combines it with false, it will be false
        bool all_at_max = true;
        for(const auto& type_and_policy : policies) {
            if(!std::holds_alternative<SubgroupAllocationPolicy>(type_and_policy.second)) {
                continue;
            }
            const SubgroupAllocationPolicy& subgroup_type_policy
                    = std::get<SubgroupAllocationPolicy>(type_and_policy.second);
            for(int subgroup_num = 0; subgroup_num < subgroup_type_policy.num_subgroups; ++subgroup_num) {
                const ShardAllocationPolicy& sharding_policy
                        = subgroup_type_policy.identical_subgroups
                                  ? subgroup_type_policy.shard_policy_by_subgroup[0]
                                  : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];
                for(int shard_num = 0; shard_num < sharding_policy.num_shards; ++shard_num) {
                    uint max_shard_members = sharding_policy.even_shards
                                                     ? sharding_policy.max_nodes_per_shard
                                                     : sharding_policy.max_num_nodes_by_shard[shard_num];
                    if(nodes_needed >= curr_view.num_members) {
                        done_adding = true;
                        break;
                    }
                    if(shard_sizes[type_and_policy.first][subgroup_num][shard_num] < max_shard_members) {
                        shard_sizes[type_and_policy.first][subgroup_num][shard_num]++;
                        nodes_needed++;
                    }
                    all_at_max = all_at_max
                                 && shard_sizes[type_and_policy.first][subgroup_num][shard_num]
                                            == max_shard_members;
                }
            }
        }
        if(all_at_max) {
            done_adding = true;
        }
    }
    return shard_sizes;
}

subgroup_shard_layout_t DefaultSubgroupAllocator::allocate_standard_subgroup_type(
        const std::type_index subgroup_type,
        View& curr_view,
        const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes) const {
    //The size of shard_sizes[subgroup_type] is the number of subgroups of this type
    subgroup_shard_layout_t subgroup_allocation(shard_sizes.at(subgroup_type).size());
    for(uint32_t subgroup_num = 0; subgroup_num < subgroup_allocation.size(); ++subgroup_num) {
        //The size of shard_sizes[subgroup_type][subgroup_num] is the number of shards
        for(uint32_t shard_num = 0; shard_num < shard_sizes.at(subgroup_type)[subgroup_num].size();
            ++shard_num) {
            uint32_t shard_size = shard_sizes.at(subgroup_type)[subgroup_num][shard_num];
            //Grab the next shard_size nodes
            std::vector<node_id_t> desired_nodes(&curr_view.members[curr_view.next_unassigned_rank],
                                                 &curr_view.members[curr_view.next_unassigned_rank + shard_size]);
            curr_view.next_unassigned_rank += shard_size;
            //Figure out what the Mode policy for this shard is
            const SubgroupAllocationPolicy& subgroup_type_policy
                    = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));
            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];
            Mode delivery_mode = sharding_policy.even_shards
                                         ? sharding_policy.shards_mode
                                         : sharding_policy.modes_by_shard[shard_num];
            //Put the SubView at the end of subgroup_allocation[subgroup_num]
            //Since we go through shards in order, this is at index shard_num
            subgroup_allocation[subgroup_num].emplace_back(
                    curr_view.make_subview(desired_nodes, delivery_mode));
        }
    }
    return subgroup_allocation;
}

subgroup_shard_layout_t DefaultSubgroupAllocator::update_standard_subgroup_type(
        const std::type_index subgroup_type,
        const subgroup_type_id_t subgroup_type_id,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes,
        std::list<node_id_t>& free_reassigned_nodes) const {
    /* Subgroups of the same type will have contiguous IDs because they were created in order.
     * So the previous assignment is the slice of the previous subgroup_shard_views vector
     * starting at the first subgroup's ID, and extending for num_subgroups entries.
     */
    const subgroup_id_t previous_assignment_offset = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
    subgroup_shard_layout_t next_assignment(shard_sizes.at(subgroup_type).size());
    for(uint32_t subgroup_num = 0; subgroup_num < next_assignment.size(); ++subgroup_num) {
        //The size of shard_sizes[subgroup_type][subgroup_num] is the number of shards
        for(uint32_t shard_num = 0; shard_num < shard_sizes.at(subgroup_type)[subgroup_num].size();
            ++shard_num) {
            const SubView& previous_shard_assignment
                    = prev_view->subgroup_shard_views[previous_assignment_offset + subgroup_num]
                                                     [shard_num];
            std::vector<node_id_t> next_shard_members;
            std::vector<int> next_is_sender;
            uint32_t allocated_shard_size = shard_sizes.at(subgroup_type)[subgroup_num][shard_num];
            //Add as many nodes as possible from the previous assignment
            for(std::size_t rank = 0; rank < previous_shard_assignment.members.size(); ++rank) {
                if(curr_view.rank_of(previous_shard_assignment.members[rank]) == -1) {
                    continue;
                }
                //Stop as soon as we reach the allocated size; the remaining live nodes
                //have already been marked as "reassigned nodes"
                if(next_shard_members.size() == allocated_shard_size) {
                    break;
                }
                next_shard_members.push_back(previous_shard_assignment.members[rank]);
                next_is_sender.push_back(previous_shard_assignment.is_sender[rank]);
            }
            //Add additional members if needed
            while(next_shard_members.size() < allocated_shard_size) {
                //Pull from reassigned_nodes first, since there might not be enough unassigned nodes
                if(free_reassigned_nodes.size() > 0) {
                    next_shard_members.push_back(free_reassigned_nodes.front());
                    free_reassigned_nodes.pop_front();
                } else {
                    //This must be true if compute_standard_shard_sizes said our view was adequate
                    assert(curr_view.next_unassigned_rank < (int)curr_view.members.size());
                    next_shard_members.push_back(curr_view.members[curr_view.next_unassigned_rank]);
                    curr_view.next_unassigned_rank++;
                }
                //All members start out as senders with the default allocator
                next_is_sender.push_back(true);
            }
            next_assignment[subgroup_num].emplace_back(curr_view.make_subview(next_shard_members,
                                                                              previous_shard_assignment.mode,
                                                                              next_is_sender));
        }
    }
    return next_assignment;
}

std::list<node_id_t> DefaultSubgroupAllocator::find_reassigned_nodes(
        const std::vector<std::type_index>& subgroup_type_order,
        const std::unique_ptr<View>& prev_view, const View& curr_view,
        const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes) const {
    std::list<node_id_t> reassigned_nodes;
    for(uint32_t subgroup_type_id = 0; subgroup_type_id < subgroup_type_order.size(); ++subgroup_type_id) {
        const std::type_index& subgroup_type = subgroup_type_order[subgroup_type_id];
        if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
            continue;
        }
        //Computed the same way as in update_standard_subgroup_type
        const subgroup_id_t previous_assignment_offset
                = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
        for(uint32_t subgroup_num = 0; subgroup_num < shard_sizes.at(subgroup_type).size(); ++subgroup_num) {
            for(uint32_t shard_num = 0; shard_num < shard_sizes.at(subgroup_type)[subgroup_num].size();
                ++shard_num) {
                const SubView& previous_shard_assignment
                        = prev_view->subgroup_shard_views[previous_assignment_offset + subgroup_num]
                                                         [shard_num];
                uint32_t allocated_shard_size = shard_sizes.at(subgroup_type)[subgroup_num][shard_num];
                uint32_t num_nodes_kept = 0;
                /* Go through the previous assignment and check if each one is still alive.
                 * The first allocated_shard_size live nodes can be kept for the next
                 * assignment, but any beyond that must be reassigned to other shards. */
                for(std::size_t rank = 0; rank < previous_shard_assignment.members.size(); ++rank) {
                    if(curr_view.rank_of(previous_shard_assignment.members[rank]) == -1) {
                        continue;
                    }
                    if(num_nodes_kept < allocated_shard_size) {
                        num_nodes_kept++;
                    } else {
                        reassigned_nodes.push_back(previous_shard_assignment.members[rank]);
                    }
                }
            }
        }
    }
    return reassigned_nodes;
}

void DefaultSubgroupAllocator::compute_cross_product_memberships(
        const std::vector<std::type_index>& subgroup_type_order,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        subgroup_allocation_map_t& subgroup_layouts) const {
    for(uint32_t subgroup_type_id = 0; subgroup_type_id < subgroup_type_order.size(); ++subgroup_type_id) {
        //We need to both iterate through this vector and keep the counter in order to know the type IDs
        const std::type_index& subgroup_type = subgroup_type_order[subgroup_type_id];
        //Only consider CrossProductPolicy subgroup types
        if(!std::holds_alternative<CrossProductPolicy>(policies.at(subgroup_type))) {
            continue;
        }
        const CrossProductPolicy& cross_product_policy
                = std::get<CrossProductPolicy>(policies.at(subgroup_type));
        /* This function runs after compute_standard_memberships, so the source and
         * target subgroups will have entries in subgroup_layouts.
         * Check to make sure the source and target subgroup types actually
         * provisioned enough subgroups for the subgroup index to make sense. */
        if(cross_product_policy.source_subgroup.second
                   >= subgroup_layouts[cross_product_policy.source_subgroup.first].size()
           || cross_product_policy.target_subgroup.second
                      >= subgroup_layouts[cross_product_policy.target_subgroup.first].size()) {
            throw subgroup_provisioning_exception();
        }

        const std::vector<SubView>& source_subgroup_layout
                = subgroup_layouts[cross_product_policy.source_subgroup.first]
                                  [cross_product_policy.source_subgroup.second];
        const std::vector<SubView>& target_subgroup_layout
                = subgroup_layouts[cross_product_policy.target_subgroup.first]
                                  [cross_product_policy.target_subgroup.second];

        /* Ignore prev_view and next_unassigned_rank, because this subgroup's assignment is based
         * entirely on the source and target subgroups, and doesn't provision any new nodes. */
        int num_source_members = 0;
        for(const auto& shard_view : source_subgroup_layout) {
            num_source_members += shard_view.members.size();
        }
        int num_target_shards = target_subgroup_layout.size();
        //Each subgroup will have only one shard, since they'll all overlap, so there are source * target subgroups
        subgroup_shard_layout_t assignment(num_source_members * num_target_shards);
        //I want a list of all members of the source subgroup, "flattened" out of shards, but we don't have that
        //Instead, iterate through the source's shards in order and keep a consistent index
        int source_member_index = 0;
        for(std::size_t source_shard = 0; source_shard < source_subgroup_layout.size(); ++source_shard) {
            for(const auto& source_node : source_subgroup_layout[source_shard].members) {
                for(int target_shard = 0; target_shard < num_target_shards; ++target_shard) {
                    const SubView& target_shard_view = target_subgroup_layout[target_shard];
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
        subgroup_layouts[subgroup_type] = std::move(assignment);
    }
}
subgroup_allocation_map_t DefaultSubgroupAllocator::operator()(
        const std::vector<std::type_index>& subgroup_type_order,
        const std::unique_ptr<View>& prev_view,
        View& curr_view) const {
    subgroup_allocation_map_t subgroup_allocations;
    compute_standard_memberships(subgroup_type_order, prev_view, curr_view, subgroup_allocations);
    compute_cross_product_memberships(subgroup_type_order, prev_view, curr_view, subgroup_allocations);
    return subgroup_allocations;
}

}  // namespace derecho
