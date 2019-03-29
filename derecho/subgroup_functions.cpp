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

void one_subgroup_entire_view(const std::type_index& subgroup_type,
                              const std::unique_ptr<View>& prev_view,
                              View& curr_view,
                              std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) {
    //Do nothing on the second pass
    if(subgroup_layouts.at(subgroup_type)) {
        return;
    }
    auto subgroup_vector = std::make_unique<subgroup_shard_layout_t>(1);
    subgroup_vector->at(0).emplace_back(curr_view.make_subview(curr_view.members));
    curr_view.next_unassigned_rank = curr_view.members.size();
    subgroup_layouts.at(subgroup_type) = std::move(subgroup_vector);
}
void one_subgroup_entire_view_raw(const std::type_index& subgroup_type,
                                  const std::unique_ptr<View>& prev_view,
                                  View& curr_view,
                                  std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) {
    if(subgroup_layouts.at(subgroup_type)) {
        return;
    }
    auto subgroup_vector = std::make_unique<subgroup_shard_layout_t>(1);
    subgroup_vector->at(0).emplace_back(curr_view.make_subview(curr_view.members, Mode::UNORDERED));
    curr_view.next_unassigned_rank = curr_view.members.size();
    subgroup_layouts.at(subgroup_type) = std::move(subgroup_vector);
}

ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, min_nodes_per_shard, max_nodes_per_shard, Mode::ORDERED, {}, {}, {}};
}

ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, nodes_per_shard, Mode::ORDERED, {}, {}, {}};
}

ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard) {
    return ShardAllocationPolicy{num_shards, true, nodes_per_shard, nodes_per_shard, Mode::UNORDERED, {}, {}, {}};
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

/**
 * Allocates members to a single subgroup, using that subgroup's
 * ShardAllocationPolicy, and returns the resulting vector of SubViews.
 * This should be called num_subgroups times, in order of subgroup number.
 * @param prev_view A reference to the same prev_view pointer passed to operator()
 * @param curr_view A reference to the same curr_view passed to operator()
 * @param subgroup_policy The ShardAllocationPolicy to use for this subgroup
 * @return The SubViews containing the members of each shard of this subgroup
 * @throws subgroup_provisioning_exception if the View ran out of nodes and
 * the subgroup could not reach its minimum population.
 */
std::vector<SubView> DefaultSubgroupAllocator::assign_subgroup(const std::unique_ptr<View>& prev_view,
                                                               View& curr_view,
                                                               const ShardAllocationPolicy& subgroup_policy) const {
    if(subgroup_policy.even_shards) {
        if(static_cast<int>(curr_view.members.size()) - curr_view.next_unassigned_rank
           < subgroup_policy.num_shards * subgroup_policy.min_nodes_per_shard) {
            throw subgroup_provisioning_exception();
        }
    }
    std::vector<SubView> subgroup_allocation;
    for(int shard_num = 0; shard_num < subgroup_policy.num_shards; ++shard_num) {
        if(!subgroup_policy.even_shards
           && curr_view.next_unassigned_rank + subgroup_policy.min_num_nodes_by_shard[shard_num]
                      >= (int)curr_view.members.size()) {
            throw subgroup_provisioning_exception();
        }
        int nodes_needed = subgroup_policy.even_shards
                                   ? subgroup_policy.min_nodes_per_shard
                                   : subgroup_policy.min_num_nodes_by_shard[shard_num];
        std::vector<node_id_t> desired_nodes(&curr_view.members[curr_view.next_unassigned_rank],
                                             &curr_view.members[curr_view.next_unassigned_rank + nodes_needed]);
        curr_view.next_unassigned_rank += nodes_needed;
        Mode delivery_mode = subgroup_policy.even_shards
                                     ? subgroup_policy.shards_mode
                                     : subgroup_policy.modes_by_shard[shard_num];
        subgroup_allocation.emplace_back(curr_view.make_subview(desired_nodes, delivery_mode));
    }
    return subgroup_allocation;
}

void DefaultSubgroupAllocator::compute_standard_membership_phase_1(
        const std::type_index& subgroup_type,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) const {
    const SubgroupAllocationPolicy& subgroup_policy
            = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));
    if(prev_view) {
        const uint32_t subgroup_type_id = index_of(prev_view->subgroup_type_order, subgroup_type);
        /* Subgroups of the same type will have contiguous IDs because they were created in order.
         * So the previous assignment is the slice of the previous subgroup_shard_views vector
         * starting at the first subgroup's ID, and extending for num_subgroups entries.
         */
        const subgroup_id_t previous_assignment_offset = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
        auto next_assignment = std::make_unique<subgroup_shard_layout_t>(subgroup_policy.num_subgroups);
        for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
            const ShardAllocationPolicy& shard_policy
                    = subgroup_policy.identical_subgroups
                              ? subgroup_policy.shard_policy_by_subgroup[0]
                              : subgroup_policy.shard_policy_by_subgroup[subgroup_num];
            for(int shard_num = 0; shard_num < shard_policy.num_shards; ++shard_num) {
                const SubView& previous_shard_assignment
                        = prev_view->subgroup_shard_views[previous_assignment_offset + subgroup_num]
                                                         [shard_num];
                std::vector<node_id_t> next_shard_members;
                std::vector<int> next_is_sender;
                //For each member of the shard in the previous assignment, add it to this assignment if it's still alive
                for(std::size_t rank = 0; rank < previous_shard_assignment.members.size(); ++rank) {
                    if(curr_view.rank_of(previous_shard_assignment.members[rank]) != -1) {
                        next_shard_members.push_back(previous_shard_assignment.members[rank]);
                        next_is_sender.push_back(previous_shard_assignment.is_sender[rank]);
                    }
                }
                //Check to see if the shard will still have the minimum number of members
                //If not, attempt to take new members from the unassigned ones, and abort if this fails
                uint min_shard_members = shard_policy.even_shards ? shard_policy.min_nodes_per_shard
                                                                  : shard_policy.min_num_nodes_by_shard[shard_num];
                while(next_shard_members.size() < min_shard_members) {
                    if(curr_view.next_unassigned_rank >= static_cast<int>(curr_view.members.size())) {
                        throw subgroup_provisioning_exception();
                    }
                    next_shard_members.push_back(curr_view.members[curr_view.next_unassigned_rank]);
                    next_is_sender.push_back(true);  //All members start out as senders with the default allocator
                    curr_view.next_unassigned_rank++;
                }
                (*next_assignment)[subgroup_num].emplace_back(curr_view.make_subview(next_shard_members,
                                                                                     previous_shard_assignment.mode,
                                                                                     next_is_sender));
            }
        }
        subgroup_layouts[subgroup_type] = std::move(next_assignment);
    } else {
        auto next_assignment = std::make_unique<subgroup_shard_layout_t>();
        for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
            if(subgroup_policy.identical_subgroups) {
                next_assignment->push_back(assign_subgroup(prev_view, curr_view,
                                                           subgroup_policy.shard_policy_by_subgroup[0]));
            } else {
                next_assignment->push_back(assign_subgroup(prev_view, curr_view,
                                                           subgroup_policy.shard_policy_by_subgroup[subgroup_num]));
            }
        }
        subgroup_layouts[subgroup_type] = std::move(next_assignment);
    }
}

void DefaultSubgroupAllocator::compute_standard_membership_phase_2(
        const std::type_index& subgroup_type,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) const {
    const SubgroupAllocationPolicy& subgroup_policy
            = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));

    assert(subgroup_layouts[subgroup_type]);
    std::vector<std::vector<std::vector<node_id_t>>> additional_members(subgroup_policy.num_subgroups);
    bool done_adding = false;
    while(!done_adding) {
        //This starts at true, but if any shard combines it with false, it will be false
        bool all_at_max = true;
        for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
            const ShardAllocationPolicy& shard_policy
                    = subgroup_policy.identical_subgroups
                              ? subgroup_policy.shard_policy_by_subgroup[0]
                              : subgroup_policy.shard_policy_by_subgroup[subgroup_num];
            additional_members[subgroup_num].resize(shard_policy.num_shards);
            for(int shard_num = 0; shard_num < shard_policy.num_shards; ++shard_num) {
                const SubView& existing_shard_assignment
                        = (*subgroup_layouts[subgroup_type])[subgroup_num][shard_num];
                uint max_shard_members = shard_policy.even_shards ? shard_policy.max_nodes_per_shard
                                                                  : shard_policy.max_num_nodes_by_shard[shard_num];
                if(curr_view.next_unassigned_rank >= static_cast<int>(curr_view.members.size())) {
                    done_adding = true;
                    //Break the inner loop, but we must still finish the outer one
                    //to finish resizing additional_members
                    break;
                }
                if(existing_shard_assignment.members.size()
                           + additional_members[subgroup_num][shard_num].size()
                   < max_shard_members) {
                    additional_members[subgroup_num][shard_num].push_back(
                            curr_view.members[curr_view.next_unassigned_rank]);
                    curr_view.next_unassigned_rank++;
                }
                all_at_max = all_at_max && (existing_shard_assignment.members.size() + additional_members[subgroup_num][shard_num].size() == max_shard_members);
            }
        }
        if(all_at_max) {
            done_adding = true;
        }
    }

    //For each existing SubView, add additional_members[subgroup][shard] to its members
    for(int subgroup_num = 0; subgroup_num < subgroup_policy.num_subgroups; ++subgroup_num) {
        const ShardAllocationPolicy& shard_policy
                = subgroup_policy.identical_subgroups
                          ? subgroup_policy.shard_policy_by_subgroup[0]
                          : subgroup_policy.shard_policy_by_subgroup[subgroup_num];
        for(int shard_num = 0; shard_num < shard_policy.num_shards; ++shard_num) {
            SubView& existing_shard_assignment
                    = (*subgroup_layouts[subgroup_type])[subgroup_num][shard_num];
            for(node_id_t new_member_id : additional_members[subgroup_num][shard_num]) {
                std::size_t rank_of_new_member = curr_view.rank_of(new_member_id);
                existing_shard_assignment.members.push_back(new_member_id);
                existing_shard_assignment.member_ips_and_ports.push_back(
                        curr_view.member_ips_and_ports[rank_of_new_member]);
                existing_shard_assignment.is_sender.push_back(true);
            }
        }
    }
}

void DefaultSubgroupAllocator::compute_cross_product_membership(
        const std::type_index& subgroup_type,
        const std::unique_ptr<View>& prev_view,
        View& curr_view,
        std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) const {
    const CrossProductPolicy& cross_product_policy = std::get<CrossProductPolicy>(policies.at(subgroup_type));
    /* The source and target subgroups must exist in the in-progress subgroup_layouts map.
     * If the source and target subgroups haven't been provisioned yet when this is called,
     * the types were ordered wrong and there's nothing we can do. If the types were provisioned,
     * but they didn't create enough subgroups, we're also stuck. */
    if(!subgroup_layouts[cross_product_policy.source_subgroup.first]
       || cross_product_policy.source_subgroup.second
                  >= subgroup_layouts[cross_product_policy.source_subgroup.first]->size()
       || !subgroup_layouts[cross_product_policy.target_subgroup.first]
       || cross_product_policy.target_subgroup.second
                  >= subgroup_layouts[cross_product_policy.target_subgroup.first]->size()) {
        throw subgroup_provisioning_exception();
    }
    const std::vector<SubView>& source_subgroup_layout
            = (*subgroup_layouts[cross_product_policy.source_subgroup.first])
                    [cross_product_policy.source_subgroup.second];
    const std::vector<SubView>& target_subgroup_layout
            = (*subgroup_layouts[cross_product_policy.target_subgroup.first])
                    [cross_product_policy.target_subgroup.second];

    /* Ignore prev_view and next_unassigned_rank, because this subgroup's assignment is based
     * entirely on the source and target subgroups, and doesn't provision any new nodes. */
    int num_source_members = 0;
    for(const auto& shard_view : source_subgroup_layout) {
        num_source_members += shard_view.members.size();
    }
    int num_target_shards = target_subgroup_layout.size();
    //Each subgroup will have only one shard, since they'll all overlap, so there are source * target subgroups
    auto assignment = std::make_unique<subgroup_shard_layout_t>(num_source_members * num_target_shards);
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
                assignment->at(source_member_index * num_target_shards + target_shard).push_back(curr_view.make_subview(desired_nodes, Mode::ORDERED, sender_flags));
                //Now, to send from source_member_index to target_shard, we can use the subgroup at
                //source_member_index * num_target_shards + target_shard
            }
            source_member_index++;
        }
    }
    subgroup_layouts[subgroup_type] = std::move(assignment);
}
void DefaultSubgroupAllocator::operator()(const std::type_index& subgroup_type,
                                          const std::unique_ptr<View>& prev_view,
                                          View& curr_view,
                                          std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>&
                                                  subgroup_layouts) const {
    if(std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
        if(!subgroup_layouts.at(subgroup_type)) {
            compute_standard_membership_phase_1(subgroup_type, prev_view, curr_view, subgroup_layouts);
        } else {
            compute_standard_membership_phase_2(subgroup_type, prev_view, curr_view, subgroup_layouts);
        }
    } else {
        /* For cross-product subgroups, just overwrite the first assignment in phase 2.
         * This is easier than figuring out if the cross-product needs to change because the
         * source and/or target subgroups added members in phase 2.
         */
        compute_cross_product_membership(subgroup_type, prev_view, curr_view, subgroup_layouts);
    }
}

}  // namespace derecho
