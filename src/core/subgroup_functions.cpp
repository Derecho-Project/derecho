/**
 * @file subgroup_functions.cpp
 */

#include <iterator>
#include <vector>

#include <derecho/core/derecho_modes.hpp>
#include <derecho/core/detail/derecho_internal.hpp>
#include <derecho/core/subgroup_functions.hpp>
#include <derecho/core/view.hpp>
#include <derecho/utils/container_template_functions.hpp>
#include <derecho/utils/logger.hpp>

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

ShardAllocationPolicy flexible_even_shards(const std::string& profile) {
    const std::string conf_profile_prefix = "SUBGROUP/" + profile + "/";
    if(!hasCustomizedConfKey(conf_profile_prefix + num_shards_profile_field)) {
        dbg_default_error("{} not found in config.", conf_profile_prefix + num_shards_profile_field);
        return {};
    }
    if(!hasCustomizedConfKey(conf_profile_prefix + min_nodes_profile_field)) {
        dbg_default_error("{} not found in config.", conf_profile_prefix + min_nodes_profile_field);
        return {};
    }
    if(!hasCustomizedConfKey(conf_profile_prefix + max_nodes_profile_field)) {
        dbg_default_error("{} not found in config.", conf_profile_prefix + max_nodes_profile_field);
        return {};
    }
    int num_shards = getConfUInt32(conf_profile_prefix + num_shards_profile_field);
    int min_nodes = getConfUInt32(conf_profile_prefix + min_nodes_profile_field);
    int max_nodes = getConfUInt32(conf_profile_prefix + max_nodes_profile_field);
    return flexible_even_shards(num_shards, min_nodes, max_nodes, profile);
}

ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard, const std::string& profile) {
    return ShardAllocationPolicy{
            num_shards, true, min_nodes_per_shard, max_nodes_per_shard, Mode::ORDERED, profile, {}, {}, {}, {}};
}

ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard,
                                        const std::string& profile) {
    return ShardAllocationPolicy{
            num_shards, true, nodes_per_shard, nodes_per_shard, Mode::ORDERED, profile, {}, {}, {}, {}};
}

ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard,
                                            const std::string& profile) {
    return ShardAllocationPolicy{
            num_shards, true, nodes_per_shard, nodes_per_shard, Mode::UNORDERED, profile, {}, {}, {}, {}};
}

ShardAllocationPolicy custom_shard_policy(const std::vector<Mode>& delivery_modes_by_shard,
                                          const std::vector<std::string>& profiles_by_shard) {
    std::vector<int> min_nodes_by_shard;
    std::vector<int> max_nodes_by_shard;
    for(const std::string& profile : profiles_by_shard) {
        const std::string conf_profile_prefix = "SUBGROUP/" + profile + "/";
        if(!hasCustomizedConfKey(conf_profile_prefix + min_nodes_profile_field)) {
            dbg_default_error("{} not found in config.", conf_profile_prefix + min_nodes_profile_field);
            return {};
        }
        if(!hasCustomizedConfKey(conf_profile_prefix + max_nodes_profile_field)) {
            dbg_default_error("{} not found in config.", conf_profile_prefix + max_nodes_profile_field);
            return {};
        }
        min_nodes_by_shard.emplace_back(getConfUInt32(conf_profile_prefix + min_nodes_profile_field));
        max_nodes_by_shard.emplace_back(getConfUInt32(conf_profile_prefix + max_nodes_profile_field));
    }
    return custom_shards_policy(min_nodes_by_shard, max_nodes_by_shard,
                                delivery_modes_by_shard, profiles_by_shard);
}

ShardAllocationPolicy custom_shards_policy(const std::vector<int>& min_nodes_by_shard,
                                           const std::vector<int>& max_nodes_by_shard,
                                           const std::vector<Mode>& delivery_modes_by_shard,
                                           const std::vector<std::string>& profiles_by_shard) {
    return ShardAllocationPolicy{static_cast<int>(min_nodes_by_shard.size()), false, -1, -1,
                                 Mode::ORDERED, "default", min_nodes_by_shard, max_nodes_by_shard,
                                 delivery_modes_by_shard, profiles_by_shard};
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
    dbg_default_trace("Ready to calculate size");
    std::map<std::type_index, std::vector<std::vector<uint32_t>>> shard_sizes
            = compute_standard_shard_sizes(subgroup_type_order, prev_view, curr_view);
    //Now we can go through and actually allocate nodes to each shard,
    //knowing exactly how many nodes they will get

    dbg_default_trace("Ready to really assign nodes");
    if(!prev_view) {
        /* allocate_standard_subgroup_type is invoked when we have no prev_view, and thus
         * next_unassigned_rank is 0. If we have reserved node_ids, we need to rearrange
         * node_ids in curr_view.members into two "parts": the first part holds current
         * active reserved node_ids, while the second part holds normal node_ids. We then
         * rearrange next_unassigned_rank to be the length of the first part, since nodes
         * in the first part are inherent nodes for some shards, and definitely will be
         * assigned, sometimes more than once if we want to overlap shards.
         */
        // We cannot modify curr_view.members in-place, which will corrupt curr_view.my_rank, curr_view.node_id_to_rank, etc. Besides, View::members is const.
        std::vector<node_id_t> curr_members;
        std::set<node_id_t> curr_member_set(curr_view.members.begin(), curr_view.members.end());
        dbg_default_trace("Initial curr_view.next_unassigned_rank is {}", curr_view.next_unassigned_rank);
        if(all_reserved_node_ids.size() > 0) {
            std::set_intersection(
                    curr_member_set.begin(), curr_member_set.end(),
                    all_reserved_node_ids.begin(), all_reserved_node_ids.end(),
                    std::inserter(curr_members, curr_members.end()));
            curr_view.next_unassigned_rank = curr_members.size();
            dbg_default_trace("After rearranging inherent node_ids, curr_view.next_unassigned_rank is {}", curr_view.next_unassigned_rank);
            std::set_difference(
                    curr_member_set.begin(), curr_member_set.end(),
                    all_reserved_node_ids.begin(), all_reserved_node_ids.end(),
                    std::inserter(curr_members, curr_members.end()));
        } else {
            curr_members = curr_view.members;
        }

        for(const auto& subgroup_type : subgroup_type_order) {
            //Ignore cross-product-allocated types
            if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
                continue;
            }
            dbg_default_trace("Without prev_view, assign node to type {}", std::string(subgroup_type.name()));

            subgroup_layouts[subgroup_type] = allocate_standard_subgroup_type(
                    subgroup_type, curr_view, shard_sizes, curr_members, curr_member_set);
        }
    } else {
        // surviving_member_set holds non-failed node_ids from prev_view,
        // added_member_set holds newly added node_ids in curr_view.
        std::set<node_id_t> surviving_member_set(curr_view.members.begin(), curr_view.members.begin() + curr_view.next_unassigned_rank);
        std::set<node_id_t> added_member_set(curr_view.members.begin() + curr_view.next_unassigned_rank, curr_view.members.end());

        /* update_standard_subgroup_type is invoked if there is a prev_view, and the
         * curr_view.members is already arranged into two parts: the first part holds
         * surviving nodes from prev_view, and the second part holds newly added nodes.
         * The next_unassigned_rank is the length of the first part. If we have reserved
         * node_ids, we need to rearrange curr_view.members into 2 parts: the first part
         * holds inherent node_ids for shards, which is composed with surviving node_ids
         * and reserved node_ids the second part holds newly added non-reserved node_ids.
         * We then rearrange next_unassigned_rank to be the length of the first part,
         * since they will definitely be assigned.
         */
        std::vector<node_id_t> curr_members;
        std::set<node_id_t> curr_member_set(curr_view.members.begin(), curr_view.members.end());
        dbg_default_trace("Initial curr_view.next_unassigned_rank is {}", curr_view.next_unassigned_rank);
        if(all_reserved_node_ids.size() > 0) {
            std::set<node_id_t> active_reserved_node_id_set;
            std::set_intersection(
                    curr_member_set.begin(), curr_member_set.end(),
                    all_reserved_node_ids.begin(), all_reserved_node_ids.end(),
                    std::inserter(active_reserved_node_id_set, active_reserved_node_id_set.end()));
            std::set_union(
                    surviving_member_set.begin(), surviving_member_set.end(),
                    active_reserved_node_id_set.begin(), active_reserved_node_id_set.end(),
                    std::inserter(curr_members, curr_members.end()));
            dbg_default_trace("With inherent nodes, curr_members is: {}", curr_members);

            curr_view.next_unassigned_rank = curr_members.size();
            dbg_default_trace("After rearranging inherent node_ids, curr_view.next_unassigned_rank is {}", curr_view.next_unassigned_rank);

            std::set_difference(
                    added_member_set.begin(), added_member_set.end(),
                    all_reserved_node_ids.begin(), all_reserved_node_ids.end(),
                    std::inserter(curr_members, curr_members.end()));
            dbg_default_trace("Adding newly added non-reserved nodes, curr_members is: {}", curr_members);
        }

        for(uint32_t subgroup_type_id = 0; subgroup_type_id < subgroup_type_order.size();
            ++subgroup_type_id) {
            //We need to both iterate through this vector and keep the counter in order to know the type IDs
            const std::type_index& subgroup_type = subgroup_type_order[subgroup_type_id];
            if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
                continue;
            }
            dbg_default_trace("With prev_view, assigning node to type {}", std::string(subgroup_type.name()));
            subgroup_layouts[subgroup_type] = update_standard_subgroup_type(
                    subgroup_type, subgroup_type_id,
                    prev_view, curr_view, shard_sizes,
                    surviving_member_set,
                    added_member_set,
                    curr_members, curr_member_set);
        }
    }
}

std::map<std::type_index, std::vector<std::vector<uint32_t>>>
DefaultSubgroupAllocator::compute_standard_shard_sizes(
        const std::vector<std::type_index>& subgroup_type_order,
        const std::unique_ptr<View>& prev_view,
        const View& curr_view) const {
    //First, determine how many nodes we will need for a minimal allocation
    int nodes_needed = 0;

    // If there are reserved node_ids, and some appear in curr_view, calculate the
    // intersection and count them once, in case that we want shard overlapping.
    std::set<node_id_t> all_active_reserved_node_id_set;
    std::set<node_id_t> curr_member_set(curr_view.members.begin(), curr_view.members.end());
    if(all_reserved_node_ids.size() > 0) {
        std::set_intersection(
                curr_member_set.begin(), curr_member_set.end(),
                all_reserved_node_ids.begin(), all_reserved_node_ids.end(),
                std::inserter(all_active_reserved_node_id_set, all_active_reserved_node_id_set.begin()));

        dbg_default_trace("Parsing all_active_reserved_node_id_set: {}", all_active_reserved_node_id_set);

        nodes_needed = all_active_reserved_node_id_set.size();
        dbg_default_trace("After counting all_active_reserved_node_id_set, nodes_needed is {}", nodes_needed);
    }

    std::map<std::type_index, std::vector<std::vector<uint32_t>>> shard_sizes;
    for(uint32_t subgroup_type_id = 0; subgroup_type_id < subgroup_type_order.size(); ++subgroup_type_id) {
        const std::type_index& subgroup_type = subgroup_type_order[subgroup_type_id];
        //Get the policy for this subgroup type, if and only if it's a "standard" policy
        if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
            continue;
        }
        const SubgroupAllocationPolicy& subgroup_type_policy
                = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));

        shard_sizes.emplace(subgroup_type,
                            std::vector<std::vector<uint32_t>>(subgroup_type_policy.num_subgroups));
        for(int subgroup_num = 0; subgroup_num < subgroup_type_policy.num_subgroups; ++subgroup_num) {
            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];
            shard_sizes[subgroup_type][subgroup_num].resize(sharding_policy.num_shards);
            for(int shard_num = 0; shard_num < sharding_policy.num_shards; ++shard_num) {
                size_t min_shard_size = sharding_policy.even_shards ? sharding_policy.min_nodes_per_shard
                                                                    : sharding_policy.min_num_nodes_by_shard[shard_num];

                /* With reserved nodes, we do not assign nodes evenly across shards.
                 * All current nodes may be occupied by 1 shard because it reserved all of them.
                 * Therefore we need to check if min_shard_size for each shard is satisfied,
                 * and thus we need to maintain nodes_needed more carefully.
                 */

                dbg_default_trace("Calculate node size for type {}, subgroup_num {}, shard_num {}", std::string(subgroup_type.name()), subgroup_num, shard_num);

                std::set<node_id_t> survived_node_set;
                //If there was a previous view, we must include all non-failed nodes from that view
                if(prev_view) {
                    const subgroup_id_t previous_assignment_offset
                            = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
                    const SubView& previous_shard_assignment
                            = prev_view->subgroup_shard_views[previous_assignment_offset + subgroup_num]
                                                             [shard_num];
                    for(std::size_t rank = 0; rank < previous_shard_assignment.members.size(); ++rank) {
                        if(curr_view.rank_of(previous_shard_assignment.members[rank]) != -1) {
                            survived_node_set.insert(previous_shard_assignment.members[rank]);
                        }
                    }
                }

                // Check whether this shard reserves existing nodes.
                std::set<node_id_t> active_reserved_node_id_set;

                if(sharding_policy.reserved_node_ids_by_shard.size() > 0) {
                    std::set_intersection(
                            sharding_policy.reserved_node_ids_by_shard[shard_num].begin(),
                            sharding_policy.reserved_node_ids_by_shard[shard_num].end(),
                            curr_member_set.begin(),
                            curr_member_set.end(),
                            std::inserter(active_reserved_node_id_set, active_reserved_node_id_set.begin()));
                } else {
                    dbg_default_trace("There is no reserved node_id configured.");
                }

                dbg_default_trace("The active_reserved_node_id_set for current shard is: {}", active_reserved_node_id_set);

                /* The inherent_node_id_set holds node_ids that are "inherent" or "intrinsic"
                 * to the this shard, for the node_ids are either surviving nodes from "the same shard"
                 * in the prev_view or reserved for this shard, or both.
                 */
                std::set<node_id_t> inherent_node_id_set;
                std::set_union(
                        survived_node_set.begin(), survived_node_set.end(),
                        active_reserved_node_id_set.begin(), active_reserved_node_id_set.end(),
                        std::inserter(inherent_node_id_set, inherent_node_id_set.end()));
                dbg_default_trace("The inherent_node_id_set for current shard is: {}", inherent_node_id_set);
                // All active reserved nodes just count once.
                nodes_needed += inherent_node_id_set.size() - active_reserved_node_id_set.size();

                if(inherent_node_id_set.size() >= min_shard_size) {
                    min_shard_size = inherent_node_id_set.size();
                } else {
                    nodes_needed += min_shard_size - inherent_node_id_set.size();
                }

                // If we add a lot of nodes reserved for a shard, the number of which is larger than this shard's max_num_nodes, we will still add those nodes to it.
                // Seems OK?
                shard_sizes[subgroup_type][subgroup_num][shard_num] = min_shard_size;
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
        for(const auto& subgroup_type : subgroup_type_order) {
            if(!std::holds_alternative<SubgroupAllocationPolicy>(policies.at(subgroup_type))) {
                continue;
            }
            const SubgroupAllocationPolicy& subgroup_type_policy
                    = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));
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
                    if(shard_sizes[subgroup_type][subgroup_num][shard_num] < max_shard_members) {
                        shard_sizes[subgroup_type][subgroup_num][shard_num]++;
                        nodes_needed++;
                    }
                    all_at_max = all_at_max
                                 && shard_sizes[subgroup_type][subgroup_num][shard_num]
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
        const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes,
        const std::vector<node_id_t>& curr_members,
        const std::set<node_id_t>& curr_member_set) const {
    //The size of shard_sizes[subgroup_type] is the number of subgroups of this type
    subgroup_shard_layout_t subgroup_allocation(shard_sizes.at(subgroup_type).size());

    const SubgroupAllocationPolicy& subgroup_type_policy
            = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));

    for(uint32_t subgroup_num = 0; subgroup_num < subgroup_allocation.size(); ++subgroup_num) {
        //The size of shard_sizes[subgroup_type][subgroup_num] is the number of shards
        for(uint32_t shard_num = 0; shard_num < shard_sizes.at(subgroup_type)[subgroup_num].size();
            ++shard_num) {
            uint32_t shard_size = shard_sizes.at(subgroup_type)[subgroup_num][shard_num];
            std::vector<node_id_t> desired_nodes;

            //The allocation policy for this subgroup is either the shard_policy_by_subgroup entry at subgroup_num,
            //or the first entry in shard_policy_by_subgroup if identical_subgroups is true
            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];

            dbg_default_trace("Subgroup {}, shard {}, is assigned {} nodes", subgroup_num, shard_num, shard_size);

            // Allocate active reserved nodes first.
            if(sharding_policy.reserved_node_ids_by_shard.size() > 0) {
                const std::set<node_id_t> reserved_node_id_set(
                        sharding_policy.reserved_node_ids_by_shard[shard_num].begin(),
                        sharding_policy.reserved_node_ids_by_shard[shard_num].end());
                if(reserved_node_id_set.size() > 0) {
                    std::set_intersection(
                            reserved_node_id_set.begin(), reserved_node_id_set.end(),
                            curr_member_set.begin(), curr_member_set.end(),
                            std::inserter(desired_nodes, desired_nodes.end()));
                    shard_size -= desired_nodes.size();
                    dbg_default_trace("Assigning shard {} active reserved nodes: {}", desired_nodes.size(), desired_nodes);
                }
            } else {
                dbg_default_trace("There is no reserved node_id configured.");
            }

            //Grab the next shard_size nodes
            desired_nodes.insert(desired_nodes.end(),
                                 &curr_members[curr_view.next_unassigned_rank],
                                 &curr_members[curr_view.next_unassigned_rank + shard_size]);
            // NOTE: If there are unassigned reserved nodes (which should not happen in regular use), next_unassigned_rank only points to
            // unassigned normal nodes, which I (Lichen) think is just OK and not in conflict with its definition.
            curr_view.next_unassigned_rank += shard_size;

            dbg_default_trace("Assigning shard {} nodes in total, with curr_view.next_unassigned_rank {}: {}", desired_nodes.size(), curr_view.next_unassigned_rank, desired_nodes);

            // Figure out the sender list
            std::vector<int> is_sender;
            if (sharding_policy.reserved_sender_ids_by_shard.size() > 0 &&
                !sharding_policy.reserved_sender_ids_by_shard[shard_num].empty()) {
                for(const auto& nid: desired_nodes) {
                    if (sharding_policy.reserved_sender_ids_by_shard[shard_num].find(nid) != sharding_policy.reserved_sender_ids_by_shard[shard_num].end()) {
                        // contained in sender list
                        is_sender.push_back(true);
                    } else {
                        is_sender.push_back(false);
                    }
                }
            }

            //Figure out what the Mode policy for this shard is
            Mode delivery_mode = sharding_policy.even_shards
                                         ? sharding_policy.shards_mode
                                         : sharding_policy.modes_by_shard[shard_num];
            std::string profile = sharding_policy.shards_profile;
            if(!sharding_policy.even_shards) {
                profile = sharding_policy.profiles_by_shard[shard_num];
            }
            //Put the SubView at the end of subgroup_allocation[subgroup_num]
            //Since we go through shards in order, this is at index shard_num
            subgroup_allocation[subgroup_num].emplace_back(
                    curr_view.make_subview(desired_nodes, delivery_mode, is_sender, profile));
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
        const std::set<node_id_t>& surviving_member_set,
        const std::set<node_id_t>& added_member_set,
        const std::vector<node_id_t>& curr_members,
        const std::set<node_id_t>& curr_member_set) const {
    /* Subgroups of the same type will have contiguous IDs because they were created in order.
     * So the previous assignment is the slice of the previous subgroup_shard_views vector
     * starting at the first subgroup's ID, and extending for num_subgroups entries.
     */
    const subgroup_id_t previous_assignment_offset = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)[0];
    subgroup_shard_layout_t next_assignment(shard_sizes.at(subgroup_type).size());

    dbg_default_trace("The surviving_member_set is: {}", surviving_member_set);
    dbg_default_trace("The added_member_set is: {}", added_member_set);

    const SubgroupAllocationPolicy& subgroup_type_policy
            = std::get<SubgroupAllocationPolicy>(policies.at(subgroup_type));

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
            dbg_default_trace("Subgroup {}, shard {}, is assigned {} nodes", subgroup_num, shard_num, allocated_shard_size);

            //Add all the non-failed nodes from the previous assignment
            for(std::size_t rank = 0; rank < previous_shard_assignment.members.size(); ++rank) {
                if(curr_view.rank_of(previous_shard_assignment.members[rank]) == -1) {
                    continue;
                }
                next_shard_members.push_back(previous_shard_assignment.members[rank]);
                next_is_sender.push_back(previous_shard_assignment.is_sender[rank]);
            }
            dbg_default_trace("After assigning surviving nodes, next_shard_members is: {}", next_shard_members);

            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];

            //Add newly added reserved nodes
            if(sharding_policy.reserved_node_ids_by_shard.size() > 0) {
                std::set<node_id_t> added_reserved_node_id_set;
                std::set_intersection(
                        added_member_set.begin(), added_member_set.end(),
                        sharding_policy.reserved_node_ids_by_shard[shard_num].begin(),
                        sharding_policy.reserved_node_ids_by_shard[shard_num].end(),
                        std::inserter(added_reserved_node_id_set, added_reserved_node_id_set.end()));
                if(added_reserved_node_id_set.size() > 0) {
                    dbg_default_trace("The added_reserved_node_id_set is not empty: {}", added_reserved_node_id_set);

                    for(auto node_id : added_reserved_node_id_set) {
                        next_shard_members.push_back(node_id);
                        if (sharding_policy.reserved_sender_ids_by_shard[shard_num].empty()) {
                            // we didn't specify a sender list for this shard. So all nodes can send.
                            next_is_sender.push_back(true);
                        } else {
                            if (sharding_policy.reserved_sender_ids_by_shard[shard_num].find(node_id)!=
                                sharding_policy.reserved_sender_ids_by_shard[shard_num].end()) {
                                next_is_sender.push_back(true);
                            } else {
                                next_is_sender.push_back(false);
                            }
                        }
                    }
                }
                dbg_default_trace("After assigning newly added reserved nodes, we get {} inherent node_id(s) assigned, next_shard_members is: {}", next_shard_members.size(), next_shard_members);
            } else {
                dbg_default_trace("There is no reserved node_id configured.");
            }

            //Add additional members if needed
            while(next_shard_members.size() < allocated_shard_size) {
                //This must be true if compute_standard_shard_sizes said our view was adequate
                assert(curr_view.next_unassigned_rank < (int)curr_view.members.size());
                next_shard_members.push_back(curr_view.members[curr_view.next_unassigned_rank]);
                curr_view.next_unassigned_rank++;
                //If senders are not specified, all nodes are senders; otherwise, additional members are not senders.
                next_is_sender.push_back(sharding_policy.reserved_sender_ids_by_shard.empty()?
                        true : sharding_policy.reserved_sender_ids_by_shard[shard_num].empty());
            }
            dbg_default_trace("Assigned shard {} nodes in total, with curr_view.next_unassigned_rank {}: {}", next_shard_members.size(), curr_view.next_unassigned_rank, next_shard_members);

            next_assignment[subgroup_num].emplace_back(curr_view.make_subview(next_shard_members,
                                                                              previous_shard_assignment.mode,
                                                                              next_is_sender,
                                                                              previous_shard_assignment.profile));
        }
    }
    return next_assignment;
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

DefaultSubgroupAllocator::DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types, const json& layout_array) {
    for(std::size_t subgroup_type_index = 0; subgroup_type_index < subgroup_types.size(); ++subgroup_type_index) {
        policies.emplace(subgroup_types[subgroup_type_index],
                         parse_json_subgroup_policy(layout_array[subgroup_type_index], all_reserved_node_ids));
    }
}

DefaultSubgroupAllocator::DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types, const std::string& json_file_path) {
    json layout_array;

    std::ifstream json_file_stream(json_file_path);
    if(!json_file_stream) {
        //Hopefully this won't be necessary because Conf::initialize checked that the file exists
        throw derecho_exception("Failed to initialize subgroup allocator! JSON layout file " + json_file_path + " not found");
    }

    json_file_stream >> layout_array;
    for(std::size_t subgroup_type_index = 0; subgroup_type_index < subgroup_types.size(); ++subgroup_type_index) {
        policies.emplace(subgroup_types[subgroup_type_index],
                         parse_json_subgroup_policy(layout_array[subgroup_type_index], all_reserved_node_ids));
    }
}

DefaultSubgroupAllocator::DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types) {
    //It's not possible to delegate to a different constructor based on a boolean,
    //so I have to copy and paste from the other two constructors
    if(hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT)) {
        json layout_array = json::parse(getConfString(CONF_LAYOUT_JSON_LAYOUT));
        for(std::size_t subgroup_type_index = 0; subgroup_type_index < subgroup_types.size(); ++subgroup_type_index) {
            policies.emplace(subgroup_types[subgroup_type_index],
                             parse_json_subgroup_policy(layout_array[subgroup_type_index], all_reserved_node_ids));
        }
    } else if(hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT_FILE)) {
        json layout_array;

        std::ifstream json_file_stream(getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE));
        if(!json_file_stream) {
            throw derecho_exception("Failed to initialize subgroup allocator! JSON layout file " + getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE) + " not found");
        }

        json_file_stream >> layout_array;
        for(std::size_t subgroup_type_index = 0; subgroup_type_index < subgroup_types.size(); ++subgroup_type_index) {
            policies.emplace(subgroup_types[subgroup_type_index],
                             parse_json_subgroup_policy(layout_array[subgroup_type_index], all_reserved_node_ids));
        }
    } else {
        throw derecho_exception("Either json_layout or json_layout_file is required when constructing DefaultSubgroupAllocator with no arguments");
    }
}

SubgroupAllocationPolicy parse_json_subgroup_policy(const json& jconf, std::set<node_id_t>& all_reserved_node_ids) {
    if(!jconf.is_object() || !jconf[json_layout_field].is_array()) {
        dbg_default_error("parse_json_subgroup_policy cannot parse {}.", jconf.get<std::string>());
        throw derecho_exception("parse_json_subgroup_policy cannot parse" + jconf.get<std::string>());
    }

    SubgroupAllocationPolicy subgroup_allocation_policy;
    subgroup_allocation_policy.identical_subgroups = false;
    subgroup_allocation_policy.num_subgroups = jconf[json_layout_field].size();
    subgroup_allocation_policy.shard_policy_by_subgroup = std::vector<ShardAllocationPolicy>();

    for(auto subgroup_it : jconf[json_layout_field]) {
        ShardAllocationPolicy shard_allocation_policy;
        size_t num_shards = subgroup_it[min_nodes_by_shard_field].size();
        if(subgroup_it[max_nodes_by_shard_field].size() != num_shards
           || subgroup_it[delivery_modes_by_shard_field].size() != num_shards
           || subgroup_it[profiles_by_shard_field].size() != num_shards ||
           // "reserved_node_ids_by_shard" is not a mandatory field
           (subgroup_it[reserved_node_ids_by_shard_field].size() != 0
            && subgroup_it[reserved_node_ids_by_shard_field].size() != num_shards)) {
            dbg_default_error("parse_json_subgroup_policy: shards does not match in at least one subgroup: {}",
                              subgroup_it.get<std::string>());

            throw derecho_exception("parse_json_subgroup_policy: shards does not match in at least one subgroup:" + subgroup_it.get<std::string>());
        }
        shard_allocation_policy.even_shards = false;
        shard_allocation_policy.num_shards = num_shards;
        std::vector<int> min_num_nodes_by_shard,max_num_nodes_by_shard;
        for(const auto& num:subgroup_it[min_nodes_by_shard_field].get<std::vector<std::string>>()) {
            min_num_nodes_by_shard.push_back(std::stoi(num));
        }
        for(const auto& num:subgroup_it[max_nodes_by_shard_field].get<std::vector<std::string>>()) {
            max_num_nodes_by_shard.push_back(std::stoi(num));
        }
        shard_allocation_policy.min_num_nodes_by_shard = std::move(min_num_nodes_by_shard);
        shard_allocation_policy.max_num_nodes_by_shard = std::move(max_num_nodes_by_shard);
        std::vector<Mode> delivery_modes_by_shard;
        for(auto it : subgroup_it[delivery_modes_by_shard_field]) {
            if(it == delivery_mode_raw) {
                shard_allocation_policy.modes_by_shard.push_back(Mode::UNORDERED);
            } else {
                shard_allocation_policy.modes_by_shard.push_back(Mode::ORDERED);
            }
        }
        shard_allocation_policy.profiles_by_shard = subgroup_it[profiles_by_shard_field].get<std::vector<std::string>>();

        // "reserved_node_ids_by_shard" is not a mandatory field
        if(subgroup_it.contains(reserved_node_ids_by_shard_field)) {
            auto reserved_nodes_and_senders = subgroup_it[reserved_node_ids_by_shard_field].get<std::vector<std::set<std::string>>>();
            
            for (const auto& per_shard_set : reserved_nodes_and_senders) {
                std::set<node_id_t> nodes,senders;
                for (const auto& node_string : per_shard_set) {
                    if (node_string.at(0) == reserved_node_is_sender_tag) {
                        node_id_t nid = static_cast<node_id_t>(std::stoi(node_string.substr(1)));
                        nodes.insert(nid);
                        senders.insert(nid);
                    } else {
                        nodes.insert(static_cast<node_id_t>(std::stoi(node_string)));
                    }
                }
                shard_allocation_policy.reserved_node_ids_by_shard.emplace_back(std::move(nodes));
                shard_allocation_policy.reserved_sender_ids_by_shard.emplace_back(std::move(senders));
            }

            for(const auto& reserved_id_set : shard_allocation_policy.reserved_node_ids_by_shard) {
                std::set_union(all_reserved_node_ids.begin(), all_reserved_node_ids.end(), reserved_id_set.begin(), reserved_id_set.end(), std::inserter(all_reserved_node_ids, all_reserved_node_ids.begin()));
            }
            /*
			 * Make sure that no shards inside a subgroup reserve the same node IDs. Shards in
			 * different subgroups of the same type or from different types can share nodes,
			 * which is one reason to use the reserved_node_id feature. For example, we can
             * assign 2 subgroups for type "PersistentCascadeStoreWithStringKey" to store the
			 * data and model respectively for an ML application, and actually reserve the
             * same node IDs for shards in these two subgroups. This way the data and the model
			 * coexist on the same node, reducing the amount of network traffic between them.
			 */
            std::set<node_id_t> intersect_reserved_node_ids_in_subgroup;
            std::set<node_id_t> temp(shard_allocation_policy.reserved_node_ids_by_shard[0]);
            for(int shard_num = 1; shard_num < shard_allocation_policy.num_shards; ++shard_num) {
                intersect_reserved_node_ids_in_subgroup = std::set<node_id_t>();
                std::set_intersection(
                        temp.begin(), temp.end(),
                        shard_allocation_policy.reserved_node_ids_by_shard[shard_num].begin(),
                        shard_allocation_policy.reserved_node_ids_by_shard[shard_num].end(),
                        std::inserter(intersect_reserved_node_ids_in_subgroup,
                                      intersect_reserved_node_ids_in_subgroup.begin()));
            }
            if(intersect_reserved_node_ids_in_subgroup.size() > 0) {
                throw derecho_exception("Shards in one subgroup have the same reserved node_ids!");
            }
        } else {
            dbg_default_trace("There is no reserved node_id configured.");
        }

        subgroup_allocation_policy.shard_policy_by_subgroup.emplace_back(std::move(shard_allocation_policy));
    }
    return subgroup_allocation_policy;
}

void check_reserved_node_id_pool(const std::map<std::type_index, SubgroupPolicyVariant>& dsa_map) {
    for(auto& item : dsa_map) {
        if(!std::holds_alternative<SubgroupAllocationPolicy>(item.second)) {
            continue;
        }
        const SubgroupAllocationPolicy& subgroup_type_policy
                = std::get<SubgroupAllocationPolicy>(item.second);
        for(int subgroup_num = 0; subgroup_num < subgroup_type_policy.num_subgroups; ++subgroup_num) {
            const ShardAllocationPolicy& sharding_policy
                    = subgroup_type_policy.identical_subgroups
                              ? subgroup_type_policy.shard_policy_by_subgroup[0]
                              : subgroup_type_policy.shard_policy_by_subgroup[subgroup_num];
            if(sharding_policy.reserved_node_ids_by_shard.size() > 0) {
                //Make sure that no shards inside a subgroup reserve the same node IDs
                std::set<node_id_t> intersect_reserved_node_ids_in_subgroup;
                std::set<node_id_t> temp(sharding_policy.reserved_node_ids_by_shard[0]);
                for(int shard_num = 1; shard_num < sharding_policy.num_shards; ++shard_num) {
                    intersect_reserved_node_ids_in_subgroup = std::set<node_id_t>();
                    std::set_intersection(
                            temp.begin(), temp.end(),
                            sharding_policy.reserved_node_ids_by_shard[shard_num].begin(),
                            sharding_policy.reserved_node_ids_by_shard[shard_num].end(),
                            std::inserter(intersect_reserved_node_ids_in_subgroup,
                                          intersect_reserved_node_ids_in_subgroup.begin()));
                }
                if(intersect_reserved_node_ids_in_subgroup.size() > 0) {
                    throw derecho_exception("Shards in one subgroup have same reserved node_ids!");
                }
            }
        }
    }
}

}  // namespace derecho
