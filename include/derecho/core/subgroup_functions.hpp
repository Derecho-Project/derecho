/**
 * @file subgroup_functions.h
 *
 * @date Feb 28, 2017
 */

#pragma once

#include <memory>
#include <variant>

#include "derecho_modes.hpp"
#include "detail/derecho_internal.hpp"
#include "subgroup_info.hpp"
#include "derecho/mincostflow/mincostflow.hpp"

using namespace mincostflow;
namespace derecho {

/**
 * A copy constructor for objects owned by unique_ptr. Does the obvious thing
 * and invokes the copy constructor of the object being pointed to, or returns
 * nullptr if the unique_ptr is empty.
 * @param to_copy A unique_ptr to the object to copy
 * @return A new object in a new unique_ptr that is a copy of the old object.
 */
template <typename T>
std::unique_ptr<T> deep_pointer_copy(const std::unique_ptr<T>& to_copy) {
    if(to_copy) {
        return std::make_unique<T>(*to_copy);
    } else {
        return nullptr;
    }
}

/**
 * A simple implementation of shard_view_generator_t that creates a single,
 * un-sharded subgroup containing all the members of curr_view for every subgroup
 * type in the list. This is best used when there is only one subgroup type.
 */
subgroup_allocation_map_t one_subgroup_entire_view(const std::vector<std::type_index>& subgroup_type_order,
                                                   const std::unique_ptr<View>& prev_view, View& curr_view);
/**
 * A simple implementation of shard_view_generator_t that returns a single,
 * un-sharded subgroup in Unordered (Raw) mode containing all the members of
 * curr_view for every type in the list. This is best used when there is only
 * one subgroup type.
 */
subgroup_allocation_map_t one_subgroup_entire_view_raw(const std::vector<std::type_index>& subgroup_type_order,
                                                       const std::unique_ptr<View>& prev_view, View& curr_view);

struct ShardAllocationPolicy {
    /** The number of shards; set to 1 for a non-sharded subgroup */
    int num_shards;
    /** Whether all shards should contain the same number of members. */
    bool even_shards;
    /** If even_shards is true, this is the minimum number of nodes per shard.
     * (Ignored if even_shards is false). */
    int min_nodes_per_shard;
    /** If even_shards is true, this is the maximum number of nodes per shard. */
    int max_nodes_per_shard;
    /** Minimum number of distinct failure correlation sets that nodes in this shard must be in. */
    int min_fcs;
    /** If even_shards is true, this is the delivery mode that will be used for
     * every shard. (Ignored if even_shards is false). */
    Mode shards_mode;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the minimum number of members it should have.
     * (Ignored if even_shards is true). */
    std::vector<int> min_num_nodes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the maximum number of members it should have. */
    std::vector<int> max_num_nodes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the minimum number of members from distinct failure correlation sets it should have. */
    std::vector<int> min_fcs_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating which delivery mode it should use. (Ignored if even_shards is
     * true). */
    std::vector<Mode> modes_by_shard;
};

/**
 * A data structure defining the parameters of the default subgroup allocation
 * function for a single subgroup type.
 */
struct SubgroupAllocationPolicy {
    /** The number of subgroups of the same Replicated type to create */
    int num_subgroups;
    /** Whether all subgroups of this type will have an identical shard layout */
    bool identical_subgroups;
    /** If identical_subgroups is true, contains a single entry with the allocation
     * policy for all subgroups of this type. If identical_subgroups is false,
     * contains an entry for each subgroup describing that subgroup's shards. */
    std::vector<ShardAllocationPolicy> shard_policy_by_subgroup;
};

/**
 * An alternate type of subgroup allocation policy for subgroup types whose
 * membership will be defined as a cross-product of other subgroups. Each node
 * in the source subgroup will be placed in T subgroups, one for each shard in
 * the target subgroup (the target subgroup has T shards). Thus, if there are S
 * members in the source subgroup, and T shards in the target subgroup, S * T
 * subgroups of a single type will be created. The nodes in the source subgroup
 * will be marked as the only senders in these subgroups. A node that has rank
 * i within the source subgroup can send a multicast to shard j of the target
 * subgroup by selecting the cross-product subgroup at index (i * T + j).
 */
struct CrossProductPolicy {
    /** The (type, index) pair identifying the "source" subgroup of the cross-product.
     * Each member of this subgroup will be a sender in T subgroups, where T is the
     * number of shards in the target subgroup. */
    std::pair<std::type_index, uint32_t> source_subgroup;
    /** The (type, index) pair identifying the "target" subgroup of the cross-product.
     * Each shard in this subgroup will have all of its members assigned to S subgroups
     * as receivers, where S is the number of members in the source subgroup. */
    std::pair<std::type_index, uint32_t> target_subgroup;
};

/* Helper functions that construct ShardAllocationPolicy values for common cases. */

/**
 * Returns a ShardAllocationPolicy that specifies num_shards "flexible" or
 * fault-tolerant shards, each of which has the same minimum number of nodes
 * and maximuim number of nodes
 * @param num_shards The number of shards to request in this policy
 * @param min_nodes_per_shard The minimum number of nodes that each shard can have
 * @param max_nodes_per_shard The maximum number of nodes that each shard can have
 * @param min_failure_maps The minimum number of distinct failure maps that nodes in this shard
 * must be from
 * @return A ShardAllocationPolicy value with these parameters
 */
ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard, int min_fcs = 1);

/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard; each shard must have
 * exactly nodes_per_shard members.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @param min_failure_maps The minimum number of distinct failure maps that nodes in this shard
 * must be from
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard, int min_fcs = 1);
/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard, and every shard running in
 * "raw" delivery mode.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @param min_failure_maps The minimum number of distinct failure maps that nodes in this shard
 * must be from
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard, int min_fcs = 1);
/**
 * Returns a ShardAllocationPolicy for a subgroup that has a different number of
 * members in each shard, and possibly has each shard in a different delivery mode.
 * Note that the parameter vectors must all be the same length.
 * @param min_nodes_by_shard A vector specifying the minimum number of nodes for each
 * shard; the ith shard must have at least min_nodes_by_shard[i] members.
 * @param max_nodes_by_shard A vector specifying the maximum number of nodes for each
 * shard; the ith shard can have up to max_nodes_by_shard[i] members.
 * @param min_failure_maps_by_shard The minimum number of distinct failure maps that nodes in each
 * shard must be from; the ith shard must have nodes from at least min_failure_maps_by_shard[i] unique
 * failure maps.
 * @param delivery_modes_by_shard A vector specifying the delivery mode (Raw or
 * Ordered) for each shard, in the same order as the other vectors.
 * @return A ShardAllocationPolicy that specifies these shard sizes and modes.
 */
ShardAllocationPolicy custom_shards_policy(const std::vector<int>& min_nodes_by_shard,
                                           const std::vector<int>& max_nodes_by_shard,
                                           const std::vector<int>& min_fcs_by_shard,
                                           const std::vector<Mode>& delivery_modes_by_shard);

/**
 * Returns a SubgroupAllocationPolicy for a replicated type that only has a
 * single subgroup. The ShardAllocationPolicy argument can be the result of
 * one of the ShardAllocationPolicy helper functions.
 * @param policy The allocation policy to use for the single subgroup.
 * @return A SubgroupAllocationPolicy for a single-subgroup type.
 */
SubgroupAllocationPolicy one_subgroup_policy(const ShardAllocationPolicy& policy);

/**
 * Returns a SubgroupAllocationPolicy for a replicated type that needs n
 * subgroups with identical sharding policies.
 * @param num_subgroups The number of subgroups to create.
 * @param subgroup_policy The policy to use for sharding each subgroup.
 * @return A SubgroupAllocationPolicy for a replicated type with num_subgroups
 * copies of the same subgroup.
 */
SubgroupAllocationPolicy identical_subgroups_policy(int num_subgroups, const ShardAllocationPolicy& subgroup_policy);

/**
 * Returns a ShardAllocationPolicy from a SubgroupAllocationPolicy.
 * @param subgroup_type_policy The policy of the subgroup.
 * @param subgroup_num If the subgroups do not have identical policies, the index of the subgroup.
 * @return A ShardAllocationPolicy corresponding to the subgroup.
 */
ShardAllocationPolicy shard_policy_from_subgroup(const SubgroupAllocationPolicy& subgroup_type_policy, int subgroup_num);

/**
 * Returns the delivery mode of messages in the shard.
 * @param sharding_policy The policy of the shard.
 * @param shard_num If the shards do not have identical policies, the index of the shard.
 * @return The delivery mode of messages.
 */
Mode mode_from_shard_policy(ShardAllocationPolicy sharding_policy, int shard_num);

/**
 * Functor of type shard_view_generator_t that implements the default subgroup
 * allocation algorithm, parameterized based on a policy for each subgroup type
 * (i.e. Replicated Object type). Its operator() will throw a
 * subgroup_provisioning_exception if there are not enough nodes in the current
 * view to populate all of the subgroups and shards.
 */
class DefaultSubgroupAllocator {
protected:
    /**
     * The entry for each type of subgroup is either a SubgroupAllocationPolicy
     * if that type should use the standard "partitioning" allocator, or a
     * CrossProductPolicy if that type should use the "cross-product" allocator
     * instead.
     */
    const std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> policies;

    /**
     * Determines how many members each shard can have in the current view, based
     * on each shard's policy (minimum and maximum number of nodes) and the size
     * of the current view. This function first assigns the minimum number of
     * nodes to each shard, then evenly increments every shard's size by 1 (in
     * order of subgroup_type_order and in order of shard number within each
     * subgroup) until either all shards are at their maximum size or all of the
     * View's members are accounted for. It throws a subgroup_provisioning_exception
     * if the View doesn't have enough members for even the minimum-size
     * allocation.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The previous View, if there is one. This is used to
     * ensure that every shard can keep the non-failed nodes it had in the
     * previous View, even if the even-incrementing rule would assign it fewer
     * nodes in this View.
     * @param curr_view The current View
     * @param minimize True to only satisfy shards' min constraint; false to assign extra nodes.
     * @return A map from subgroup type -> subgroup index -> shard number
     * -> number of nodes in that shard
     */
    std::map<std::type_index, std::vector<std::vector<uint32_t>>> compute_standard_shard_sizes(
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<View>& prev_view,
            const View& curr_view,
            const bool minimize = false) const;

    /**
     * Helper function for allocate_fcs_subgroup_type() and update_fcs_subgroup_type(),
     * @param subgroup_type The subgroup type to allocate members for
     * @param prev_assignment The previous node memberships. Can be empty
     * @param curr_view The current view
     * @param min_shard_sizes The map of minimum membership sizes for every subgroup and shard
     * @return A subgroup layout for this subgroup type
     */
    subgroup_shard_layout_t update_fcs(
            const std::type_index subgroup_type,
            const std::vector<std::vector<SubView>>& prev_assignment,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& min_shard_sizes) const;

    /**
     * Creates and returns an initial membership allocation for a single
     * subgroup type, based on the FCSs required by each shard. Uses greedy algorithm to allocating
     * nodes from the most populated FCSs to shards first. Then satisfies min_shard_size constraint
     * by allocating nodes from the most populated FCSs in order, allowing repetitions.
     * @param subgroup_type The subgroup type to allocate members for
     * @param curr_view The current view
     * @param min_shard_sizes The map of minimum membership sizes for every subgroup and shard
     * @return A subgroup layout for this subgroup type
     */
    subgroup_shard_layout_t allocate_fcs_subgroup_type(
            const std::type_index subgroup_type,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& min_shard_sizes) const;

    /**
     * Creates and returns a new membership allocation for a single subgroup
     * type, based on its previous allocation and its newly-assigned sizes. If the FCS requirement
     * for a shard is not met anymore, attempt to allocate a node from another FCS, from the most
     * populated FCSs in order. If the min_shard_size requirement is not satisfied, allocate a node
     * from the most populated FCSs in order, allowing repetitions.
     * @param subgroup_type The subgroup type to allocate members for
     * @param subgroup_type_id The numeric "type ID" for this subgroup type
     * (its position in the subgroup_type_order list)
     * @param prev_view The previous View, now known to be non-null
     * @param curr_view The current View
     * @param shard_sizes The map of minimum membership sizes for every subgroup and shard in curr_view
     * @return A subgroup layout for this subgroup type.
     */
    subgroup_shard_layout_t update_fcs_subgroup_type(
            const std::type_index subgroup_type,
            const subgroup_type_id_t subgroup_type_id,
            const std::unique_ptr<View>& prev_view,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes) const;

    FcsAllocator to_fcs_allocator(
            const std::type_index subgroup_type,
            const std::vector<std::vector<SubView>>& prev_assignment,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& min_shard_sizes) const;
    subgroup_shard_layout_t from_shard_vertices(
            const std::unordered_set<Shard>& shards_vertices,
            const std::type_index subgroup_type,
            const std::vector<std::vector<SubView>>& prev_assignment,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& min_shard_sizes) const;
    std::unordered_map<node_id_t, derecho::fcs_id_t> to_node_to_fcs_map(const View& view) const;
    std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>> to_fcs_to_nodes_map(
            const std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs) const;

    /**
     * Creates and returns an initial membership allocation for a single
     * subgroup type, based on the input map of shard sizes.
     * @param subgroup_type The subgroup type to allocate members for
     * @param curr_view The current view, whose next_unassigned_rank will be updated
     * @param shard_sizes The map of membership sizes for every subgroup and shard
     * @return A subgroup layout for this subgroup type
     */
    subgroup_shard_layout_t allocate_standard_subgroup_type(
            const std::type_index subgroup_type,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes) const;

    /**
     * Creates and returns a new membership allocation for a single subgroup
     * type, based on its previous allocation and its newly-assigned sizes.
     * @param subgroup_type The subgroup type to allocate members for
     * @param subgroup_type_id The numeric "type ID" for this subgroup type
     * (its position in the subgroup_type_order list)
     * @param prev_view The previous View, now known to be non-null
     * @param curr_view The current View, whose next_unassigned_rank will be updated
     * @param shard_sizes The map of membership sizes for every subgroup and shard in curr_view
     * @return A subgroup layout for this subgroup type.
     */
    subgroup_shard_layout_t update_standard_subgroup_type(
            const std::type_index subgroup_type,
            const subgroup_type_id_t subgroup_type_id,
            const std::unique_ptr<View>& prev_view,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes) const;

    /**
     * Helper function that implements the subgroup allocation algorithm for all
     * "standard" (non-cross-product) subgroups. It creates entries for those
     * subgroups in the out-parameter subgroup_layouts.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The same previous view passed in to the operator() function
     * @param curr_view The same current view passed in to the operator() function
     * @param subgroup_layouts The map of subgroup types to subgroup layouts that operator()
     * will end up returning; this function will fill in the entries for some of the types.
     */
    void compute_standard_memberships(const std::vector<std::type_index>& subgroup_type_order,
                                      const std::unique_ptr<View>& prev_view,
                                      View& curr_view,
                                      subgroup_allocation_map_t& subgroup_layouts) const;

    /**
     * Helper function that implements the subgroup allocation algorithm for all
     * cross-product subgroups. It must be run second so that it can refer to the allocations
     * created for the "standard" subgroups. It creates entries for the cross-product
     * subgroups in the out-parameter subgroup_layouts.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The same previous view passed in to the operator() function
     * @param curr_view The same current view passed in to the operator() function
     * @param subgroup_layouts The map of subgroup types to subgroup layouts that operator()
     * will end up returning; this function will fill in the entries for some of the types.
     */
    void compute_cross_product_memberships(const std::vector<std::type_index>& subgroup_type_order,
                                           const std::unique_ptr<View>& prev_view,
                                           View& curr_view,
                                           subgroup_allocation_map_t& subgroup_layouts) const;

public:
    DefaultSubgroupAllocator(const std::map<std::type_index,
                                            std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>&
                                     policies_by_subgroup_type)
            : policies(policies_by_subgroup_type) {}
    DefaultSubgroupAllocator(const DefaultSubgroupAllocator& to_copy)
            : policies(to_copy.policies) {}
    DefaultSubgroupAllocator(DefaultSubgroupAllocator&&) = default;

    subgroup_allocation_map_t operator()(const std::vector<std::type_index>& subgroup_type_order,
                                         const std::unique_ptr<View>& prev_view,
                                         View& curr_view) const;
};

}  // namespace derecho
