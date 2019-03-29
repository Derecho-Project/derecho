/**
 * @file subgroup_functions.h
 *
 * @date Feb 28, 2017
 */

#pragma once

#include <memory>
#include <variant>

#include "derecho_internal.h"
#include "derecho_modes.h"
#include "subgroup_info.h"

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
 * un-sharded subgroup containing all the members of curr_view, regardless of
 * what subgroup type is supplied. This is best used when there is only one
 * subgroup type.
 */
void one_subgroup_entire_view(const std::type_index& subgroup_type,
                              const std::unique_ptr<View>& prev_view,
                              View& curr_view,
                              std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts);
/**
 * A simple implementation of shard_view_generator_t that returns a single,
 * un-sharded subgroup in Unordered (Raw) mode containing all the members of
 * curr_view, regardless of what subgroup type is supplied. This is best used
 * when there is only one subgroup type.
 */
void one_subgroup_entire_view_raw(const std::type_index& subgroup_type,
                                  const std::unique_ptr<View>& prev_view,
                                  View& curr_view,
                                  std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts);

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
 * @return A ShardAllocationPolicy value with these parameters
 */
ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard);

/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard; each shard must have
 * exactly nodes_per_shard members.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard);
/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard, and every shard running in
 * "raw" delivery mode.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard);
/**
 * Returns a ShardAllocationPolicy for a subgroup that has a different number of
 * members in each shard, and possibly has each shard in a different delivery mode.
 * Note that the parameter vectors must all be the same length.
 * @param min_nodes_by_shard A vector specifying the minimum number of nodes for each
 * shard; the ith shard must have at least min_nodes_by_shard[i] members.
 * @param max_nodes_by_shard A vector specifying the maximum number of nodes for each
 * shard; the ith shard can have up to max_nodes_by_shard[i] members.
 * @param delivery_modes_by_shard A vector specifying the delivery mode (Raw or
 * Ordered) for each shard, in the same order as the other vectors.
 * @return A ShardAllocationPolicy that specifies these shard sizes and modes.
 */
ShardAllocationPolicy custom_shards_policy(const std::vector<int>& min_nodes_by_shard,
                                           const std::vector<int>& max_nodes_by_shard,
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

    std::vector<SubView> assign_subgroup(const std::unique_ptr<View>& prev_view,
                                         View& curr_view,
                                         const ShardAllocationPolicy& subgroup_policy) const;
    void compute_standard_membership_phase_1(const std::type_index& subgroup_type,
                                             const std::unique_ptr<View>& prev_view,
                                             View& curr_view,
                                             std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>&
                                             subgroup_layouts) const;
    void compute_standard_membership_phase_2(const std::type_index& subgroup_type,
                                             const std::unique_ptr<View>& prev_view,
                                             View& curr_view,
                                             std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>&
                                             subgroup_layouts) const;
    void compute_cross_product_membership(const std::type_index& subgroup_type,
                                          const std::unique_ptr<View>& prev_view,
                                          View& curr_view,
                                          std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>&
                                          subgroup_layouts) const;

public:
    DefaultSubgroupAllocator(const std::map<std::type_index,
                                            std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>&
                                     policies_by_subgroup_type)
            : policies(policies_by_subgroup_type) {}
    DefaultSubgroupAllocator(const DefaultSubgroupAllocator& to_copy)
            : policies(to_copy.policies) {}
    DefaultSubgroupAllocator(DefaultSubgroupAllocator&&) = default;

    void operator()(const std::type_index& subgroup_type,
                    const std::unique_ptr<View>& prev_view,
                    View& curr_view,
                    std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>& subgroup_layouts) const;
};

}  // namespace derecho
