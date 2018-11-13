/**
 * @file subgroup_functions.h
 *
 * @date Feb 28, 2017
 */

#pragma once

#include <memory>

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

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view, int& next_unassigned_rank);
subgroup_shard_layout_t one_subgroup_entire_view_raw(const View& curr_view, int& next_unassigned_rank);

struct ShardAllocationPolicy {
    /** The number of shards; set to 1 for a non-sharded subgroup */
    int num_shards;
    /** Whether all shards should contain the same number of members. */
    bool even_shards;
    /** If even_shards is true, this is the number of nodes per shard. (Ignored
     * if even_shards is false). */
    int nodes_per_shard;
    /** If even_shards is true, this is the delivery mode that will be used for
     * every shard. (Ignored if even_shards is false). */
    Mode shards_mode;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the number of members it should have. (Ignored if even_shards
     * is true). */
    std::vector<int> num_nodes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating which delivery mode it should use. (Ignored if even_shards is
     * true). */
    std::vector<Mode> modes_by_shard;
};

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

/* Helper functions that construct ShardAllocationPolicy values for common cases. */

/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same number of nodes in each shard.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy even_sharding_policy(int num_shards, int nodes_per_shard);
/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same number of nodes in each shard, and every shard running in "raw"
 * delivery mode.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy raw_even_sharding_policy(int num_shards, int nodes_per_shard);
/**
 * Returns a ShardAllocationPolicy for a subgroup that has a different number of
 * members in each shard, and possibly has each shard in a different delivery mode.
 * Note that the two parameter vectors must be the same length.
 * @param num_nodes_by_shard A vector specifying how many nodes should be in each
 * shard; the ith shard will have num_nodes_by_shard[i] members.
 * @param delivery_modes_by_shard A vector specifying the delivery mode (Raw or
 * Ordered) for each shard, in the same order as the other vector.
 * @return A ShardAllocationPolicy that specifies these shard sizes and modes.
 */
ShardAllocationPolicy custom_shards_policy(const std::vector<int>& num_nodes_by_shard,
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
 * allocation algorithm, parameterized based on a SubgroupAllocationPolicy. Its
 * operator() will throw a subgroup_provisioning_exception if there are not
 * enough nodes in the current view to populate all of the subgroups and shards.
 */
class DefaultSubgroupAllocator {
protected:
    std::unique_ptr<subgroup_shard_layout_t> previous_assignment;
    const SubgroupAllocationPolicy policy;

    bool assign_subgroup(const View& curr_view, int& next_unassigned_rank, const ShardAllocationPolicy& subgroup_policy);

public:
    DefaultSubgroupAllocator(const SubgroupAllocationPolicy& allocation_policy)
            : policy(allocation_policy) {}
    DefaultSubgroupAllocator(const DefaultSubgroupAllocator& to_copy)
            : previous_assignment(deep_pointer_copy(to_copy.previous_assignment)),
              policy(to_copy.policy) {}
    DefaultSubgroupAllocator(DefaultSubgroupAllocator&&) = default;

    subgroup_shard_layout_t operator()(const View& curr_view, int& next_unassigned_rank);
};

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

/**
 * A shard_view_generator_t functor that creates a set of subgroups that is the
 * "cross-product" of two subgroups, source and target. Each node in the source
 * subgroup will be placed in T subgroups, one for each shard in the target
 * subgroup (where the target subgroup has T shards). Thus, if there are S members
 * in the source subgroup, and T shards in the target subgroup, S * T subgroups
 * of a single type will be created. The nodes in the source subgroup will be
 * marked as the only senders in these subgroups. A node that has rank i within
 * the source subgroup can send a multicast to shard j of the target subgroup
 * by selecting the cross-product subgroup at index (i * T + j).
 */
class CrossProductAllocator {
    const CrossProductPolicy policy;

public:
    CrossProductAllocator(const CrossProductPolicy& allocation_policy) : policy(allocation_policy) {}
    CrossProductAllocator(const CrossProductAllocator& to_copy) : policy(to_copy.policy) {}
    CrossProductAllocator(CrossProductAllocator&&) = default;

    subgroup_shard_layout_t operator()(const View& curr_view, int& next_unassigned_rank);
};
}  // namespace derecho
