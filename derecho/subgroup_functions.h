/**
 * @file subgroup_functions.h
 *
 * @date Feb 28, 2017
 */

#pragma once

#include <memory>

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

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view, int& next_unassigned_rank, bool previous_was_successful);
subgroup_shard_layout_t one_subgroup_entire_view_raw(const View& curr_view, int& next_unassigned_rank, bool previous_was_successful);

struct ShardAllocationPolicy {
    /** The number of shards; set to 1 for a non-sharded subgroup */
    int num_shards;
    /** Whether all shards should contain the same number of members. */
    bool even_shards;
    /** If even_shards is true, this is the number of nodes per shard.
     * (Ignored if even_shards is false). */
    int nodes_per_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the number of members it should have. (Ignored if
     * even_shards is true). */
    std::vector<int> num_nodes_by_shard;
};

struct SubgroupAllocationPolicy {
    /** The number of subgroups of the same Replicated type to create */
    int num_subgroups;
    /** Whether all subgroups of this type will have an identical shard layout */
    bool identical_subgroups;
    /** If identical_subgroups is true, contains a single entry with the allocation
     * policy for all subgroups of this type. If identical_subgroups is false,
     * contains an entry for each subgroup describing that subgroup's shards. */
    std::vector<ShardAllocationPolicy> shard_policy;
};

/**
 * Functor of type shard_view_generator_t that implements the default subgroup
 * allocation algorithm, parameterized based on a SubgroupAllocationPolicy.
 */
class DefaultSubgroupAllocator {
    std::unique_ptr<subgroup_shard_layout_t> previous_assignment;
    std::unique_ptr<subgroup_shard_layout_t> last_good_assignment;
    const SubgroupAllocationPolicy policy;

    void assign_subgroup(const View& curr_view, int& next_unassigned_rank, const ShardAllocationPolicy& subgroup_policy);

public:
    DefaultSubgroupAllocator(const SubgroupAllocationPolicy& allocation_policy)
            : policy(allocation_policy) {}
    DefaultSubgroupAllocator(const DefaultSubgroupAllocator& to_copy)
            : previous_assignment(deep_pointer_copy(to_copy.previous_assignment)),
              last_good_assignment(deep_pointer_copy(to_copy.last_good_assignment)),
              policy(to_copy.policy) {}
    DefaultSubgroupAllocator(DefaultSubgroupAllocator&&) = default;

    subgroup_shard_layout_t operator()(const View& curr_view, int& next_unassigned_rank, bool previous_was_successful);
};
}
