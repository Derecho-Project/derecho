/**
 * @file subgroup_info.hpp
 *
 * @date Feb 17, 2017
 */

#pragma once

#include "derecho/config.h"
#include "derecho_exception.hpp"

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <typeindex>
#include <vector>

namespace derecho {

class View;
class SubView;

/**
 * An exception that indicates that a subgroup membership function was unable
 * to finish executing because its enclosing Group was not in a valid state.
 * Usually this means that the Group did not have enough members to completely
 * provision the subgroup, or specific nodes that the subgroup needed were not
 * available.
 */
class subgroup_provisioning_exception : public derecho_exception {
public:
    subgroup_provisioning_exception(const std::string& message = "") : derecho_exception(message) {}
};

/**
 * The data structure used to store a subgroups-and-shards layout for a single
 * subgroup type (i.e. Replicated Object type). The outer vector represents
 * subgroups of the same type, and the inner vector represents shards of each
 * subgroup, so the vectors map subgroup index -> shard index -> sub-view of
 * that shard.
 */
using subgroup_shard_layout_t = std::vector<std::vector<SubView>>;

/**
 * The data structure used to store the subgroups-and-shards layouts for all
 * subgroup types in a Group (at least during the subgroup allocation process).
 * The keys are Replicated Object types, and the values are subgroups-and-shards
 * layouts as defined in subgroup_shard_layout_t.
 *
 * Note that since the subgroup_shard_layout_t elements are stored by value,
 * placing a local variable of type subgroup_shard_layout_t in the map will
 * copy all of the vectors and SubViews; it is preferable to use std::move to
 * move-assign the local variable into the map, like this:
 * subgroup_allocation.emplace(subgroup_type, std::move(subgroup_layout));
 *
 * Combining this type with subgroup_shard_layout_t, the map is organized by:
 * subgroup type -> subgroup index -> shard index -> sub-view of that shard
 */
using subgroup_allocation_map_t = std::map<std::type_index, subgroup_shard_layout_t>;

/**
 * The type of a lambda function that generates subgroup and shard views
 * for a Derecho group.
 *
 * @param const std::vector<std::type_index>& - A list of type_indexes of all
 * the subgroup types in this Group, arranged in the same order as the template
 * parameters to Group. This is the order in which the subgroup types should be
 * allocated.
 * @param const std::unique_ptr<View>& - A pointer to the previous View, if there is one
 * @param View& - A reference to the current View, which is in the process of being
 * provisioned into subgroups and shards
 * @return subgroup_allocation_map_t - A subgroup allocation map in which each
 * subgroup type in the Group has an entry containing its subgroup and shard
 * SubViews. Thus, there should be a key in this map for every element in the
 * std::vector<std::type_index> that is the first parameter.
 * @throws subgroup_provisioning_exception if there are not enough nodes in the
 * current view to allocate all the subgroups and shards
 */
using shard_view_generator_t = std::function<subgroup_allocation_map_t(
        const std::vector<std::type_index>&,
        const std::unique_ptr<View>&,
        View&)>;

/**
 * Container for whatever information is needed to describe a Group's subgroups
 * and shards.
 */
struct SubgroupInfo {
    /**
     * The function that generates all the SubViews for a View
     */
    const shard_view_generator_t subgroup_membership_function;

    SubgroupInfo(const shard_view_generator_t& subgroup_membership_function)
            : subgroup_membership_function(subgroup_membership_function) {}
};

}  // namespace derecho
