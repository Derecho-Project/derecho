/**
 * @file subgroup_info.h
 *
 * @date Feb 17, 2017
 */

#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <typeindex>
#include <vector>

#include "derecho_exception.h"

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
 * The type of a lambda function that generates subgroup and shard views
 * for a specific subgroup type. It will be called twice for each subgroup
 * type, in order to facilitate a two-phase node allocation process: in the
 * first phase the function should ensure every shard has the minimum necessary
 * number of members (if possible), and in the second phase the function should
 * allocate as many members as it needs to fill shards to their desired size.
 * The results of the function's allocation should be placed in the
 * out-parameter of type std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>&,
 * at the entry corresponding to the input subgroup type.
 *
 * @param const std::type_index& - The type of subgroup being allocated
 * @param const std::unique_ptr<View>& - A pointer to the previous View, if there is one
 * @param View& - A reference to the current View (in which the shards will be allocated)
 * @param std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>& - A reference
 * to the subgroup allocation map, into which this function should place its output (at the
 * key corresponding to its type_index)
 */
using shard_view_generator_t = std::function<void(const std::type_index&,
                                                  const std::unique_ptr<View>&,
                                                  View&,
                                                  std::map<std::type_index, std::unique_ptr<subgroup_shard_layout_t>>&)>;

/**
 * Container for whatever information is needed to describe a Group's subgroups
 * and shards.
 */
struct SubgroupInfo {
    /**
     * The function that generates SubViews for each subgroup and shard
     * of a given replicated type.
     */
    const shard_view_generator_t subgroup_membership_function;

    SubgroupInfo(const shard_view_generator_t& subgroup_membership_function)
            : subgroup_membership_function(subgroup_membership_function) {}
};

}  // namespace derecho
