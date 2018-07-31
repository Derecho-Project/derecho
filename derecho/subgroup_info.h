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

/** The type to use in the SubgroupInfo maps for a subgroup
 * that doesn't implement a Replicated Object */
struct RawObject {};

/**
 * The data structure used to store a subgroups-and-shards layout for a single
 * subgroup type (i.e. Replicated Object type). The outer vector represents
 * subgroups of the same type, and the inner vector represents shards of each
 * subgroup, so the vectors map subgroup index -> shard index -> sub-view of
 * that shard.
 */
using subgroup_shard_layout_t = std::vector<std::vector<SubView>>;

/** The type of a lambda function that generates subgroup and shard views
 * for a specific subgroup type. This is a function that takes the current View
 * as input and outputs a vector-of-vectors representing subgroups and shards. */
using shard_view_generator_t = std::function<subgroup_shard_layout_t(const View&, int&, bool)>;

/**
 * Container for whatever information is needed to describe a Group's subgroups
 * and shards.
 */
struct SubgroupInfo {
    /**
     * A map of functions that generate SubViews for each subgroup and shard
     * of a given type, indexed by the type of Replicated Object whose subgroups
     * they describe.
     */
    const std::map<std::type_index, shard_view_generator_t> subgroup_membership_functions;
    /**
     * This list should contain the same Replicated Object types as the keys of
     * subgroup_membership_functions, listed in the order those membership
     * functions should be executed. This can be used to specify dependencies between
     * membership functions: if one membership function depends on knowing the
     * result of another membership function, they can be ordered so the
     * dependent function runs second.
     */
    const std::list<std::type_index> membership_function_order;

    SubgroupInfo(std::map<std::type_index, shard_view_generator_t> subgroup_membership_functions,
                 std::list<std::type_index> membership_function_order)
            : subgroup_membership_functions(subgroup_membership_functions),
              membership_function_order(membership_function_order) {
    }
    SubgroupInfo(std::map<std::type_index, shard_view_generator_t> subgroup_membership_functions)
            : subgroup_membership_functions(subgroup_membership_functions),
              membership_function_order([subgroup_membership_functions]() {
                  std::list<std::type_index> membership_function_order;
                  // if no ordering is given, choose the ordering as given by the keys of the subgroup_membership functions map
                  for(const auto& p : subgroup_membership_functions) {
                      membership_function_order.push_back(p.first);
                  }
                  return membership_function_order;
              }()) {
    }
};

/**
 * Constructs a std::list of the keys in a std::map, in the same order as they
 * appear in the std::map. This is a general-purpose function that could have
 * many uses, but it's included here because it is helpful in constructing
 * membership_function_order when there are no dependencies between functions
 * (thus, they should just run in the order of their appearance in the map).
 * @param map A std::map
 * @return A std::list containing a copy of each key in the map, in the same order
 */
template <typename K, typename V>
std::list<K> keys_as_list(const std::map<K, V>& map) {
    std::list<K> keys;
    for(const auto& pair : map) {
        keys.emplace_back(pair.first);
    }
    return keys;
}
}  // namespace derecho
