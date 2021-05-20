/**
 * @file subgroup_functions_impl.h
 * @brief Contains implementations of functions that parse json layout
 * @date May 20, 2021
 */

#include <fstream>
#include "../subgroup_functions.hpp"

namespace derecho {

/**
 * parse_json_subgroup_policy()
 *
 * Generate a single-type subgroup allocation policy from json string
 * @param json_config subgroup configuration represented in json format.
 * @return SubgroupAllocationPolicy
 */
SubgroupAllocationPolicy parse_json_subgroup_policy(const json&);

template <typename ReplicatedType>
void populate_policy_by_subgroup_type_map(
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(ReplicatedType)), parse_json_subgroup_policy(layout[type_idx]));
}

template <typename FirstReplicatedType, typename SecondReplicatedType, typename... RestReplicatedTypes>
void populate_policy_by_subgroup_type_map(
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(FirstReplicatedType)), parse_json_subgroup_policy(layout[type_idx]));
    populate_policy_by_subgroup_type_map<SecondReplicatedType, RestReplicatedTypes...>(dsa_map, layout, type_idx + 1);
}

template <typename... ReplicatedTypes>
DefaultSubgroupAllocator::DefaultSubgroupAllocator(const json& layout) {
    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;

    populate_policy_by_subgroup_type_map<ReplicatedTypes...>(dsa_map, layout, 0);

    policies = std::move(dsa_map);
}

template <typename... ReplicatedTypes>
DefaultSubgroupAllocator::DefaultSubgroupAllocator(const std::string& layout_path) {
    json layout;

    std::ifstream json_layout_stream(layout_path.c_str());
    if (!json_layout_stream) {
        throw derecho_exception("The json layout file " + layout_path + " not found.");
        // TODO: do we need further actions like return something?
    }

    json_layout_stream >> layout;

    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;

    populate_policy_by_subgroup_type_map<ReplicatedTypes...>(dsa_map, layout, 0);

    policies = std::move(dsa_map);
}

} /* namespace derecho */