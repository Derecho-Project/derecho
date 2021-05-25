/**
 * @file subgroup_functions_impl.h
 * @brief Contains implementations of functions that parse json layout
 * @date May 20, 2021
 */

#include "../subgroup_functions.hpp"
#include <fstream>

namespace derecho {

/**
 * defining key strings used in the layout json file.
 */
#define JSON_CONF_LAYOUT "layout"
#define JSON_CONF_TYPE_ALIAS "type_alias"
#define MIN_NODES_BY_SHARD "min_nodes_by_shard"
#define MAX_NODES_BY_SHARD "max_nodes_by_shard"
#define RESERVED_NODE_ID_BY_SHRAD "reserved_node_id_by_shard"
#define DELIVERY_MODES_BY_SHARD "delivery_modes_by_shard"
#define DELIVERY_MODE_ORDERED "Ordered"
#define DELIVERY_MODE_RAW "Raw"
#define PROFILES_BY_SHARD "profiles_by_shard"
/**
 * derecho_parse_json_subgroup_policy()
 *
 * Generate a single-type subgroup allocation policy from json string
 * @param json_config subgroup configuration represented in json format.
 * @return SubgroupAllocationPolicy
 */
SubgroupAllocationPolicy derecho_parse_json_subgroup_policy(const json&, std::set<node_id_t>&);

template <typename ReplicatedType>
void derecho_populate_policy_by_subgroup_type_map(
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        std::set<node_id_t>& all_reserved_node_ids,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(ReplicatedType)), derecho_parse_json_subgroup_policy(layout[type_idx], all_reserved_node_ids));
}

template <typename FirstReplicatedType, typename SecondReplicatedType, typename... RestReplicatedTypes>
void derecho_populate_policy_by_subgroup_type_map(
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        std::set<node_id_t>& all_reserved_node_ids,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(FirstReplicatedType)), derecho_parse_json_subgroup_policy(layout[type_idx], all_reserved_node_ids));
    derecho_populate_policy_by_subgroup_type_map<SecondReplicatedType, RestReplicatedTypes...>(dsa_map, all_reserved_node_ids, layout, type_idx + 1);
}

template <typename... ReplicatedTypes>
DefaultSubgroupAllocator construct_DSA_with_layout(const json& layout) {
    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;

    std::set<node_id_t> all_reserved_node_ids;

    derecho_populate_policy_by_subgroup_type_map<ReplicatedTypes...>(
            dsa_map, all_reserved_node_ids, layout, 0);

    return DefaultSubgroupAllocator(dsa_map, all_reserved_node_ids);
}

template <typename... ReplicatedTypes>
DefaultSubgroupAllocator construct_DSA_with_layout_path(const std::string& layout_path) {
    json layout;

    std::ifstream json_layout_stream(layout_path.c_str());
    if(!json_layout_stream) {
        throw derecho_exception("The json layout file " + layout_path + " not found.");
        // TODO: do we need further actions like return something?
    }

    json_layout_stream >> layout;

    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;

    std::set<node_id_t> all_reserved_node_ids;

    derecho_populate_policy_by_subgroup_type_map<ReplicatedTypes...>(
            dsa_map, all_reserved_node_ids, layout, 0);

    return DefaultSubgroupAllocator(dsa_map, all_reserved_node_ids);
}

} /* namespace derecho */