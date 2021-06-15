/**
 * @file subgroup_functions_impl.h
 * @brief Contains implementations of functions that parse json layout
 * @date May 20, 2021
 */

#include "../subgroup_functions.hpp"
#include <derecho/utils/logger.hpp>
#include <fstream>
#include <sstream>

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
 * Generate a single-type subgroup allocation policy from json string
 * @param jconf subgroup configuration represented in json format.
 * @param all_reserved_node_ids a set that holds the union of all reserved node_ids.
 * @return SubgroupAllocationPolicy
 */
SubgroupAllocationPolicy derecho_parse_json_subgroup_policy(const json&, std::set<node_id_t>&);

/**
 * TODO: If we just need to check shards within one subgroup, this function is redundant.
 * Make sure that no shards inside a subgroup reserve same node_ids. Shards in 
 * different subgroups of one same type or from different types can share nodes,
 * and this why we use the reserved_node_id feature.
 * For example, we can assign 2 subgroups for type "PersistentCascadeStoreWithStringKey"
 * to store data and model respectively for an ML application, and actually reserve
 * the same node_ids for shards in this two subgroup. This way the data and the model
 * coexist in the same node, thus delivering performance gains.
 * @param dsa_map the subgroup allocation map derived from json configuration.
 */
void check_reserved_node_id_pool(const std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>&);

template <typename Type = node_id_t>
void print_set(const std::set<Type>& uset) {
    std::stringstream stream;
    for(auto thing : uset) {
        stream << thing << ' ';
    }

    std::string out = stream.str();
    dbg_default_debug(out);
}
template <typename Type = node_id_t>
void print_set(const std::vector<Type>& uset) {
    std::stringstream stream;
    for(auto thing : uset) {
        stream << thing << ' ';
    }

    std::string out = stream.str();
    dbg_default_debug(out);
}

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
    }

    json_layout_stream >> layout;

    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;

    std::set<node_id_t> all_reserved_node_ids;

    derecho_populate_policy_by_subgroup_type_map<ReplicatedTypes...>(
            dsa_map, all_reserved_node_ids, layout, 0);

    return DefaultSubgroupAllocator(dsa_map, all_reserved_node_ids);
}

} /* namespace derecho */
