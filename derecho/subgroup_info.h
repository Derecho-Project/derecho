/**
 * @file subgroup_info.h
 *
 * @date Feb 17, 2017
 * @author edward
 */

#pragma once

#include <functional>
#include <map>
#include <typeindex>
#include <cstdint>

namespace derecho {

class View;

struct SubgroupInfo {
    /** subgroup type -> number of subgroups */
    std::map<std::type_index, uint32_t> num_subgroups;
    /** (subgroup type, subgroup index) -> number of shards */
    std::map<std::pair<std::type_index, uint32_t>, uint32_t> num_shards;
    /** (current view, subgroup type, subgroup index, shard index) -> IDs of members */
    std::function<std::vector<node_id_t>(const View&,
            std::type_index, uint32_t, uint32_t)> subgroup_membership;
};

}


