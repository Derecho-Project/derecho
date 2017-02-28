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

#include "derecho_exception.h"

namespace derecho {

class View;
class SubView;

class subgroup_provisioning_exception : derecho_exception {
public:
    subgroup_provisioning_exception(const std::string& message = "") :
        derecho_exception(message) {}
};

/** The type to use in the SubgroupInfo maps for a subgroup
 * that doesn't implement a Replicated Object */
struct RawObject { };

/** The type of a lambda function that generates subgroup and shard views
 * for a specific subgroup type. The return type is a two-dimensional vector
 * mapping (subgroup index, shard index) to the SubView for that subgroup
 * and shard. */
using shard_view_generator_t = std::function<std::vector<
        std::vector<std::unique_ptr<SubView>>>(const View&)>;

struct SubgroupInfo {
    /** subgroup type -> number of subgroups */
    std::map<std::type_index, uint32_t> num_subgroups;
    /** subgroup type -> [](current view){subgroup index -> shard index -> SubView for shard}*/
    std::map<std::type_index, shard_view_generator_t> subgroup_membership_functions;
};

}


