/**
 * @file subgroup_functions.cpp
 *
 * @date Feb 28, 2017
 * @author edward
 */

#include <vector>

#include "subgroup_functions.h"
#include "view.h"

namespace derecho {

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view) {
    subgroup_shard_layout_t subgroup_vector(1);
    subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
    return subgroup_vector;
}
}
