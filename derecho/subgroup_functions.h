/**
 * @file subgroup_functions.h
 *
 * @date Feb 28, 2017
 * @author edward
 */

#pragma once

#include "subgroup_info.h"

namespace derecho {

subgroup_shard_layout_t one_subgroup_entire_view(const View& curr_view);
subgroup_shard_layout_t one_subgroup_entire_view_raw(const View& curr_view);
}
