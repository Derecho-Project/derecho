/**
 * @file subgroup_function_tester.h
 *
 * @date May 24, 2017
 * @author edward
 */

#pragma once

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <vector>

#include <derecho/core/derecho_type_definitions.hpp>
#include <derecho/core/subgroup_functions.hpp>
#include <derecho/core/subgroup_info.hpp>
#include <derecho/core/view.hpp>

namespace std {

//This allows std::sets to be printed out in the obvious way
template <typename T>
std::ostream& operator<<(std::ostream& out, const std::set<T>& s) {
    if(!s.empty()) {
        out << '{';
        std::copy(s.begin(), s.end(), std::ostream_iterator<T>(out, ", "));
        out << "\b\b}";
    }
    return out;
}
}  // namespace std

namespace derecho {
//Functions that assist with testing subgroup layout allocation

/**
 * Constructs the next View given the current View and the set of failures and
 * joins. Uses exactly the same logic as the "initialize the next view" section
 * of the start_view_change predicate, with the crucial difference that
 * retrieving the joiner IPs from the SST has been stripped out (the joiner IPs
 * are assumed to be known already) so that it doesn't need an SST to run correctly.
 * @param curr_view The current View
 * @param leave_ranks The ranks (in the current View's members list) of members
 * that are leaving
 * @param joiner_ids The IDs of new nodes that are joining
 * @param joiner_ips The IP addresses of the new nodes that are joining, in the
 * same order as their corresponding IDs
 * @return A new View with the joins and leaves applied
 */
std::unique_ptr<View> make_next_view(const View& curr_view,
                                     const std::set<int>& leave_ranks,
                                     const std::vector<node_id_t>& joiner_ids,
                                     const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& joiner_ips_and_ports);

/**
 * Prints the membership of a subgroup/shard layout to stdout
 * @param layout
 */
void print_subgroup_layout(const subgroup_shard_layout_t& layout);

/**
 * Runs the same logic as ViewManager::make_subgroup_maps(), only without
 * actually saving the subgroup_to_x maps. curr_view is still updated with the
 * subgroup assignments, though.
 * @param subgroup_info The SubgroupInfo to use for provisioning subgroups
 * @param prev_view The previous view, if there was one, or nullptr
 * @param curr_view The current view in which to assign subgroup membership
 */
void test_provision_subgroups(const SubgroupInfo& subgroup_info,
                              const std::unique_ptr<View>& prev_view,
                              View& curr_view);
}  // namespace derecho
