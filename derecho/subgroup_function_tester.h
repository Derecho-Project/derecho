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
#include <set>

#include "subgroup_info.h"

namespace std {

//This allows std::sets to be printed out in the obvious way
template <typename T>
std::ostream& operator<< (std::ostream& out, const std::set<T>& s) {
  if ( !s.empty() ) {
    out << '{';
    std::copy(s.begin(), s.end(), std::ostream_iterator<T>(out, ", "));
    out << "\b\b}";
  }
  return out;
}

}

namespace derecho {

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
                                     const std::vector<ip_addr>& joiner_ips);

//Functions that assist with testing subgroup layout allocation

void print_subgroup_layout(const subgroup_shard_layout_t& layout);

void run_subgroup_allocators(std::vector<DefaultSubgroupAllocator>& allocators,
                             const std::unique_ptr<View>& prev_view,
                             View& curr_view);

}
