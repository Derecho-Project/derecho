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

template <typename T>
std::ostream& operator<< (std::ostream& out, const std::set<T>& s) {
  if ( !s.empty() ) {
    out << '[';
    std::copy (s.begin(), s.end(), std::ostream_iterator<T>(out, ", "));
    out << "\b\b]";
  }
  return out;
}

}

namespace derecho {

void print_subgroup_layout(const subgroup_shard_layout_t& layout);

std::unique_ptr<View> make_next_view(const View& curr_view,
                                     const std::set<int>& leave_ranks,
                                     const std::vector<node_id_t>& joiner_ids,
                                     const std::vector<ip_addr>& joiner_ips);
}
