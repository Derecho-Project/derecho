#include "view.hpp"

#include <algorithm>

namespace group {

View::View(uint32_t vid, const node::NodeCollection& members,
           const std::vector<std::tuple<ip_addr_t, port_t, port_t, port_t, port_t>>& member_ips_and_ports,
           const std::vector<bool>& failures_info)
        : vid(vid),
          members(members),
          member_ips_and_ports(member_ips_and_ports),
          failures_info(failures_info),
          num_failures(std::count(failures_info.begin(), failures_info.end(), true)),
          leader_rank(std::distance(failures_info.begin(), std::find_first_of(failures_info.begin(), failures_info.end(), false))) {
}
}  // namespace group
