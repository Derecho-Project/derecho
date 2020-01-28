#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>

#include <derecho/core/view.hpp>

namespace derecho {

using std::shared_ptr;
using std::string;

SubView::SubView(int32_t num_members)
        : mode(Mode::ORDERED),
          members(num_members),
          is_sender(num_members, 1),
          member_ips_and_ports(num_members),
          joined(0),
          departed(0),
          my_rank(-1),
          profile("default") {}

SubView::SubView(Mode mode,
                 const std::vector<node_id_t>& members,
                 std::vector<int> is_sender,
                 const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t,
                                              uint16_t>>& member_ips_and_ports,
                 const std::string profile)
        : mode(mode),
          members(members),
          is_sender(members.size(), 1),
          member_ips_and_ports(member_ips_and_ports),
          my_rank(-1),
          profile(profile) {
    // if the sender information is not provided, assume that all members are senders
    if(is_sender.size()) {
        this->is_sender = is_sender;
    }
}

int SubView::rank_of(const node_id_t& who) const {
    for(std::size_t rank = 0; rank < members.size(); ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}

int SubView::sender_rank_of(uint32_t rank) const {
    if(!is_sender[rank]) {
        return -1;
    }
    int num = 0;
    for(uint i = 0; i < rank; ++i) {
        if(is_sender[i]) {
            num++;
        }
    }
    return num;
}

uint32_t SubView::num_senders() const {
    uint32_t num = 0;
    for(const auto i : is_sender) {
        if(i) {
            num++;
        }
    }
    return num;
}

void SubView::init_joined_departed(const SubView& previous_subview) {
    //To ensure this method is idempotent
    joined.clear();
    departed.clear();
    std::set<node_id_t> prev_members(previous_subview.members.begin(),
                                     previous_subview.members.end());
    std::set<node_id_t> curr_members(members.begin(),
                                     members.end());
    std::set_difference(curr_members.begin(), curr_members.end(),
                        prev_members.begin(), prev_members.end(),
                        std::back_inserter(joined));
    std::set_difference(prev_members.begin(), prev_members.end(),
                        curr_members.begin(), curr_members.end(),
                        std::back_inserter(departed));
}

View::View(const int32_t vid, const std::vector<node_id_t>& members,
           const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
           const std::vector<char>& failed, const int32_t num_failed,
           const std::vector<node_id_t>& joined,
           const std::vector<node_id_t>& departed,
           const int32_t num_members,
           const int32_t next_unassigned_rank,
           const std::map<subgroup_type_id_t, std::vector<subgroup_id_t>>& subgroup_ids_by_type_id,
           const std::vector<std::vector<SubView>>& subgroup_shard_views,
           const std::map<subgroup_id_t, uint32_t>& my_subgroups)
        : vid(vid),
          members(members),
          member_ips_and_ports(member_ips_and_ports),
          failed(failed),
          num_failed(num_failed),
          joined(joined),
          departed(departed),
          num_members(num_members),
          my_rank(0),  // This will always get overwritten by the receiver after deserializing
          next_unassigned_rank(next_unassigned_rank),
          subgroup_ids_by_type_id(subgroup_ids_by_type_id),
          subgroup_shard_views(subgroup_shard_views),
          my_subgroups(my_subgroups) {
    for(int rank = 0; rank < num_members; ++rank) {
        node_id_to_rank[members[rank]] = rank;
    }
}

int View::find_rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

View::View(const int32_t vid, const std::vector<node_id_t>& members,
           const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
           const std::vector<char>& failed, const std::vector<node_id_t>& joined,
           const std::vector<node_id_t>& departed,
           const int32_t my_rank,
           const int32_t next_unassigned_rank,
           const std::vector<std::type_index>& subgroup_type_order)
        : vid(vid),
          members(members),
          member_ips_and_ports(member_ips_and_ports),
          failed(failed),
          num_failed(0),
          joined(joined),
          departed(departed),
          num_members(members.size()),
          my_rank(my_rank),
          next_unassigned_rank(next_unassigned_rank),
          subgroup_type_order(subgroup_type_order) {
    for(int rank = 0; rank < num_members; ++rank) {
        node_id_to_rank[members[rank]] = rank;
    }
    for(auto c : failed) {
        if(c) {
            num_failed++;
        }
    }
}

int View::rank_of(const std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips_and_ports[rank] == who) {
            return rank;
        }
    }
    return -1;
}

int View::rank_of(const node_id_t& who) const {
    auto it = node_id_to_rank.find(who);
    if(it != node_id_to_rank.end()) {
        return it->second;
    }
    return -1;
}

SubView View::make_subview(const std::vector<node_id_t>& with_members,
                           const Mode mode,
                           const std::vector<int>& is_sender,
                           std::string profile) const {
    // Make the profile string all uppercase so that it is effectively case-insensitive
    std::transform(profile.begin(), profile.end(), profile.begin(), ::toupper);
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>> subview_member_ips_and_ports(with_members.size());
    for(std::size_t subview_rank = 0; subview_rank < with_members.size(); ++subview_rank) {
        int view_rank_of_member = rank_of(with_members[subview_rank]);
        if(view_rank_of_member == -1) {
            // The ID wasn't found in members[]
            throw subgroup_provisioning_exception();
        }
        subview_member_ips_and_ports[subview_rank] = member_ips_and_ports[view_rank_of_member];
    }
    // Note that joined and departed do not need to get initialized here; they will be initialized by ViewManager
    return SubView(mode, with_members, is_sender, subview_member_ips_and_ports, profile);
}

int View::subview_rank_of_shard_leader(subgroup_id_t subgroup_id,
                                       uint32_t shard_index) const {
    if(shard_index >= subgroup_shard_views.at(subgroup_id).size()) {
        return -1;
    }
    const SubView& shard_view = subgroup_shard_views.at(subgroup_id).at(shard_index);
    for(std::size_t rank = 0; rank < shard_view.members.size(); ++rank) {
        // Inefficient to call rank_of every time, but no guarantee the subgroup
        // members will have ascending ranks
        if(!failed[rank_of(shard_view.members[rank])]) {
            return rank;
        }
    }
    return -1;
}

bool View::i_am_leader() const {
    return (find_rank_of_leader() == my_rank);  // True if I know myself to be the leader
}

void View::wedge() {
    multicast_group->wedge();  // RDMC finishes sending, stops new sends or receives in Vc
    gmssst::set(gmsSST->wedged[my_rank], true);
    gmsSST->put(gmsSST->wedged.get_base() - gmsSST->getBaseAddress(),
                sizeof(gmsSST->wedged[0]));
}

bool View::is_wedged() {
    return gmsSST->wedged[my_rank];
}

std::string View::debug_string() const {
    // need to add member ips and ports and other fields
    std::stringstream s;
    s << "View " << vid << ": MyRank=" << my_rank << ". ";
    s << "Members={ ";
    for(int m = 0; m < num_members; m++) {
        s << members[m] << "  ";
    }
    s << "}, ";
    string fs = (" ");
    for(int m = 0; m < num_members; m++) {
        fs += failed[m] ? string(" T ") : string(" F ");
    }

    s << "Failed={" << fs << " }, num_failed=" << num_failed;
    s << ", Departed: { ";
    for(const node_id_t& departed_node : departed) {
        s << departed_node << " ";
    }
    s << "} , Joined: { ";
    for(const node_id_t& joined_node : joined) {
        s << joined_node << " ";
    }
    s << "}" << std::endl;
    s << "SubViews: ";
    for(subgroup_id_t subgroup = 0; subgroup < subgroup_shard_views.size(); ++subgroup) {
        for(uint32_t shard = 0; shard < subgroup_shard_views[subgroup].size(); ++shard) {
            s << "Shard (" << subgroup << ", " << shard << "): Members={";
            for(const node_id_t& member : subgroup_shard_views[subgroup][shard].members) {
                s << member << " ";
            }
            s << "}, is_sender={";
            for(uint i = 0; i < subgroup_shard_views[subgroup][shard].members.size(); ++i) {
                if(subgroup_shard_views[subgroup][shard].is_sender[i]) {
                    s << "T ";
                } else {
                    s << "F ";
                }
            }
            s << "}.  ";
        }
    }
    return s.str();
}

}  // namespace derecho
