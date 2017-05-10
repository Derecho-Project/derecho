#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>

#include "view.h"

namespace derecho {

using std::string;
using std::shared_ptr;

SubView::SubView(int32_t num_members)
        : mode(Mode::ORDERED),
          members(num_members),
          is_sender(num_members, 1),
          member_ips(num_members),
          joined(0),
          departed(0),
          my_rank(-1) {}

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

View::View(const int32_t vid, const std::vector<node_id_t>& members, const std::vector<ip_addr>& member_ips,
           const std::vector<char>& failed, const int32_t num_failed, const std::vector<node_id_t>& joined,
           const std::vector<node_id_t>& departed, const int32_t num_members, const int32_t my_rank)
        : vid(vid),
          members(members),
          member_ips(member_ips),
          failed(failed),
          num_failed(num_failed),
          joined(joined),
          departed(departed),
          num_members(num_members),
          my_rank(my_rank) {
    for(int rank = 0; rank < num_members; ++rank) {
        node_id_to_rank[members[rank]] = rank;
    }
}

int View::rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

View::View(const int32_t vid, const std::vector<node_id_t>& members, const std::vector<ip_addr>& member_ips,
           const std::vector<char>& failed, const std::vector<node_id_t>& joined,
           const std::vector<node_id_t>& departed, const int32_t my_rank)
        : vid(vid),
          members(members),
          member_ips(member_ips),
          failed(failed),
          joined(joined),
          departed(departed),
          num_members(members.size()),
          my_rank(my_rank) {
    for(int rank = 0; rank < num_members; ++rank) {
        node_id_to_rank[members[rank]] = rank;
    }
    for(auto c : failed) {
        if(c) {
            num_failed++;
        }
    }
}

int View::rank_of(const ip_addr& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips[rank] == who) {
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

std::unique_ptr<SubView> View::make_subview(const std::vector<node_id_t>& with_members, const Mode mode, const std::vector<int>& is_sender) const {
    std::unique_ptr<SubView> sub_view = std::make_unique<SubView>(with_members.size());
    sub_view->members = with_members;
    sub_view->mode = mode;
    // if the sender information is not provided, assume that all members are senders
    if(is_sender.size()) {
        sub_view->is_sender = is_sender;
    }
    for(std::size_t subview_rank = 0; subview_rank < with_members.size(); ++subview_rank) {
        std::size_t member_pos = std::distance(
                members.begin(), std::find(members.begin(), members.end(), with_members[subview_rank]));
        if(member_pos == members.size()) {
            //The ID wasn't found in members[]
            throw subgroup_provisioning_exception();
        }
        sub_view->member_ips[subview_rank] = member_ips[member_pos];
    }
    return sub_view;
}

int View::subview_rank_of_shard_leader(subgroup_id_t subgroup_id, int shard_index) const {
    SubView& shard_view = *subgroup_shard_views.at(subgroup_id).at(shard_index);
    for(std::size_t rank = 0; rank < shard_view.members.size(); ++rank) {
        //Inefficient to call rank_of every time, but no guarantee the subgroup members will have ascending ranks
        if(!failed[rank_of(shard_view.members[rank])]) {
            return rank;
        }
    }
    return -1;
}

bool View::i_am_leader() const {
    return (rank_of_leader() == my_rank);  // True if I know myself to be the leader
}

bool View::i_am_new_leader() {
    if(i_know_i_am_leader) {
        return false;  // I am the OLD leader
    }

    for(int n = 0; n < my_rank; n++) {
        for(int row = 0; row < my_rank; row++) {
            if(!failed[n] && !gmsSST->suspected[row][n]) {
                return false;  // I'm not the new leader, or some failure suspicion hasn't fully propagated
            }
        }
    }
    i_know_i_am_leader = true;
    return true;
}

void View::merge_changes() {
    int myRank = my_rank;
    // Merge the change lists
    for(int n = 0; n < num_members; n++) {
        if(gmsSST->num_changes[myRank] < gmsSST->num_changes[n]) {
            gmssst::set(gmsSST->changes[myRank], gmsSST->changes[n], gmsSST->changes.size());
            gmssst::set(gmsSST->num_changes[myRank], gmsSST->num_changes[n]);
        }

        if(gmsSST->num_committed[myRank] < gmsSST->num_committed[n])  // How many I know to have been committed
        {
            gmssst::set(gmsSST->num_committed[myRank], gmsSST->num_committed[n]);
        }
    }
    bool found = false;
    for(int n = 0; n < num_members; n++) {
        if(failed[n]) {
            // Make sure that the failed process is listed in the Changes vector as a proposed change
            for(int c = gmsSST->num_committed[myRank]; c < gmsSST->num_changes[myRank] && !found; c++) {
                if(gmsSST->changes[myRank][c % gmsSST->changes.size()] == members[n]) {
                    // Already listed
                    found = true;
                }
            }
        } else {
            // Not failed
            found = true;
        }

        if(!found) {
            gmssst::set(gmsSST->changes[myRank][gmsSST->num_changes[myRank] % gmsSST->changes.size()],
                        members[n]);
            gmssst::increment(gmsSST->num_changes[myRank]);
        }
    }
    gmsSST->put((char*)std::addressof(gmsSST->changes[0][0]) - gmsSST->getBaseAddress(), gmsSST->changes.size() * sizeof(node_id_t) + gmsSST->joiner_ips.size() * sizeof(uint32_t) + sizeof(int) + sizeof(int));
}

void View::wedge() {
    multicast_group->wedge();  // RDMC finishes sending, stops new sends or receives in Vc
    gmssst::set(gmsSST->wedged[my_rank], true);
    gmsSST->put((char*)std::addressof(gmsSST->wedged[0]) - gmsSST->getBaseAddress(), sizeof(bool));
}

std::string View::debug_string() const {
    // need to add member ips and other fields
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
    for(uint i = 0; i < departed.size(); ++i) {
        s << members[departed[i]] << " ";
    }
    s << "} , Joined: { ";
    for(uint i = 0; i < joined.size(); ++i) {
        s << members[joined[i]] << " ";
    }
    s << "}";
    return s.str();
}

std::unique_ptr<View> load_view(const std::string& view_file_name) {
    std::ifstream view_file(view_file_name);
    std::ifstream view_file_swap(view_file_name + persistence::SWAP_FILE_EXTENSION);
    std::unique_ptr<View> view;
    std::unique_ptr<View> swap_view;
    //The expected view file might not exist, in which case we'll fall back to the swap file
    if(view_file.good()) {
        //Each file contains the size of the view (an int copied as bytes),
        //followed by a serialized view
        std::size_t size_of_view;
        view_file.read((char*)&size_of_view, sizeof(size_of_view));
        char buffer[size_of_view];
        view_file.read(buffer, size_of_view);
        //If the view file doesn't contain a complete view (due to a crash
        //during writing), the read() call will set failbit
        if(!view_file.fail()) {
            view = mutils::from_bytes<View>(nullptr, buffer);
        }
    }
    if(view_file_swap.good()) {
        std::size_t size_of_view;
        view_file_swap.read((char*)&size_of_view, sizeof(size_of_view));
        char buffer[size_of_view];
        view_file_swap.read(buffer, size_of_view);
        if(!view_file_swap.fail()) {
            swap_view = mutils::from_bytes<View>(nullptr, buffer);
        }
    }
    if(swap_view == nullptr || (view != nullptr && view->vid >= swap_view->vid)) {
        return view;
    } else {
        return swap_view;
    }
}

std::ostream& operator<<(std::ostream& stream, const View& view) {
    stream << view.vid << std::endl;
    std::copy(view.members.begin(), view.members.end(), std::ostream_iterator<node_id_t>(stream, " "));
    stream << std::endl;
    std::copy(view.member_ips.begin(), view.member_ips.end(), std::ostream_iterator<ip_addr>(stream, " "));
    stream << std::endl;
    for(const auto& fail_val : view.failed) {
        stream << (fail_val ? "T" : "F") << " ";
    }
    stream << std::endl;
    stream << view.num_failed << std::endl;
    stream << view.num_members << std::endl;
    stream << view.my_rank << std::endl;
    return stream;
}

View parse_view(std::istream& stream) {
    std::string line;
    int32_t vid;
    if(std::getline(stream, line)) {
        vid = std::stoi(line);
    }
    std::vector<node_id_t> members;
    //"List of member IDs" line
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::copy(std::istream_iterator<node_id_t>(linestream), std::istream_iterator<node_id_t>(),
                  std::back_inserter(members));
    }
    std::vector<ip_addr> member_ips;
    //"List of member IPs" line
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::copy(std::istream_iterator<ip_addr>(linestream), std::istream_iterator<ip_addr>(),
                  std::back_inserter(member_ips));
    }
    std::vector<char> failed;
    //Failures array line, which was printed as "T" or "F" strings
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::string fail_str;
        while(linestream >> fail_str) {
            failed.emplace_back(fail_str == "T" ? true : false);
        }
    }
    int32_t num_failed;
    //The last three lines each contain a single number
    if(std::getline(stream, line)) {
        num_failed = std::stoi(line);
    }
    int32_t num_members;
    if(std::getline(stream, line)) {
        num_members = std::stoi(line);
    }
    int32_t my_rank;
    if(std::getline(stream, line)) {
        my_rank = std::stoi(line);
    }
    return View(vid, members, member_ips, failed, num_failed, {}, {}, num_members, my_rank);
}
}
