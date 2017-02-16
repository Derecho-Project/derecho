#include <memory>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

#include "view.h"

namespace derecho {

using std::string;
using std::shared_ptr;

View::View()
        : View(0) {}


View::View(int num_members)
        : vid(0),
          members(num_members),
          member_ips(num_members),
          failed(num_members, 0),
          num_failed(0),
          joined(0),
          departed(0),
          num_members(num_members),
          my_rank(0),
          multicast_group(nullptr),
          gmsSST(nullptr) {}

//
//void View::init_vectors() {
//    members.resize(num_members);
//    member_ips.resize(num_members);
//    failed.resize(num_members, 0);
//}


int View::rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
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
    for(int rank = 0; rank < num_members; ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}


void View::announce_new_view(const View& Vc) {
    //    std::cout <<"Process " << Vc.members[Vc.my_rank] << " New view: " <<
    //    Vc.ToString() << std::endl;
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
    for(int i = 0; i < departed.size(); ++i) {
        s << members[departed[i]] << " ";
    }
    s << "} , Joined: { ";
    for(int i = 0; i < joined.size(); ++i) {
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
    if(swap_view == nullptr ||
       (view != nullptr && view->vid >= swap_view->vid)) {
        return view;
    } else {
        return swap_view;
    }
}

std::unique_ptr<View> make_initial_view(const node_id_t my_id, const ip_addr my_ip) {
    std::unique_ptr<View> new_view = std::make_unique<View>(1);
    new_view->members[0] = my_id;
    new_view->my_rank = 0;
    new_view->member_ips[0] = my_ip;
    new_view->failed[0] = false;
    new_view->i_know_i_am_leader = true;
    return new_view;
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


std::istream& operator>>(std::istream& stream, View& view) {
    std::string line;
    if(std::getline(stream, line)) {
        view.vid = std::stoi(line);
    }
    //"List of member IDs" line
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::copy(std::istream_iterator<node_id_t>(linestream), std::istream_iterator<node_id_t>(),
                  std::back_inserter(view.members));
    }
    //"List of member IPs" line
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::copy(std::istream_iterator<ip_addr>(linestream), std::istream_iterator<ip_addr>(),
                  std::back_inserter(view.member_ips));
    }
    //Failures array line, which was printed as "T" or "F" strings
    if(std::getline(stream, line)) {
        std::istringstream linestream(line);
        std::string fail_str;
        while(linestream >> fail_str) {
            view.failed.emplace_back(fail_str == "T" ? true : false);
        }
    }
    //The last three lines each contain a single number
    if(std::getline(stream, line)) {
        view.num_failed = std::stoi(line);
    }
    if(std::getline(stream, line)) {
        view.num_members = std::stoi(line);
    }
    if(std::getline(stream, line)) {
        view.my_rank = std::stoi(line);
    }
    return stream;
}
}
