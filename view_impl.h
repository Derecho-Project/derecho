#include <memory>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

#include "view.h"

namespace derecho {

using std::string;
using std::shared_ptr;

template <typename handlersType>
View<handlersType>::View()
    : View(0) {}

template <typename handlersType>
View<handlersType>::View(int num_members)
    : vid(0),
      members(num_members),
      member_ips(num_members),
      failed(num_members, 0),
      nFailed(0),
      who(nullptr),
      num_members(num_members),
      my_rank(0),
      derecho_group(nullptr),
      gmsSST(nullptr) {}

template <typename handlersType>
void View<handlersType>::init_vectors() {
    members.resize(num_members);
    member_ips.resize(num_members);
    failed.resize(num_members, 0);
}

template <typename handlersType>
int View<handlersType>::rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

template <typename handlersType>
int View<handlersType>::rank_of(const ip_addr& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips[rank] == who) {
            return rank;
        }
    }
    return -1;
}

template <typename handlersType>
int View<handlersType>::rank_of(const node_id_t& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}

template <typename handlersType>
void View<handlersType>::newView(const View& Vc) {
    // I don't know what this is supposed to do in real life. In the C#
    // simulation it just prints to stdout.
    //    std::cout <<"Process " << Vc.members[Vc.my_rank] << " New view: " <<
    //    Vc.ToString() << std::endl;
}

template <typename handlersType>
bool View<handlersType>::IAmLeader() const {
    return (rank_of_leader() == my_rank);  // True if I know myself to be the leader
}

template <typename handlersType>
bool View<handlersType>::IAmTheNewLeader() {
    if(IKnowIAmLeader) {
        return false;  // I am the OLD leader
    }

    for(int n = 0; n < my_rank; n++) {
        for(int row = 0; row < my_rank; row++) {
            if(!failed[n] && !(*gmsSST)[row].suspected[n]) {
                return false;  // I'm not the new leader, or some failure suspicion hasn't fully propagated
            }
        }
    }
    IKnowIAmLeader = true;
    return true;
}

template<typename handlersType>
void View<handlersType>::merge_changes() {
    int myRank = my_rank;
    // Merge the change lists
    for(int n = 0; n < num_members; n++) {
        if((*gmsSST)[myRank].nChanges < (*gmsSST)[n].nChanges) {
            gmssst::set((*gmsSST)[myRank].changes, (*gmsSST)[n].changes);
            gmssst::set((*gmsSST)[myRank].nChanges, (*gmsSST)[n].nChanges);
        }

        if((*gmsSST)[myRank].nCommitted < (*gmsSST)[n].nCommitted)  // How many I know to have been committed
        {
            gmssst::set((*gmsSST)[myRank].nCommitted, (*gmsSST)[n].nCommitted);
        }
    }
    bool found = false;
    for(int n = 0; n < num_members; n++) {
        if(failed[n]) {
            // Make sure that the failed process is listed in the Changes vector as a proposed change
            for(int c = (*gmsSST)[myRank].nCommitted; c < (*gmsSST)[myRank].nChanges && !found; c++) {
                if((*gmsSST)[myRank].changes[c % MAX_MEMBERS] == members[n]) {
                    // Already listed
                    found = true;
                }
            }
        } else {
            // Not failed
            found = true;
        }

        if(!found) {
            gmssst::set((*gmsSST)[myRank].changes[(*gmsSST)[myRank].nChanges % MAX_MEMBERS],
                        members[n]);
            gmssst::increment((*gmsSST)[myRank].nChanges);
        }
    }
    gmsSST->put();
}

template<typename handlersType>
void View<handlersType>::wedge() {
    derecho_group->wedge();  // RDMC finishes sending, stops new sends or receives in Vc
    gmssst::set((*gmsSST)[my_rank].wedged, true);
    gmsSST->put();
}

template <typename handlersType>
shared_ptr<node_id_t> View<handlersType>::Joined() const {
    if(who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for(int r = 0; r < num_members; r++) {
        if(members[r] == *who) {
            return who;
        }
    }
    return shared_ptr<node_id_t>();
}

template <typename handlersType>
shared_ptr<node_id_t> View<handlersType>::Departed() const {
    if(who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for(int r = 0; r < num_members; r++) {
        if(members[r] == *who) {
            return shared_ptr<node_id_t>();
        }
    }
    return who;
}

template <typename handlersType>
std::string View<handlersType>::ToString() const {
    // need to add member ips and other fields
    std::stringstream s;
    s << "View " << vid << ": MyRank=" << my_rank << ". ";
    string ms = " ";
    for(int m = 0; m < num_members; m++) {
        ms += std::to_string(members[m]) + string("  ");
    }

    s << "Members={" << ms << "}, ";
    string fs = (" ");
    for(int m = 0; m < num_members; m++) {
        fs += failed[m] ? string(" T ") : string(" F ");
    }

    s << "Failed={" << fs << " }, nFailed=" << nFailed;
    shared_ptr<node_id_t> dep = Departed();
    if(dep != nullptr) {
        s << ", Departed: " << *dep;
    }

    shared_ptr<node_id_t> join = Joined();
    if(join != nullptr) {
        s << ", Joined: " << *join;
    }

    //    s += string("\n") + gmsSST->ToString();
    return s.str();
}

template <typename handlersType>
void persist_view(const View<handlersType>& view, const std::string& view_file_name) {
    using namespace std::placeholders;
    //Use the "safe save" paradigm to ensure a crash won't corrupt our saved View file
    std::ofstream view_file_swap(view_file_name + persistence::SWAP_FILE_EXTENSION);
    auto view_file_swap_write = [&](char const* const c, std::size_t n) {
        //std::bind(&std::ofstream::write, &view_file_swap, _1, _2);
        view_file_swap.write(c,n);
    };
    mutils::post_object(view_file_swap_write, mutils::bytes_size(view));
    mutils::post_object(view_file_swap_write, view);
    view_file_swap.close();
    if(std::rename((view_file_name + persistence::SWAP_FILE_EXTENSION).c_str(), view_file_name.c_str()) < 0) {
        std::cerr << "Error updating saved-state file on disk! " << strerror(errno) << endl;
    }
}

template <typename handlersType>
std::unique_ptr<View<handlersType>> load_view(const std::string& view_file_name) {
    std::ifstream view_file(view_file_name);
    std::ifstream view_file_swap(view_file_name + persistence::SWAP_FILE_EXTENSION);
    std::unique_ptr<View<handlersType>> view;
    std::unique_ptr<View<handlersType>> swap_view;
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
            view = mutils::from_bytes<View<handlersType>>(nullptr, buffer);
        }
    }
    if(view_file_swap.good()) {
        std::size_t size_of_view;
        view_file_swap.read((char*)&size_of_view, sizeof(size_of_view));
        char buffer[size_of_view];
        view_file_swap.read(buffer, size_of_view);
        if(!view_file_swap.fail()) {
            swap_view = mutils::from_bytes<View<handlersType>>(nullptr, buffer);
        }
    }
    if(swap_view == nullptr ||
       (view != nullptr && view->vid >= swap_view->vid)) {
        return view;
    } else {
        return swap_view;
    }
}

template <typename handlersType>
std::ostream& operator<<(std::ostream& stream, const View<handlersType>& view) {
    stream << view.vid << std::endl;
    std::copy(view.members.begin(), view.members.end(), std::ostream_iterator<node_id_t>(stream, " "));
    stream << std::endl;
    std::copy(view.member_ips.begin(), view.member_ips.end(), std::ostream_iterator<ip_addr>(stream, " "));
    stream << std::endl;
    for(const auto& fail_val : view.failed) {
        stream << (fail_val ? "T" : "F") << " ";
    }
    stream << std::endl;
    stream << view.nFailed << endl;
    stream << view.num_members << endl;
    stream << view.my_rank << endl;
    return stream;
}

template <typename handlersType>
std::istream& operator>>(std::istream& stream, View<handlersType>& view) {
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
        view.nFailed = std::stoi(line);
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
