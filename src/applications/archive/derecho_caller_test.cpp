#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <spdlog/spdlog.h>
#include <derecho/conf/conf.hpp>

using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;
using namespace std;
using namespace mutils;

using namespace derecho;
using namespace persistent;

int count = 0;

struct test1_str {
    int state;
    int read_state() {
        cout << "Returning state, it is: " << state << endl;
        return state;
    }
    bool change_state(int new_state) {
        cout << "Previous state was: " << state << endl;
        state = new_state;
        cout << "Current state is: " << state << endl;
        return true;
    }

    REGISTER_RPC_FUNCTIONS(test1_str, read_state, change_state);
};

template <typename T>
void output_result(typename rpc::QueryResults<T>::ReplyMap& rmap) {
    cout << "Obtained a reply map" << endl;
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        try {
            cout << "Reply from node " << it->first << ": " << it->second.get()
                 << endl;
        } catch(const std::exception& e) {
            cout << e.what() << endl;
        }
    }
}

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "derecho_caller");
    spdlog::set_level(spdlog::level::trace);

    Conf::initialize(argc, argv);

    auto stability_callback = [](uint32_t subgroup_num, int sender_id, long long int index, 
                                 std::optional<std::pair<char*, long long int>>,
                                 persistent::version_t ver) {};

    SubgroupInfo subgroup_info{&one_subgroup_entire_view};
    
    auto new_view_callback = [](const View& new_view) {
        std::vector<node_id_t> old_members;
        old_members.insert(old_members.begin(), new_view.departed.begin(), new_view.departed.end());
        //"copy from members to old_members as long as members[i] is not in joined"
        std::copy_if(new_view.members.begin(), new_view.members.end(), std::back_inserter(old_members),
                     [&new_view](const node_id_t& elem) {
                         return std::find(new_view.joined.begin(), new_view.joined.end(), elem) == new_view.joined.end();
                     });
        cout << "New members are : " << endl;
        for(auto n : new_view.members) {
            cout << n << " ";
        }
        cout << endl;
        cout << "Old members were :" << endl;
        for(auto o : old_members) {
            cout << o << " ";
        }
        cout << endl;
    };

    Group<test1_str> managed_group({stability_callback}, subgroup_info, nullptr,
                                   {new_view_callback},
                                   [](PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<test1_str>(); });

    cout << "Finished constructing/joining Group" << endl;

    auto my_rank = managed_group.get_my_rank();
    // other nodes (first two) change each other's state
    if(my_rank != 2) {
        cout << "Changing other's state to " << 36 - my_rank << endl;
        Replicated<test1_str>& rpc_handle = managed_group.get_subgroup<test1_str>(0);
        output_result<bool>(rpc_handle.p2p_send<RPC_NAME(change_state)>(1 - my_rank, 36 - my_rank).get());
    }

    while(managed_group.get_members().size() < 3) {
    }

    // all members verify every node's state
    cout << "Reading everyone's state" << endl;
    Replicated<test1_str>& rpc_handle = managed_group.get_subgroup<test1_str>(0);
    output_result<int>(rpc_handle.ordered_send<RPC_NAME(read_state)>().get());

    cout << "Done" << endl;
    cout << "Reached here" << endl;
    // wait forever
    while(true) {
    }
}
