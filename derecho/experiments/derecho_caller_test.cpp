#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "block_size.h"
#include "derecho/derecho.h"
#include "rdmc/util.h"

using std::vector;
using std::map;
using std::string;
using std::cout;
using std::endl;
using namespace std;
using namespace mutils;

using derecho::MulticastGroup;
using derecho::DerechoSST;

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

    enum Functions { READ_STATE,
                     CHANGE_STATE };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<READ_STATE>(&test1_str::read_state),
                               derecho::rpc::tag<CHANGE_STATE>(&test1_str::change_state));
    }
};

template <typename T>
void output_result(typename derecho::rpc::QueryResults<T>::ReplyMap& rmap) {
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
    srand(time(NULL));

    string leader_ip;
    uint32_t my_id;
    string my_ip;
    cout << "Enter my id" << endl;
    cin >> my_id;
    cout << "Enter my ip" << endl;
    cin >> my_ip;
    cout << "Enter leader ip" << endl;
    cin >> leader_ip;

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = get_block_size(max_msg_size);
    // int num_messages = 10;

    auto stability_callback = [](uint32_t subgroup_num, int sender_id, long long int index, char* buf,
                                 long long int msg_size) {};

    derecho::DerechoParams derecho_params{max_msg_size, block_size};
    derecho::SubgroupInfo subgroup_info{{{std::type_index(typeid(test1_str)), &derecho::one_subgroup_entire_view}}};
    derecho::Group<test1_str>* managed_group;

    auto new_view_callback = [](const derecho::View& new_view) {
        std::vector<derecho::node_id_t> old_members;
        old_members.insert(old_members.begin(), new_view.departed.begin(), new_view.departed.end());
        //"copy from members to old_members as long as members[i] is not in joined"
        std::copy_if(new_view.members.begin(), new_view.members.end(), std::back_inserter(old_members),
                     [&new_view](const derecho::node_id_t& elem) {
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

    if(my_id == 0) {
        managed_group = new derecho::Group<test1_str>(
                my_ip, {stability_callback, {}}, subgroup_info,
                derecho_params, {new_view_callback}, 12345,
                []() { return std::make_unique<test1_str>(); });
    }

    else {
        managed_group = new derecho::Group<test1_str>(
                my_id, my_ip, leader_ip,
                {stability_callback, {}}, subgroup_info,
                {new_view_callback}, 12345,
                []() { return std::make_unique<test1_str>(); });
    }

    cout << "Finished constructing/joining Group" << endl;

    // other nodes (first two) change each other's state
    if(my_id != 2) {
        cout << "Changing each other's state to 35" << endl;
        derecho::Replicated<test1_str>& rpc_handle = managed_group->get_subgroup<test1_str>(0);
        output_result<bool>(rpc_handle.ordered_query<test1_str::CHANGE_STATE>({1 - my_id},
                                                                              36 - my_id)
                                    .get());
    }

    while(managed_group->get_members().size() < 3) {
    }

    // all members verify every node's state
    cout << "Reading everyone's state" << endl;
    derecho::Replicated<test1_str>& rpc_handle = managed_group->get_subgroup<test1_str>(0);
    output_result<int>(rpc_handle.ordered_query<test1_str::READ_STATE>({}).get());

    cout << "Done" << endl;
    cout << "Reached here" << endl;
    // wait forever
    while(true) {
    }
}
