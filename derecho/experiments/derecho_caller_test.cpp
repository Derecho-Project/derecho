#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "derecho/derecho.h"
#include "block_size.h"
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

struct test1_str{
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

    template <typename Dispatcher>
    auto register_functions(Dispatcher &d, std::unique_ptr<test1_str> *ptr) {
      assert(this == ptr->get());
        return d.register_functions(ptr, &test1_str::read_state,
                                    &test1_str::change_state);
    }
};

void output_result(auto& rmap) {
    cout << "Obtained a reply map" << endl;
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        try {
            cout << "Reply from node " << it->first << ": " << it->second.get()
                 << endl;
        } catch(const std::exception &e) {
            cout << e.what() << endl;
        }
    }
}

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t leader_id = 0;
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

    auto stability_callback = [](int sender_id, long long int index, char *buf,
                                 long long int msg_size) {};

    Dispatcher<test1_str> dispatchers(my_id, std::make_tuple());

    derecho::DerechoParams derecho_params{max_msg_size, block_size};
    derecho::Group<decltype(dispatchers)>* managed_group;

    if(my_id == 0) {
        managed_group = new derecho::Group<decltype(dispatchers)>(
            my_ip, std::move(dispatchers), {stability_callback, {}},
            derecho_params, {[](vector<derecho::node_id_t> new_members,
                                vector<derecho::node_id_t> old_members) {
                cout << "New members are : " << endl;
                for(auto n : new_members) {
                    cout << n << " ";
                }
                cout << endl;
                cout << "Old members were :" << endl;
                for(auto o : old_members) {
                    cout << o << " ";
                }
                cout << endl;
            }});
    }

    else {
        managed_group = new derecho::Group<decltype(dispatchers)>(
            my_id, my_ip, leader_id, leader_ip, std::move(dispatchers),
            {stability_callback, {}},
            {[](vector<derecho::node_id_t> new_members,
                vector<derecho::node_id_t> old_members) {
                cout << "New members are : " << endl;
                for(auto n : new_members) {
                    cout << n << " ";
                }
                cout << endl;
                cout << "Old members were :" << endl;
                for(auto o : old_members) {
                    cout << o << " ";
                }
                cout << endl;
            }});
    }

    cout << "Finished constructing/joining ManagedGroup" << endl;
    
    // other nodes (first two) change each other's state
    if(my_id != 2) {
      cout << "Changing each other's state to 35" << endl;
      output_result(managed_group->template orderedQuery<test1_str, 0>({1 - my_id},
								      36 - my_id).get());
    }

    while(managed_group->get_members().size() < 3) {
    }
    
    // all members verify every node's state
    cout << "Reading everyone's state" << endl;
    output_result(managed_group->template orderedQuery<test1_str, 0>({}).get());
    
    cout << "Done" << endl;
    cout << "Reached here" << endl;
    // wait forever
    while(true) {
    }
}
