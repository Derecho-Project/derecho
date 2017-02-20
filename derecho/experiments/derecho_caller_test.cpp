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

    /**
     * This function will be called by Dispatcher to register functions from
     * this class as RPC functions. When called, it should call Dispatcher's
     * setup_rpc_class and supply each method that should be an RPC function
     * as an argument.
     * @param d The Dispatcher instance calling this function
     * @param ptr A pointer to an instance of this class
     * @return Whatever the result of Dispatcher::setup_rpc_class() is.
     */
    auto register_functions(derecho::rpc::RPCManager& m, std::unique_ptr<test1_str> *ptr) {
        assert(this == ptr->get());
        return m.setup_rpc_class(ptr, &test1_str::read_state,
                                    &test1_str::change_state);
    }
	enum class Functions : long long unsigned int { read_state, change_state};
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

    auto stability_callback = [](uint32_t subgroup_num, int sender_id, long long int index, char *buf,
                                 long long int msg_size) {};

    derecho::DerechoParams derecho_params{max_msg_size, block_size};
	derecho::SubgroupInfo subgroup_info { {
		{std::type_index(typeid(test1_str)), 1}
	}, { 
		{ {std::type_index(typeid(test1_str)), 0} , 1}
	},
		[](const derecho::View& curr_view, std::type_index subgroup_type, uint32_t subgroup_num, uint32_t shard_num) {
			if(subgroup_type == std::type_index(typeid(test1_str))) {
				return curr_view.members;
			}
			return std::vector<derecho::node_id_t>();
		}};
    derecho::Group<test1_str>* managed_group;

    if(my_id == 0) {
        managed_group = new derecho::Group<test1_str>(
            my_ip, {stability_callback, {}}, subgroup_info,
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
            }}, 12345, [](){return test1_str();});
    }

    else {
        managed_group = new derecho::Group<test1_str>(
            my_id, my_ip, leader_id, leader_ip, 
            {stability_callback, {}}, subgroup_info,
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
            }}, 12345, [](){return test1_str();});
    }

    cout << "Finished constructing/joining ManagedGroup" << endl;
    
    // other nodes (first two) change each other's state
    if(my_id != 2) {
      cout << "Changing each other's state to 35" << endl;
      derecho::Replicated<test1_str>& rpc_handle = managed_group->get_subgroup<test1_str>(0);
      output_result(rpc_handle.ordered_query<test1_str::Functions::change_state>({1 - my_id},
								      36 - my_id).get());
    }

    while(managed_group->get_members().size() < 3) {
    }
    
    // all members verify every node's state
    cout << "Reading everyone's state" << endl;
    derecho::Replicated<test1_str>& rpc_handle = managed_group->get_subgroup<test1_str>(0);
    output_result(rpc_handle.ordered_query<test1_str::Functions::read_state>({}).get());
    
    cout << "Done" << endl;
    cout << "Reached here" << endl;
    // wait forever
    while(true) {
    }
}
