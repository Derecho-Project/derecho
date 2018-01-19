/**
 * @file persistent_rejoin_test.cpp
 *
 * An experiment that tests the log-appending mechanism for recovering nodes
 * with persistent state.
 */

#include <iostream>

#include "derecho/derecho.h"
#include "initialize.h"

using derecho::Replicated;

class PersistentThing : public mutils::ByteRepresentable {
    Persistent<int> state;
public:
    PersistentThing(Persistent<int>& init_state) : state(std::move(init_state)) {}
    PersistentThing(PersistentRegistry* registry) : state(nullptr, registry) {}
    int read_state() {
        return *state;
    }
    void change_state(int new_int) {
        *state = new_int;
    }
    void print_log() {
        int64_t num_versions = state.getNumOfVersions();
        int64_t index_num = state.getEarliestIndex();
        std::cout << "PersistentThing.state log: [";
        for(int64_t version_count = 0; version_count < num_versions; ++version_count) {
            std::cout << "(" << index_num << "," << *state.getByIndex(index_num) << ") ";
            index_num++;
        }
        std::cout << "]" << std::endl;
    }
    DEFAULT_SERIALIZATION_SUPPORT(PersistentThing, state);
    REGISTER_RPC_FUNCTIONS(PersistentThing, read_state, change_state, print_log);
};


int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Derecho message parameters
    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};
    derecho::CallbackSet callback_set{
        nullptr,
        [](derecho::subgroup_id_t subgroup,ns_persistent::version_t ver){
            std::cout<<"Subgroup "<<subgroup<<", version "<<ver<<" is persisted."<<std::endl;
        }
    };
    derecho::SubgroupInfo subgroup_info{{
        {std::type_index(typeid(PersistentThing)), &derecho::one_subgroup_entire_view}},
        {std::type_index(typeid(PersistentThing))}
    };

    auto thing_factory = [](PersistentRegistry* pr) { return std::make_unique<PersistentThing>(pr); };

    std::unique_ptr<derecho::Group<PersistentThing>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<PersistentThing>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, 12345,
                thing_factory);
    } else {
        group = std::make_unique<derecho::Group<PersistentThing>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, 12345,
                thing_factory);
    }
    std::cout << "Successfully joined group" << std::endl;
    Replicated<PersistentThing>& persistent_thing_handle = group->get_subgroup<PersistentThing>();
    if(node_id == 3) {
        std::cout << "Printing initial Persistent log" << std::endl;
        persistent_thing_handle.ordered_send<RPC_NAME(print_log)>();
    }
    int num_updates;
    if(node_id == 3) {
        num_updates = 300;
    } else {
        num_updates = 1000;
    }
    for(int counter = 0; counter < num_updates; ++counter) {
        int new_value = counter * 10 + node_id;
        std::cout << "Updating state to " << new_value << std::endl;
        persistent_thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
        derecho::rpc::QueryResults<int> results = persistent_thing_handle.ordered_query<RPC_NAME(read_state)>();
        derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
        int curr_state = 0;
        for(auto& reply_pair : replies) {
            curr_state = reply_pair.second.get();
        }
        std::cout << "Current state according to ordered_query: " << curr_state << std::endl;
    }
    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
