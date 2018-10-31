/**
 * @file total_restart_test.cpp
 *
 * This experiment creates a group with a simple persistent object (containing
 * only an integer as state) and continuously sends updates to it. We can use it
 * to test total restart by manually killing all the nodes while it is running,
 * then re-starting them in different orders.
 */
#include <iostream>

#include "derecho/derecho.h"
#include "initialize.h"

/*
 * The Eclipse CDT parser crashes if it tries to expand the REGISTER_RPC_FUNCTIONS
 * macro, probably because there are too many layers of variadic argument expansion.
 * This definition makes the RPC macros no-ops when the CDT parser tries to expand
 * them, which allows it to continue syntax-highlighting the rest of the file.
 */
#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

using namespace persistent;
using derecho::Replicated;

class PersistentThing : public mutils::ByteRepresentable, public derecho::PersistsFields {
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
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};
    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << std::endl;
            }};
    derecho::SubgroupInfo subgroup_info({{std::type_index(typeid(PersistentThing)),
                                          derecho::DefaultSubgroupAllocator(
                                                  derecho::one_subgroup_policy(derecho::even_sharding_policy(2, 3)))}});

    auto thing_factory = [](PersistentRegistry* pr) { return std::make_unique<PersistentThing>(pr); };

    std::unique_ptr<derecho::Group<PersistentThing>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<PersistentThing>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{},
                thing_factory);
    } else {
        group = std::make_unique<derecho::Group<PersistentThing>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{},
                thing_factory);
    }
    std::cout << "Successfully joined group" << std::endl;
    Replicated<PersistentThing>& thing_handle = group->get_subgroup<PersistentThing>();
    int num_updates = 1000;
    for(int counter = 0; counter < num_updates; ++counter) {
        //This ensures the state changes with every update from every node
        int new_value = counter * 10 + node_id;
        std::cout << "Updating state to " << new_value << std::endl;
        thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
        derecho::rpc::QueryResults<int> results = thing_handle.ordered_query<RPC_NAME(read_state)>();
        derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
        int curr_state = 0;
        for(auto& reply_pair : replies) {
            try {
                curr_state = reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
        std::cout << "Current state according to ordered_query: " << curr_state << std::endl;
    }
    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
