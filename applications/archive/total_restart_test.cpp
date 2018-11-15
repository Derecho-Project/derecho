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
    void change_state(const int& new_int) {
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
    derecho::Conf::initialize(argc, argv);

    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << std::endl;
            }};
    const int num_subgroups = 1;
    const int num_shards = 2;
    const int members_per_shard = 3;
    derecho::SubgroupInfo subgroup_info(
            {{std::type_index(typeid(PersistentThing)),
              [&](const derecho::View& curr_view, int& next_unassigned_rank) {
                  if(curr_view.num_members < (num_subgroups * num_shards * members_per_shard)) {
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t layout_vector(num_subgroups);
                  int member_rank = 0;
                  for(uint subgroup_num = 0; subgroup_num < num_subgroups; ++subgroup_num) {
                      for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                          std::vector<node_id_t> subview_members(&curr_view.members[member_rank],
                                                                 &curr_view.members[member_rank + members_per_shard]);
                          layout_vector[subgroup_num].push_back(curr_view.make_subview(subview_members));
                          member_rank += members_per_shard;
                      }
                  }
                  return layout_vector;
              }}});

    auto thing_factory = [](PersistentRegistry* pr) { return std::make_unique<PersistentThing>(pr); };

    derecho::Group<PersistentThing> group(callback_set, subgroup_info,
                                          std::vector<derecho::view_upcall_t>{},
                                          thing_factory);
    std::cout << "Successfully joined group" << std::endl;
    Replicated<PersistentThing>& thing_handle = group.get_subgroup<PersistentThing>();
    int num_updates = 1000;
    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
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
