/**
 * An experiment that tests the log-appending mechanism for recovering nodes
 * with persistent state.
 */

#include <iostream>

#include <derecho/core/derecho.hpp>

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
    PersistentThing(PersistentRegistry* registry) : state([](){return std::make_unique<int>(0);}, nullptr, registry) {}
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

class TestThing : public mutils::ByteRepresentable {
    int state;

public:
    TestThing(const int init_state) : state(init_state) {}
    int read_state() {
        return state;
    }
    void change_state(int new_int) {
        state = new_int;
    }
    DEFAULT_SERIALIZATION_SUPPORT(TestThing, state);
    REGISTER_RPC_FUNCTIONS(TestThing, read_state, change_state);
};

int main(int argc, char** argv) {
  derecho::Conf::initialize(argc, argv);

    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << std::endl;
            }};
    derecho::SubgroupInfo subgroup_info{&derecho::one_subgroup_entire_view};

    auto thing_factory = [](PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<PersistentThing>(pr); };
    //    auto test_factory = [](PersistentRegistry* pr) { return std::make_unique<TestThing>(0); };

    derecho::Group<PersistentThing> group(callback_set, subgroup_info, nullptr,
                                          std::vector<derecho::view_upcall_t>{},
                                          thing_factory);
    std::cout << "Successfully joined group" << std::endl;
    Replicated<PersistentThing>& thing_handle = group.get_subgroup<PersistentThing>();
    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    //Node 3 will rejoin as node 4
    if(node_id == 4) {
        std::cout << "Printing initial Persistent log" << std::endl;
        thing_handle.ordered_send<RPC_NAME(print_log)>();
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
        thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
        derecho::rpc::QueryResults<int> results = thing_handle.ordered_send<RPC_NAME(read_state)>();
        derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
        int curr_state = 0;
        for(auto& reply_pair : replies) {
            try {
                curr_state = reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
        std::cout << "Current state according to ordered_send: " << curr_state << std::endl;
    }
    if(node_id != 3) {
        std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
        while(true) {
        }
    }
}
