/**
 * @file total_restart_test.cpp
 *
 * This experiment creates a group with a simple persistent object (containing
 * only an integer as state) and continuously sends updates to it. We can use it
 * to test total restart by manually killing all the nodes while it is running,
 * then re-starting them in different orders.
 */
#include <iostream>
#include <time.h>

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
using std::cout;
using std::endl;

class PersistentThing : public mutils::ByteRepresentable, public derecho::PersistsFields {
    Persistent<int> state;

public:
    PersistentThing(Persistent<int>& init_state) : state(std::move(init_state)) {}
    PersistentThing(PersistentRegistry* registry) : state([]() { return std::make_unique<int>(); }, nullptr, registry) {}
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
    pthread_setname_np(pthread_self(), "restart");
    srand(getpid());
    int num_args = 2;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] num_nodes members_per_shard" << endl;
        return -1;
    }

    const uint num_nodes = std::stoi(argv[argc - num_args]);
    const uint members_per_shard = std::stoi(argv[argc - num_args + 1]);
    if(num_nodes < members_per_shard) {
        cout << "Must have at least " << members_per_shard << " members" << endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                // std::cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << std::endl;
            }};

    const int num_shards = num_nodes / members_per_shard;
    const int fault_tolerance = 1;
    derecho::SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator({
        {std::type_index(typeid(PersistentThing)), derecho::one_subgroup_policy(derecho::flexible_even_shards(
                num_shards, members_per_shard - fault_tolerance, members_per_shard))}
    }));


    auto thing_factory = [](PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PersistentThing>(pr);
    };

    derecho::Group<PersistentThing> group(callback_set, subgroup_info, nullptr,
                                          std::vector<derecho::view_upcall_t>{},
                                          thing_factory);

    auto my_rank = group.get_my_rank();
    if (my_rank == -1) {
        std::cout << "ERROR!! My rank is -1" << std::endl;
	return 1;
    }

    if(static_cast<unsigned>(my_rank) <= (num_shards * members_per_shard)) {
        Replicated<PersistentThing>& thing_handle = group.get_subgroup<PersistentThing>();
        int num_updates = 1000000;
        for(int counter = 0; counter < num_updates; ++counter) {
            derecho::rpc::QueryResults<int> results = thing_handle.ordered_send<RPC_NAME(read_state)>();
            derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
            // int curr_state = 0;
            for(auto& reply_pair : replies) {
                try {
                    dbg_default_debug("Waiting on read_state reply from node {}", reply_pair.first);
                    reply_pair.second.get();
                } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                    dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
                }
            }
            // std::cout << "Current state according to ordered_send: " << curr_state << std::endl;

//            std::this_thread::sleep_for(std::chrono::milliseconds(300));

            //This ensures the state changes with every update from every node
            // int new_value = counter * 10 + node_id;
            int new_value = rand() % 100;
            // std::cout << "Updating state to " << new_value << std::endl;
            thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            if(counter % 1000 == 0) {
                std::cout << "Done with counter = " << counter << std::endl;
            }
        }
    }
    // std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
