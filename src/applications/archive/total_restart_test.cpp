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
#include <chrono>

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

using std::cout;
using std::endl;

class ResultSST : public sst::SST<ResultSST> {
public:
    sst::SSTField<double> time_in_ms;
    ResultSST(const sst::SSTParams& params)
            : SST<ResultSST>(this, params) {
        SSTInit(time_in_ms);
    }
};

class PersistentThing : public mutils::ByteRepresentable, public derecho::PersistsFields {
    Persistent<std::string> state;

public:
    PersistentThing(Persistent<std::string>& init_state) : state(std::move(init_state)) {}
    PersistentThing(PersistentRegistry* registry) : state([]() { return std::make_unique<std::string>(); }, nullptr, registry) {}
    std::string read_state() {
        return *state;
    }
    void change_state(const std::string& new_stuff) {
        *state = new_stuff;
    }
    void print_log() {
        int64_t num_versions = state.getNumOfVersions();
        int64_t index_num = state.getEarliestIndex();
        cout << "PersistentThing.state log: [";
        for(int64_t version_count = 0; version_count < num_versions; ++version_count) {
            cout << "(" << index_num << "," << *state.getByIndex(index_num) << ") ";
            index_num++;
        }
        cout << "]" << endl;
    }

    DEFAULT_SERIALIZATION_SUPPORT(PersistentThing, state);
    REGISTER_RPC_FUNCTIONS(PersistentThing, read_state, change_state, print_log);
};

std::string make_random_string(uint length) {
    std::stringstream string;
    for(uint i = 0; i < length; ++i) {
        char next = 'a'+ (rand() % 26);
        string << next;
    }
    return string.str();
}

int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "restart_test");
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
                // cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << endl;
            }};

    const int num_shards = num_nodes / members_per_shard;
    const int fault_tolerance = 0;
    derecho::SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator({
        {std::type_index(typeid(PersistentThing)), derecho::one_subgroup_policy(derecho::flexible_even_shards(
                num_shards, members_per_shard - fault_tolerance, members_per_shard))}
    }));

    auto thing_factory = [](PersistentRegistry* pr) {
        return std::make_unique<PersistentThing>(pr);
    };

    auto start_time = std::chrono::high_resolution_clock::now();
    derecho::Group<PersistentThing> group(callback_set, subgroup_info, nullptr,
                                          std::vector<derecho::view_upcall_t>{},
                                          thing_factory);
    // end timer
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> time_in_ms = end_time - start_time;
    std::cout << "Done starting up" << std::endl;
    uint32_t my_rank = group.get_my_rank();
    if (my_rank == 0) {
        std::ofstream fout;
        fout.open("total_restart_data", std::ofstream::app);
        fout << num_nodes << " " << members_per_shard << " " << time_in_ms.count() << std::endl;
        fout.close();
        std::cout << "Done writing the time measurement" << std::endl;
    }

    const int update_size = 1024;

    if(my_rank <= num_shards * members_per_shard) {
        Replicated<PersistentThing>& thing_handle = group.get_subgroup<PersistentThing>();
        while (true) {
            derecho::rpc::QueryResults<std::string> results = thing_handle.ordered_send<RPC_NAME(read_state)>();
            derecho::rpc::QueryResults<std::string>::ReplyMap& replies = results.get();
            for(auto& reply_pair : replies) {
                try {
                    dbg_default_debug("Waiting on read_state reply from node {}", reply_pair.first);
                    reply_pair.second.get();
                } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                    dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
                }
            }

//            std::this_thread::sleep_for(std::chrono::milliseconds(300));

            std::string new_value = make_random_string(update_size);
            // cout << "Updating state to " << new_value << endl;
            thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
        }
    }

    std::cout << "Done with main, waiting..." << std::endl;
    while(true) {
    }
    return 0;
}
