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
                // cout << "Subgroup " << subgroup << ", version " << ver << " is persisted." << endl;
            }};
    derecho::SubgroupInfo subgroup_info([num_nodes, members_per_shard](const std::type_index& subgroup_type,
                                                                       const std::unique_ptr<derecho::View>& prev_view,
                                                                       derecho::View& curr_view) {
        const uint num_subgroups = 1;
        const uint num_shards = num_nodes/members_per_shard;
        if((uint32_t)curr_view.num_members < num_nodes) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t layout_vector(num_subgroups);
        int member_rank = 0;
        for(uint subgroup_num = 0; subgroup_num < num_subgroups; ++subgroup_num) {
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                std::vector<node_id_t> subview_members(&curr_view.members[member_rank],
                                                       &curr_view.members[member_rank + members_per_shard]);
		// cout << "subgroup = " << subgroup_num << ", shard = " << shard_num << ", members = ";
                // for(auto m : subview_members) {
                //     cout << m << " ";
                // }
		// cout << endl;
                layout_vector[subgroup_num].push_back(curr_view.make_subview(subview_members));
                member_rank += members_per_shard;
            }
        }
        return layout_vector; });

    auto thing_factory = [](PersistentRegistry* pr) {
        return std::make_unique<PersistentThing>(pr);
    };

    std::ofstream fout;
    fout.open("metadata_data", std::ofstream::app);
    fout << num_nodes << " " << members_per_shard << " ";
    fout.close();

    derecho::Group<PersistentThing> group(callback_set, subgroup_info, nullptr,
                                          std::vector<derecho::view_upcall_t>{},
                                          thing_factory);
    uint32_t my_rank = group.get_my_rank();
    if(my_rank < (num_nodes / members_per_shard) * members_per_shard) {
        Replicated<PersistentThing>& thing_handle = group.get_subgroup<PersistentThing>();
        while (true) {
            derecho::rpc::QueryResults<int> results = thing_handle.ordered_send<RPC_NAME(read_state)>();
            derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
            // int curr_state = 0;
            for(auto& reply_pair : replies) {
                try {
                    // curr_state =
                    reply_pair.second.get();
                } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                    // cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << endl;
                }
            }
            // cout << "Current state according to ordered_send: " << curr_state << endl;

            //This ensures the state changes with every update from every node
            // int new_value = counter * 10 + node_id;
            int new_value = rand() % 100;
            // cout << "Updating state to " << new_value << endl;
            thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
        }
    }

    while(true) {
      
    }
}
