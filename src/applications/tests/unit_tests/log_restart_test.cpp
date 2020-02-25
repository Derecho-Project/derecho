/**
 * @file log_restart_test.cpp
 *
 * This program can be used to test Derecho's ability to restart a service from
 * persistent logs on disk. It will create a group with one persistent subgroup
 * and one non-persistent subgroup, with the persistent subgroup split into a
 * configurable number of shards, and have the persistent subgroup make many
 * random changes to each shard's state.
 */

#include <cstring>
#include <iostream>
#include <random>

#include <derecho/core/derecho.hpp>

/*
 * The Eclipse CDT parser crashes if it tries to expand the REGISTER_RPC_FUNCTIONS
 * macro and thus fails to syntax-highlight any file that uses it. This definition
 * makes the RPC macros no-ops when the CDT parser tries to expand them, which
 * allows it to continue syntax-highlighting the rest of the file. It has no effect
 * on the actual compiler used to compile this program.
 */
#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

using derecho::Replicated;
using namespace persistent;

/* Simple classes to use as Replicated Objects for this test, one persistent and one non-persistent */

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

    DEFAULT_SERIALIZATION_SUPPORT(PersistentThing, state);
    REGISTER_RPC_FUNCTIONS(PersistentThing, read_state, change_state);
};

class NonPersistentThing : public mutils::ByteRepresentable {
    int state;

public:
    NonPersistentThing(const int init_state) : state(init_state) {}
    int read_state() {
        return state;
    }
    void change_state(int new_int) {
        state = new_int;
    }
    DEFAULT_SERIALIZATION_SUPPORT(NonPersistentThing, state);
    REGISTER_RPC_FUNCTIONS(NonPersistentThing, read_state, change_state);
};

/* Test parameters */

//Number of nodes per shard that can fail before the shard is considered inadequate
constexpr int fault_tolerance = 1;
//Number of nodes in the non-persistent subgroup, which has no state to recover
constexpr int non_persistent_subgroup_size = 2;

//Number of updates each member of a persistent shard will multicast to the others
constexpr int num_persistent_updates = 100000;
//Number of updates each member of the non-persistent shard will multicast to the others
constexpr int num_volatile_updates = 100000;

/*
 * Command-line arguments:
 * 1. total number of nodes in this test
 * 2. nodes per shard of the persistent subgroup
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "restart");
    std::mt19937 random_generator(getpid());
    int num_args = 2;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]))) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] total_nodes members_per_persistent_shard" << std::endl;
        return -1;
    }

    const uint num_nodes = std::stoi(argv[argc - num_args]);
    const uint members_per_shard = std::stoi(argv[argc - num_args + 1]);
    if(num_nodes < members_per_shard + non_persistent_subgroup_size) {
        std::cout << "Must have at least " << (members_per_shard + non_persistent_subgroup_size)
                  << " members" << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    //The number of shards for the persistent subgroup is configurable;
    //the non-persistent subgroup will always have 1 shard
    const uint num_shards = (num_nodes - non_persistent_subgroup_size) / members_per_shard;
    derecho::SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(PersistentThing)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(
                      num_shards, members_per_shard - fault_tolerance, members_per_shard))},
             {std::type_index(typeid(NonPersistentThing)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(
                      1, non_persistent_subgroup_size - fault_tolerance, non_persistent_subgroup_size))}}));

    auto persistent_factory = [](PersistentRegistry* pr) {
        return std::make_unique<PersistentThing>(pr);
    };

    auto nonpersistent_factory = [](PersistentRegistry* pr) {
        return std::make_unique<NonPersistentThing>(0);
    };

    derecho::Group<PersistentThing, NonPersistentThing> group(subgroup_info,
                                                              persistent_factory,
                                                              nonpersistent_factory);
    if(group.get_my_rank() == -1) {
        std::cout << "Error joining group! My rank is -1" << std::endl;
        return 1;
    }
    //Determine whether the running process is in the persistent or non-persistent subgroup
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> non_persistent_members = group.get_subgroup_members<NonPersistentThing>(0)[0];
    if(std::find(non_persistent_members.begin(), non_persistent_members.end(), my_id)
       == non_persistent_members.end()) {
        //"Main" code for the members of PersistentThing shards
        std::cout << "In the PersistentThing subgroup" << std::endl;
        Replicated<PersistentThing>& persistent_handle = group.get_subgroup<PersistentThing>();
        for(int counter = 0; counter < num_persistent_updates; ++counter) {
            derecho::rpc::QueryResults<int> results = persistent_handle.ordered_send<RPC_NAME(read_state)>();
            derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
            for(auto& reply_pair : replies) {
                try {
                    dbg_default_debug("Waiting on read_state reply from node {}", reply_pair.first);
                    reply_pair.second.get();
                } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                    dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
                }
            }
            int new_value = random_generator();
            persistent_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            if(counter % 1000 == 0) {
                std::cout << "Done with counter = " << counter << std::endl;
            }
        }
    } else {
        //"Main" code for the members of the NonPersistentThing subgroup
        std::cout << "In the NonPersistentThing subgroup" << std::endl;
        Replicated<NonPersistentThing>& thing_handle = group.get_subgroup<NonPersistentThing>();
        for(int counter = 0; counter < num_volatile_updates; ++counter) {
            derecho::rpc::QueryResults<int> results = thing_handle.ordered_send<RPC_NAME(read_state)>();
            derecho::rpc::QueryResults<int>::ReplyMap& replies = results.get();
            for(auto& reply_pair : replies) {
                try {
                    dbg_default_debug("Waiting on read_state reply from node {}", reply_pair.first);
                    reply_pair.second.get();
                } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                    dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
                }
            }
            int new_value = random_generator();
            thing_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            if(counter % 1000 == 0) {
                std::cout << "Done with counter = " << counter << std::endl;
            }
        }
    }
    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
