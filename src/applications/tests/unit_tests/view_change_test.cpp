/**
 * @file view_change_test.cpp
 *
 * This program can be used to test View changes that happen while a subgroup
 * is continuously multicasting RPCs. It will create a subgroup with one or
 * more flexible-sized shards, with a minimum of 1 node each, so that the group
 * will remain adequately provisioned as nodes join and leave repeatedly. Note
 * that it is still up to the user to manually kill nodes to simulate failures,
 * since we don't have a good programmatic way to make a node "crash."
 */

#include <iostream>
#include <memory>

#include <derecho/core/derecho.hpp>

class TestObject : public mutils::ByteRepresentable {
    int state;

public:
    TestObject(int init_state) : state(init_state) {}

    int read_state() const {
        return state;
    }

    //Returns true so that replicas will send an acknowledgement when the update is complete
    bool update(int new_state) {
        state = new_state;
        return true;
    }

    DEFAULT_SERIALIZATION_SUPPORT(TestObject, state);
    REGISTER_RPC_FUNCTIONS(TestObject, P2P_TARGETS(read_state), ORDERED_TARGETS(update));
};

class PersistentTestObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
    persistent::Persistent<std::string> data;

public:
    PersistentTestObject(persistent::Persistent<std::string>& init_data) : data(std::move(init_data)) {}
    PersistentTestObject(persistent::PersistentRegistry* registry) : data(std::make_unique<std::string>, "PersistentTestObjectData", registry) {}

    std::string read_state() const {
        return *data;
    }

    bool update(const std::string& new_data) {
        *data = new_data;
        return true;
    }

    DEFAULT_SERIALIZATION_SUPPORT(PersistentTestObject, data);
    REGISTER_RPC_FUNCTIONS(PersistentTestObject, P2P_TARGETS(read_state), ORDERED_TARGETS(update));
};

constexpr int updates_per_loop = 100000;

void persistent_test(uint32_t num_shards, uint32_t max_shard_size);
void nonpersistent_test(uint32_t num_shards, uint32_t max_shard_size);

int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "view_change_test");

    int num_args = 3;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]))) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] <num_shards> <max_shard_size> <persistence_mode>" << std::endl;
        return -1;
    }

    const uint32_t num_shards = std::stoi(argv[argc - num_args]);
    const uint32_t max_shard_size = std::stoi(argv[argc - num_args + 1]);
    const bool use_persistence = (std::string(argv[argc - num_args + 2]) == "on");

    derecho::Conf::initialize(argc, argv);

    if(use_persistence) {
        persistent_test(num_shards, max_shard_size);
    } else {
        nonpersistent_test(num_shards, max_shard_size);
    }
}

void nonpersistent_test(uint32_t num_shards, uint32_t max_shard_size) {
    derecho::SubgroupInfo layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(TestObject)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(num_shards, 1, max_shard_size))}}));

    auto test_object_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
        return std::make_unique<TestObject>(0);
    };

    derecho::Group<TestObject> group(layout, test_object_factory);

    bool test_done = false;
    while(!test_done) {
        std::cout << "Sending " << updates_per_loop << " multicast updates" << std::endl;
        derecho::Replicated<TestObject>& replica_group = group.get_subgroup<TestObject>();
        for(int counter = 0; counter < updates_per_loop; ++counter) {
            derecho::rpc::QueryResults<bool> update_results = replica_group.ordered_send<RPC_NAME(update)>(counter);
            try {
                //Wait for the first entry in the reply map to get its results
                //This will confirm that the update was delivered to all replicas
                update_results.get().begin()->second.get();
            } catch(derecho::derecho_exception& ex) {
                dbg_default_warn("Exception occurred while awaiting reply to update #{}. What(): {}", counter, ex.what());
            }
            if(counter % 1000 == 0) {
                std::cout << "Done with " << counter << " updates" << std::endl;
            }
        }
        //Maybe this will ensure all the log messages finish printing before the stdout line
        dbg_default_flush();
        std::cout << "Done sending updates. Send " << updates_per_loop << " more? [y/N] ";
        std::string response;
        std::cin >> response;
        if(response == "y" || response == "Y") {
            test_done = false;
        } else {
            test_done = true;
        }
    }
    group.leave();
}

void persistent_test(uint32_t num_shards, uint32_t max_shard_size) {
    derecho::SubgroupInfo layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(PersistentTestObject)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(num_shards, 1, max_shard_size))}}));

    auto test_object_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
        return std::make_unique<PersistentTestObject>(pr);
    };

    derecho::Group<PersistentTestObject> group(layout, test_object_factory);

    bool test_done = false;
    while(!test_done) {
        std::cout << "Sending " << updates_per_loop << " multicast updates" << std::endl;
        derecho::Replicated<PersistentTestObject>& replica_group = group.get_subgroup<PersistentTestObject>();
        for(int counter = 0; counter < updates_per_loop; ++counter) {
            derecho::rpc::QueryResults<bool> update_results = replica_group.ordered_send<RPC_NAME(update)>("Update number " + std::to_string(counter));
            try {
                //Wait for the first entry in the reply map to get its results
                //This will confirm that the update was delivered to all replicas
                bool success = update_results.get().begin()->second.get();
            } catch(derecho::derecho_exception& ex) {
                dbg_default_warn("Exception occurred while awaiting reply to update #{}. What(): {}", counter, ex.what());
            }
        }
        //Maybe this will ensure all the log messages finish printing before the stdout line
        dbg_default_flush();
        std::cout << "Done sending updates. Send " << updates_per_loop << " more? [y/N] ";
        std::string response;
        std::cin >> response;
        if(response == "y" || response == "Y") {
            test_done = false;
        } else {
            test_done = true;
        }
    }
    group.leave();
}
