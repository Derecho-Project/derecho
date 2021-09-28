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
        dbg_default_trace("Received RPC update {}", new_state);
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

void persistent_test(uint32_t num_shards, uint32_t max_shard_size, uint32_t num_updates);
void nonpersistent_test(uint32_t num_shards, uint32_t max_shard_size, uint32_t num_updates);
void new_view_callback(const derecho::View& new_view);

constexpr const char default_proc_name[] = "view_change_test";
constexpr int num_required_args = 4;

int main(int argc, char** argv) {
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < num_required_args) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] <num_shards> <max_shard_size> <num_updates> <persistence_on/off> [proc_name]" << std::endl;
        return -1;
    }

    const uint32_t num_shards = std::stoi(argv[dashdash_pos + 1]);
    const uint32_t max_shard_size = std::stoi(argv[dashdash_pos + 2]);
    const uint32_t num_updates = std::stoi(argv[dashdash_pos + 3]);
    const bool use_persistence = (std::string(argv[dashdash_pos + 4]) == "on");
    if(dashdash_pos + 5 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 5]);
    } else {
        pthread_setname_np(pthread_self(), default_proc_name);
    }

    derecho::Conf::initialize(argc, argv);

    if(use_persistence) {
        persistent_test(num_shards, max_shard_size, num_updates);
    } else {
        nonpersistent_test(num_shards, max_shard_size, num_updates);
    }
}

void new_view_callback(const derecho::View& new_view) {
    std::cout << "Transitioned to view " << new_view.vid << " with members " << new_view.members << std::endl;
}

void nonpersistent_test(uint32_t num_shards, uint32_t max_shard_size, uint32_t num_updates) {
    derecho::SubgroupInfo layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(TestObject)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(num_shards, 1, max_shard_size))}}));

    auto test_object_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
        return std::make_unique<TestObject>(0);
    };

    derecho::Group<TestObject> group({}, layout, {}, {&new_view_callback}, test_object_factory);

    std::cout << "Sending " << num_updates << " multicast updates" << std::endl;
    derecho::Replicated<TestObject>& replica_group = group.get_subgroup<TestObject>();
    for(uint32_t counter = 0; counter < num_updates ; ++counter) {
        derecho::rpc::QueryResults<bool> update_results = replica_group.ordered_send<RPC_NAME(update)>(counter);
        try {
            using namespace std::chrono_literals;
            //Wait for the first entry in the reply map to get its results
            //This will confirm that the update was delivered to all replicas
            decltype(update_results)::ReplyMap* reply_map = update_results.wait(5s);
            if(!reply_map) {
                dbg_default_warn("Failed to get a ReplyMap for update {} after 5 seconds!", counter);
            } else {
                reply_map->begin()->second.get();
            }
        } catch(derecho::derecho_exception& ex) {
            dbg_default_warn("Exception occurred while awaiting reply to update #{}. What(): {}", counter, ex.what());
        }
        if(counter % 1000 == 0) {
            std::cout << "Done with " << counter << " updates" << std::endl;
        }
    }
    //Maybe this will ensure all the log messages finish printing before the stdout line
    dbg_default_flush();
    std::cout << "Done sending all updates." << std::endl;
    group.barrier_sync();
    group.leave(true);
}

void persistent_test(uint32_t num_shards, uint32_t max_shard_size, uint32_t num_updates) {
    derecho::SubgroupInfo layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(PersistentTestObject)),
              derecho::one_subgroup_policy(derecho::flexible_even_shards(num_shards, 1, max_shard_size))}}));

    auto test_object_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
        return std::make_unique<PersistentTestObject>(pr);
    };

    derecho::Group<PersistentTestObject> group({}, layout, {}, {&new_view_callback}, test_object_factory);

    std::cout << "Sending " << num_updates << " multicast updates" << std::endl;
    derecho::Replicated<PersistentTestObject>& replica_group = group.get_subgroup<PersistentTestObject>();
    for(uint32_t counter = 0; counter < num_updates; ++counter) {
        derecho::rpc::QueryResults<bool> update_results = replica_group.ordered_send<RPC_NAME(update)>("Update number " + std::to_string(counter));
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
    std::cout << "Done sending all updates." << std::endl;
    group.barrier_sync();
    group.leave(true);
}
