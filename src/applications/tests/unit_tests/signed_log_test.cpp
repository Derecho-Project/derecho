/**
 * @file signed_log_test.cpp
 *
 * This program can be used to test the signed persistent log feature in Derecho.
 */

#include <array>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include <derecho/core/derecho.hpp>

class OneFieldObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
    persistent::Persistent<std::string> string_field;

public:
    OneFieldObject(persistent::PersistentRegistry* registry)
            : string_field(std::make_unique<std::string>,
                           "OneFieldObjectStringField", registry) {}
    OneFieldObject(persistent::Persistent<std::string>& other_value)
            : string_field(std::move(other_value)) {}

    std::string get_state() {
        return *string_field;
    }

    void update_state(const std::string& new_value) {
        *string_field = new_value;
    }

    DEFAULT_SERIALIZATION_SUPPORT(OneFieldObject, string_field);
    REGISTER_RPC_FUNCTIONS(OneFieldObject, get_state, update_state);
};

class TwoFieldObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
    persistent::Persistent<std::string> foo;
    persistent::Persistent<std::string> bar;

public:
    TwoFieldObject(persistent::PersistentRegistry* registry)
            : foo(std::make_unique<std::string>, "TwoFieldObjectStringOne", registry),
              bar(std::make_unique<std::string>, "TwoFieldObjectStringTwo", registry) {}
    TwoFieldObject(persistent::Persistent<std::string>& other_foo,
                   persistent::Persistent<std::string>& other_bar)
            : foo(std::move(other_foo)),
              bar(std::move(other_bar)) {}

    std::string get_foo() {
        return *foo;
    }

    std::string get_bar() {
        return *bar;
    }

    void update(const std::string& new_foo, const std::string& new_bar) {
        *foo = new_foo;
        *bar = new_bar;
    }

    DEFAULT_SERIALIZATION_SUPPORT(TwoFieldObject, foo, bar);
    REGISTER_RPC_FUNCTIONS(TwoFieldObject, get_foo, get_bar, update);
};

int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "test_main");
    const std::string characters("abcdefghijklmnopqrstuvwxyz");
    std::mt19937 random_generator(getpid());
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size() - 1);
    std::mutex finish_mutex;
    //One condition variable per subgroup to represent when they are finished verifying all updates
    std::vector<std::condition_variable> subgroup_finished_condition(2);
    //The actual boolean for the condition, since wakeups can be spurious
    std::vector<bool> subgroup_finished = {false, false};

    const int num_args = 3;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]) != 0)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [derecho-config-options -- ] subgroup_1_size subgroup_2_size num_updates" << std::endl;
        return -1;
    }

    const unsigned int subgroup_1_size = std::stoi(argv[argc - num_args]);
    const unsigned int subgroup_2_size = std::stoi(argv[argc - num_args + 1]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);
    if(derecho::getConfBoolean(CONF_PERS_SIGNED_LOG) == false) {
        std::cout << "Error: Signed log is not enabled, but this test requires it. Check your config file." << std::endl;
        return -1;
    }
    derecho::SubgroupInfo subgroup_info(
            derecho::DefaultSubgroupAllocator({{std::type_index(typeid(OneFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_1_size))},
                                               {std::type_index(typeid(TwoFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_2_size))}}));

    //Count the total number of messages delivered in each subgroup to figure out what version is assigned to the last one
    std::array<uint32_t, 2> subgroup_total_messages = {subgroup_1_size * num_updates,
                                                       subgroup_2_size * num_updates};
    std::array<persistent::version_t, 2> subgroup_last_version;
    std::array<std::atomic<bool>, 2> last_version_ready = {false, false};
    auto stability_callback = [&subgroup_total_messages,
                               &subgroup_last_version,
                               &last_version_ready,
                               num_delivered = std::vector<uint32_t>{0u, 0u}](uint32_t subgroup,
                                                                              uint32_t sender_id,
                                                                              long long int index,
                                                                              std::optional<std::pair<char*, long long int>> data,
                                                                              persistent::version_t ver) mutable {
        num_delivered[subgroup]++;
        if(num_delivered[subgroup] == subgroup_total_messages[subgroup]) {
            subgroup_last_version[subgroup] = ver;
            last_version_ready[subgroup] = true;
        }
    };
    auto global_persist_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        dbg_default_info("Persisted: Subgroup {}, version {}.", subgroup_id, version);
    };
    auto global_verified_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        dbg_default_info("Verified: Subgroup {}, version {}.", subgroup_id, version);
        dbg_default_flush();
        if(last_version_ready[subgroup_id] && version == subgroup_last_version[subgroup_id]) {
            {
                std::unique_lock<std::mutex> finish_lock(finish_mutex);
                subgroup_finished[subgroup_id] = true;
            }
            subgroup_finished_condition[subgroup_id].notify_all();
        }
    };
    auto new_view_callback = [](const derecho::View& view) {
        dbg_default_info("Now on View {}", view.vid);
    };
    derecho::Group<OneFieldObject, TwoFieldObject> group(
            {nullptr, nullptr, global_persist_callback, global_verified_callback},
            subgroup_info, {}, {new_view_callback},
            [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) { return std::make_unique<OneFieldObject>(pr); },
            [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) { return std::make_unique<TwoFieldObject>(pr); });
    //Figure out which subgroup this node got assigned to
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> subgroup_1_members = group.get_subgroup_members<OneFieldObject>(0)[0];
    std::vector<node_id_t> subgroup_2_members = group.get_subgroup_members<TwoFieldObject>(0)[0];
    if(std::find(subgroup_1_members.begin(), subgroup_1_members.end(), my_id) != subgroup_1_members.end()) {
        std::cout << "In the OneFieldObject subgroup" << std::endl;
        derecho::Replicated<OneFieldObject>& object_handle = group.get_subgroup<OneFieldObject>();
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
        //Read the final state, just to make sure we wait for a response from all replicas
        derecho::rpc::QueryResults<std::string> results = object_handle.ordered_send<RPC_NAME(get_state)>();
        derecho::rpc::QueryResults<std::string>::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            try {
                reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
            }
        }
    } else if(std::find(subgroup_2_members.begin(), subgroup_2_members.end(), my_id) != subgroup_2_members.end()) {
        std::cout << "In the TwoFieldObject subgroup" << std::endl;
        derecho::Replicated<TwoFieldObject>& object_handle = group.get_subgroup<TwoFieldObject>();
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_foo('a', 32);
            std::string new_bar('a', 32);
            std::generate(new_foo.begin(), new_foo.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            std::generate(new_bar.begin(), new_bar.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update)>(new_foo, new_bar);
        }
        //Read the final state, just to make sure we wait for a response from all replicas
        derecho::rpc::QueryResults<std::string> results = object_handle.ordered_send<RPC_NAME(get_foo)>();
        derecho::rpc::QueryResults<std::string>::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            try {
                reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                dbg_default_info("No query reply due to node_removed_from_group_exception: {}", ex.what());
            }
        }
    }
    //Wait for all updates to finish being verified, using the condition variables updated by the callback
    if(std::find(subgroup_1_members.begin(), subgroup_1_members.end(), my_id) != subgroup_1_members.end()) {
        std::cout << "Waiting for final version " << subgroup_last_version[0] << " to be verified" << std::endl;
        std::unique_lock<std::mutex> lock(finish_mutex);
        subgroup_finished_condition[0].wait(lock, [&]() { return subgroup_finished[0]; });
    } else if(std::find(subgroup_2_members.begin(), subgroup_2_members.end(), my_id) != subgroup_2_members.end()) {
        std::cout << "Waiting for final version " << subgroup_last_version[1] << " to be verified" << std::endl;
        std::unique_lock<std::mutex> lock(finish_mutex);
        subgroup_finished_condition[1].wait(lock, [&]() { return subgroup_finished[1]; });
    }
    std::cout << "Done" << std::endl;
    group.barrier_sync();
    group.leave(true);
}
