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

class OneFieldObject : public mutils::ByteRepresentable, public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> string_field;

public:
    OneFieldObject(persistent::PersistentRegistry* registry)
            : string_field(std::make_unique<std::string>,
                           "OneFieldObjectStringField", registry, true) {}
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

class TwoFieldObject : public mutils::ByteRepresentable, public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> foo;
    persistent::Persistent<std::string> bar;

public:
    TwoFieldObject(persistent::PersistentRegistry* registry)
            : foo(std::make_unique<std::string>, "TwoFieldObjectStringOne", registry, true),
              bar(std::make_unique<std::string>, "TwoFieldObjectStringTwo", registry, true) {}
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

class UnsignedObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
    persistent::Persistent<std::string> string_field;

public:
    UnsignedObject(persistent::PersistentRegistry* registry)
            : string_field(std::make_unique<std::string>, "UnsignedObjectField", registry, false) {}
    UnsignedObject(persistent::Persistent<std::string>& other_field)
            : string_field(std::move(other_field)) {}
    std::string get_state() {
        return *string_field;
    }

    void update_state(const std::string& new_value) {
        *string_field = new_value;
    }

    DEFAULT_SERIALIZATION_SUPPORT(UnsignedObject, string_field);
    REGISTER_RPC_FUNCTIONS(UnsignedObject, get_state, update_state);
};

/**
 * Command-line arguments: <one_field_size> <two_field_size> <unsigned_size> <num_updates>
 * one_field_size: Size of the subgroup that replicates the one-field signed object
 * two_field_size: Size of the subgroup that replicates the two-field signed object
 * unsigned_size: Size of the subgroup that replicates the persistent-but-not-signed object
 * num_updates: Number of randomly-generated 32-byte updates to send to each subgroup
 * Setting any of the subgroup sizes to 0 will result in that type of object not being created at all
 */
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

    const int num_args = 4;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]) != 0)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [derecho-config-options -- ] one_field_size two_field_size unsigned_size num_updates" << std::endl;
        return -1;
    }

    const unsigned int subgroup_1_size = std::stoi(argv[argc - num_args]);
    const unsigned int subgroup_2_size = std::stoi(argv[argc - num_args + 1]);
    const unsigned int subgroup_unsigned_size = std::stoi(argv[argc - num_args + 2]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_info(
            derecho::DefaultSubgroupAllocator({{std::type_index(typeid(OneFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_1_size))},
                                               {std::type_index(typeid(TwoFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_2_size))},
                                               {std::type_index(typeid(UnsignedObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_unsigned_size))}}));

    //Count the total number of messages delivered in each subgroup to figure out what version is assigned to the last one
    std::array<uint32_t, 3> subgroup_total_messages = {subgroup_1_size * num_updates,
                                                       subgroup_2_size * num_updates,
                                                       subgroup_unsigned_size * num_updates};
    std::array<persistent::version_t, 3> subgroup_last_version;
    std::array<std::atomic<bool>, 3> last_version_ready = {false, false, false};
    auto stability_callback = [&subgroup_total_messages,
                               &subgroup_last_version,
                               &last_version_ready,
                               num_delivered = std::vector<uint32_t>{0u, 0u, 0u}](uint32_t subgroup,
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
        //Mark the unsigned subgroup as finished when it has finished persisting, since it won't be "verified"
        //NOTE: This relies on UnsignedObject always being the third subgroup (with ID 2)
        if(subgroup_id == 2 && last_version_ready[subgroup_id] && version == subgroup_last_version[subgroup_id]) {
            {
                std::unique_lock<std::mutex> finish_lock(finish_mutex);
                subgroup_finished[subgroup_id] = true;
            }
            subgroup_finished_condition[subgroup_id].notify_all();
        }
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
    derecho::Group<OneFieldObject, TwoFieldObject, UnsignedObject> group(
            {stability_callback, nullptr, global_persist_callback, global_verified_callback},
            subgroup_info, {}, {new_view_callback},
            [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) { return std::make_unique<OneFieldObject>(pr); },
            [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) { return std::make_unique<TwoFieldObject>(pr); },
            [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) { return std::make_unique<UnsignedObject>(pr); });
    //Figure out which subgroup this node got assigned to
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> subgroup_1_members = group.get_subgroup_members<OneFieldObject>(0)[0];
    std::vector<node_id_t> subgroup_2_members = group.get_subgroup_members<TwoFieldObject>(0)[0];
    std::vector<node_id_t> subgroup_unsigned_members = group.get_subgroup_members<UnsignedObject>(0)[0];
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
    } else if(std::find(subgroup_unsigned_members.begin(), subgroup_unsigned_members.end(), my_id) != subgroup_unsigned_members.end()) {
        std::cout << "In the UnsignedObject subgroup" << std::endl;
        derecho::Replicated<UnsignedObject>& object_handle = group.get_subgroup<UnsignedObject>();
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
    }
    //Wait for all updates to finish being verified, using the condition variables updated by the callback
    if(std::find(subgroup_1_members.begin(), subgroup_1_members.end(), my_id) != subgroup_1_members.end()) {
        std::cout << "Waiting for final version to be verified" << std::endl;
        std::unique_lock<std::mutex> lock(finish_mutex);
        subgroup_finished_condition[0].wait(lock, [&]() { return subgroup_finished[0]; });
    } else if(std::find(subgroup_2_members.begin(), subgroup_2_members.end(), my_id) != subgroup_2_members.end()) {
        std::cout << "Waiting for final version to be verified" << std::endl;
        std::unique_lock<std::mutex> lock(finish_mutex);
        subgroup_finished_condition[1].wait(lock, [&]() { return subgroup_finished[1]; });
    } else if(std::find(subgroup_unsigned_members.begin(), subgroup_unsigned_members.end(), my_id) != subgroup_unsigned_members.end()) {
        std::cout << "Waiting for final version to be persisted" << std::endl;
        std::unique_lock<std::mutex> lock(finish_mutex);
        subgroup_finished_condition[2].wait(lock, [&]() { return subgroup_finished[2]; });
    }
    std::cout << "Done" << std::endl;
    group.barrier_sync();
    group.leave(true);
}
