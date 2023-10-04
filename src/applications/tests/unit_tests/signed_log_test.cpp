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

/**
 * This object contains state that is shared between the replicated test objects
 * and the main thread, rather than stored inside the replicated objects. It's
 * used to provide a way for the replicated objects to "call back" to the main
 * thread. Each replicated object will get a pointer to this object when it is
 * constructed or deserialized, set up by the deserialization manager.
 */
struct TestState : public derecho::DeserializationContext {
    // Set by the main thread after it figures out which subgroup this node was assigned to
    derecho::subgroup_id_t my_subgroup_id;
    // Set by the main thread after it figures out which subgroup this node was assigned to
    uint32_t subgroup_total_updates;
    // Counts the number of updates delivered within this node's subgroup
    uint32_t subgroup_updates_delivered;
    persistent::version_t last_version;
    std::atomic<bool> last_version_ready;
    // Mutex for subgroup_finished
    std::mutex finish_mutex;
    // Condition variable used to indicate when this node's subgroup has finished persisting/verifying all updates
    std::condition_variable subgroup_finished_condition;
    // Boolean to set to true when signaling the condition variable
    bool subgroup_finished;
    // Called from replicated object update_state methods to notify the main thread that an update was delivered
    void update_delivered(persistent::version_t version) {
        subgroup_updates_delivered++;
        if(subgroup_updates_delivered == subgroup_total_updates) {
            last_version = version;
            last_version_ready = true;
        }
    }
    // Called by Derecho's global persistence callback
    void global_persistence_callback(derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        dbg_default_info("Persisted: Subgroup {}, version {}.", subgroup_id, version);
        //Mark the unsigned subgroup as finished when it has finished persisting, since it won't be "verified"
        //NOTE: This relies on UnsignedObject always being the third subgroup (with ID 2)
        if(subgroup_id == 2 && last_version_ready && version == last_version) {
            {
                std::unique_lock<std::mutex> finish_lock(finish_mutex);
                subgroup_finished = true;
            }
            subgroup_finished_condition.notify_all();
        }
    }
    // Called by Derecho's global verified callback
    void global_verified_callback(derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        dbg_default_info("Verified: Subgroup {}, version {}.", subgroup_id, version);
        dbg_default_flush();
        // Each node should only be placed in one subgroup, so this callback should not be invoked for any other subgroup IDs
        assert(subgroup_id == my_subgroup_id);
        if(last_version_ready && version == last_version) {
            {
                std::unique_lock<std::mutex> finish_lock(finish_mutex);
                subgroup_finished = true;
            }
            subgroup_finished_condition.notify_all();
        }
    }
};

class OneFieldObject : public mutils::ByteRepresentable,
                       public derecho::GroupReference,
                       public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> string_field;
    TestState* test_state;

public:
    /** Factory constructor */
    OneFieldObject(persistent::PersistentRegistry* registry, TestState* test_state)
            : string_field(std::make_unique<std::string>,
                           "OneFieldObjectStringField", registry, true),
              test_state(test_state) {
        assert(test_state);
    }
    /** Deserialization constructor */
    OneFieldObject(persistent::Persistent<std::string>& other_value,
                   TestState* test_state)
            : string_field(std::move(other_value)),
              test_state(test_state) {
        assert(test_state);
    }

    std::string get_state() const {
        return *string_field;
    }

    void update_state(const std::string& new_value);

    REGISTER_RPC_FUNCTIONS(OneFieldObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));

    DEFAULT_SERIALIZE(string_field);
    DEFAULT_DESERIALIZE_NOALLOC(OneFieldObject);
    static std::unique_ptr<OneFieldObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};

// Can't be declared inline because it uses get_subgroup
void OneFieldObject::update_state(const std::string& new_value) {
        auto& this_subgroup_reference = this->group->template get_subgroup<OneFieldObject>(this->subgroup_index);
        auto version_and_timestamp = this_subgroup_reference.get_current_version();
        *string_field = new_value;
        test_state->update_delivered(std::get<0>(version_and_timestamp));
    }

// Custom deserializer that retrieves the TestState pointer from the DeserializationManager
std::unique_ptr<OneFieldObject> OneFieldObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto field_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<OneFieldObject>(*field_ptr, test_state_ptr);
}

class TwoFieldObject : public mutils::ByteRepresentable,
                       public derecho::GroupReference,
                       public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> foo;
    persistent::Persistent<std::string> bar;
    TestState* test_state;

public:
    /** Factory constructor */
    TwoFieldObject(persistent::PersistentRegistry* registry, TestState* test_state)
            : foo(std::make_unique<std::string>, "TwoFieldObjectStringOne", registry, true),
              bar(std::make_unique<std::string>, "TwoFieldObjectStringTwo", registry, true),
              test_state(test_state) {
        assert(test_state);
    }
    /** Deserialization constructor */
    TwoFieldObject(persistent::Persistent<std::string>& other_foo,
                   persistent::Persistent<std::string>& other_bar,
                   TestState* test_state)
            : foo(std::move(other_foo)),
              bar(std::move(other_bar)),
              test_state(test_state) {
        assert(test_state);
    }

    std::string get_foo() const {
        return *foo;
    }

    std::string get_bar() const {
        return *bar;
    }

    void update(const std::string& new_foo, const std::string& new_bar);

    REGISTER_RPC_FUNCTIONS(TwoFieldObject, P2P_TARGETS(get_foo, get_bar), ORDERED_TARGETS(update));
    DEFAULT_SERIALIZE(foo, bar);
    DEFAULT_DESERIALIZE_NOALLOC(TwoFieldObject);
    static std::unique_ptr<TwoFieldObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};

void TwoFieldObject::update(const std::string& new_foo, const std::string& new_bar) {
        auto& this_subgroup_reference = this->group->template get_subgroup<TwoFieldObject>(this->subgroup_index);
        auto version_and_timestamp = this_subgroup_reference.get_current_version();
        *foo = new_foo;
        *bar = new_bar;
        test_state->update_delivered(std::get<0>(version_and_timestamp));
    }

std::unique_ptr<TwoFieldObject> TwoFieldObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto foo_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    std::size_t bytes_read = mutils::bytes_size(*foo_ptr);
    auto bar_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer + bytes_read);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<TwoFieldObject>(*foo_ptr, *bar_ptr, test_state_ptr);
}

class UnsignedObject : public mutils::ByteRepresentable,
public derecho::GroupReference,
public derecho::PersistsFields {
    persistent::Persistent<std::string> string_field;
    TestState* test_state;

public:
    /** Factory constructor */
    UnsignedObject(persistent::PersistentRegistry* registry, TestState* test_state)
            : string_field(std::make_unique<std::string>, "UnsignedObjectField", registry, false),
            test_state(test_state) {
                assert(test_state);
            }
    /** Deserialization constructor */
    UnsignedObject(persistent::Persistent<std::string>& other_field, TestState* test_state)
            : string_field(std::move(other_field)),
            test_state(test_state) {
                assert(test_state);
            }
    std::string get_state() const {
        return *string_field;
    }

    void update_state(const std::string& new_value);

    REGISTER_RPC_FUNCTIONS(UnsignedObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));
    DEFAULT_SERIALIZE(string_field);
    DEFAULT_DESERIALIZE_NOALLOC(UnsignedObject);
    static std::unique_ptr<UnsignedObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};

void UnsignedObject::update_state(const std::string& new_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<UnsignedObject>(this->subgroup_index);
    auto version_and_timestamp = this_subgroup_reference.get_current_version();
    *string_field = new_value;
    test_state->update_delivered(std::get<0>(version_and_timestamp));
}

std::unique_ptr<UnsignedObject> UnsignedObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto field_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<UnsignedObject>(*field_ptr, test_state_ptr);
}


/**
 * Command-line arguments: <one_field_size> <two_field_size> <unsigned_size> <num_updates>
 * one_field_size: Maximum size of the subgroup that replicates the one-field signed object
 * two_field_size: Maximum size of the subgroup that replicates the two-field signed object
 * unsigned_size: Maximum size of the subgroup that replicates the persistent-but-not-signed object
 * num_updates: Number of randomly-generated 32-byte updates to send to each subgroup
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "test_main");
    const std::string characters("abcdefghijklmnopqrstuvwxyz");
    std::mt19937 random_generator(getpid());
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size() - 1);
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
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_1_size))},
                                               {std::type_index(typeid(TwoFieldObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_2_size))},
                                               {std::type_index(typeid(UnsignedObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_unsigned_size))}}));

    //Count the total number of messages delivered in each subgroup to figure out what version is assigned to the last one
    std::array<uint32_t, 3> subgroup_total_messages = {subgroup_1_size * num_updates,
                                                       subgroup_2_size * num_updates,
                                                       subgroup_unsigned_size * num_updates};

    TestState test_state;
    test_state.subgroup_updates_delivered = 0;
    test_state.subgroup_finished = false;
    test_state.last_version_ready = false;
    auto global_persist_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        test_state.global_persistence_callback(subgroup_id, version);
    };
    auto global_verified_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        test_state.global_verified_callback(subgroup_id, version);
    };
    auto new_view_callback = [](const derecho::View& view) {
        dbg_default_info("Now on View {}", view.vid);
    };
    // Pass test_state to the Group constructor as a DeserializationContext
    derecho::Group<OneFieldObject, TwoFieldObject, UnsignedObject> group(
            {nullptr, nullptr, global_persist_callback, global_verified_callback},
            subgroup_info, {&test_state}, {new_view_callback},
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<OneFieldObject>(pr, &test_state); },
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<TwoFieldObject>(pr, &test_state); },
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<UnsignedObject>(pr, &test_state); });
    //Figure out which subgroup this node got assigned to
    int32_t my_shard_subgroup_1 = group.get_my_shard<OneFieldObject>();
    int32_t my_shard_subgroup_2 = group.get_my_shard<TwoFieldObject>();
    int32_t my_shard_unsigned_subgroup = group.get_my_shard<UnsignedObject>();
    if(my_shard_subgroup_1 != -1) {
        std::cout << "In the OneFieldObject subgroup" << std::endl;
        derecho::Replicated<OneFieldObject>& object_handle = group.get_subgroup<OneFieldObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[0];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
    } else if(my_shard_subgroup_2 != -1) {
        std::cout << "In the TwoFieldObject subgroup" << std::endl;
        derecho::Replicated<TwoFieldObject>& object_handle = group.get_subgroup<TwoFieldObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[1];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
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
    } else if(my_shard_unsigned_subgroup != -1) {
        std::cout << "In the UnsignedObject subgroup" << std::endl;
        derecho::Replicated<UnsignedObject>& object_handle = group.get_subgroup<UnsignedObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[2];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
    }
    //Wait for all updates to finish being verified, using the condition variables updated by the callback
    std::cout << "Waiting for final version to be verified" << std::endl;
    {
        std::unique_lock<std::mutex> lock(test_state.finish_mutex);
        test_state.subgroup_finished_condition.wait(lock, [&]() { return test_state.subgroup_finished; });
    }
    std::cout << "Done" << std::endl;
    group.barrier_sync();
    group.leave(true);
}
