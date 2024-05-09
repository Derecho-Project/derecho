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

#include "signed_log_test.hpp"

/* --- TestState implementation --- */

void TestState::notify_update_delivered(uint64_t update_counter, persistent::version_t version, bool is_signed) {
    dbg_default_debug("Update {}/{} delivered", update_counter, subgroup_total_updates);
    // For signed subgroups, record every signed-update version, in case the last update is not a signed update
    if(!my_subgroup_is_unsigned && is_signed) {
        last_version = version;
    }
    if(update_counter == subgroup_total_updates) {
        // For signed subgroups, only update last_version if this is a signed update
        if(my_subgroup_is_unsigned || is_signed) {
            last_version = version;
        }
        dbg_default_info("Final update (#{}) delivered, last version is {}", update_counter, last_version);
        last_version_ready = true;
    }
}

void TestState::notify_global_persistence(derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_info("Persisted: Subgroup {}, version {}.", subgroup_id, version);
    //Mark the unsigned subgroup as finished when it has finished persisting, since it won't be "verified"
    if(my_subgroup_is_unsigned && last_version_ready && version == last_version) {
        {
            std::unique_lock<std::mutex> finish_lock(finish_mutex);
            subgroup_finished = true;
        }
        subgroup_finished_condition.notify_all();
    }
}
void TestState::notify_global_verified(derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_info("Verified: Subgroup {}, version {}.", subgroup_id, version);
    dbg_default_flush();
    // Each node should only be placed in one subgroup, so this callback should not be invoked for any other subgroup IDs
    assert(subgroup_id == my_subgroup_id);
    if(last_version_ready && version >= last_version) {
        {
            std::unique_lock<std::mutex> finish_lock(finish_mutex);
            subgroup_finished = true;
        }
        subgroup_finished_condition.notify_all();
    }
}

/* --- StringWithDelta implementation --- */

StringWithDelta::StringWithDelta(const std::string& init_string) : current_state(init_string) {}

void StringWithDelta::append(const std::string& str_val) {
    dbg_default_trace("StringWithDelta: append called, adding {} to delta", str_val);
    current_state.append(str_val);
    delta.append(str_val);
}

std::string StringWithDelta::get_current_state() const {
    return current_state;
}

size_t StringWithDelta::currentDeltaSize() {
    if (delta.size()==0) {
        return 0;
    } else {
        return mutils::bytes_size(delta);
    }
}

size_t StringWithDelta::currentDeltaToBytes(uint8_t * const buf, size_t buf_size) {
    if (delta.size() == 0) {
        dbg_default_trace("StringWithDelta: Calling currentDeltaToBytes with null buffer\n");
        return 0;
    } else if (buf_size < mutils::bytes_size(delta)) {
        dbg_default_error("{} failed because the buffer({}) given is smaller than needed({}).\n",
                __func__,buf_size,delta.size());
        return 0;
    } else {
        size_t nbytes = mutils::to_bytes(delta,buf);
        delta.clear();
        return nbytes;
    }
}

void StringWithDelta::applyDelta(uint8_t const* const data) {
    dbg_default_trace("StringWithDelta: Appending a serialized string in applyDelta");
    // The delta is a serialized std::string that we want to pass to the string::append function on current_state
    mutils::deserialize_and_run(nullptr, data, [this](const std::string& arg) { current_state.append(arg); });
}

std::unique_ptr<StringWithDelta> StringWithDelta::create(mutils::DeserializationManager* dm) {
    return std::make_unique<StringWithDelta>();
}

/* --- OneFieldObject implementation --- */

OneFieldObject::OneFieldObject(persistent::PersistentRegistry* registry, TestState* test_state)
        : string_field(std::make_unique<std::string>,
                       "OneFieldObjectStringField", registry, true),
          updates_delivered(0),
          test_state(test_state) {
    assert(test_state);
}

OneFieldObject::OneFieldObject(persistent::Persistent<std::string>& other_value,
                               uint64_t other_updates_delivered,
                               TestState* test_state)
        : string_field(std::move(other_value)),
          updates_delivered(other_updates_delivered),
          test_state(test_state) {
    assert(test_state);
}

void OneFieldObject::update_state(const std::string& new_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<OneFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("OneFieldObject: Entering update (RPC function), current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    *string_field = new_value;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), true);
    dbg_default_debug("OneFieldObject: Leaving update");
}

// Custom deserializer that retrieves the TestState pointer from the DeserializationManager
std::unique_ptr<OneFieldObject> OneFieldObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto field_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    std::size_t bytes_read = mutils::bytes_size(*field_ptr);
    auto counter_ptr = mutils::from_bytes<uint64_t>(dsm, buffer + bytes_read);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<OneFieldObject>(*field_ptr, *counter_ptr, test_state_ptr);
}

/* --- TwoFieldObject implementation --- */

TwoFieldObject::TwoFieldObject(persistent::PersistentRegistry* registry, TestState* test_state)
        : foo(std::make_unique<std::string>, "TwoFieldObjectStringOne", registry, true),
          bar(std::make_unique<std::string>, "TwoFieldObjectStringTwo", registry, true),
          updates_delivered(0),
          test_state(test_state) {
    assert(test_state);
}

TwoFieldObject::TwoFieldObject(persistent::Persistent<std::string>& other_foo,
                               persistent::Persistent<std::string>& other_bar,
                               uint64_t other_updates_delivered,
                               TestState* test_state)
        : foo(std::move(other_foo)),
          bar(std::move(other_bar)),
          updates_delivered(other_updates_delivered),
          test_state(test_state) {
    assert(test_state);
}

void TwoFieldObject::update(const std::string& new_foo, const std::string& new_bar) {
    auto& this_subgroup_reference = this->group->template get_subgroup<TwoFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("TwoFieldObject: Entering update (RPC function), current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    *foo = new_foo;
    *bar = new_bar;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), true);
    dbg_default_debug("TwoFieldObject: Leaving update");
}

std::unique_ptr<TwoFieldObject> TwoFieldObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto foo_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    std::size_t bytes_read = mutils::bytes_size(*foo_ptr);
    auto bar_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer + bytes_read);
    bytes_read += mutils::bytes_size(*bar_ptr);
    auto update_counter_ptr = mutils::from_bytes<uint64_t>(dsm, buffer + bytes_read);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<TwoFieldObject>(*foo_ptr, *bar_ptr, *update_counter_ptr, test_state_ptr);
}

/* --- MixedFieldObject implementation --- */

MixedFieldObject::MixedFieldObject(persistent::PersistentRegistry* registry, TestState* test_state)
        : unsigned_field(std::make_unique<std::string>, "MixedFieldObjectUnsignedField", registry, false),
          signed_delta_field(std::make_unique<StringWithDelta>, "MixedFieldObjectDeltaString", registry, true),
          updates_delivered(0),
          test_state(test_state) {
    assert(test_state);
}

MixedFieldObject::MixedFieldObject(persistent::Persistent<std::string>& other_unsigned_field,
                                   persistent::Persistent<StringWithDelta>& other_delta_field,
                                   std::string& other_non_persistent_field,
                                   uint64_t other_updates_delivered,
                                   TestState* test_state)
        : unsigned_field(std::move(other_unsigned_field)),
          signed_delta_field(std::move(other_delta_field)),
          non_persistent_field(std::move(other_non_persistent_field)),
          updates_delivered(other_updates_delivered),
          test_state(test_state) {
    assert(test_state);
}

std::unique_ptr<MixedFieldObject> MixedFieldObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    std::size_t bytes_read = 0;
    auto unsigned_field_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer + bytes_read);
    bytes_read += mutils::bytes_size(*unsigned_field_ptr);
    auto delta_field_ptr = mutils::from_bytes<persistent::Persistent<StringWithDelta>>(dsm, buffer + bytes_read);
    bytes_read += mutils::bytes_size(*delta_field_ptr);
    auto non_persistent_ptr = mutils::from_bytes<std::string>(dsm, buffer + bytes_read);
    bytes_read += mutils::bytes_size(*non_persistent_ptr);
    auto update_counter_ptr = mutils::from_bytes<uint64_t>(dsm, buffer + bytes_read);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<MixedFieldObject>(*unsigned_field_ptr, *delta_field_ptr,
                                              *non_persistent_ptr, *update_counter_ptr, test_state_ptr);
}

void MixedFieldObject::unsigned_update(const std::string& new_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<MixedFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("MixedFieldObject: Entering unsigned_update, current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    *unsigned_field = new_value;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), false);
    dbg_default_debug("MixedFieldObject: Leaving unsigned_update");
}

void MixedFieldObject::signed_delta_update(const std::string& append_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<MixedFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("MixedFieldObject: Entering signed_delta_update, current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    signed_delta_field->append(append_value);
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), true);
    dbg_default_debug("MixedFieldObject: Leaving signed_delta_update");
}

void MixedFieldObject::non_persistent_update(const std::string& new_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<MixedFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("MixedFieldObject: Entering non_persistent_update, current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    non_persistent_field = new_value;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), false);
    dbg_default_debug("MixedFieldObject: Leaving non_persistent_update");
}

void MixedFieldObject::update_all(const std::string& new_unsigned,
                                  const std::string& delta_append_value,
                                  const std::string& new_non_persistent) {
    auto& this_subgroup_reference = this->group->template get_subgroup<MixedFieldObject>(this->subgroup_index);
    auto version_and_hlc = this_subgroup_reference.get_current_version();
    dbg_default_debug("MixedFieldObject: Entering update_all, current version is {}", std::get<0>(version_and_hlc));
    ++updates_delivered;
    *unsigned_field = new_unsigned;
    signed_delta_field->append(delta_append_value);
    non_persistent_field = new_non_persistent;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_hlc), true);
    dbg_default_debug("MixedFieldObject: Leaving update_all");
}

/* --- UnsignedObject implementation --- */

UnsignedObject::UnsignedObject(persistent::PersistentRegistry* registry, TestState* test_state)
        : string_field(std::make_unique<std::string>, "UnsignedObjectField", registry, false),
          updates_delivered(0),
          test_state(test_state) {
    assert(test_state);
}

UnsignedObject::UnsignedObject(persistent::Persistent<std::string>& other_field,
                               uint64_t other_updates_delivered,
                               TestState* test_state)
        : string_field(std::move(other_field)),
          updates_delivered(other_updates_delivered),
          test_state(test_state) {
    assert(test_state);
}

void UnsignedObject::update_state(const std::string& new_value) {
    auto& this_subgroup_reference = this->group->template get_subgroup<UnsignedObject>(this->subgroup_index);
    auto version_and_timestamp = this_subgroup_reference.get_current_version();
    ++updates_delivered;
    *string_field = new_value;
    test_state->notify_update_delivered(updates_delivered, std::get<0>(version_and_timestamp), false);
}

std::unique_ptr<UnsignedObject> UnsignedObject::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto field_ptr = mutils::from_bytes<persistent::Persistent<std::string>>(dsm, buffer);
    std::size_t bytes_read = mutils::bytes_size(*field_ptr);
    auto counter_ptr = mutils::from_bytes<uint64_t>(dsm, buffer + bytes_read);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<UnsignedObject>(*field_ptr, *counter_ptr, test_state_ptr);
}

/**
 * Command-line arguments: <one_field_size> <two_field_size> <unsigned_size> <num_updates>
 * one_field_size: Maximum size of the subgroup that replicates the one-field signed object
 * two_field_size: Maximum size of the subgroup that replicates the two-field signed object
 * mixed_field_size: Maximum size of the subgroup that replicates the mixed-signed-and-unsigned-field object
 * unsigned_size: Maximum size of the subgroup that replicates the persistent-but-not-signed object
 * num_updates: Number of randomly-generated 32-byte updates to send to each subgroup
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "test_main");
    const std::string characters("abcdefghijklmnopqrstuvwxyz");
    std::mt19937 random_generator(getpid());
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size() - 1);
    const int num_args = 5;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]) != 0)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [derecho-config-options -- ] one_field_size two_field_size mixed_field_size unsigned_size num_updates" << std::endl;
        return -1;
    }

    const unsigned int subgroup_1_size = std::stoi(argv[argc - num_args]);
    const unsigned int subgroup_2_size = std::stoi(argv[argc - num_args + 1]);
    const unsigned int subgroup_mixed_size = std::stoi(argv[argc - num_args + 2]);
    const unsigned int subgroup_unsigned_size = std::stoi(argv[argc - num_args + 3]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_info(
            derecho::DefaultSubgroupAllocator({{std::type_index(typeid(OneFieldObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_1_size))},
                                               {std::type_index(typeid(TwoFieldObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_2_size))},
                                               {std::type_index(typeid(MixedFieldObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_mixed_size))},
                                               {std::type_index(typeid(UnsignedObject)),
                                                derecho::one_subgroup_policy(derecho::flexible_even_shards(
                                                        1, 1, subgroup_unsigned_size))}}));

    //Count the total number of messages to be delivered in each subgroup, so we can
    //identify when the last message has been delivered and discover what version it got
    std::array<uint32_t, 4> subgroup_total_messages = {subgroup_1_size * num_updates,
                                                       subgroup_2_size * num_updates,
                                                       subgroup_mixed_size * num_updates,
                                                       subgroup_unsigned_size * num_updates};

    TestState test_state;
    test_state.subgroup_finished = false;
    test_state.last_version_ready = false;
    auto global_persist_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        test_state.notify_global_persistence(subgroup_id, version);
    };
    auto global_verified_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        test_state.notify_global_verified(subgroup_id, version);
    };
    auto new_view_callback = [](const derecho::View& view) {
        dbg_default_info("Now on View {}", view.vid);
    };
    // Pass test_state to the Group constructor as a DeserializationContext
    derecho::Group<OneFieldObject, TwoFieldObject, MixedFieldObject, UnsignedObject> group(
            {nullptr, nullptr, global_persist_callback, global_verified_callback},
            subgroup_info, {&test_state}, {new_view_callback},
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<OneFieldObject>(pr, &test_state);
            },
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<TwoFieldObject>(pr, &test_state);
            },
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<MixedFieldObject>(pr, &test_state);
            },
            [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
                return std::make_unique<UnsignedObject>(pr, &test_state);
            });
    //Figure out which subgroup this node got assigned to
    int32_t my_shard_subgroup_1 = group.get_my_shard<OneFieldObject>();
    int32_t my_shard_subgroup_2 = group.get_my_shard<TwoFieldObject>();
    int32_t my_shard_mixed_subgroup = group.get_my_shard<MixedFieldObject>();
    int32_t my_shard_unsigned_subgroup = group.get_my_shard<UnsignedObject>();
    if(my_shard_subgroup_1 != -1) {
        std::cout << "In the OneFieldObject subgroup" << std::endl;
        derecho::Replicated<OneFieldObject>& object_handle = group.get_subgroup<OneFieldObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[0];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
        test_state.my_subgroup_is_unsigned = false;
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
        test_state.my_subgroup_is_unsigned = false;
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
    } else if(my_shard_mixed_subgroup != -1) {
        std::cout << "In the MixedFieldObject subgroup" << std::endl;
        derecho::Replicated<MixedFieldObject>& object_handle = group.get_subgroup<MixedFieldObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[2];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
        test_state.my_subgroup_is_unsigned = false;
        //Send random updates, alternating between the signed, unsigned, and nonpersistent fields
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string_value('a', 32);
            std::generate(new_string_value.begin(), new_string_value.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            if(counter % 3 == 0) {
                object_handle.ordered_send<RPC_NAME(signed_delta_update)>(new_string_value);
            } else if(counter % 3 == 1) {
                object_handle.ordered_send<RPC_NAME(unsigned_update)>(new_string_value);
            } else {
                object_handle.ordered_send<RPC_NAME(non_persistent_update)>(new_string_value);
            }
        }
    } else if(my_shard_unsigned_subgroup != -1) {
        std::cout << "In the UnsignedObject subgroup" << std::endl;
        derecho::Replicated<UnsignedObject>& object_handle = group.get_subgroup<UnsignedObject>();
        test_state.subgroup_total_updates = subgroup_total_messages[3];
        test_state.my_subgroup_id = object_handle.get_subgroup_id();
        test_state.my_subgroup_is_unsigned = true;
        //Send random updates
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            object_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
    } else {
        std::cout << "WARNING: Not assigned to any subgroup. This node won't be able to detect when the test is finished." << std::endl;
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
