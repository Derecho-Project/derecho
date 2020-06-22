/**
 * @file signed_log_test.cpp
 *
 * This program can be used to test the signed persistent log feature in Derecho.
 */

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
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size());
    std::mutex finish_mutex;
    //One condition variable per subgroup to represent when they are finished logging all updates
    std::vector<std::condition_variable> subgroup_finished_condition(2);
    //The actual boolean for the condition, since wakeups can be spurious
    std::vector<bool> subgroup_finished = {false, false};

    const int num_args = 3;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]) != 0)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [derecho-config-options -- ] subgroup_1_size subgroup_2_size num_updates" << std::endl;
        return -1;
    }

    const int subgroup_1_size = std::stoi(argv[argc - num_args]);
    const int subgroup_2_size = std::stoi(argv[argc - num_args + 1]);
    const int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);
    derecho::SubgroupInfo subgroup_info(
            derecho::DefaultSubgroupAllocator({{std::type_index(typeid(OneFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_1_size))},
                                               {std::type_index(typeid(TwoFieldObject)),
                                                derecho::one_subgroup_policy(derecho::fixed_even_shards(
                                                        1, subgroup_2_size))}}));

    //Figure out the last version number that will be created based on the number of updates
    std::vector<derecho::message_id_t> subgroup_last_seq_num = {subgroup_1_size * num_updates - 1,
                                                                subgroup_2_size * num_updates - 1};
    auto global_persist_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        int32_t version_vid;
        derecho::message_id_t version_seq_num;
        std::tie(version_vid, version_seq_num) = persistent::unpack_version<int32_t>(version);
        dbg_default_info("Persisted: Subgroup {}, version {}. Sequence number is {}", subgroup_id, version, version_seq_num);
        if(version_seq_num >= subgroup_last_seq_num[subgroup_id]) {
            {
                std::unique_lock<std::mutex> finish_lock(finish_mutex);
                subgroup_finished[subgroup_id] = true;
            }
            subgroup_finished_condition[subgroup_id].notify_all();
        }
    };
    auto global_verified_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        dbg_default_info("Verified: Subgroup {}, version {}", subgroup_id, version);
    };
    auto new_view_callback = [](const derecho::View& view) {
        dbg_default_info("Now on View {}", view.vid);
    };
    derecho::Group<OneFieldObject, TwoFieldObject> group(
            {nullptr, nullptr, global_persist_callback, global_verified_callback},
            subgroup_info, nullptr, {new_view_callback},
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
        for(int counter = 0; counter < num_updates; ++counter) {
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
        for(int counter = 0; counter < num_updates; ++counter) {
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
    //Temporary: Manually verify all the updates
    openssl::EnvelopeKey local_key = openssl::EnvelopeKey::from_pem_private(derecho::getConfString(CONF_PERS_PRIVATE_KEY_FILE));
    openssl::Verifier local_verifier(local_key, openssl::DigestAlgorithm::SHA256);
    if(std::find(subgroup_1_members.begin(), subgroup_1_members.end(), my_id) != subgroup_1_members.end()) {
        {
            std::unique_lock<std::mutex> lock(finish_mutex);
            subgroup_finished_condition[0].wait(lock, [&]() { return subgroup_finished[0]; });
        }
        persistent::version_t latest_version = group.get_subgroup<OneFieldObject>().get_minimum_latest_persisted_version();
        dbg_default_info("Verifying log for OneFieldObject against itself up to version {}", latest_version);
        for(persistent::version_t verify_version = 0; verify_version < latest_version; ++verify_version) {
            std::vector<unsigned char> signature = group.get_subgroup<OneFieldObject>().get_signature(verify_version);
            if(signature.size() == 0) {
                dbg_default_info("No signature for version {}", verify_version);
                continue;
            }
            bool verify_success = group.get_subgroup<OneFieldObject>().verify_log(verify_version, local_verifier, signature.data());
            if(verify_success) {
                dbg_default_info("Successfully verified version {}", verify_version);
            } else {
                dbg_default_info("Failed to verify version {}! {}", verify_version, openssl::get_error_string(ERR_get_error(), "OpenSSL Error:"));
            }
        }
    } else if(std::find(subgroup_2_members.begin(), subgroup_2_members.end(), my_id) != subgroup_2_members.end()) {
        {
            std::unique_lock<std::mutex> lock(finish_mutex);
            subgroup_finished_condition[1].wait(lock, [&]() { return subgroup_finished[1]; });
        }
        persistent::version_t latest_version = group.get_subgroup<TwoFieldObject>().get_minimum_latest_persisted_version();
        dbg_default_info("Verifying log for TwoFieldObject against itself up to version {}", latest_version);
        for(persistent::version_t verify_version = 0; verify_version < latest_version; ++verify_version) {
            std::vector<unsigned char> signature = group.get_subgroup<TwoFieldObject>().get_signature(verify_version);
            if(signature.size() == 0) {
                dbg_default_info("No signature for version {}", verify_version);
                continue;
            }
            bool verify_success = group.get_subgroup<TwoFieldObject>().verify_log(verify_version, local_verifier, signature.data());
            if(verify_success) {
                dbg_default_info("Successfully verified version {}", verify_version);
            } else {
                dbg_default_info("Failed to verify version {}! {}", verify_version, openssl::get_error_string(ERR_get_error(), "OpenSSL Error:"));
            }
        }
    }

    group.leave(true);
}