/**
 * @file subgroup_view_callbacks.cpp
 *
 * This program can be used to test the GetsViewChangeCallback interface, which
 * allows user-defined objects to get a new-view upcall when the Group installs
 * a new View. It also tests the GroupReference interface, which allows user-
 * defined objects to access a type-erased Group pointer, since this is necessary
 * for the user-defined object to know which subgroup it is in (and thus find its
 * subgroup in the new view).
 */

#include <cstdint>
#include <iostream>
#include <random>
#include <string>

#include <derecho/core/derecho.hpp>

/**
 * @brief CallbackTestObject type
 */
class CallbackTestObject : public mutils::ByteRepresentable,
                           public derecho::GetsViewChangeCallback,
                           public derecho::GroupReference {
    using derecho::GroupReference::group;
    using derecho::GroupReference::subgroup_index;

    derecho::subgroup_id_t my_subgroup_id;

    std::string state;

public:
    CallbackTestObject(derecho::subgroup_id_t subgroup_id, const std::string& initial_state = "")
            : my_subgroup_id(subgroup_id), state(initial_state) {}
    std::string get_state() const {
        return state;
    }
    void update_state(const std::string& new_value) {
        state = new_value;
    }
    void new_view_callback(const derecho::View& new_view);

    DEFAULT_SERIALIZATION_SUPPORT(CallbackTestObject, my_subgroup_id, state);
    REGISTER_RPC_FUNCTIONS(CallbackTestObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));
};

void CallbackTestObject::new_view_callback(const derecho::View& new_view) {
    std::uint32_t my_shard_num = new_view.my_subgroups.at(my_subgroup_id);
    const derecho::SubView& my_shard_view = new_view.subgroup_shard_views.at(my_subgroup_id).at(my_shard_num);
    std::cout << "Got a new view callback for view " << new_view.vid << "."
              << " My CallbackTestObject shard members are: " << my_shard_view.members << std::endl;
    if(my_shard_view.members[0] == group->get_my_id()) {
        std::cout << "This node is the shard leader!" << std::endl;
    }
}

class NoCallbackTestObject : public mutils::ByteRepresentable {
    std::string state;

public:
    NoCallbackTestObject(const std::string& initial_state = "") : state(initial_state) {}
    std::string get_state() const {
        return state;
    }
    void update_state(const std::string& new_value) {
        state = new_value;
    }

    DEFAULT_SERIALIZATION_SUPPORT(NoCallbackTestObject, state);
    REGISTER_RPC_FUNCTIONS(NoCallbackTestObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));
};

int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "test_main");
    const std::string characters("abcdefghijklmnopqrstuvwxyz");
    std::mt19937 random_generator(getpid());
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size() - 1);

    const int num_args = 3;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]) != 0)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [derecho-config-options -- ] callback_subgroup_size no_callback_subgroup_size num_updates" << std::endl;
        return -1;
    }

    const unsigned int callback_size = std::stoi(argv[argc - num_args]);
    const unsigned int no_callback_size = std::stoi(argv[argc - num_args + 1]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_info(
            derecho::DefaultSubgroupAllocator({
                    {std::type_index(typeid(CallbackTestObject)),
                     derecho::one_subgroup_policy(derecho::flexible_even_shards(
                             1, 1, callback_size))},
                    {std::type_index(typeid(NoCallbackTestObject)),
                     derecho::one_subgroup_policy(derecho::flexible_even_shards(
                             1, 1, no_callback_size))},
            }));

    derecho::Group<CallbackTestObject, NoCallbackTestObject> group(
            subgroup_info,
            [](persistent::PersistentRegistry*, derecho::subgroup_id_t subgroup_id) {
                return std::make_unique<CallbackTestObject>(subgroup_id);
            },
            [](persistent::PersistentRegistry*, derecho::subgroup_id_t) {
                return std::make_unique<NoCallbackTestObject>();
            });
    // Figure out which subgroup this node got assigned to
    int32_t my_shard_callback_subgroup = group.get_my_shard<CallbackTestObject>();
    int32_t my_shard_nocallback_subgroup = group.get_my_shard<NoCallbackTestObject>();
    if(my_shard_callback_subgroup != -1) {
        std::cout << "In the CallbackTestObject subgroup" << std::endl;
        derecho::Replicated<CallbackTestObject>& subgroup_handle = group.get_subgroup<CallbackTestObject>();
        // Send some random updates so there is some activity besides the view changes
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            subgroup_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
        // At some point while these updates are sending, you should kill a group member to cause a view change
    } else if(my_shard_nocallback_subgroup != -1) {
        std::cout << "In the NoCallbackTestObject subgroup" << std::endl;
        derecho::Replicated<NoCallbackTestObject>& subgroup_handle = group.get_subgroup<NoCallbackTestObject>();
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::string new_string('a', 32);
            std::generate(new_string.begin(), new_string.end(),
                          [&]() { return characters[char_distribution(random_generator)]; });
            subgroup_handle.ordered_send<RPC_NAME(update_state)>(new_string);
        }
    }

    std::cout << "Press enter to shut down" << std::endl;
    std::cin.get();
    group.barrier_sync();
    group.leave(true);
    return 0;
}
