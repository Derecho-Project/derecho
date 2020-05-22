#include <algorithm>
#include <derecho/core/derecho.hpp>
#include <derecho/core/view.hpp>
#include <vector>
#include <mutex>
#include <condition_variable>

using namespace derecho;
using namespace persistent;

namespace myTests {
enum Tests {
    MIGRATION = 0,
    INIT_EMPTY,
    INTER_EMPTY,
    DISJOINT_MEM
};

static const Tests AllTests[] = {MIGRATION, INIT_EMPTY, INTER_EMPTY, DISJOINT_MEM};
}  // namespace myTests

using namespace myTests;

/**
 * A simple class that maintains a single variable.
 */
class State : public mutils::ByteRepresentable {
    int value;

public:
    State(int v) : value(v) {}
    State(const State&) = default;

    int read_value() { return value; }
    bool change_value(int v) {
        if(value == v)
            return false;
        else {
            value = v;
            return true;
        }
    }

    DEFAULT_SERIALIZATION_SUPPORT(State, value);
    REGISTER_RPC_FUNCTIONS(State, read_value, change_value);
};

/**
 * sub_tests, table
 * Helpers to generate layouts, according to the following configurations:
                       n=3     |     n=4      |     n=5
   ------------------------------------------------------
   migration     | [0, 2] [1]  | [0] [1, 2]   | [1, 0] [2]
   init_empty    | []          | [0, 1, 2, 3] | [0, 1, 2, 3]
   inter_empty   | [0, 1, 2]   | []           | [4, 2, 3]
   disjoint_mem  | [0, 1]      | [2, 3]       | [2, 3]
*/
void migration_layout(std::vector<SubView>& layout, const View& view) {
    unsigned int n = view.members.size();
    if(n == 3) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[0], view.members[2]}));
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[1]}));
    } else if(n == 4) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[0]}));
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[1], view.members[2]}));
    } else if(n >= 5) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[1], view.members[0]}));
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[2]}));
    }
}

void init_empty_layout(std::vector<SubView>& layout, const View& view) {
    unsigned int n = view.members.size();
    if(n == 3) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{}));
    } else if(n == 4) {
        layout.push_back(view.make_subview(
                std::vector<node_id_t>{view.members[0], view.members[1], view.members[2], view.members[3]}));
    } else if(n >= 5) {
        layout.push_back(view.make_subview(
                std::vector<node_id_t>{view.members[0], view.members[1], view.members[2], view.members[3]}));
    }
}

void inter_empty_layout(std::vector<SubView>& layout, const View& view) {
    unsigned int n = view.members.size();
    if(n == 3) {
        layout.push_back(view.make_subview(
                std::vector<node_id_t>{view.members[0], view.members[1], view.members[2]}));
    } else if(n == 4) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{}));
    } else if(n >= 5) {
        layout.push_back(view.make_subview(
                std::vector<node_id_t>{view.members[4], view.members[2], view.members[3]}));
    }
}

void dis_mem_layout(std::vector<SubView>& layout, const View& view) {
    unsigned int n = view.members.size();
    if(n == 3) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[0], view.members[1]}));
    } else if(n == 4) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[2], view.members[3]}));
    } else if(n >= 5) {
        layout.push_back(view.make_subview(std::vector<node_id_t>{view.members[2], view.members[3]}));
    }
}

/**
 * Return handles of subgroups that current node is a member of.
 */
std::map<Tests, Replicated<State>&> get_subgroups(Group<State>& group, std::map<Tests, bool>& tests) {
    std::map<Tests, Replicated<State>&> subgroups;
    uint32_t subgroup_index = 0;
    for(auto t : AllTests) {
        if(!tests[t]) {
            continue;
        }
        try {
            auto& handle = group.get_subgroup<State>(subgroup_index);
            subgroups.insert({t, handle});
        } catch(const invalid_subgroup_exception& e) {
        } catch(...) {
            std::cout << "Got an unknown exception: " << std::endl;
            exit(1);
        }
        subgroup_index++;
    }
    return subgroups;
}

/**
 * Tests membership by reading initial state, changing it, and reading again.
 */
int test_state(Replicated<State>& stateHandle, int prev_state) {
    // Read initial state
    auto initial_results = stateHandle.ordered_send<RPC_NAME(read_value)>();
    auto& initial_replies = initial_results.get();
    for(auto& reply_pair : initial_replies) {
        auto other_state = reply_pair.second.get();
        if(prev_state != -1 && other_state != prev_state) {
            std::cout << "Failure from node " << reply_pair.first
                      << ": Expected " << prev_state << " but got " << other_state << std::endl;
            return -1;
        } else {
            std::cout << "Reply from node " << reply_pair.first << ": " << other_state << std::endl;
        }
    }

    // Change state
    int new_state = rand() % 440;
    while(new_state == prev_state)
        new_state = rand() % 440;
    stateHandle.ordered_send<RPC_NAME(change_value)>(new_state);

    // Read new state
    auto new_results = stateHandle.ordered_send<RPC_NAME(read_value)>();
    auto& new_replies = new_results.get();
    for(auto& reply_pair : new_replies) {
        auto other_state = reply_pair.second.get();
        if(other_state != new_state) {
            std::cout << "Failure from node " << reply_pair.first
                      << ": Expected " << new_state << " but got " << other_state << std::endl;
            return -1;
        }

        else {
            std::cout << "Reply from node " << reply_pair.first << ": " << other_state << std::endl;
        }
    }

    return new_state;
}

/**
 * Main.
 */
int main(int argc, char* argv[]) {
    if(argc < 5 || (argc > 5 && strcmp("--", argv[argc - 5]))) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] migration(0/1) init_empty(0/1) inter_empty(0/1) disjoint_mem(0/1)" << std::endl;
        return -1;
    }
    pthread_setname_np(pthread_self(), "member_test");

    // initialize test flags
    std::map<Tests, bool> tests = {{Tests::MIGRATION, std::stoi(argv[argc - 4])},
                                   {Tests::INIT_EMPTY, std::stoi(argv[argc - 3])},
                                   {Tests::INTER_EMPTY, std::stoi(argv[argc - 2])},
                                   {Tests::DISJOINT_MEM, std::stoi(argv[argc - 1])}};
    std::cout << "tests initialized " << std::endl;

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    srand(time(NULL));
    node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);

    auto state_membership_function = [&tests](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.members.size() < 3) {
            throw subgroup_provisioning_exception();
        }

        subgroup_shard_layout_t layout_vec(std::count_if(tests.begin(), tests.end(), [](auto& p) { return p.second; }));

        int t = 0;
        if(tests[Tests::MIGRATION]) migration_layout(layout_vec[t++], curr_view);
        if(tests[Tests::INIT_EMPTY]) init_empty_layout(layout_vec[t++], curr_view);
        if(tests[Tests::INTER_EMPTY]) inter_empty_layout(layout_vec[t++], curr_view);
        if(tests[Tests::DISJOINT_MEM]) dis_mem_layout(layout_vec[t++], curr_view);

        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(State)), std::move(layout_vec));
        return subgroup_allocation;
    };

    const int INIT_STATE = 100;
    auto state_subgroup_factory = [INIT_STATE](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<State>(INIT_STATE); };
    SubgroupInfo subgroup_info{state_membership_function};

    std::mutex main_mutex;
    std::condition_variable main_cv;
    volatile bool new_view_installed = false;

    auto announce_view_changed = [&new_view_installed, &main_cv, &main_mutex](const derecho::View& view) {
        std::cout << "number of members: " << view.members.size() << std::endl;
        std::unique_lock<std::mutex> lock(main_mutex);
        new_view_installed = true;
        main_cv.notify_all();
    };

    Group<State> group({}, subgroup_info, nullptr, {announce_view_changed}, state_subgroup_factory);

    auto group_members = group.get_members();
    uint32_t my_rank = -1;
    for(uint i = 0; i < group_members.size(); ++i) {
        if(group_members[i] == my_id) {
            my_rank = i;
        }
    }

    std::map<Tests, int> prev_states;
    for(auto t : AllTests) {
        prev_states[t] = INIT_STATE;
    }

    auto run_test = [&](Tests t, std::map<Tests, Replicated<State>&> subgroups) {
        if(tests[t]) {
            prev_states[t] = test_state(subgroups.at(t), prev_states[t]);
            return prev_states[t] != -1;
        }
        return true;
    };

    for (int i=0; i < 3 - std::max((int) my_rank - 2, 0); i++) {
        std::unique_lock<std::mutex> main_lock(main_mutex);
        main_cv.wait(main_lock, [&new_view_installed]() { return new_view_installed; });
        new_view_installed = false;

        auto subgroups = get_subgroups(group, tests);
        int num_members = group.get_members().size();

        switch(my_rank) {
            case 0:
                if (!run_test(Tests::MIGRATION, subgroups))
                    return 1;
                if(num_members == 3) {
                    if (!run_test(Tests::DISJOINT_MEM, subgroups))
                      return 1;
                }
                break;
            case 1:
                if(num_members >= 4) {
                    if (!run_test(Tests::INIT_EMPTY, subgroups))
                      return 1;
                }
                break;
            case 2:
                if(num_members != 4) {
                    if (!run_test(Tests::INTER_EMPTY, subgroups))
                      return 1;
                    prev_states[Tests::INTER_EMPTY] = INIT_STATE;
                }
                break;
            case 3:
                if(num_members >= 4) {
                    if (num_members == 4) {
                      prev_states[Tests::DISJOINT_MEM] = -1;
                    }
                    if (!run_test(Tests::DISJOINT_MEM, subgroups))
                      return 1;
                }
                break;
        }
    }
    std::cout << "Preparing to leave...\n";
    group.barrier_sync();
    group.leave();
}
