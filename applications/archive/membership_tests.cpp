#include <algorithm>
#include <derecho/derecho.h>
#include <derecho/view.h>
#include <vector>

using namespace derecho;

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
        } catch(const invalid_subgroup_exception e) {
            continue;
        } catch(...) {
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
  std::cout << "In test state" << std::endl;
    // Read initial state
    auto initial_results = stateHandle.ordered_send<RPC_NAME(read_value)>();
    auto& initial_replies = initial_results.get();
    for(auto& reply_pair : initial_replies) {
        auto other_state = reply_pair.second.get();
        if(other_state != prev_state) {
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
    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);
    std::cout << ">>> Got items from conf file" << std::endl;

    srand(time(NULL));
    node_id_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::cout << ">>> ID is " << my_id << std::endl;

    // could take these from command line
    std::map<Tests, bool> tests = {{Tests::MIGRATION, false},
                                   {Tests::INIT_EMPTY, true},
                                   {Tests::INTER_EMPTY, false},
                                   {Tests::DISJOINT_MEM, false}};
    std::cout << ">>> tests initialized " << std::endl;

    auto state_membership_function = [&tests](const std::type_index& subgroup_type, const std::unique_ptr<View>& prev_view, View& curr_view) {
                                               if(curr_view.members.size() < 3) {
                                                   throw subgroup_provisioning_exception();
                                               }

                                               std::cout << ">>> Enough members present" << std::endl;

                                               subgroup_shard_layout_t layout_vec(std::count_if(tests.begin(), tests.end(), [](auto& p) { return p.second; }));

                                               // TODO: Fix this!
                                               if(tests[Tests::MIGRATION]) migration_layout(layout_vec[Tests::MIGRATION], curr_view);
                                               if(tests[Tests::INIT_EMPTY]) init_empty_layout(layout_vec[Tests::INIT_EMPTY], curr_view);
                                               if(tests[Tests::INTER_EMPTY]) inter_empty_layout(layout_vec[Tests::INTER_EMPTY], curr_view);
                                               if(tests[Tests::DISJOINT_MEM]) dis_mem_layout(layout_vec[Tests::DISJOINT_MEM], curr_view);

                                               std::cout << ">>> Created layouts" << std::endl;

                                               return layout_vec;
                                           };

    const int INIT_STATE = 100;
    auto state_subgroup_factory = [INIT_STATE](PersistentRegistry*) { return std::make_unique<State>(INIT_STATE); };
    SubgroupInfo subgroup_info{state_membership_function};

    Group<State> group({}, subgroup_info, nullptr, {}, state_subgroup_factory);

    std::cout << ">>> Created group" << std::endl;

    auto group_members = group.get_members();
    uint32_t my_rank = -1;
    for(uint i = 0; i < group_members.size(); ++i) {
        if(group_members[i] == my_id) {
            my_rank = i;
        }
    }

    std::cout << ">>> Got ranks" << std::endl;

    std::map<Tests, int> prev_states;
    for(auto t : AllTests) {
      prev_states[t] = INIT_STATE;
    }

    std::cout << ">>> Initialized prev_states" << std::endl;

    auto run_test = [&](Tests t, std::map<Tests, Replicated<State>&> subgroups){
                      if (tests[t]) {
                        prev_states[t] = test_state(subgroups.at(t), prev_states[t]);
                        if(prev_states[t] == -1) {
                          exit(1);
                        }
                      }
                    };
    
    while(true) {
        // stupid hack to wait for user to prompt start of each test
        std::string input;
        std::cout << "Waiting for input to start... " << std::endl;
        std::getline(std::cin, input);

        auto subgroups = get_subgroups(group, tests);
        int num_members = group.get_members().size();

        switch (my_rank) {
        case 0:
          run_test(Tests::MIGRATION, subgroups);
          if (num_members == 3) {
            run_test(Tests::DISJOINT_MEM, subgroups);
          }
          break;
        case 1:
          if (num_members >= 4) {
            run_test(Tests::INIT_EMPTY, subgroups);
          }
          break;
        case 2:
          if (num_members != 4) {
            run_test(Tests::INTER_EMPTY, subgroups);
            prev_states[Tests::INTER_EMPTY] = INIT_STATE;
          }
          break;
        case 3:
          if (num_members >= 4) {
            run_test(Tests::DISJOINT_MEM, subgroups);
          }
          break;
        }
    }
}
