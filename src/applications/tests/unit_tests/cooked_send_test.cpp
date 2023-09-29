#include <derecho/core/derecho.hpp>
#include <derecho/core/view.hpp>

#include <atomic>
#include <map>
#include <vector>

using namespace derecho;
using std::cout;
using std::endl;
using std::map;
using std::pair;
using std::vector;

/**
 * This object contains state that is shared between the replicated test objects
 * and the main thread, rather than stored inside the replicated objects. It's
 * used to provide a way for the replicated objects to "call back" to the main
 * thread. Each replicated object will get a pointer to this object when it is
 * constructed or deserialized, set up by the deserialization manager.
 */
struct TestState : public derecho::DeserializationContext {
    uint32_t num_messages;
    uint32_t num_nodes;
    uint32_t counter;
    std::atomic<bool> done;
    void check_test_done() {
        if(counter == num_messages * num_nodes) {
            done = true;
        }
    }
    TestState(uint32_t num_messages, uint32_t num_nodes)
            : num_messages(num_messages),
              num_nodes(num_nodes),
              counter(0),
              done(false) {}
};

class CookedMessages : public mutils::ByteRepresentable {
    vector<pair<uint, uint>> msgs;  // vector of (nodeid, msg #)
    TestState* test_state;

public:
    // Factory constructor
    CookedMessages(TestState* test_state) : test_state(test_state) {}
    // Deserialization constructor
    CookedMessages(const vector<pair<uint, uint>>& msgs, TestState* test_state)
            : msgs(msgs), test_state(test_state) {}

    void send(uint nodeid, uint msg) {
        msgs.push_back(std::make_pair(nodeid, msg));
        // Count the number of RPC messages received here
        test_state->counter++;
        test_state->check_test_done();
    }

    vector<pair<uint, uint>> get_msgs(uint start_index, uint end_index) {
        uint num_msgs = msgs.size();
        if(end_index > num_msgs) {
            end_index = num_msgs;
        }
        return vector<pair<uint, uint>>(msgs.begin() + start_index, msgs.begin() + end_index);
    }

    DEFAULT_SERIALIZE(msgs);
    static std::unique_ptr<CookedMessages> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
    DEFAULT_DESERIALIZE_NOALLOC(CookedMessages);

    // what operations you want as part of the subgroup
    REGISTER_RPC_FUNCTIONS(CookedMessages, ORDERED_TARGETS(send, get_msgs));
};

// Custom deserializer that retrieves the TestState pointer from the DeserializationManager
std::unique_ptr<CookedMessages> CookedMessages::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer) {
    auto msgs_ptr = mutils::from_bytes<vector<pair<uint, uint>>>(dsm, buffer);
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<CookedMessages>(*msgs_ptr, test_state_ptr);
}

bool verify_local_order(vector<pair<uint, uint>> msgs) {
    map<uint, uint> order;
    for(auto [nodeid, msg] : msgs) {
        if(msg != order[nodeid] + 1) {  // order.count(nodeid) != 0 && <= order[nodeid]
            return false;
        }
        order[nodeid]++;  // order[nodeid] = msg
    }
    return true;
}

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "cooked_send");
    if(argc < 2) {
        cout << "Error: Provide the number of nodes as a command line argument!!!" << endl;
        exit(1);
    }
    const uint32_t num_nodes = atoi(argv[1]);
    Conf::initialize(argc, argv);

    // Configure the default subgroup allocator to put all the nodes in one fixed-size subgroup
    SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(CookedMessages)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_nodes))}}));

    uint32_t num_msgs = 500;
    TestState test_state(num_msgs, num_nodes);
    auto cooked_subgroup_factory = [&](persistent::PersistentRegistry*, derecho::subgroup_id_t) {
        return std::make_unique<CookedMessages>(&test_state);
    };
    // Put a pointer to test_state in Group's vector of DeserializationContexts so it will be passed to CookedMessages
    Group<CookedMessages> group({}, subgroup_info, {&test_state}, {}, cooked_subgroup_factory);

    cout << "Finished constructing/joining the group" << endl;
    auto group_members = group.get_members();
    uint32_t my_id = getConfUInt32(Conf::DERECHO_LOCAL_ID);
    uint32_t my_rank = -1;
    cout << "Members are" << endl;
    for(uint i = 0; i < group_members.size(); ++i) {
        cout << group_members[i] << " ";
        if(group_members[i] == my_id) {
            my_rank = i;
        }
    }
    cout << endl;
    if(my_rank == (uint32_t)-1) {
        cout << "Error: Could not join the group!!!" << endl;
        exit(1);
    }

    Replicated<CookedMessages>& cookedMessagesHandle = group.get_subgroup<CookedMessages>();
    for(uint i = 1; i < num_msgs + 1; ++i) {
        cookedMessagesHandle.ordered_send<RPC_NAME(send)>(my_rank, i);
    }
    while(!test_state.done) {
    }
    if(my_rank == 0) {
        uint32_t max_msg_size = getConfUInt64(Conf::SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
        uint32_t num_entries = max_msg_size / sizeof(pair<uint, uint>) - 50;
        if(num_entries < 0) {
            cout << "Error: Maximum message size too small!" << endl;
            exit(1);
        }
        map<node_id_t, vector<pair<uint, uint>>> msgs_map;
        for(uint i = 0; i < num_msgs * num_nodes; i += num_entries) {
            auto&& results = cookedMessagesHandle.ordered_send<RPC_NAME(get_msgs)>(i, i + num_entries);
            auto& replies = results.get();
            for(auto& reply_pair : replies) {
                vector<pair<uint, uint>> v = reply_pair.second.get();
                msgs_map[reply_pair.first].insert(msgs_map[reply_pair.first].end(), v.begin(), v.end());
            }
        }
        vector<pair<uint, uint>> first_reply = msgs_map.begin()->second;
        if(!verify_local_order(first_reply)) {
            cout << "Error: Violation of local order!!!" << endl;
            exit(1);
        }
        cout << "Local ordering test successful!" << endl;
        for(auto& msgs_map_entry : msgs_map) {
            vector<pair<uint, uint>>& v = msgs_map_entry.second;
            if(first_reply != v) {
                cout << "Error: Violation of global order!!!" << endl;
                exit(1);
            }
        }
        cout << "Global ordering test successful!" << endl;
    }

    group.barrier_sync();
    group.leave();
    return 0;
}
