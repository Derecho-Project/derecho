#include <derecho/derecho.h>
#include <derecho/view.h>

#include <map>
#include <vector>

using namespace derecho;
using std::map;
using std::pair;
using std::vector;

class CookedMessages : public mutils::ByteRepresentable {
    vector<pair<uint, uint>> msgs;  // vector of (nodeid, msg #)

public:
    CookedMessages() = default;
    CookedMessages(const vector<pair<uint, uint>>& msgs) : msgs(msgs) {
    }

    void send(uint nodeid, uint msg) {
        msgs.push_back(std::make_pair(nodeid, msg));
        std::cout << "Node " << nodeid << " sent msg " << msg << std::endl;
    }

    vector<pair<uint, uint>> get_msgs(uint start_index, uint end_index) {
        uint num_msgs = msgs.size();
        if(end_index > num_msgs) {
            end_index = num_msgs;
            std::cout << "Msgs size: " << num_msgs << std::endl;
        }
        return vector<pair<uint, uint>>(msgs.begin() + start_index, msgs.begin() + end_index);
    }

    // default state
    DEFAULT_SERIALIZATION_SUPPORT(CookedMessages, msgs);

    // what operations you want as part of the subgroup
    REGISTER_RPC_FUNCTIONS(CookedMessages, send, get_msgs);
};

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
        std::cout << "Error: Provide the number of nodes as a command line argument!!!" << std::endl;
        exit(1);
    }
    const uint32_t num_nodes = atoi(argv[1]);
    Conf::initialize(argc, argv);
    std::map<std::type_index, shard_view_generator_t>
            subgroup_membership_functions{
                    {std::type_index(typeid(CookedMessages)),
                     [num_nodes](const View& view, int&) {
                         auto& members = view.members;
                         auto num_members = members.size();
                         if(num_members < num_nodes) {
                             throw subgroup_provisioning_exception();
                         }
                         subgroup_shard_layout_t layout(num_members);
                         layout[0].push_back(view.make_subview(vector<uint32_t>(members)));
                         return layout;
                     }}};

    auto cooked_subgroup_factory = [](PersistentRegistry*) { return std::make_unique<CookedMessages>(); };

    SubgroupInfo subgroup_info(subgroup_membership_functions);

    Group<CookedMessages> group({}, subgroup_info, {}, cooked_subgroup_factory);

    std::cout << "Finished constructing/joining the group" << std::endl;
    auto group_members = group.get_members();
    uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    uint32_t my_rank = -1;
    std::cout << "Members are" << std::endl;
    for(uint i = 0; i < group_members.size(); ++i) {
        std::cout << group_members[i] << " ";
        if(group_members[i] == my_id) {
            my_rank = i;
        }
    }
    std::cout << std::endl;
    if(my_rank == (uint32_t)-1) {
        std::cout << "Error: Could not join the group!!!" << std::endl;
        exit(1);
    }

    Replicated<CookedMessages>& cookedMessagesHandle = group.get_subgroup<CookedMessages>();
    uint32_t num_msgs = 500;
    for(uint i = 1; i < num_msgs + 1; ++i) {
        cookedMessagesHandle.ordered_send<RPC_NAME(send)>(my_rank, i);
    }

    if(my_rank == 0) {
        uint32_t max_msg_size = getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
        uint32_t num_entries = max_msg_size / sizeof(pair<uint, uint>) - 5;
        if(num_entries < 0) {
            std::cout << "Error: Maximum message size too small!" << std::endl;
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
            std::cout << "Error: Violation of local order!!!" << std::endl;
            exit(1);
        }
        std::cout << "Local ordering test successful!" << std::endl;
        for(auto& msgs_map_entry : msgs_map) {
            vector<pair<uint, uint>>& v = msgs_map_entry.second;
            if(first_reply != v) {
                std::cout << "Error: Violation of global order!!!" << std::endl;
                exit(1);
            }
        }
        std::cout << "Global ordering test successful!" << std::endl;
    }

    group.barrier_sync();
    group.leave();
    return 0;
}
