#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

class ConstTest : public mutils::ByteRepresentable {
    int state;

public:
    // Const member functions should be allowed
    int read_state() const {
        return state;
    }
    void change_state(const int& new_state) {
        state = new_state;
    }
    ConstTest(int initial_state = 0) : state(initial_state) {}

    DEFAULT_SERIALIZATION_SUPPORT(ConstTest, state);
    REGISTER_RPC_FUNCTIONS(ConstTest, P2P_TARGETS(read_state), ORDERED_TARGETS(read_state, change_state));
};

class ReferenceTest : public mutils::ByteRepresentable {
    std::string state;

public:
    // Causes a compile error: you can't return a reference from an RPC function
    //     std::string& get_state() {
    // Correct version:
    std::string get_state() {
        return state;
    }

    // Causes a compile error: RPC functions must pass arguments by reference
    //     void set_state(std::string new_state) {
    // Correct version:
    void set_state(const std::string& new_state) {
        state = new_state;
    }

    void append_string(const std::string& text) {
        state.append(text);
    }

    ReferenceTest(const std::string& initial_state = "") : state(initial_state) {}

    DEFAULT_SERIALIZATION_SUPPORT(ReferenceTest, state);
    REGISTER_RPC_FUNCTIONS(ReferenceTest, ORDERED_TARGETS(get_state, set_state, append_string));
};

class MapTest : public mutils::ByteRepresentable {
    std::map<uint32_t, uint64_t> map_state;

public:
    // Not a compile error: passing an int argument by value is allowed
    void put(uint32_t key, uint64_t value) {
        map_state[key] = value;
    }
    uint64_t get(uint32_t key) const {
        return map_state.at(key);
    }
    // Passing an entire map as a const reference should work
    void set_map(const std::map<uint32_t, uint64_t>& new_map) {
        map_state = new_map;
    }
    std::map<uint32_t, uint64_t> get_map() const {
        return map_state;
    }

    MapTest(const std::map<uint32_t, uint64_t>& initial_map = {}) : map_state(initial_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(MapTest, map_state);
    REGISTER_RPC_FUNCTIONS(MapTest, P2P_TARGETS(get, get_map), ORDERED_TARGETS(put, get, set_map, get_map))
};

using derecho::flexible_even_shards;
using derecho::one_subgroup_policy;

int main(int argc, char** argv) {
    // Read configurations from the command line options as well as the default config file
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_function(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(ConstTest)), one_subgroup_policy(flexible_even_shards(1, 1, 3))},
             {std::type_index(typeid(ReferenceTest)), one_subgroup_policy(flexible_even_shards(1, 1, 3))},
             {std::type_index(typeid(MapTest)), one_subgroup_policy(flexible_even_shards(1, 1, 3))}}));
    auto const_test_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<ConstTest>(); };
    auto reference_test_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<ReferenceTest>(); };
    auto map_test_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<MapTest>(); };

    derecho::Group<ConstTest, ReferenceTest, MapTest> group(derecho::UserMessageCallbacks{}, subgroup_function, {},
                                                            std::vector<derecho::view_upcall_t>{},
                                                            const_test_factory, reference_test_factory, map_test_factory);
    int my_id = derecho::getConfInt32(derecho::Conf::DERECHO_LOCAL_ID);
    const std::vector<node_id_t> const_test_group_members = group.get_subgroup_members<ConstTest>()[0];
    bool in_const_test_group = std::find(const_test_group_members.begin(),
                                         const_test_group_members.end(), my_id)
                               != const_test_group_members.end();
    const std::vector<node_id_t> ref_test_group_members = group.get_subgroup_members<ReferenceTest>()[0];
    bool in_ref_test_group = std::find(ref_test_group_members.begin(), ref_test_group_members.end(), my_id)
                             != ref_test_group_members.end();
    if(in_const_test_group) {
        std::cout << "In the ConstTest subgroup" << std::endl;
        derecho::Replicated<ConstTest>& const_test = group.get_subgroup<ConstTest>();
        uint32_t my_node_rank = group.get_my_rank();
        const_test.ordered_send<RPC_NAME(change_state)>(my_node_rank);
        derecho::rpc::QueryResults<int> results = const_test.ordered_send<RPC_NAME(read_state)>();
        decltype(results)::ReplyMap& replies = results.get();
        int curr_state = 0;
        for(auto& reply_pair : replies) {
            try {
                curr_state = reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
        std::cout << "Current state according to ordered_send: " << curr_state << std::endl;
    } else if(in_ref_test_group) {
        std::cout << "In the ReferenceTest subgroup" << std::endl;
        derecho::Replicated<ReferenceTest>& reference_test = group.get_subgroup<ReferenceTest>();
        reference_test.ordered_send<RPC_NAME(set_state)>("Hello, testing.");
        reference_test.ordered_send<RPC_NAME(append_string)>(" Another string. ");
        derecho::rpc::QueryResults<std::string> results = reference_test.ordered_send<RPC_NAME(get_state)>();
        decltype(results)::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            try {
                std::cout << "State read from node " << reply_pair.first << " was: " << reply_pair.second.get() << std::endl;
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
    } else {
        std::cout << "In the MapTest subgroup" << std::endl;
        derecho::Replicated<MapTest>& map_test = group.get_subgroup<MapTest>();
        map_test.ordered_send<RPC_NAME(put)>(18, 36);
        map_test.ordered_send<RPC_NAME(put)>(66666, 8888888);
        derecho::rpc::QueryResults<std::map<uint32_t, uint64_t>> get_map_results = map_test.ordered_send<RPC_NAME(get_map)>();
        decltype(get_map_results)::ReplyMap& replies = get_map_results.get();
        std::map<uint32_t, uint64_t> map_state = replies.begin()->second.get();
        map_state.emplace(64, 128);
        map_state.emplace(65536, 1024);
        map_test.ordered_send<RPC_NAME(set_map)>(map_state);
    }
}
