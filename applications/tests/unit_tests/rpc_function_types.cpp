#include "derecho/derecho.h"
#include <mutils-serialization/SerializationSupport.hpp>

/*
 * The Eclipse CDT parser crashes if it tries to expand the REGISTER_RPC_FUNCTIONS
 * macro, probably because there are too many layers of variadic argument expansion.
 * This definition makes the RPC macros no-ops when the CDT parser tries to expand
 * them, which allows it to continue syntax-highlighting the rest of the file.
 */
#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

class ConstTest : public mutils::ByteRepresentable {
    int state;

public:
    //Const member functions should be allowed
    int read_state() const {
        return state;
    }
    void change_state(const int& new_state) {
        state = new_state;
    }
    ConstTest(int initial_state = 0) : state(initial_state) {}

    DEFAULT_SERIALIZATION_SUPPORT(ConstTest, state);
    REGISTER_RPC_FUNCTIONS(ConstTest, read_state, change_state);

};

class ReferenceTest : public mutils::ByteRepresentable {
    std::string state;

public:
    //Causes a compile error: you can't return a reference from an RPC function
//    std::string& get_state() {
    //Correct version:
    std::string get_state() {
        return state;
    }

    //Causes a compile error: RPC functions must pass arguments by reference
//    void set_state(std::string new_state) {
    //Correct version:
    void set_state(const std::string& new_state) {
        state = new_state;
    }

    void append_string(const std::string& text) {
        state.append(text);
    }

    ReferenceTest(const std::string& initial_state = "") : state(initial_state) {}

    DEFAULT_SERIALIZATION_SUPPORT(ReferenceTest, state);
    REGISTER_RPC_FUNCTIONS(ReferenceTest, get_state, set_state, append_string);
};

int main(int argc, char** argv) {
    // Read configurations from the command line options as well as the default config file
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_info(
            {{std::type_index(typeid(ConstTest)),
                derecho::DefaultSubgroupAllocator(derecho::one_subgroup_policy(derecho::even_sharding_policy(1, 3))) },
            {std::type_index(typeid(ReferenceTest)),
                derecho::DefaultSubgroupAllocator(derecho::one_subgroup_policy(derecho::even_sharding_policy(1, 3))) }}
    );

    auto const_test_factory = [](PersistentRegistry*) { return std::make_unique<ConstTest>(); };
    auto reference_test_factory = [](PersistentRegistry*) {return std::make_unique<ReferenceTest>(); };

    derecho::Group<ConstTest, ReferenceTest> group(derecho::CallbackSet{}, subgroup_info,
                                                   std::vector<derecho::view_upcall_t>{},
                                                   const_test_factory, reference_test_factory);

    bool in_const_test_group;
    try {
        group.get_subgroup<ConstTest>();
        in_const_test_group = true;
    } catch (derecho::invalid_subgroup_exception& ex) {
        in_const_test_group = false;
    }
    if(in_const_test_group) {
        derecho::Replicated<ConstTest>& const_test = group.get_subgroup<ConstTest>();
        uint32_t my_node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
        const_test.ordered_send<RPC_NAME(change_state)>(my_node_id);
        derecho::rpc::QueryResults<int> results = const_test.ordered_query<RPC_NAME(read_state)>();
        decltype(results)::ReplyMap& replies = results.get();
        int curr_state = 0;
        for(auto& reply_pair : replies) {
            try {
                curr_state = reply_pair.second.get();
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
        std::cout << "Current state according to ordered_query: " << curr_state << std::endl;
    } else {
        derecho::Replicated<ReferenceTest>& reference_test = group.get_subgroup<ReferenceTest>();
        reference_test.ordered_send<RPC_NAME(set_state)>("Hello, testing.");
        reference_test.ordered_send<RPC_NAME(append_string)>(" Another string. ");
        derecho::rpc::QueryResults<std::string> results = reference_test.ordered_query<RPC_NAME(get_state)>();
        decltype(results)::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            try {
                std::cout << "State read from node " << reply_pair.first << " was: " << reply_pair.second.get() << std::endl;
            } catch(derecho::rpc::node_removed_from_group_exception& ex) {
                std::cout << "No query reply due to node_removed_from_group_exception: " << ex.what() << std::endl;
            }
        }
    }
}






