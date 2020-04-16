/**
 * @file rpc_reply_maps.cpp
 *
 * @date Nov 16, 2018
 * @author edward
 */
#include <iostream>
#include <string>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

class StringObject : public mutils::ByteRepresentable {
    std::string log;

public:
    void append(const std::string& words) {
        log += words;
    }
    void clear() {
        log.clear();
    }
    std::string print() const {
        return log;
    }

    StringObject(const std::string& s = "") : log(s) {}

    DEFAULT_SERIALIZATION_SUPPORT(StringObject, log);
    REGISTER_RPC_FUNCTIONS(StringObject, append, clear, print);
};

int main(int argc, char** argv) {
    // Read configurations from the command line options as well as the default config file
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_function(&derecho::one_subgroup_entire_view);

    auto string_object_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<StringObject>(); };

    derecho::Group<StringObject> group(derecho::CallbackSet{},
                                       subgroup_function, nullptr,
                                       std::vector<derecho::view_upcall_t>{},
                                       string_object_factory);

    int my_id = derecho::getConfInt32(CONF_DERECHO_LOCAL_ID);
    derecho::Replicated<StringObject>& rpc_handle = group.get_subgroup<StringObject>();

    using namespace derecho::rpc;
    const int updates = 10;
    for(int i = 0; i < updates; ++i) {
        QueryResults<void> append_results = rpc_handle.ordered_send<RPC_NAME(append)>("Update from " + std::to_string(my_id) + "| ");
        QueryResults<void>::ReplyMap& sent_node_list = append_results.get();
        std::cout << "Append delivered to nodes: ";
        for(const node_id_t& node : sent_node_list) {
            std::cout << node << " ";
        }
        std::cout << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    const int reads = 10;
    for(int i = 0; i < reads; ++i) {
        QueryResults<std::string> print_results = rpc_handle.ordered_send<RPC_NAME(print)>();
        QueryResults<std::string>::ReplyMap& string_reply_map = print_results.get();
        std::cout << "RPC print() call delivered. Waiting for responses..." << std::endl;
        for(auto& reply : string_reply_map) {
            std::string function_result = reply.second.get();
            std::cout << "Reply from node " << reply.first << ": " << function_result << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
