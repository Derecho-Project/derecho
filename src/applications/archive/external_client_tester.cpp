#include <iostream>
#include <sstream>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include "test_objects.hpp"

using derecho::ExternalClientCaller;
using std::cout;
using std::endl;
using namespace persistent;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);
    

    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state
    // auto foo_factory = [](PersistentRegistry*) { return std::make_unique<Foo>(-1); };

    derecho::ExternalGroup<Foo> group;

    cout << "Finished constructing ExternalGroup" << endl;

    std::vector<node_id_t> members = group.get_members();
    std::vector<node_id_t> shard_members = group.get_shard_members(0, 0);
    ExternalClientCaller<Foo, decltype(group)>& foo_p2p_handle = group.get_subgroup_caller<Foo>();
    {
        
        auto result = foo_p2p_handle.p2p_send<RPC_NAME(read_state)>(1);
        auto response = result.get().get(1);
        cout << "Node 1 had state = " << response << endl;
    }
    {
        foo_p2p_handle.p2p_send<RPC_NAME(change_state)>(1, 75);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    {
        
        auto result = foo_p2p_handle.p2p_send<RPC_NAME(read_state)>(0);
        auto response = result.get().get(0);
        cout << "Node 0 had state = " << response << endl;
        
    }
    group.update_view();
    {
        foo_p2p_handle.p2p_send<RPC_NAME(change_state)>(0, 85);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    {
        
        auto result = foo_p2p_handle.p2p_send<RPC_NAME(read_state)>(2);
        auto response = result.get().get(2);
        cout << "Node 2 had state = " << response << endl;
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
