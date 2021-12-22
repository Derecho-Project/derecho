#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "sample_objects.hpp"
#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>

using derecho::PeerCaller;
using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    // Read configurations from the command line options as well as the default config file
    derecho::Conf::initialize(argc, argv);

    //Define subgroup membership using the default subgroup allocator function
    //This test assumes derecho.cfg specifies a JSON layout path to a file that allocates
    //Foo and Bar overlapping sets of node IDs.
    derecho::SubgroupInfo subgroup_function{derecho::make_subgroup_allocator<Foo, Bar>(
            derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE))};
    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state. These must take a PersistentRegistry* argument, but
    //in this case we ignore it because the replicated objects aren't persistent.
    auto foo_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<Bar>(); };

    derecho::Group<Foo, Bar> group(derecho::UserMessageCallbacks{}, subgroup_function, {},
                                   std::vector<derecho::view_upcall_t>{},
                                   foo_factory, bar_factory);

    cout << "Finished constructing/joining Group" << endl;

    //Now have each node send some updates to the Replicated objects
    //The code must be different depending on which subgroup this node is in,
    //which we can determine based on which membership list it appears in
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> foo_members = group.get_subgroup_members<Foo>(0)[0];
    std::vector<node_id_t> bar_members = group.get_subgroup_members<Bar>(0)[0];
    auto find_in_foo_results = std::find(foo_members.begin(), foo_members.end(), my_id);
    if(find_in_foo_results != foo_members.end()) {
        uint32_t rank_in_foo = std::distance(foo_members.begin(), find_in_foo_results);
        // Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        dbg_default_crit("Here is FOO {}!", rank_in_foo);
        dbg_default_crit("I see members of my shard: {}", foo_members);
    }
    auto find_in_bar_results = std::find(bar_members.begin(), bar_members.end(), my_id);
    if(find_in_bar_results != bar_members.end()) {
        uint32_t rank_in_bar = derecho::index_of(bar_members, my_id);
        // Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
        dbg_default_crit("Here is BAR {}!", rank_in_bar);
        dbg_default_crit("I see members of my shard: {}", bar_members);
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
