#include <iostream>

#include <derecho/core/derecho.hpp>

using namespace derecho;

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "fcs_failure");
    srand(getpid());

    if(argc < 2) {
        std::cout << "Usage: " << argv[0] << " <num_nodes> [configuration options...]" << std::endl;
        return -1;
    }
    // the number of nodes for this test
    const uint32_t num_nodes = std::stoi(argv[1]);

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // Use the standard layout manager provided by derecho
    // allocate a single subgroup with a single shard consisting of all the nodes
    SubgroupAllocationPolicy all_nodes_one_subgroup_policy =
            one_subgroup_policy(fixed_even_shards(1, num_nodes, num_nodes));
    SubgroupInfo one_raw_group(DefaultSubgroupAllocator({{std::type_index(typeid(RawObject)),
                                                          all_nodes_one_subgroup_policy}}));

    int num_members = 0;
    auto view_upcall = [&num_members](const View& view) {
        std::cout << "View changed, member count = " << view.members.size() << std::endl;
        if (view.num_members != num_members) {
            std::cout << "Members: " << view.members << std::endl;
            num_members = view.num_members;
        }
    };

    // join the group
    Group<RawObject> group(CallbackSet{}, one_raw_group, nullptr, {view_upcall},
                           &raw_object_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    while (num_members != 1) {}

    group.barrier_sync();
    group.leave();
}