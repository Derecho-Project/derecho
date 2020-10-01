/*
 * This test creates one raw (untyped) subgroup consisting of all the nodes.
 * The number of nodes is given as a parameter. After joning, each node sends
 * a fixed number of messages of fixed size, where the content of the messages
 * is generated at random.
 */
#include <iostream>
#include <map>
#include <set>
#include <time.h>

#include <derecho/core/derecho.hpp>

using std::cout;
using std::endl;

using namespace derecho;

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "random_messages");
    srand(getpid());

    if(argc < 4) {
        cout << "Usage: " << argv[0] << " <num_nodes> <num_subgroups> <num_msgs> [configuration options...]" << endl;
        return -1;
    }
    // the number of nodes for this test
    const uint32_t num_nodes = std::stoi(argv[1]);
    const uint32_t num_subgroups = std::stoi(argv[2]);
    const uint32_t num_msgs = std::stoi(argv[3]);
    const uint32_t msg_size = 8;

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

   

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done, num_nodes, num_msgs,
                               num_delivered = 0u](uint32_t subgroup, int sender_id,
                                                                      long long int index,
                                                                      std::optional<std::pair<char*, long long int>> data,
                                                                      persistent::version_t ver) mutable {
        ++num_delivered;
        // char* buf;
        // long long int msg_size;
        // std::tie(buf, msg_size) = data.value();
        // cout << "sender id " << sender_id << ": ";
        // for(auto i = 0; i < msg_size; ++i) {
        //     cout << buf[i];
        // }
        // cout << endl;

        if(num_delivered == num_msgs * num_nodes) {
            done = true;
        }

    };

auto membership_function = [num_subgroups, num_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<View>& prev_view, View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(num_subgroups);
        auto num_members = curr_view.members.size();
        // wait for all nodes to join the group
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        for(uint i = 0; i < num_subgroups; ++i) {
            subgroup_vector[i].emplace_back(curr_view.make_subview(curr_view.members));
        }
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo raw_groups(membership_function);

    // join the group
    Group<RawObject> group(CallbackSet{stability_callback},
                           raw_groups, nullptr, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;

    //auto members_order = group.get_members();
    // cout << "The order of members is :" << endl;
    // for(uint i = 0; i < num_nodes; ++i) {
    //     cout << members_order[i] << " ";
    // }
    // cout << endl;

    auto send_in_one_subgroup = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>(0);
        for(uint i = 0; i < num_msgs; ++i) {
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(msg_size, [msg_size](char* buf) {            
                for(uint i = 0; i < msg_size; ++i) {
                    buf[i] = 'a' + rand() % 26;
                }
            });
        }
    };

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    send_in_one_subgroup();
    // wait for delivery of all messages
    while(!done) {
    }
    // wait for all nodes to be done
    group.barrier_sync();
    group.leave();
}
