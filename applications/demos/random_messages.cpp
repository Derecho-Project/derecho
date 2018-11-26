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

#include "derecho/derecho.h"

using std::cout;
using std::endl;

using namespace derecho;

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "random_messages");
    srand(getpid());

    if(argc < 2) {
        cout << "Usage: " << argv[0] << " <num_nodes> [configuration options...]" << endl;
        return -1;
    }
    // the number of nodes for this test
    const uint32_t num_nodes = std::stoi(argv[1]);

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done, num_nodes,
                               finished_nodes = std::set<uint32_t>()](uint32_t subgroup, int sender_id,
                                                                      long long int index, char* buf,
                                                                      long long int msg_size,
                                                                      persistent::version_t ver) mutable {
        // terminal message is of size 1. This signals that the sender has finished sending
        if(msg_size == 1) {
            // add the sender to the list of finished nodes
            finished_nodes.insert(sender_id);
            if(finished_nodes.size() == num_nodes) {
                done = true;
            }
            return;
        }
	// print the sender id and message contents
        cout << "sender id " << sender_id << ": ";
        for(auto i = 0; i < msg_size; ++i) {
            cout << buf[i];
        }
        cout << endl;
    };

    auto membership_function = [num_nodes](const View& curr_view,
                                           int& next_unassigned_rank) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
	// wait for all nodes to join
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
	// just one subgroup consisting of all the members of the top-level view
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        next_unassigned_rank = curr_view.members.size();
        return subgroup_vector;
    };

    std::map<std::type_index, shard_view_generator_t> subgroup_map = {
            {std::type_index(typeid(RawObject)), membership_function}};
    SubgroupInfo one_raw_group(subgroup_map);

    // join the group
    Group<> group(CallbackSet{stability_callback},
                          one_raw_group);

    cout << "Finished constructing/joining Group" << endl;

    auto members_order = group.get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
    }
    cout << endl;

    RawSubgroup& raw_subgroup = group.get_subgroup<RawObject>();
    uint32_t num_msgs = 10;
    uint32_t msg_size = 10;
    for(uint i = 0; i < num_msgs; ++i) {
        // the lambda function writes the message contents into the provided memory buffer
        // message content is generated at random
        raw_subgroup.send(msg_size, [msg_size](char* buf) {
            for(uint i = 0; i < msg_size; ++i) {
                buf[i] = 'a' + rand() % 26;
            }
        });
    }
    // send a 1-byte message to signal completion
    raw_subgroup.send(1, [](char* buf) {});

    // wait for delivery of all messages
    while(!done) {
    }
    // wait for all nodes to be done
    group.barrier_sync();
    group.leave();
}
