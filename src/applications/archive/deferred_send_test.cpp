#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <time.h>
#include <vector>

#include <derecho/core/derecho.hpp>

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "deferred_send");

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done, count = 0u](uint32_t subgroup, uint32_t sender_id,
                                                  long long int index,
                                                  std::optional<std::pair<char*, long long int>> data,
                                                  persistent::version_t ver) mutable {
        count++;
        std::cout << data.value().first << std::endl;
        if(count == 3) {
            done = true;
        }
    };

    auto membership_function = [](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<View>& prev_view, View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(1);
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo one_raw_group(membership_function);

    // join the group
    Group<RawObject> group(CallbackSet{stability_callback},
                           one_raw_group, nullptr, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;
    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();

    long long unsigned int max_msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>();

    if (node_rank == 0) {
        raw_subgroup.send(max_msg_size, [](char* buf) {
            std::string message = "This is message 1";
            message.copy(buf, message.size());
        });
	// get two buffers from RDMC - assumes that the window size is at least 2
        auto token_and_buffer_one = raw_subgroup.get_buffer(max_msg_size);
        while(!token_and_buffer_one.second) {
	    token_and_buffer_one = raw_subgroup.get_buffer(max_msg_size);
        }
	auto token_and_buffer_two = raw_subgroup.get_buffer(max_msg_size);
        while(!token_and_buffer_two.second) {
	    token_and_buffer_two = raw_subgroup.get_buffer(max_msg_size);
        }
	// send the second buffer first
	std::string message = "This is message 3";
	message.copy(token_and_buffer_two.second, message.size());
	raw_subgroup.send(token_and_buffer_two.first);

	message = "This is message 2";
	message.copy(token_and_buffer_one.second, message.size());
	raw_subgroup.send(token_and_buffer_one.first);
    }

    // loop until done
    while(!done) {
    }

    group.barrier_sync();
    group.leave();
}
