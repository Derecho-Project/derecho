#include <derecho/derecho.h>
#include <derecho/view.h>

#include <vector>

using namespace derecho;
using std::vector;

int main() {
    node_id_t my_id;
    ip_addr leader_ip, my_ip;

    std::cout << "Enter my id: " << std::endl;
    std::cin >> my_id;

    std::cout << "Enter my ip: " << std::endl;
    std::cin >> my_ip;

    std::cout << "Enter leader's ip: " << std::endl;
    std::cin >> leader_ip;

    Group<>* group;

    auto delivery_callback = [my_id](subgroup_id_t subgroup_id, node_id_t sender_id, message_id_t index, char* buf, long long int size) {
        // null message filter
        if(size == 0) {
            return;
        }
        std::cout << "In the delivery callback for subgroup_id=" << subgroup_id << ", sender=" << sender_id << std::endl;
        std::cout << "Message: " << buf << std::endl;
        return;
    };

    CallbackSet callbacks{delivery_callback};

    std::map<std::type_index, shard_view_generator_t>
            subgroup_membership_functions{{std::type_index(typeid(RawObject)),
                                           [](const View& view, int&, bool) {
                                               if(view.members.size() < 5) {
                                                   std::cout << "Throwing subgroup exception: not enough members" << std::endl;
                                                   throw subgroup_provisioning_exception();
                                               }
                                               subgroup_shard_layout_t layout(2);
                                               vector<node_id_t> members_for_subgroup_one{view.members[0], view.members[2], view.members[3]};
                                               layout[0].push_back(view.make_subview(members_for_subgroup_one));
                                               vector<node_id_t> members_for_subgroup_two{view.members[0], view.members[1], view.members[2], view.members[4]};
                                               layout[1].push_back(view.make_subview(members_for_subgroup_two));
                                               return layout;
                                           }}};

    unsigned long long int max_msg_size = 100;
    DerechoParams derecho_params{max_msg_size, max_msg_size, max_msg_size};

    SubgroupInfo subgroup_info(subgroup_membership_functions);

    std::cout << "Going to construct the group" << std::endl;
    if(my_id == 0) {
        group = new Group<>(my_id, my_ip, callbacks, subgroup_info, derecho_params);
    } else {
        group = new Group<>(my_id, my_ip, leader_ip, callbacks, subgroup_info);
    }

    std::cout << "Finished constructing/joining the group" << std::endl;
    auto group_members = group->get_members();
    uint32_t my_rank = -1;
    std::cout << "Members are" << std::endl;
    for(uint i = 0; i < group_members.size(); ++i) {
        std::cout << group_members[i] << " ";
        if(group_members[i] == my_id) {
            my_rank = i;
        }
    }
    std::cout << std::endl;
    if (my_rank == (uint32_t) -1) {
        exit(1);
    }

    if(my_rank == 0) {
        RawSubgroup& subgroupHandle1 = group->get_subgroup<RawObject>(); 
        char* buf = subgroupHandle1.get_sendbuffer_ptr(6);
        while(!buf) {
            buf = subgroupHandle1.get_sendbuffer_ptr(6);
        }
        buf[0] = 'H';
        buf[1] = 'e';
        buf[2] = 'l';
        buf[3] = 'l';
        buf[4] = 'o';
        buf[5] = 0;
        subgroupHandle1.send();

	RawSubgroup& subgroupHandle2 = group->get_subgroup<RawObject>(1);
        char* buf2 = subgroupHandle2.get_sendbuffer_ptr(3);
        while(!buf2) {
            buf2 = subgroupHandle2.get_sendbuffer_ptr(3);
        }
        buf2[0] = 'H';
        buf2[1] = 'i';
        buf2[2] = 0;
        subgroupHandle2.send();
    }
    if(my_rank == 4) {
        RawSubgroup& subgroupHandle2 = group->get_subgroup<RawObject>(1);
        char* buf = subgroupHandle2.get_sendbuffer_ptr(3);
        while(!buf) {
            buf = subgroupHandle2.get_sendbuffer_ptr(3);
        }
        buf[0] = 'H';
        buf[1] = 'i';
        buf[2] = 0;
        subgroupHandle2.send();
    }

    while(true) {
    }
    return 0;
}
