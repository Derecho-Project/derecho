#include <vector>

#include "derecho/derecho.h"
#include "initialize.h"

using namespace sst;
using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::RawObject;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    auto stability_callback = [](uint32_t subgroup, int sender_id, long long int index, char* buf,
                                 long long int msg_size) {
        cout << "Some message is stable" << endl;
    };
    derecho::CallbackSet callbacks{stability_callback,
                                   derecho::message_callback{}};
    derecho::DerechoParams parameters{max_msg_size, block_size};
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}};

    std::unique_ptr<derecho::Group<>> g;
    if(my_ip == leader_ip) {
        g = std::make_unique<derecho::Group<>>(my_ip, callbacks, one_raw_group, parameters);
    } else {
        g = std::make_unique<derecho::Group<>>(node_id, my_ip, leader_ip, callbacks, one_raw_group);
    }

    cout << "Derecho group created" << endl;

    if(node_id == 0) {
        derecho::RawSubgroup& sg = g->get_subgroup<RawObject>();
        int msg_size = 50;
        char* buf = sg.get_sendbuffer_ptr(msg_size);
        for(int i = 0; i < msg_size; ++i) {
            buf[i] = 'a';
        }
        cout << "Calling send" << endl;
        sg.send();
        cout << "send call finished" << endl;
    }
    while(true) {
        int n;
        cin >> n;
        g->debug_print_status();
    }

    return 0;
}
