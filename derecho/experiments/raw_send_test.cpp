#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <time.h>
#include <memory>

#include "derecho/derecho.h"
#include "block_size.h"
#include "rdmc/util.h"

static const int GMS_PORT = 12345;

using std::vector;
using std::map;
using std::string;
using std::cout;
using std::endl;

using derecho::MulticastGroup;


int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t server_rank = 0;
    uint32_t node_rank;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

	rdmc::query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = get_block_size(max_msg_size);
    int num_messages = 10;
    uint num_messages_received = 0;

    auto stability_callback = [&num_messages_received](
        int sender_id, long long int index, char *buf,
        long long int msg_size) mutable {
        cout << "Here" << endl;
	cout << buf << endl;
        num_messages_received++;
    };

    rpc::Dispatcher<> empty_dispatcher(node_rank);
    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams parameters{max_msg_size, block_size};

    std::unique_ptr<derecho::Group<rpc::Dispatcher<>>> managed_group;

    if(node_rank == server_rank) {
        managed_group = std::make_unique<derecho::Group<rpc::Dispatcher<>>>(
                node_addresses[node_rank], std::move(empty_dispatcher),
                callbacks, parameters);
    } else {
        managed_group = std::make_unique<derecho::Group<rpc::Dispatcher<>>>(
                node_rank, node_addresses[node_rank], server_rank,
                node_addresses[server_rank], std::move(empty_dispatcher),
                callbacks);
    }

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group->get_members().size() < num_nodes) {
    }
    auto members_order = managed_group->get_members();
    cout << "The order of members is :" << endl;
    for(auto id : members_order) {
        cout << id << " ";
    }
    cout << endl;
    for(int i = 0; i < num_messages; ++i) {
        char *buf = managed_group->get_sendbuffer_ptr(max_msg_size);
        while(!buf) {
            buf = managed_group->get_sendbuffer_ptr(max_msg_size);
        }
	for (uint i = 0; i < max_msg_size; ++i) {
	  buf[i] = 'a'+(rand()%26);
	}
	cout << buf << endl;
	managed_group->send();
    }

    while(num_messages_received < num_messages * num_nodes) {
    }
    cout << "Done" << endl;
}
