#include <iostream>
#include <vector>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>

using std::cout;
using std::endl;
using std::string;
using namespace derecho;

class Server : public mutils::ByteRepresentable {
    string bytes;

public:
    string exchange(const string& client_bytes) {
        return bytes;
    }

    REGISTER_RPC_FUNCTIONS(Server, exchange);
    DEFAULT_SERIALIZATION_SUPPORT(Server, bytes);

    Server(string bytes) : bytes(bytes) {}
};

int main(int argc, char* argv[]) {
    if(argc < 4) {
        cout << "Insufficient number of command line arguments" << endl;
        cout << "Enter num_servers, payload_size, reply_size" << endl;
        cout << "Thank you" << endl;
        exit(1);
    }


    derecho::Conf::initialize(argc, argv);

    const uint32_t S = atoi(argv[1]);
    const uint32_t B = atoi(argv[2]);
    const uint32_t R = atoi(argv[3]);

    SubgroupInfo subgroup_info{[S](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if((uint32_t)curr_view.num_members < S) {
            throw subgroup_provisioning_exception();
        }
        subgroup_shard_layout_t subgroup_vector(1);
        std::vector<node_id_t> first_S_nodes(&curr_view.members[0], &curr_view.members[0] + S);
        subgroup_vector[0].emplace_back(curr_view.make_subview(first_S_nodes));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, (int)S);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(Server)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};

    auto server_factory = [R](persistent::PersistentRegistry*,derecho::subgroup_id_t) {
        return std::make_unique<Server>(string(R, 'a'));
    };

    std::mutex main_mutex;
    std::condition_variable main_cv;

    bool groups_provisioned = false;
    auto announce_groups_provisioned = [&groups_provisioned, &main_cv, &main_mutex](const derecho::View& view) {
        if(view.is_adequately_provisioned) {
            std::unique_lock<std::mutex> lock(main_mutex);
            groups_provisioned = true;
            main_cv.notify_all();
        }
    };

    Group<Server> group({}, subgroup_info, nullptr,
                        std::vector<view_upcall_t>{announce_groups_provisioned},
                        server_factory);

    cout << "Finished constructing/joining Group" << endl;

    std::unique_lock<std::mutex> main_lock(main_mutex);
    main_cv.wait(main_lock, [&groups_provisioned]() { return groups_provisioned; });
    std::cout << "Subgroups provisioned" << std::endl;
    uint32_t node_rank = group.get_my_rank();

    // client
    if(node_rank >= S) {
        ExternalCaller<Server>& server_p2p_handle = group.get_nonmember_subgroup<Server>();
        int server = node_rank % S;
        string bytes(B, 'a');
        for(unsigned long long int i = 0;; i++) {
            rpc::QueryResults<string> result = server_p2p_handle.p2p_send<RPC_NAME(exchange)>(server, bytes);
            string response = result.get().get(server);
            cout << "Received response number " << i << endl;
            server = (server + 1) % S;
        }
    }

    while(true) {
    }
}
