#include <iostream>
#include <sstream>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

using derecho::ExternalClientCaller;
using derecho::Replicated;
using std::cout;
using std::endl;
using derecho::Bytes;

class TestObject {
public:
    void fun(const std::string& words) const {
    }

    void bytes_fun(const Bytes& bytes) const {
    }

    bool finishing_call(int x) const {
        return true;
    }

    REGISTER_RPC_FUNCTIONS(TestObject, P2P_TARGETS(fun, bytes_fun, finishing_call));
};

int main(int argc, char** argv) {
    if(argc < 5 || (argc > 5 && strcmp("--", argv[argc - 5]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] is_external (0 - internal, 1 - external) is_sender num_nodes num_messages" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    derecho::Conf::initialize(argc, argv);
    const uint is_external = std::stoi(argv[argc-4]);
    const uint is_sender = std::stoi(argv[argc-3]);
    int num_of_nodes = std::stoi(argv[argc-2]);
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    const uint count = std::stoi(argv[argc-1]);

    if (!is_external) {
        derecho::SubgroupInfo subgroup_info{[num_of_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_of_nodes) {
            std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_layout(1);

        std::vector<uint32_t> members(num_of_nodes);
        for(int i = 0; i < num_of_nodes; i++) {
            members[i] = i;
        }

        subgroup_layout[0].emplace_back(curr_view.make_subview(members));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_of_nodes);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(TestObject)), std::move(subgroup_layout));
        return subgroup_allocation;
        }};
        auto ba_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<TestObject>(); };

        derecho::Group<TestObject> group({},subgroup_info,{},std::vector<derecho::view_upcall_t>{},ba_factory);
        std::cout << "Finished constructing/joining Group" << std::endl;

        if(is_sender) {
            Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
            uint64_t msg_size = max_msg_size - 128;
            uint8_t* bbuf = (uint8_t*)malloc(msg_size);
            bzero(bbuf, msg_size);
            Bytes bytes(bbuf, msg_size);
            struct timespec t1, t2;
            clock_gettime(CLOCK_REALTIME, &t1);

            for(uint i = 0; i < count; i++) {
                handle.p2p_send<RPC_NAME(bytes_fun)>(0, bytes);
            }
            auto results = handle.p2p_send<RPC_NAME(finishing_call)>(0, 0);
#pragma GCC diagnostic ignored "-Wunused-variable"
            auto response = results.get().get(0);
#pragma GCC diagnostic pop

            clock_gettime(CLOCK_REALTIME, &t2);
            free(bbuf);

            int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
            double msec = (double)nsec / 1000000;
            double thp_gbps = ((double)count * max_msg_size * 8) / nsec;
            double thp_ops = ((double)count * 1000000000) / nsec;
            std::cout << "timespan:" << msec << " millisecond." << std::endl;
            std::cout << "throughput:" << thp_gbps << "Gbit/s." << std::endl;
            std::cout << "throughput:" << thp_ops << "ops." << std::endl;
        }

        cout << "Reached end of scope, entering infinite loop so program doesn't exit" << std::endl;
        while(true) {
        }
    } else {
        derecho::ExternalGroupClient<TestObject> group;

        cout << "Finished constructing ExternalGroupClient" << endl;

        std::vector<node_id_t> members = group.get_members();
        std::vector<node_id_t> shard_members = group.get_shard_members(0, 0);
        ExternalClientCaller<TestObject, decltype(group)>& handle = group.get_subgroup_caller<TestObject>();
        uint64_t msg_size = max_msg_size - 128;
        uint8_t* bbuf = (uint8_t*)malloc(msg_size);
        bzero(bbuf, msg_size);
        Bytes bytes(bbuf, msg_size);
        struct timespec t1, t2;
        clock_gettime(CLOCK_REALTIME, &t1);

        for(uint i = 0; i < count; i++) {
            handle.p2p_send<RPC_NAME(bytes_fun)>(0, bytes);
        }
        auto results = handle.p2p_send<RPC_NAME(finishing_call)>(0, 0);
#pragma GCC diagnostic ignored "-Wunused-variable"
        auto response = results.get().get(0);
#pragma GCC diagnostic pop

        clock_gettime(CLOCK_REALTIME, &t2);
        free(bbuf);

        int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
        double msec = (double)nsec / 1000000;
        double thp_gbps = ((double)count * max_msg_size * 8) / nsec;
        double thp_ops = ((double)count * 1000000000) / nsec;
        std::cout << "timespan:" << msec << " millisecond." << std::endl;
        std::cout << "throughput:" << thp_gbps << "Gbit/s." << std::endl;
        std::cout << "throughput:" << thp_ops << "ops." << std::endl;

        cout << "Reached end of scope, entering infinite loop so program doesn't exit" << std::endl;
        while(true) {
        }
    }

}
