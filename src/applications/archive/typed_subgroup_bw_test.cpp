#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>

#include "log_results.hpp"
#include "aggregate_bandwidth.cpp"
#include "aggregate_bandwidth.hpp"
#include "bytes_object.hpp"
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/conf/conf.hpp>

using derecho::Bytes;
using std::endl;

/**
 * RPC Object with a single function that accepts a string
 */
class TestObject {
public:
    void fun(const std::string& words) {
    }

    void bytes_fun(const Bytes& bytes) {
    }

    bool finishing_call(int x) {
        return true;
    }

    REGISTER_RPC_FUNCTIONS(TestObject, fun, bytes_fun, finishing_call);
};

struct exp_result {
    int num_nodes;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    int count;
    double avg_msec;
    double avg_gbps;
    double avg_ops;


    void print(std::ofstream& fout) {
        fout << num_nodes <<  " "
             << max_msg_size << " " << window_size << " "
             << count << " "
             << avg_msec << " " << avg_gbps << " " 
	     << avg_ops <<  endl;
    }
};

int main(int argc, char* argv[]) {
    if(argc < 3) {
        std::cout << "Usage:" << argv[0] << " <num_of_nodes> <count> [configuration options...]" << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    int num_of_nodes = std::stoi(argv[1]);
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    int count = std::stoi(argv[2]);

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

    derecho::Group<TestObject> group({},subgroup_info,nullptr,std::vector<derecho::view_upcall_t>{},ba_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    derecho::Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
    //std::string str_1k(max_msg_size, 'x');
    char* bbuf = (char*)malloc(max_msg_size);
    bzero(bbuf, max_msg_size);
    Bytes bytes(bbuf, max_msg_size);

    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);

    for(int i = 0; i < count; i++) {
        //handle.ordered_send<RPC_NAME(fun)>(str_1k);
        handle.ordered_send<RPC_NAME(bytes_fun)>(bytes);
    }

    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();
    if(node_rank == 0) {
        derecho::rpc::QueryResults<bool> results = handle.ordered_send<RPC_NAME(finishing_call)>(0);
        std::cout << "waiting for response..." << std::endl;
#pragma GCC diagnostic ignored "-Wunused-variable"
        decltype(results)::ReplyMap& replies = results.get();
#pragma GCC diagnostic pop
    }

    clock_gettime(CLOCK_REALTIME, &t2);
    free(bbuf);

    int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
    double msec = (double)nsec / 1000000;
    double thp_gbps = ((double)count * max_msg_size * 8) / nsec;
    double thp_ops = ((double)count * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "Gbit/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;

    // aggregate bandwidth from all nodes
    double avg_msec = aggregate_bandwidth(members_order, members_order[node_rank], msec);
    double avg_gbps = aggregate_bandwidth(members_order, members_order[node_rank], thp_gbps);
    double avg_ops = aggregate_bandwidth(members_order, members_order[node_rank], thp_ops);


    if(node_rank == 0) {
        log_results(exp_result{num_of_nodes, max_msg_size,
                               derecho::getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE), count,
                               avg_msec, avg_gbps, avg_ops},
                    "data_derecho_typed_subgroup_bw");
    }

    group.barrier_sync();
    group.leave();
}
