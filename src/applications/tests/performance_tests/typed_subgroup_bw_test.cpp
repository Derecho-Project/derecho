#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.cpp"
#include "aggregate_bandwidth.hpp"
#include "bytes_object.hpp"
#include "log_results.hpp"
#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>
#include "partial_senders_allocator.hpp"


using std::endl;
using test::Bytes;

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

    REGISTER_RPC_FUNCTIONS(TestObject, ORDERED_TARGETS(fun, bytes_fun, finishing_call));
};

struct exp_result {
    int num_nodes;
    uint32_t num_sender_sel;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint32_t count;
    double avg_msec;
    double avg_gbps;

    void print(std::ofstream& fout) {
        fout << num_nodes << " " << num_sender_sel << " "
             << max_msg_size << " " << window_size << " "
             << count << " "
             << avg_msec << " " << avg_gbps << endl;
    }
};

#define DEFAULT_PROC_NAME "typed_bw_test"

int main(int argc, char* argv[]) {
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < 4) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE: " << argv[0] << " [ derecho-config-list -- ] <num_nodes> <count> <num_senders_selector>[proc_name]" << std::endl;
        std::cout << "Note: proc_name sets the process's name as displayed in ps and pkill commands, default is " DEFAULT_PROC_NAME << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    const int num_nodes = std::stoi(argv[dashdash_pos + 1]);
    const uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    const uint32_t count = std::stoi(argv[dashdash_pos + 2]);
    const uint32_t num_senders_selector = std::stoi(argv[dashdash_pos + 3]);

    // Convert this integer to a more readable enum value
    const PartialSendMode senders_mode = num_senders_selector == 0
                                                 ? PartialSendMode::ALL_SENDERS
                                                 : (num_senders_selector == 1
                                                            ? PartialSendMode::HALF_SENDERS
                                                            : PartialSendMode::ONE_SENDER);

    
    // Compute the total number of messages that should be delivered
    uint64_t total_num_messages = 0;
    switch(senders_mode) {
        case PartialSendMode::ALL_SENDERS:
            total_num_messages = count * num_nodes;
            break;
        case PartialSendMode::HALF_SENDERS:
            total_num_messages = count * (num_nodes / 2);
            break;
        case PartialSendMode::ONE_SENDER:
            total_num_messages = count;
            break;
    }
    
    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<char*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        // Count the total number of messages delivered
        ++num_delivered;
        // Check for completion
        if(num_delivered == total_num_messages) {
            done = true;
        }
    };

    if(dashdash_pos + 4 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 3]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    derecho::SubgroupInfo subgroup_info{[num_nodes](
                                                const std::vector<std::type_index>& subgroup_type_order,
                                                const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_nodes) {
            std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_nodes << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_layout(1);

        std::vector<uint32_t> members(num_nodes);
        for(int i = 0; i < num_nodes; i++) {
            members[i] = i;
        }

        subgroup_layout[0].emplace_back(curr_view.make_subview(members));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_nodes);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(TestObject)), std::move(subgroup_layout));
        return subgroup_allocation;
    }};

    auto ba_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<TestObject>(); };

    derecho::Group<TestObject> group(derecho::CallbackSet{stability_callback}, subgroup_info, {}, std::vector<derecho::view_upcall_t>{}, ba_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    derecho::Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
    //std::string str_1k(max_msg_size, 'x');
    char* bbuf = (char*)malloc(max_msg_size);
    bzero(bbuf, max_msg_size);
    Bytes bytes(bbuf, max_msg_size);

    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);

    // this function sends all the messages
    auto send_all = [&]() {
        for(uint i = 0; i < count; i++) {
        //handle.ordered_send<RPC_NAME(fun)>(str_1k);
        handle.ordered_send<RPC_NAME(bytes_fun)>(bytes);
    }
    };

    int node_rank = group.get_my_rank();

    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        send_all();
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            send_all();
        }
    }

    /*
    if(node_rank == 0) {
        derecho::rpc::QueryResults<bool> results = handle.ordered_send<RPC_NAME(finishing_call)>(0);
        std::cout << "waiting for response..." << std::endl;
#pragma GCC diagnostic ignored "-Wunused-variable"
        decltype(results)::ReplyMap& replies = results.get();
#pragma GCC diagnostic pop
    } 
    */

    while(!done){
    }

    clock_gettime(CLOCK_REALTIME, &t2);
    free(bbuf);

    double thp_gbps;
    int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;

    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        thp_gbps = ((double)count * max_msg_size * num_nodes) / nsec;
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        thp_gbps = ((double)count * max_msg_size * num_nodes/2) / nsec;
    } else {
        thp_gbps = ((double)count * max_msg_size) / nsec;
    }

    double msec = (double)nsec / 1000000;
    double thp_ops = ((double)count * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "GB/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;

    // aggregate bandwidth from all nodes
    std::pair<double, double> bw_laten(thp_gbps,msec);

    auto members_order = group.get_members();
    bw_laten = aggregate_bandwidth(members_order, members_order[node_rank], bw_laten);

    double avg_gbps = bw_laten.first;
    double avg_msec = bw_laten.second;

    if(node_rank == 0) {
        log_results(exp_result{num_nodes, num_senders_selector, max_msg_size,
                               derecho::getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE), count,
                               avg_msec, avg_gbps},
                    "data_derecho_typed_subgroup_bw");
    }

    group.barrier_sync();
    group.leave();
}
