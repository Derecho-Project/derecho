#include "aggregate_bandwidth.cpp"
#include "aggregate_bandwidth.hpp"
#include "bytes_object.hpp"
#include "log_results.hpp"
#include "partial_senders_allocator.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

using std::endl;
using test::Bytes;
using namespace std::chrono;

/**
 * RPC Object with a single function that accepts a byte array
 */
class TestObject {
public:
    void bytes_fun(const Bytes& bytes) {
    }

    bool finishing_call(int x) {
        return true;
    }

    REGISTER_RPC_FUNCTIONS(TestObject, ORDERED_TARGETS(bytes_fun, finishing_call));
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
        std::cout << "USAGE: " << argv[0] << " [ derecho-config-list -- ] <num_nodes> <count> <num_senders_selector> [proc_name]" << std::endl;
        std::cout << "Note: proc_name sets the process's name as displayed in ps and pkill commands, default is " DEFAULT_PROC_NAME << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    //The maximum number of bytes that can be sent to change_pers_bytes() is not quite MAX_PAYLOAD_SIZE.
    //The serialized Bytes object will include its size field as well as the actual buffer, and
    //the RPC function header contains an InvocationID (which is a size_t) as well as the header
    //fields defined by remote_invocation_utilites::header_space().
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();

    const int num_nodes = std::stoi(argv[dashdash_pos + 1]);
    const uint64_t max_msg_size = derecho::getConfUInt64(derecho::Conf::SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;
    const uint32_t count = std::stoi(argv[dashdash_pos + 2]);
    const uint32_t num_senders_selector = std::stoi(argv[dashdash_pos + 3]);

    steady_clock::time_point begin_time, send_complete_time;

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
    std::atomic<bool> done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done,
                               &send_complete_time,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<uint8_t*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        // Count the total number of messages delivered
        ++num_delivered;
        // Check for completion
        if(num_delivered == total_num_messages) {
            send_complete_time = std::chrono::steady_clock::now();
            done = true;
        }
    };

    if(dashdash_pos + 4 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 3]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    auto membership_function = PartialSendersAllocator(num_nodes, senders_mode, derecho::Mode::ORDERED);
    derecho::SubgroupInfo subgroup_info(membership_function);

    auto test_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<TestObject>(); };

    derecho::Group<TestObject> group(derecho::UserMessageCallbacks{stability_callback}, subgroup_info, {}, std::vector<derecho::view_upcall_t>{}, test_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    uint8_t* bbuf = new uint8_t[max_msg_size];
    memset(bbuf, 0, max_msg_size);
    Bytes bytes(bbuf, max_msg_size);

    // this function sends all the messages
    auto send_all = [&]() {
        for(uint i = 0; i < count; i++) {
            derecho::Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
            handle.ordered_send<RPC_NAME(bytes_fun)>(bytes);
        }
    };

    int node_rank = group.get_my_rank();

    // Begin Clock Timer
    begin_time = std::chrono::steady_clock::now();

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

    while(!done) {
    }

    delete[] bbuf;

    int64_t nsec = duration_cast<nanoseconds>(send_complete_time - begin_time).count();

    double thp_gbps = (static_cast<double>(total_num_messages) * max_msg_size) / nsec;
    double thp_ops = (static_cast<double>(total_num_messages) * 1000000000) / nsec;

    double msec = static_cast<double>(nsec) / 1000000;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "GB/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;
    std::cout << std::flush;

    // aggregate bandwidth from all nodes
    std::pair<double, double> bw_laten(thp_gbps, msec);

    auto members_order = group.get_members();
    bw_laten = aggregate_bandwidth(members_order, members_order[node_rank], bw_laten);

    double avg_gbps = bw_laten.first;
    double avg_msec = bw_laten.second;

    if(node_rank == 0) {
        log_results(exp_result{num_nodes, num_senders_selector, max_msg_size,
                               derecho::getConfUInt32(derecho::Conf::SUBGROUP_DEFAULT_WINDOW_SIZE), count,
                               avg_msec, avg_gbps},
                    "data_derecho_typed_subgroup_bw");
    }

    group.barrier_sync();
    group.leave();
}
