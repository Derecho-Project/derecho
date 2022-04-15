#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <derecho/utils/time.h>

#include "bytes_object.hpp"
#include "partial_senders_allocator.hpp"
#include "aggregate_latency.hpp"
#include "log_results.hpp"

/**
 * This latency test will timestamp the following events to measure the breakdown latencies
 * ts1 - when a message is sent
 * ts2 - message stablized
 * ts3 - locally persisted
 * ts4 - globally persisted
 * (ts4-ts1): end-to-end latency
 * (ts2-ts1): stablizing latency
 * (ts3-ts2): local persistence latency
 * (ts4-ts3): global persistence latency
 *
 * We assume that the clock on all nodes are percisely synchronized, for example, synchronized by PTP.
 */

using std::cout;
using std::endl;
using test::Bytes;
using namespace persistent;

#define DELTA_T_US(t1, t2) ((double)(((t2).tv_sec - (t1).tv_sec) * 1e6 + ((t2).tv_nsec - (t1).tv_nsec) * 1e-3))

//the payload is used to identify the user timestamp
struct PayLoad {
    uint64_t send_timestamp_us;  // message sending time stamp
};

class ByteArrayObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
public:
    Persistent<Bytes> pers_bytes;

    void change_pers_bytes(const Bytes& bytes) {
        *pers_bytes = bytes;
    }

    REGISTER_RPC_FUNCTIONS(ByteArrayObject, ORDERED_TARGETS(change_pers_bytes));
    DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject, pers_bytes);
    // deserialization constructor
    ByteArrayObject(Persistent<Bytes>& _p_bytes) : pers_bytes(std::move(_p_bytes)) {}
    // the default constructor
    ByteArrayObject(PersistentRegistry* pr)
            : pers_bytes(pr) {}
};

#define DEFAULT_PROC_NAME "pers_lat_test"

struct exp_result {
    int num_nodes;
    uint num_senders_selector;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint num_messages;
    double latency;
    double stddev;

    void print(std::ofstream& fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " " << latency << " " << stddev << endl;
    }
};


int main(int argc, char* argv[]) {
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < 5) {
        cout << "Invalid command line arguments." << endl;
        std::cout << "USAGE:" << argv[0] << " <all|half|one> <num_of_nodes> <num_msgs> <max_ops> [proc_name]" << std::endl;
        std::cout << "Note: proc_name sets the process's name as displayed in ps and pkill commands, default is " DEFAULT_PROC_NAME << std::endl;
        return -1;
    }
    PartialSendMode sender_selector = PartialSendMode::ALL_SENDERS;
    if(strcmp(argv[dashdash_pos + 1], "half") == 0) sender_selector = PartialSendMode::HALF_SENDERS;
    if(strcmp(argv[dashdash_pos + 1], "one") == 0) sender_selector = PartialSendMode::ONE_SENDER;
    int num_of_nodes = atoi(argv[dashdash_pos + 2]);
    uint32_t num_msgs = (uint32_t)atoi(argv[dashdash_pos + 3]);
    int max_ops = atoi(argv[dashdash_pos + 4]);
    uint64_t si_us = (1000000l / max_ops);
    if(dashdash_pos + 5 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 5]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    derecho::Conf::initialize(argc, argv);

    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();
    uint64_t msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;

    bool is_sending = true;
    uint32_t node_rank = -1;

    uint32_t num_sender = 0;
    switch(sender_selector) {
        case PartialSendMode::ALL_SENDERS:
            num_sender = num_of_nodes;
            break;
        case PartialSendMode::HALF_SENDERS:
            num_sender = (num_of_nodes / 2);
            break;
        case PartialSendMode::ONE_SENDER:
            num_sender = 1;
            break;
    }
    size_t total_number_of_messages = num_sender * num_msgs;

    // Timing instruments
    uint64_t* t1_us = (uint64_t*)malloc(sizeof(uint64_t) * total_number_of_messages);
    uint64_t* t2_us = (uint64_t*)malloc(sizeof(uint64_t) * total_number_of_messages);
    uint64_t* t3_us = (uint64_t*)malloc(sizeof(uint64_t) * total_number_of_messages);
    uint64_t* t4_us = (uint64_t*)malloc(sizeof(uint64_t) * total_number_of_messages);
    if(t1_us == nullptr || t2_us == nullptr || t3_us == nullptr || t4_us == nullptr) {
        std::cerr << "allocate memory error!" << std::endl;
        return -1;
    }
    std::memset(t1_us, 0, sizeof(uint64_t) * total_number_of_messages);
    std::memset(t2_us, 0, sizeof(uint64_t) * total_number_of_messages);
    std::memset(t3_us, 0, sizeof(uint64_t) * total_number_of_messages);
    std::memset(t4_us, 0, sizeof(uint64_t) * total_number_of_messages);
    std::map<persistent::version_t, size_t> version_to_index;
    std::mutex version_to_index_mutex;
    size_t number_of_stable_messages = 0;

    // last_version and its flag is shared between the stability callback and persistence callback.
    // This is a clumsy hack to figure out what version number is assigned to the last delivered message.
    persistent::version_t last_version;
    std::atomic<bool> last_version_set = false;
    std::atomic<bool> local_persistence_done = false;
    std::atomic<bool> global_persistence_done = false;

    auto stability_callback = [&](uint32_t subgroup,
                                  uint32_t sender_id,
                                  long long int index,
                                  std::optional<std::pair<uint8_t*, long long int>> data,
                                  persistent::version_t ver) mutable {
        t2_us[number_of_stable_messages] = get_walltime() / 1000;
        {
            std::lock_guard<std::mutex> lck(version_to_index_mutex);
            version_to_index[ver] = number_of_stable_messages;
        }
        if(!data) {
            throw derecho::derecho_exception("Critical: stability_callback got no data.");
        }
        if(data->second < static_cast<long long int>(sizeof(PayLoad))) {
            throw derecho::derecho_exception("Critical: stability_callback got invalid data size.");
        }
        // 35 is the size of cooked header -- TODO: find a better way to index the parameters.
        t1_us[number_of_stable_messages] = reinterpret_cast<PayLoad*>(data->first + 35)->send_timestamp_us;
        //Count the total number of messages delivered
        ++number_of_stable_messages;
        if(number_of_stable_messages == total_number_of_messages) {
            last_version = ver;
            last_version_set = true;
        }
    };

    auto local_persistence_callback = [&](derecho::subgroup_id_t, persistent::version_t ver) {
        size_t index = 0;
        uint64_t now_us = get_walltime() / 1000;
        {
            std::lock_guard<std::mutex> lck(version_to_index_mutex);
            index = version_to_index.at(ver);
        }
        while(index >= 0 && t3_us[index] == 0) {
            t3_us[index--] = now_us;
        }

        if(last_version_set && ver == last_version) {
            local_persistence_done = true;
        }
    };

    auto global_persistence_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        size_t index = 0;
        uint64_t now_us = get_walltime() / 1000;
        {
            std::lock_guard<std::mutex> lck(version_to_index_mutex);
            index = version_to_index.at(ver);
        }
        while(index >= 0 && t4_us[index] == 0) {
            t4_us[index--] = now_us;
        }

        if(last_version_set && ver == last_version) {
            global_persistence_done = true;
        }
    };
    derecho::UserMessageCallbacks callback_set{
            stability_callback,
            local_persistence_callback,
            global_persistence_callback};

    derecho::SubgroupInfo subgroup_info{PartialSendersAllocator(num_of_nodes, sender_selector)};

    auto ba_factory = [](PersistentRegistry* pr, derecho::subgroup_id_t) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group{
            callback_set, subgroup_info, {}, std::vector<derecho::view_upcall_t>{}, ba_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;
    auto group_members = group.get_members();
    node_rank = group.get_my_rank();

    if((sender_selector == PartialSendMode::HALF_SENDERS) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) {
        is_sending = false;
    }
    if((sender_selector == PartialSendMode::ONE_SENDER) && (node_rank != (uint32_t)num_of_nodes - 1)) {
        is_sending = false;
    }

    std::cout << "my rank is:" << node_rank << ", and I'm sending: " << std::boolalpha << is_sending << std::endl;

    derecho::Replicated<ByteArrayObject>& handle = group.get_subgroup<ByteArrayObject>();

    if(is_sending) {
        uint8_t* bbuf = new uint8_t[msg_size];
        bzero(bbuf, msg_size);
        Bytes bs(bbuf, msg_size);

        try {
            struct timespec start, cur;
            clock_gettime(CLOCK_REALTIME, &start);

            for(uint32_t i = 0; i < num_msgs; i++) {
                do {
                    pthread_yield();
                    clock_gettime(CLOCK_REALTIME, &cur);
                } while(DELTA_T_US(start, cur) < i * (double)si_us);
                {
                    memset(bs.get(), 0xcc, msg_size);
                    reinterpret_cast<PayLoad*>(bs.get())->send_timestamp_us = get_walltime() / 1000;
                    handle.ordered_send<RPC_NAME(change_pers_bytes)>(bs);
                }
            }

        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
    }

    while(!local_persistence_done || !global_persistence_done)
        ;
    std::cout << "#index\t#end-to-end(us)\t#t2-t1(us)\t#t3-t2(us)\t#t4-t3(us)" << std::endl;
    for(size_t i = 0; i < total_number_of_messages; i++) {
        std::cout << i << "\t"
                  << (t4_us[i] - t1_us[i]) << "\t"
                  << (t2_us[i] - t1_us[i]) << "\t"
                  << (t3_us[i] - t2_us[i]) << "\t"
                  << (t4_us[i] - t3_us[i]) << std::endl;
    }

    std::cout << "Done!" << std::endl;

    group.barrier_sync();
    group.leave();
}
