#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>

#include <derecho/core/derecho.hpp>

#include "bytes_object.hpp"
#include "partial_senders_allocator.hpp"
#include "aggregate_latency.hpp"
#include "log_results.hpp"

using std::cout;
using std::endl;
using test::Bytes;
using namespace persistent;

#define DELTA_T_US(t1, t2) ((double)(((t2).tv_sec - (t1).tv_sec) * 1e6 + ((t2).tv_nsec - (t1).tv_nsec) * 1e-3))

//the payload is used to identify the user timestamp
struct PayLoad {
    uint32_t node_rank;  // rank of the sender
    uint32_t msg_seqno;  // sequence of the message sent by the same sender
    uint64_t tv_sec;     // second
    uint64_t tv_nsec;    // nano second
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
            : pers_bytes([]() { return std::make_unique<Bytes>(); }, nullptr, pr) {}
};

#define DEFAULT_PROC_NAME   "pers_lat_test"

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
    while (dashdash_pos > 0) {
        if (strcmp(argv[dashdash_pos],"--") == 0) {
            break;
        }
        dashdash_pos -- ;
    }

    if((argc-dashdash_pos) < 5) {
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
    if (dashdash_pos + 5 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 5]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    derecho::Conf::initialize(argc,argv);

    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();
    uint64_t msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;

    bool is_sending = true;
    uint32_t node_rank = -1;
    // message_pers_ts_us[] is the time when a message with version 'ver' is persisted.
    uint64_t* message_pers_ts_us = (uint64_t*)malloc(sizeof(uint64_t) * num_msgs * num_of_nodes);
    if(message_pers_ts_us == NULL) {
        std::cerr << "allocate memory error!" << std::endl;
        return -1;
    }
    // the total span:
    struct timespec t_begin;
    // is only for local
    uint64_t* local_message_ts_us = (uint64_t*)malloc(sizeof(uint64_t) * num_msgs);
    long total_num_messages;
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
    total_num_messages = num_sender * num_msgs;

    // variable 'done' tracks the end of the test
    volatile bool done = false;

    // last_version and its flag is shared between the stability callback and persistence callback.
    // This is a clumsy hack to figure out what version number is assigned to the last delivered message.
    persistent::version_t last_version;
    std::atomic<bool> last_version_set = false;

    auto stability_callback = [&last_version,
                               &last_version_set,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<char*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        //Count the total number of messages delivered
        ++num_delivered;
        if(num_delivered == total_num_messages) {
            last_version = ver;
            last_version_set = true;
        }
    };

    auto global_persistence_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        struct timespec ts;
        static persistent::version_t pers_ver = 0;
        if(pers_ver > ver) return;

        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t tsus = ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;

        while(pers_ver <= ver) {
            message_pers_ts_us[pers_ver++] = tsus;
        }

        if(last_version_set && ver == last_version) {
            if(is_sending) {
                for(uint32_t i = 0; i < num_msgs; i++) {
                    // std::cout << "[" << i << "]" << local_message_ts_us[i] << " "
                    //           << message_pers_ts_us[num_sender * i + node_rank] << " "
                    //           << (message_pers_ts_us[num_sender * i + node_rank] - local_message_ts_us[i]) << " us" << std::endl;
                }
            }
            double thp_mbps = (double)total_num_messages * msg_size / DELTA_T_US(t_begin, ts);
            std::cout << "throughput(pers): " << thp_mbps << " MBps" << std::endl;
            std::cout << std::flush;
            done = true;
        }
    };
    derecho::UserMessageCallbacks callback_set{
            stability_callback,
            nullptr,
            global_persistence_callback};

    derecho::SubgroupInfo subgroup_info{PartialSendersAllocator(num_of_nodes, sender_selector)};

    auto ba_factory = [](PersistentRegistry* pr, derecho::subgroup_id_t) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group{
            callback_set, subgroup_info, {},
            std::vector<derecho::view_upcall_t>{},
            ba_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;
    auto group_members = group.get_members();
    node_rank = group.get_my_rank();
    uint32_t my_id = group_members[node_rank];

    if((sender_selector == PartialSendMode::HALF_SENDERS) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) {
        is_sending = false;
    }
    if((sender_selector == PartialSendMode::ONE_SENDER) && (node_rank != (uint32_t)num_of_nodes - 1)) {
        is_sending = false;
    }

    std::cout << "my rank is:" << node_rank << ", and I'm sending: " << std::boolalpha << is_sending << std::endl;

    clock_gettime(CLOCK_REALTIME, &t_begin);

    derecho::Replicated<ByteArrayObject>& handle = group.get_subgroup<ByteArrayObject>();

    if(is_sending) {
        char* bbuf = new char[msg_size];
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
                    local_message_ts_us[i] = cur.tv_sec * 1e6 + cur.tv_nsec / 1e3;
                    handle.ordered_send<RPC_NAME(change_pers_bytes)>(bs);
                }
            }

        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
    }

    while(!done) {
    }

    std::cout << "Done!" << std::endl;

    double avg_latency, avg_std_dev;
    // the if loop selects the senders
    if(sender_selector == PartialSendMode::ALL_SENDERS|| (sender_selector == PartialSendMode::HALF_SENDERS && (int)(node_rank) > (num_of_nodes - 1) / 2) || (sender_selector == PartialSendMode::ONE_SENDER && (int)(node_rank) == num_of_nodes - 1)) {
        double total_time = 0;
        double sum_of_square = 0.0;
        double average_time = 0.0;
        for(uint i = 0; i < num_msgs; ++i) {
            total_time += (int64_t)(message_pers_ts_us[num_sender * i + node_rank]) - (int64_t)(local_message_ts_us[i]);
        }
        // average latency in nano seconds
        average_time = (total_time / num_msgs);
        std::cout << "average time: " << average_time << std::endl;
        // calculate the standard deviation
        for(uint i = 0; i < num_msgs; ++i) {
            sum_of_square += (double)((double)message_pers_ts_us[num_sender * i + node_rank] - (double)local_message_ts_us[i] - average_time) * ((double)message_pers_ts_us[num_sender * i + node_rank] - (double)local_message_ts_us[i] - average_time);
        }
        double std_dev = sqrt(sum_of_square / (num_msgs - 1));
        // aggregate latency values from all senders
        std::tie(avg_latency, avg_std_dev) = aggregate_latency(group_members, my_id, (average_time / 1000.0), (std_dev / 1000.0));
    } else {
        // if not a sender, then pass 0 as the latency (not counted)
        std::tie(avg_latency, avg_std_dev) = aggregate_latency(group_members, my_id, 0.0, 0.0);
    }

    unsigned int num_senders_selector = 0;
    if (sender_selector == PartialSendMode::HALF_SENDERS){
        num_senders_selector = 1;
    }
    else if (sender_selector == PartialSendMode::ONE_SENDER){
        num_senders_selector = 2;
    }

    // log the result at the leader node
    if(node_rank == 0) {
        log_results(exp_result{num_of_nodes, num_senders_selector, msg_size,
                               derecho::getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE), num_msgs,
                               avg_latency, avg_std_dev},
                    "data_latency");
    }
    group.barrier_sync();
    group.leave();
}
