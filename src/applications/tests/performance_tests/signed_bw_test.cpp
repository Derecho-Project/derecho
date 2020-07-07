#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <derecho/core/derecho.hpp>

#include "aggregate_bandwidth.hpp"
#include "bytes_object.hpp"
#include "log_results.hpp"
#include "partial_senders_allocator.hpp"

using std::cout;
using std::endl;
using namespace persistent;

class ByteArrayObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
public:
    Persistent<test::Bytes> pers_bytes;

    void change_pers_bytes(const test::Bytes& bytes) {
        *pers_bytes = bytes;
    }

    // deserialization constructor
    ByteArrayObject(Persistent<test::Bytes>& _p_bytes) : pers_bytes(std::move(_p_bytes)) {}
    // default constructor
    ByteArrayObject(PersistentRegistry* pr)
            : pers_bytes(std::make_unique<test::Bytes>, nullptr, pr) {}

    REGISTER_RPC_FUNCTIONS(ByteArrayObject, change_pers_bytes);
    DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject, pers_bytes);
};

struct signed_bw_result {
    int num_nodes;
    int num_senders_selector;
    int message_payload_size;
    int num_msgs;
    double persisted_bw;
    double verified_bw;

    void print(std::ofstream& fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << message_payload_size << " " << num_msgs << " "
             << persisted_bw << " " << verified_bw << std::endl;
    }
};

int main(int argc, char* argv[]) {
    //std::chrono is too verbose
    using namespace std::chrono;

    if(argc < 4) {
        std::cout << "usage:" << argv[0] << " <all|half|one> <num_of_nodes> <num_msgs>" << std::endl;
        return -1;
    }
    pthread_setname_np(pthread_self(), "signed_bw_test");

    derecho::Conf::initialize(argc, argv);

    if(derecho::getConfBoolean(CONF_PERS_SIGNED_LOG) == false) {
        std::cout << "Error: Signed log is not enabled, but this test requires it. Check your config file." << std::endl;
        return -1;
    }

    //The maximum number of bytes that can be sent to change_pers_bytes() is not quite MAX_PAYLOAD_SIZE.
    //The serialized Bytes object will include its size field as well as the actual buffer, and
    //the RPC function header contains an InvocationID (which is a size_t) as well as the header
    //fields defined by remote_invocation_utilites::header_space().
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();

    PartialSendMode sender_selector = PartialSendMode::ALL_SENDERS;
    if(strcmp(argv[1], "half") == 0) sender_selector = PartialSendMode::HALF_SENDERS;
    if(strcmp(argv[1], "one") == 0) sender_selector = PartialSendMode::ONE_SENDER;
    const int num_of_nodes = atoi(argv[2]);
    const int msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;
    const int num_msgs = atoi(argv[3]);
    steady_clock::time_point begin_time, send_complete_time, persist_complete_time, verify_complete_time;

    bool is_sending = true;

    long total_num_messages;
    switch(sender_selector) {
        case PartialSendMode::ALL_SENDERS:
            total_num_messages = num_of_nodes * num_msgs;
            break;
        case PartialSendMode::HALF_SENDERS:
            total_num_messages = (num_of_nodes / 2) * num_msgs;
            break;
        case PartialSendMode::ONE_SENDER:
            total_num_messages = num_msgs;
            break;
    }
    // variable 'done' tracks the end of the test
    volatile bool done = false;

    // last_version and its flag is shared between the stability callback and persistence callback.
    // This is a clumsy hack to figure out what version number is assigned to the last delivered message.
    persistent::version_t last_version;
    std::atomic<bool> last_version_set = false;

    auto stability_callback = [&last_version,
                               &last_version_set,
                               &send_complete_time,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<char*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        //Count the total number of messages delivered
        ++num_delivered;
        if(num_delivered == total_num_messages) {
            send_complete_time = std::chrono::steady_clock::now();
            last_version = ver;
            last_version_set = true;
        }
    };

    auto persistence_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        if(last_version_set && ver == last_version) {
            persist_complete_time = std::chrono::steady_clock::now();
        }
    };

    auto verified_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        if(last_version_set && ver == last_version) {
            verify_complete_time = std::chrono::steady_clock::now();
            done = true;
        }
    };
    derecho::CallbackSet callback_set{
            stability_callback,
            persistence_callback,
            nullptr,
            verified_callback};

    derecho::SubgroupInfo subgroup_info(PartialSendersAllocator(num_of_nodes, sender_selector));

    auto ba_factory = [](PersistentRegistry* pr, derecho::subgroup_id_t) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group(callback_set, subgroup_info, {},
                                          std::vector<derecho::view_upcall_t>{}, ba_factory);

    auto node_rank = group.get_my_rank();
    if((sender_selector == PartialSendMode::HALF_SENDERS) && (node_rank <= (num_of_nodes - 1) / 2)) {
        is_sending = false;
    }
    if((sender_selector == PartialSendMode::ONE_SENDER) && (node_rank != num_of_nodes - 1)) {
        is_sending = false;
    }
    std::cout << "My rank is: " << node_rank << ", and I'm sending: " << std::boolalpha << is_sending << std::endl;

    //Allocate this memory before starting the timer, even if we end up not needing it
    char* bbuf = new char[msg_size];
    memset(bbuf, 0, msg_size);
    test::Bytes bs(bbuf, msg_size);

    // Start the experiment timer
    begin_time = std::chrono::steady_clock::now();
    if(is_sending) {
        derecho::Replicated<ByteArrayObject>& handle = group.get_subgroup<ByteArrayObject>();
        for(int i = 0; i < num_msgs; i++) {
            handle.ordered_send<RPC_NAME(change_pers_bytes)>(bs);
        }
#if defined(_PERFORMANCE_DEBUG)
        (*handle.user_object_ptr)->pers_bytes.print_performance_stat();
#endif  //_PERFORMANCE_DEBUG
    }

    while(!done) {
    }
    int64_t send_nanosec = duration_cast<nanoseconds>(send_complete_time - begin_time).count();
    double send_millisec = static_cast<double>(send_nanosec) / 1000000;
    double send_thp_gbps = (static_cast<double>(num_msgs) * msg_size * 8) / send_nanosec;
    double send_thp_ops = (static_cast<double>(num_msgs) * 1000000000) / send_nanosec;
    std::cout << "(send)timespan: " << send_millisec << " milliseconds." << std::endl;
    std::cout << "(send)throughput: " << send_thp_gbps << "Gbit/s." << std::endl;
    std::cout << "(send)throughput: " << send_thp_ops << "ops." << std::endl;

    int64_t persist_nanosec = duration_cast<nanoseconds>(persist_complete_time - begin_time).count();
    double persist_millisec = static_cast<double>(persist_nanosec) / 1000000;
    double persist_thp_gbps = (static_cast<double>(num_msgs) * msg_size * 8) / persist_nanosec;
    double persist_thp_ops = (static_cast<double>(num_msgs) * 1000000000) / persist_nanosec;
    std::cout << "(pers)timespan: " << persist_millisec << " millisecond." << std::endl;
    std::cout << "(pers)throughput: " << persist_thp_gbps << "Gbit/s." << std::endl;
    std::cout << "(pers)throughput: " << persist_thp_ops << "ops." << std::endl;

    int64_t verified_nanosec = duration_cast<nanoseconds>(verify_complete_time - begin_time).count();
    double verified_millisec = static_cast<double>(verified_nanosec) / 1000000;
    double verified_thp_gbps = (static_cast<double>(num_msgs) * msg_size * 8) / verified_nanosec;
    double verified_thp_ops = (static_cast<double>(num_msgs) * 1000000000) / verified_nanosec;
    std::cout << "(pers)timespan: " << verified_millisec << " millisecond." << std::endl;
    std::cout << "(pers)throughput: " << verified_thp_gbps << "Gbit/s." << std::endl;
    std::cout << "(pers)throughput: " << verified_thp_ops << "ops." << std::endl;

    std::cout << std::flush;

    auto members = group.get_members();
    double avg_pers_bw, avg_verified_bw;
    std::tie(avg_pers_bw, avg_verified_bw) = aggregate_bandwidth(members, members[node_rank],
                                                                 {persist_thp_gbps, verified_thp_gbps});

    if(node_rank == 0) {
        log_results(signed_bw_result{num_of_nodes, static_cast<std::underlying_type_t<PartialSendMode>>(sender_selector),
                                     msg_size, num_msgs, avg_pers_bw, avg_verified_bw},
                    "data_signed_bw");
    }

    group.barrier_sync();
    group.leave();
}
