#include "aggregate_bandwidth.hpp"
#include "bytes_object.hpp"
#include "log_results.hpp"
#include "partial_senders_allocator.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/utils/timestamp_logger.hpp>

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

using std::cout;
using std::endl;
using std::chrono::steady_clock;
using namespace persistent;

/**
 * State shared between the replicated test objects and the main thread. The replicated
 * objects get a pointer to this object using DeserializationManager.
 */
struct TestState : public derecho::DeserializationContext {
    uint64_t total_num_messages;
    // Used to signal the main thread that the experiment is done
    std::atomic<bool> experiment_done;
    // Set by ByteArrayObject when the last RPC message is delivered and its version is known
    persistent::version_t last_version;
    // Used to alert the global persistence callback that last_version is ready
    std::atomic<bool> last_version_set;
    // The time the last RPC message is delivered to ByteArrayObject
    steady_clock::time_point send_complete_time;
    // Set by the global persistence callback when version last_version is persisted
    steady_clock::time_point persist_complete_time;
    // Set by the global verification callback when version last_version is verified
    steady_clock::time_point verify_complete_time;
};

class ByteArrayObject : public mutils::ByteRepresentable,
                        public derecho::GroupReference,
                        public derecho::SignedPersistentFields {
public:
    Persistent<test::Bytes> pers_bytes;
    uint64_t messages_received;
    TestState* test_state;

    void change_pers_bytes(const test::Bytes& bytes);

    // default constructor
    ByteArrayObject(PersistentRegistry* pr, TestState* test_state)
            : pers_bytes(pr, true),
              messages_received(0),
              test_state(test_state) {}

    // deserialization constructor
    ByteArrayObject(Persistent<test::Bytes>& _p_bytes, uint64_t _messages_received, TestState* _test_state)
            : pers_bytes(std::move(_p_bytes)),
              messages_received(_messages_received),
              test_state(_test_state) {}

    REGISTER_RPC_FUNCTIONS(ByteArrayObject, ORDERED_TARGETS(change_pers_bytes));
    DEFAULT_SERIALIZE(pers_bytes, messages_received);
    DEFAULT_DESERIALIZE_NOALLOC(ByteArrayObject);
    static std::unique_ptr<ByteArrayObject> from_bytes(mutils::DeserializationManager* dsm, const uint8_t* buffer);
};

void ByteArrayObject::change_pers_bytes(const test::Bytes& bytes) {
    *pers_bytes = bytes;
    // This logic used to be in the global stability callback, but now it needs to be here
    // because global stability callbacks were disabled for RPC messages. It also now requires
    // a call to get_subgroup().get_current_version(), which means we can't write the method inline.
    auto ver = this->group->template get_subgroup<ByteArrayObject>(this->subgroup_index).get_current_version();
    ++messages_received;
    if(messages_received == test_state->total_num_messages) {
        test_state->send_complete_time = std::chrono::steady_clock::now();
        test_state->last_version = std::get<0>(ver);
        test_state->last_version_set = true;
    }
}

std::unique_ptr<ByteArrayObject> ByteArrayObject::from_bytes(mutils::DeserializationManager* dsm, const uint8_t* buffer) {
    // Default serialization will serialize each named field in order, so deserialize in the same order
    auto pers_bytes_ptr = mutils::from_bytes<Persistent<test::Bytes>>(dsm, buffer);
    std::size_t bytes_read = mutils::bytes_size(*pers_bytes_ptr);
    auto messages_received_ptr = mutils::from_bytes<uint64_t>(dsm, buffer + bytes_read);
    // Get the TestState pointer from the DeserializationManager
    assert(dsm);
    assert(dsm->registered<TestState>());
    TestState* test_state_ptr = &(dsm->mgr<TestState>());
    return std::make_unique<ByteArrayObject>(*pers_bytes_ptr, *messages_received_ptr, test_state_ptr);
}

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

#define DEFAULT_PROC_NAME "signed_bw_test"

int main(int argc, char* argv[]) {
    //std::chrono is too verbose
    using namespace std::chrono;

    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < 4) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "Usage: " << argv[0] << " [<derecho config options> -- ] <all|half|one> <num_of_nodes> <num_msgs> [proc_name]" << std::endl;
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

    PartialSendMode sender_selector = PartialSendMode::ALL_SENDERS;
    if(strcmp(argv[dashdash_pos + 1], "half") == 0) sender_selector = PartialSendMode::HALF_SENDERS;
    if(strcmp(argv[dashdash_pos + 1], "one") == 0) sender_selector = PartialSendMode::ONE_SENDER;
    const int num_of_nodes = atoi(argv[dashdash_pos + 2]);
    const int msg_size = derecho::getConfUInt64(derecho::Conf::SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;
    const int num_msgs = atoi(argv[dashdash_pos + 3]);

    if((argc - dashdash_pos) > 4) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 4]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    TestState shared_test_state;
    shared_test_state.experiment_done = false;
    shared_test_state.last_version_set = false;
    steady_clock::time_point begin_time;
    bool is_sending = true;

    switch(sender_selector) {
        case PartialSendMode::ALL_SENDERS:
            shared_test_state.total_num_messages = num_of_nodes * num_msgs;
            break;
        case PartialSendMode::HALF_SENDERS:
            shared_test_state.total_num_messages = (num_of_nodes / 2) * num_msgs;
            break;
        case PartialSendMode::ONE_SENDER:
            shared_test_state.total_num_messages = num_msgs;
            break;
    }

    auto persistence_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        if(shared_test_state.last_version_set && ver == shared_test_state.last_version) {
            shared_test_state.persist_complete_time = steady_clock::now();
        }
    };

    auto verified_callback = [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
        if(shared_test_state.last_version_set && ver == shared_test_state.last_version) {
            shared_test_state.verify_complete_time = steady_clock::now();
            shared_test_state.experiment_done = true;
        }
    };
    derecho::UserMessageCallbacks callback_set{
            nullptr,
            nullptr,
            persistence_callback,
            verified_callback};

    derecho::SubgroupInfo subgroup_info(PartialSendersAllocator(num_of_nodes, sender_selector));

    auto ba_factory = [&shared_test_state](PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<ByteArrayObject>(pr, &shared_test_state);
    };

    derecho::Group<ByteArrayObject> group(callback_set, subgroup_info, {&shared_test_state},
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
    uint8_t* bbuf = new uint8_t[msg_size];
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

    while(!shared_test_state.experiment_done) {
    }
#ifdef TIMESTAMPS
    // Dump timestamps
    std::ofstream timestamp_file("signed_bw_timestamps.log");
    derecho::TimestampLogger::dump(timestamp_file);
    timestamp_file.close();
#endif

    int64_t send_nanosec = duration_cast<nanoseconds>(shared_test_state.send_complete_time - begin_time).count();
    double send_millisec = static_cast<double>(send_nanosec) / 1000000;
    int64_t persist_nanosec = duration_cast<nanoseconds>(shared_test_state.persist_complete_time - begin_time).count();
    double persist_millisec = static_cast<double>(persist_nanosec) / 1000000;
    int64_t verified_nanosec = duration_cast<nanoseconds>(shared_test_state.verify_complete_time - begin_time).count();
    double verified_millisec = static_cast<double>(verified_nanosec) / 1000000;

    //Calculate bandwidth
    //Bytes / nanosecond just happens to be equivalent to GigaBytes / second (in "decimal" GB)
    //Note that total_num_messages already incorporates multiplying by the number of senders
    double send_thp_gbps = (static_cast<double>(shared_test_state.total_num_messages) * msg_size) / send_nanosec;
    double send_thp_ops = (static_cast<double>(shared_test_state.total_num_messages) * 1000000000) / send_nanosec;
    std::cout << "(send)timespan: " << send_millisec << " milliseconds." << std::endl;
    std::cout << "(send)throughput: " << send_thp_gbps << "GB/s." << std::endl;
    std::cout << "(send)throughput: " << send_thp_ops << "ops." << std::endl;

    double persist_thp_gbs = (static_cast<double>(shared_test_state.total_num_messages) * msg_size) / persist_nanosec;
    double persist_thp_ops = (static_cast<double>(shared_test_state.total_num_messages) * 1000000000) / persist_nanosec;
    std::cout << "(pers)timespan: " << persist_millisec << " millisecond." << std::endl;
    std::cout << "(pers)throughput: " << persist_thp_gbs << "GB/s." << std::endl;
    std::cout << "(pers)throughput: " << persist_thp_ops << "ops." << std::endl;

    double verified_thp_gbs = (static_cast<double>(shared_test_state.total_num_messages) * msg_size) / verified_nanosec;
    double verified_thp_ops = (static_cast<double>(shared_test_state.total_num_messages) * 1000000000) / verified_nanosec;
    std::cout << "(verify)timespan: " << verified_millisec << " millisecond." << std::endl;
    std::cout << "(verify)throughput: " << verified_thp_gbs << "GB/s." << std::endl;
    std::cout << "(verify)throughput: " << verified_thp_ops << "ops." << std::endl;

    std::cout << std::flush;

    auto members = group.get_members();
    double avg_pers_bw, avg_verified_bw;
    std::tie(avg_pers_bw, avg_verified_bw) = aggregate_bandwidth(members, members[node_rank],
                                                                 {persist_thp_gbs, verified_thp_gbs});

    if(node_rank == 0) {
        log_results(signed_bw_result{num_of_nodes, static_cast<std::underlying_type_t<PartialSendMode>>(sender_selector),
                                     msg_size, num_msgs, avg_pers_bw, avg_verified_bw},
                    "data_signed_bw");
    }

    group.barrier_sync();
    group.leave();
}
