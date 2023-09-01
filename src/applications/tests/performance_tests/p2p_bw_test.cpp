#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bytes_object.hpp"
#include "log_results.hpp"
#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

using std::endl;
using test::Bytes;
using namespace std::chrono;

/**
 * RPC Object with a single function that returns a byte array over P2P
 */
class TestObject : public mutils::ByteRepresentable {
    Bytes dummy_data;

public:
    TestObject(std::size_t data_size) : dummy_data(new uint8_t[data_size], data_size) {}
    //Deserialization constructor
    TestObject(const Bytes& init_bytes) : dummy_data(init_bytes) {}

    Bytes read_bytes() const {
        return dummy_data;
    }

    bool finishing_call(int x) const {
        return true;
    }

    DEFAULT_SERIALIZATION_SUPPORT(TestObject, dummy_data);
    REGISTER_RPC_FUNCTIONS(TestObject, P2P_TARGETS(read_bytes, finishing_call));
};

struct exp_result {
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint32_t count;
    double avg_msec;
    double avg_gbps;

    void print(std::ofstream& fout) {
        fout << max_msg_size << " " << window_size << " "
             << count << " "
             << avg_msec << " " << avg_gbps << endl;
    }
};

#define DEFAULT_PROC_NAME "p2p_bw_test"

int main(int argc, char* argv[]) {
    const int NUM_ARGS = 1;
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < (NUM_ARGS + 1)) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE: " << argv[0] << " [ derecho-config-list -- ] <count> [proc_name]" << std::endl;
        std::cout << "Note: proc_name sets the process's name as displayed in ps and pkill commands, default is " DEFAULT_PROC_NAME << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    //The maximum number of bytes that can be sent in an RPC message is not quite MAX_PAYLOAD_SIZE.
    //The serialized Bytes object will include its size field as well as the actual buffer, and
    //the RPC function header contains an InvocationID (which is a size_t) as well as the header
    //fields defined by remote_invocation_utilites::header_space().
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();

    const uint32_t total_num_messages = std::stoi(argv[dashdash_pos + 1]);
    const uint64_t max_msg_size = derecho::getConfUInt64(derecho::Conf::DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE) - rpc_header_size;

    steady_clock::time_point begin_time, end_time;

    if(dashdash_pos + NUM_ARGS + 1 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + NUM_ARGS + 1]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }

    derecho::SubgroupInfo subgroup_info{&derecho::one_subgroup_entire_view};

    auto test_factory = [max_msg_size](persistent::PersistentRegistry*, derecho::subgroup_id_t) {
        return std::make_unique<TestObject>(max_msg_size);
    };

    derecho::Group<TestObject> group(derecho::UserMessageCallbacks{},
                                     subgroup_info, {}, {}, test_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    int node_rank = group.get_my_rank();

    if(node_rank == 0) {
        node_id_t other_node = group.get_members()[1];
        // Begin Timer
        begin_time = std::chrono::steady_clock::now();
        for(uint i = 0; i < total_num_messages; i++) {
            derecho::Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
            derecho::QueryResults<Bytes> p2p_result = handle.p2p_send<RPC_NAME(read_bytes)>(other_node);
            if(i == total_num_messages - 1) {
                //On the last iteration, actually read the response to ensure we wait
                //for all the replies to be delivered
                p2p_result.get().get(other_node);
            }
        }
        //End timer
        end_time = std::chrono::steady_clock::now();

        int64_t nsec = duration_cast<nanoseconds>(end_time - begin_time).count();

        double thp_gbps = (static_cast<double>(total_num_messages) * max_msg_size) / nsec;
        double msec = (double)nsec / 1000000;
        double thp_ops = ((double)total_num_messages * 1000000000) / nsec;
        std::cout << "timespan:" << msec << " millisecond." << std::endl;
        std::cout << "throughput:" << thp_gbps << "GB/s." << std::endl;
        std::cout << "throughput:" << thp_ops << "ops." << std::endl;
        std::cout << std::flush;

        log_results(exp_result{max_msg_size,
                               derecho::getConfUInt32(derecho::Conf::DERECHO_P2P_WINDOW_SIZE),
                               total_num_messages,
                               msec, thp_gbps},
                    "data_derecho_p2p_bw");
    }

    group.barrier_sync();
    group.leave();
}
