#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>

#include "bytes_object.h"
#include "derecho/derecho.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <persistent/Persistent.hpp>
#include <conf/conf.hpp>

using derecho::Bytes;

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

int main(int argc, char* argv[]) {
    if(argc < 3) {
        std::cout << "Usage:" << argv[0] << " <num_of_nodes> <count> [configuration options...]" << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    int num_of_nodes = std::stoi(argv[1]);
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
    int count = std::stoi(argv[2]);

    uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

    derecho::CallbackSet callback_set{
            nullptr,  //we don't need the stability_callback here
            nullptr   //we don't need the persistence_callback either
    };

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(TestObject)), [num_of_nodes](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < num_of_nodes) {
                      std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);

                  std::vector<uint32_t> members(num_of_nodes);
                  for(int i = 0; i < num_of_nodes; i++) {
                      members[i] = i;
                  }

                  subgroup_vector[0].emplace_back(curr_view.make_subview(members));
                  next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(TestObject))}};

    auto ba_factory = [](PersistentRegistry*) { return std::make_unique<TestObject>(); };

    derecho::Group<TestObject> group(callback_set,subgroup_info,std::vector<derecho::view_upcall_t>{},ba_factory);
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
    if(node_id == 0) {
        derecho::rpc::QueryResults<bool> results = handle.ordered_query<RPC_NAME(finishing_call)>(0);
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

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
