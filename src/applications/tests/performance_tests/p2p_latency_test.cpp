#include <chrono>
#include <iostream>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>

using std::cin;
using std::cout;
using std::endl;
using std::string;
using std::chrono::duration_cast;
using derecho::node_id_t;

class TestObject : public mutils::ByteRepresentable {
    int state;

public:
    TestObject() : state(0) {}
    TestObject(int init_state) : state(init_state) {}

    int read_state() const {
        return state;
    }
    bool change_state(int new_state) {
        state = new_state;
        return true;
    }

    DEFAULT_SERIALIZATION_SUPPORT(TestObject, state);
    REGISTER_RPC_FUNCTIONS(TestObject, P2P_TARGETS(read_state), ORDERED_TARGETS(change_state));
};

template <typename T>
void output_result(typename derecho::rpc::QueryResults<T>::ReplyMap& rmap) {
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        it->second.get();
    }
}

/**
 * This test always runs between 2 nodes, and measures the latency of a P2P RPC function call.
 * Command line arguments: [num_msgs]
 */
int main(int argc, char* argv[]) {
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    derecho::Conf::initialize(argc, argv);

    const int num_msgs = std::stoi(argv[dashdash_pos + 1]);

    derecho::SubgroupInfo subgroup_info{&derecho::one_subgroup_entire_view};
    derecho::Group<TestObject> group({nullptr, nullptr, nullptr, nullptr}, subgroup_info, {}, {},
                                     [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
                                         return std::make_unique<TestObject>();
                                     });
    //Node 0 will initiate P2P sends to node 1
    int node_rank = group.get_my_rank();
    if(node_rank == 0) {
        derecho::Replicated<TestObject>& rpc_handle = group.get_subgroup<TestObject>();
        node_id_t other_node = group.get_members()[1];

        // start timer
        auto begin_time = std::chrono::steady_clock::now();

        for(int i = 0; i < num_msgs; ++i) {
            try {
                rpc_handle.p2p_send<RPC_NAME(read_state)>(other_node).get().get(other_node);
            } catch(...) {
            }
        }

        // stop timer
        auto end_time = std::chrono::steady_clock::now();

        long long int nanoseconds_elapsed = duration_cast<std::chrono::nanoseconds>(end_time - begin_time).count();
        cout << "Average latency: " << (double)nanoseconds_elapsed / (num_msgs * 1000) << " microseconds" << endl;
    }
    group.barrier_sync();
}
