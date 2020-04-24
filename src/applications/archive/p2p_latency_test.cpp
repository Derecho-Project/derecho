#include <iostream>

#include <derecho/core/derecho.hpp>
#include <derecho/conf/conf.hpp>

using std::cin;
using std::cout;
using std::endl;
using std::string;

struct test1_str {
    int state;
    int read_state() {
        // cout << "In the read state function" << endl;
        return state;
    }
    bool change_state(int new_state) {
        state = new_state;
        return true;
    }

    REGISTER_RPC_FUNCTIONS(test1_str, read_state, change_state);
};

template <typename T>
void output_result(typename derecho::rpc::QueryResults<T>::ReplyMap& rmap) {
    // cout << "Obtained a reply map" << endl;
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        it->second.get();
    }
}

int main(int argc, char* argv[]) {
    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_info{&derecho::one_subgroup_entire_view};
    derecho::Group<test1_str> managed_group({}, subgroup_info, nullptr, {},
                                            [](persistent::PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<test1_str>(); });
    if(derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID) == 0) {
        derecho::Replicated<test1_str>& rpc_handle = managed_group.get_subgroup<test1_str>(0);

        struct timespec start_time;
        // start timer
        clock_gettime(CLOCK_REALTIME, &start_time);

        long long int num_times = 10000;
        for(long long int i = 0; i < num_times; ++i) {
            try {
                output_result<int>(rpc_handle.p2p_send<RPC_NAME(read_state)>(1).get());
            } catch(...) {
            }
        }

        struct timespec end_time;
        clock_gettime(CLOCK_REALTIME, &end_time);
        long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        cout << "Average latency: " << (double)nanoseconds_elapsed / (num_times * 1000) << " microseconds" << endl;
    }
    managed_group.barrier_sync();
}
