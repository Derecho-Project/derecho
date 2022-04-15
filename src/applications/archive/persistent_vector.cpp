#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>

using derecho::PeerCaller;
using derecho::Replicated;
using std::cout;
using std::cerr;
using std::endl;
using namespace persistent;

/**
 * Example for replicated object with Persistent<T>
 */
class PFoo : public mutils::ByteRepresentable, public derecho::PersistsFields {
    Persistent<int> pint;
#define INVALID_VALUE	(-1)

public:
    virtual ~PFoo() noexcept(true) {
    }
    int read_state(int64_t ver) {
        return *pint[ver];
    }
    int read_state_by_time(uint64_t epoch_us) {
        int val = INVALID_VALUE;
        try {
            val = *pint[HLC(epoch_us,(uint64_t)0LLU)];
        } catch (...) {
            cout << "read_state_by_time(): invalid ts=" << epoch_us << endl;
        }
        return val;
    }
    bool change_state(int new_int) {
        if(new_int == *pint) {
            return false;
        }
        *pint = new_int;
        return true;
    }
    int64_t get_latest_version() {
        return pint.getLatestVersion();
    }

    REGISTER_RPC_FUNCTIONS(PFoo, ORDERED_TARGETS(read_state, read_state_by_time, change_state, get_latest_version));

    // constructor for PersistentRegistry
    PFoo(PersistentRegistry* pr) : pint([](){return std::make_unique<int>(0);}, nullptr, pr) {}
    PFoo(Persistent<int>& init_pint) : pint(std::move(init_pint)) {}
    DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
};


int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    derecho::UserMessageCallbacks callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << "is persisted." << std::endl;
            }};

    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < 2) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
        subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1}));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 2);
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(PFoo)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};

    auto pfoo_factory = [](PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<PFoo>(pr); };

    derecho::Group<PFoo> group(callback_set, subgroup_info, {},
                               std::vector<derecho::view_upcall_t>{},
                               pfoo_factory);

    cout << "Finished constructing/joining Group" << endl;
    uint32_t node_rank = group.get_my_rank();

    // Update the states:
    Replicated<PFoo> & pfoo_rpc_handle = group.get_subgroup<PFoo>();
    int values[] = {(int)(1000 + node_rank), (int)(2000 + node_rank), (int)(3000 + node_rank) };
    for (int i=0;i<3;i++) {
        derecho::rpc::QueryResults<bool> resultx = pfoo_rpc_handle.ordered_send<RPC_NAME(change_state)>(values[i]);
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Change state to " << values[i] << endl;
        for (auto& reply_pair : repliex) {
            cout << "\tnode[" << reply_pair.first << "] replies with '" << std::boolalpha << reply_pair.second.get() << "'." << endl;
        }
    }

    if(node_rank == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Query for latest version.
        int64_t lv = 0;
        derecho::rpc::QueryResults<int64_t> resultx = pfoo_rpc_handle.ordered_send<RPC_NAME(get_latest_version)>();
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Query the latest versions:" << endl;
        for (auto& reply_pair:repliex) {
            lv = reply_pair.second.get();
            cout << "\tnode[" << reply_pair.first << "] replies with version " << lv << "." << endl;
        }

        // Query all versions.
        for(int64_t ver = 0; ver <= lv ; ver ++) {
            derecho::rpc::QueryResults<int> resultx = pfoo_rpc_handle.ordered_send<RPC_NAME(read_state)>(ver);
            cout << "Query the value of version:" << ver << endl;
            for (auto& reply_pair:resultx.get()) {
                cout <<"\tnode[" << reply_pair.first << "]: v["<<ver<<"]="<<reply_pair.second.get()<<endl;
            }
        }

        // Query state by time.
        struct timespec tv;
        if(clock_gettime(CLOCK_REALTIME,&tv)) {
            cerr << "failed to read current time" << endl;
        } else {
            uint64_t now = tv.tv_sec*1000000+tv.tv_nsec/1000;
            uint64_t too_early = now - 5000000; // 5 second before
            // wait for the temporal frontier...
            std::this_thread::sleep_for(std::chrono::seconds(1));
            derecho::rpc::QueryResults<int> resultx = pfoo_rpc_handle.ordered_send<RPC_NAME(read_state_by_time)>(now);
            cout << "Query for now: ts="<< now << "us" << endl;
            for (auto& reply_pair:resultx.get()) {
                cout << "\tnode[" << reply_pair.first << "] replies with value:" << reply_pair.second.get() << endl;
            }
           derecho::rpc::QueryResults<int> resulty = pfoo_rpc_handle.ordered_send<RPC_NAME(read_state_by_time)>(too_early);
            cout << "Query for 5 sec before: ts="<< too_early << "us" <<endl;
            for (auto& reply_pair:resulty.get()) {
                cout << "\tnode[" << reply_pair.first << "] replies with value:" << reply_pair.second.get() << endl;
            }
        }
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
