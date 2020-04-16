#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

using namespace persistent;

/**
 * Example for replicated object with Persistent<T>
 */
class PFoo : public mutils::ByteRepresentable, public derecho::PersistsFields {
    Persistent<int> pint;

public:
    virtual ~PFoo() noexcept(true) {
    }
    int read_state() {
        return *pint;
    }
    bool change_state(int new_int) {
        if(new_int == *pint) {
            return false;
        }
        *pint = new_int;
        return true;
    }

    enum Functions { READ_STATE,
                     CHANGE_STATE };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<READ_STATE>(&PFoo::read_state),
                               derecho::rpc::tag<CHANGE_STATE>(&PFoo::change_state));
    }

    // constructor for PersistentRegistry
    PFoo(PersistentRegistry* pr) : pint([](){return std::make_unique<int>(0);}, nullptr, pr) {}
    PFoo(Persistent<int>& init_pint) : pint(std::move(init_pint)) {}
    DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
};

using derecho::ExternalCaller;
using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << "is persisted." << std::endl;
            }};

    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < 6) {
            std::cout << "PFoo function throwing subgroup_provisioning_exception" << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
        subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2, 3, 4, 5}));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 6);
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(PFoo)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};

    auto pfoo_factory = [](PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<PFoo>(pr); };

    derecho::Group<PFoo> group(callback_set, subgroup_info, nullptr,
                               std::vector<derecho::view_upcall_t>{},
                               pfoo_factory);

    cout << "Finished constructing/joining Group" << endl;
    uint32_t node_rank = group.get_my_rank();

    if(node_rank == 0) {
        Replicated<PFoo>& pfoo_rpc_handle = group.get_subgroup<PFoo>();

        cout << "Reading PFoo's state just to allow node 1's message to be delivered" << endl;
        pfoo_rpc_handle.ordered_send<PFoo::READ_STATE>();
    }
    if(node_rank == 1) {
        Replicated<PFoo>& pfoo_rpc_handle = group.get_subgroup<PFoo>();
        int new_value = 3;
        cout << "Changing PFoo's state to " << new_value << endl;
        derecho::rpc::QueryResults<bool> resultx = pfoo_rpc_handle.ordered_send<PFoo::CHANGE_STATE>(new_value);
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Got a reply map!" << endl;
        for(auto& reply_pair : repliex) {
            cout << "Replyx from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
        }
    }
    if(node_rank == 2) {
    }

    if(node_rank == 3) {
    }
    if(node_rank == 4) {
    }
    if(node_rank == 5) {
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
