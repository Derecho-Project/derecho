#include <derecho/core/derecho.hpp>
#include <derecho/conf/conf.hpp>

using std::cout;
using std::endl;

class Foo : public mutils::ByteRepresentable {
    int state;

public:
    int read_state() {
        return state;
    }
    bool change_state(int new_state) {
        if(new_state == state) {
            return false;
        }
        state = new_state;
        return true;
    }

    /** Named integers that will be used to tag the RPC methods */
    enum Functions { READ_STATE,
                     CHANGE_STATE };

    /**
     * All replicated objects must provide this static method, which should
     * return a tuple containing all the methods that can be invoked by RPC.
     * Each method should be "tagged" with derecho::rpc::tag(), whose template
     * parameter indicates which numeric constant will identify the method.
     * @return A tuple of "tagged" function pointers.
     */
    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<READ_STATE>(&Foo::read_state),
                               derecho::rpc::tag<CHANGE_STATE>(&Foo::change_state));
    }

    /**
     * Constructs a Foo with an initial value.
     * @param initial_state
     */
    Foo(int initial_state = 0) : state(initial_state) {}
    DEFAULT_SERIALIZATION_SUPPORT(Foo, state);
};

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    const int32_t num_nodes = 7;

    derecho::SubgroupInfo subgroup_info{[num_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_nodes) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        // only one subgroup of type Foo, shards of size 'Too' D:
        for(uint i = 0; i < (uint32_t)num_nodes / 2; ++i) {
            subgroup_vector[0].emplace_back(curr_view.make_subview({2 * i, 2 * i + 1}));
        }
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_nodes - 1);
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(Foo)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};
    auto foo_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };

    derecho::Group<Foo> group({}, subgroup_info, nullptr,
                              std::vector<derecho::view_upcall_t>{},
                              foo_factory);

    cout << "Finished constructing/joining Group" << endl;
    uint32_t node_rank = group.get_my_rank();

    // all shards change their state to a unique integer
    if(node_rank < num_nodes - 1 && node_rank % 2 == 0) {
        auto& foo_handle = group.get_subgroup<Foo>();
        foo_handle.ordered_send<Foo::CHANGE_STATE>(node_rank);
        std::cout << "Done calling ordered_send" << std::endl;
    }
    group.barrier_sync();
    // node 13 queries for the state of each shard
    if(node_rank == num_nodes - 1) {
        auto shard_iterator = group.get_shard_iterator<Foo>();
        auto query_results_vec = shard_iterator.p2p_send<Foo::READ_STATE>();
        uint cnt = 0;
        for(auto& query_result : query_results_vec) {
            auto& reply_map = query_result.get();
            cout << "Reply from shard " << cnt++ << ": " << reply_map.begin()->second.get() << endl;
        }
        std::cout << "Done getting the replies" << std::endl;
    }
    group.barrier_sync();
    exit(0);
}
