#include "derecho/derecho.h"
#include "initialize.h"
#include "conf/conf.hpp"

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
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};

    derecho::message_callback_t stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    const int32_t num_nodes = 7;

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(Foo)), [num_nodes](const derecho::View& curr_view, int& next_unassigned_rank) {
                  if(curr_view.num_members < num_nodes) {
                      std::cout << "Foo function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  // only one subgroup of type Foo, shards of size 'Too' D:
                  for(uint i = 0; i < (uint32_t)num_nodes / 2; ++i) {
                      subgroup_vector[0].emplace_back(curr_view.make_subview({2 * i, 2 * i + 1}));
                  }
                  next_unassigned_rank = std::max(next_unassigned_rank, num_nodes - 1);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(Foo))}};
    auto foo_factory = [](PersistentRegistry*) { return std::make_unique<Foo>(-1); };

    std::unique_ptr<derecho::Group<Foo>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<Foo>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                foo_factory);
    } else {
        group = std::make_unique<derecho::Group<Foo>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                foo_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    // all shards change their state to a unique integer
    if(node_id < num_nodes - 1 && node_id % 2 == 0) {
        auto& foo_handle = group->get_subgroup<Foo>();
        foo_handle.ordered_send<Foo::CHANGE_STATE>(node_id);
        std::cout << "Done calling ordered_send" << std::endl;
    }
    group->barrier_sync();
    // node 13 queries for the state of each shard
    if(node_id == num_nodes - 1) {
        auto shard_iterator = group->get_shard_iterator<Foo>();
        auto query_results_vec = shard_iterator.p2p_query<Foo::READ_STATE>();
        uint cnt = 0;
        for(auto& query_result : query_results_vec) {
            auto& reply_map = query_result.get();
            cout << "Reply from shard " << cnt++ << ": " << reply_map.begin()->second.get() << endl;
        }
        std::cout << "Done getting the replies" << std::endl;
    }
    group->barrier_sync();
    exit(0);
}
