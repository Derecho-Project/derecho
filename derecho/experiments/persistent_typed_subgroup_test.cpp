/**
 * @file typed_subgroup_test.cpp
 *
 * @date Mar 1, 2017
 * @author edward
 */

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <persistent/Persistent.hpp>

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
    PFoo(PersistentRegistry* pr) : pint(nullptr, pr) {}
    PFoo(Persistent<int>& init_pint) : pint(std::move(init_pint)) {}
    DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
};

using derecho::ExternalCaller;
using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Derecho message parameters
    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};

    // derecho::message_callback_t stability_callback{};
    // derecho::CallbackSet callback_set{stability_callback, {}};
    derecho::CallbackSet callback_set{
            nullptr,
            [](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                std::cout << "Subgroup " << subgroup << ", version " << ver << "is persisted." << std::endl;
            }};

    derecho::SubgroupInfo subgroup_info{{{std::type_index(typeid(PFoo)), [](const derecho::View& curr_view, int& next_unassigned_rank) {
                                              if(curr_view.num_members < 6) {
                                                  std::cout << "PFoo function throwing subgroup_provisioning_exception" << std::endl;
                                                  throw derecho::subgroup_provisioning_exception();
                                              }
                                              derecho::subgroup_shard_layout_t subgroup_vector(1);
                                              //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
                                              subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2, 3, 4, 5}));
                                              next_unassigned_rank = std::max(next_unassigned_rank, 6);
                                              return subgroup_vector;
                                          }}},
                                        {std::type_index(typeid(PFoo))}};

    auto pfoo_factory = [](PersistentRegistry* pr) { return std::make_unique<PFoo>(pr); };

    std::unique_ptr<derecho::Group<PFoo>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<PFoo>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, 12345,
                pfoo_factory);
    } else {
        group = std::make_unique<derecho::Group<PFoo>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, 12345,
                pfoo_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    if(node_id == 0) {
        Replicated<PFoo>& pfoo_rpc_handle = group->get_subgroup<PFoo>();

        cout << "Reading PFoo's state just to allow node 1's message to be delivered" << endl;
        pfoo_rpc_handle.ordered_query<PFoo::READ_STATE>();
    }
    if(node_id == 1) {
        Replicated<PFoo>& pfoo_rpc_handle = group->get_subgroup<PFoo>();
        int new_value = 3;
        cout << "Changing PFoo's state to " << new_value << endl;
        derecho::rpc::QueryResults<bool> resultx = pfoo_rpc_handle.ordered_query<PFoo::CHANGE_STATE>(new_value);
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Got a reply map!" << endl;
        for(auto& reply_pair : repliex) {
            cout << "Replyx from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
        }
    }
    if(node_id == 2) {
    }

    if(node_id == 3) {
    }
    if(node_id == 4) {
    }
    if(node_id == 5) {
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
