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
    int read_state(int64_t ver) {
        return *pint[ver];
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

    enum Functions { READ_STATE,
                     CHANGE_STATE,
                     GET_LATEST_VERSION };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<READ_STATE>(&PFoo::read_state),
                               derecho::rpc::tag<CHANGE_STATE>(&PFoo::change_state),
                               derecho::rpc::tag<GET_LATEST_VERSION>(&PFoo::get_latest_version));
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
                                              if(curr_view.num_members < 2) {
                                                  std::cout << "PFoo function throwing subgroup_provisioning_exception" << std::endl;
                                                  throw derecho::subgroup_provisioning_exception();
                                              }
                                              derecho::subgroup_shard_layout_t subgroup_vector(1);
                                              //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
                                              subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1}));
                                              next_unassigned_rank = std::max(next_unassigned_rank, 2);
                                              return subgroup_vector;
                                          }}},
                                        {std::type_index(typeid(PFoo))}};

    auto pfoo_factory = [](PersistentRegistry* pr) { return std::make_unique<PFoo>(pr); };

    std::unique_ptr<derecho::Group<PFoo>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<PFoo>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{},
                pfoo_factory);
    } else {
        group = std::make_unique<derecho::Group<PFoo>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{},
                pfoo_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    // Update the states:
    Replicated<PFoo> & pfoo_rpc_handle = group->get_subgroup<PFoo>();
    int values[] = {(int)(1000 + node_id), (int)(2000 + node_id), (int)(3000 + node_id) };
    for (int i=0;i<3;i++) {
        derecho::rpc::QueryResults<bool> resultx = pfoo_rpc_handle.ordered_query<PFoo::CHANGE_STATE>(values[i]);
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Change state to " << values[i] << endl;
        for (auto& reply_pair : repliex) {
            cout << "\tnode[" << reply_pair.first << "] replies with '" << std::boolalpha << reply_pair.second.get() << "'." << endl;
        }
    }

    if(node_id == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Query for latest version.
        int64_t lv = 0;
        derecho::rpc::QueryResults<int64_t> resultx = pfoo_rpc_handle.ordered_query<PFoo::GET_LATEST_VERSION>();
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Query the latest versions:" << endl;
        for (auto& reply_pair:repliex) {
            lv = reply_pair.second.get();
            cout << "\tnode[" << reply_pair.first << "] replies with version " << lv << "." << endl;
        }

        // Query all versions.
        for(int64_t ver = 0; ver <= lv ; ver ++) {
            derecho::rpc::QueryResults<int> resultx = pfoo_rpc_handle.ordered_query<PFoo::READ_STATE>(ver);
            cout << "Query the value of version:" << ver << endl;
            for (auto& reply_pair:resultx.get()) {
                cout <<"\tnode[" << reply_pair.first << "]: v["<<ver<<"]="<<reply_pair.second.get()<<endl;
            }
        }
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
