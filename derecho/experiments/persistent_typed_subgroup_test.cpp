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
class PFoo : public mutils::ByteRepresentable {
  Persistent<int> pint;
public:
  virtual ~PFoo() noexcept (true) {
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
  PFoo(PersistentRegistry * pr):pint(nullptr,pr) {}
  PFoo(Persistent<int> & init_pint):pint(std::move(init_pint)) {}
  DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
};


using std::cout;
using std::endl;
using derecho::Replicated;
using derecho::ExternalCaller;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Derecho message parameters
    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::message_callback_t stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    //Since this is just a test, assume there will always be 6 members with IDs 0-5
    //Assign Foo and Bar to a subgroup containing 0, 1, and 2, and Cache to a subgroup containing 3, 4, and 5, PFoo to a subgroup have all 6 nodes.
    derecho::SubgroupInfo subgroup_info{{
/*
             {std::type_index(typeid(Foo)), [](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < 3) {
                      std::cout << "Foo function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
                  subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2}));
                  next_unassigned_rank = std::max(next_unassigned_rank, 3);
                  return subgroup_vector;
              }},
             {std::type_index(typeid(Bar)), [](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < 3) {
                      std::cout << "Bar function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2}));
                  next_unassigned_rank = std::max(next_unassigned_rank, 3);
                  return subgroup_vector;
              }},
             {std::type_index(typeid(Cache)), [](const derecho::View& curr_view, int & next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < 6) {
                      std::cout << "Cache function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  subgroup_vector[0].emplace_back(curr_view.make_subview({3, 4, 5}));
                  next_unassigned_rank = std::max(next_unassigned_rank, 6);
                  return subgroup_vector;
              }},
*/
             {std::type_index(typeid(PFoo)), [](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
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
//            {std::type_index(typeid(Foo)), std::type_index(typeid(Bar)), std::type_index(typeid(Cache)), std::type_index(typeid(PFoo))}};

    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state
    auto pfoo_factory = [](PersistentRegistry *pr) { return std::make_unique<PFoo>(pr); };
    
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

    //Keep attempting to get a subgroup pointer to see if the group is "adequately provisioned"
    bool inadequately_provisioned = true;
    while(inadequately_provisioned) {
        try {
            group->get_subgroup<PFoo>();
            inadequately_provisioned = false;
        } catch(derecho::subgroup_provisioning_exception& e) {
            inadequately_provisioned = true;
        }
    }

    cout << "All members have joined, subgroups are provisioned" << endl;

    if(node_id == 0) {
//        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
//        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
        Replicated<PFoo>& pfoo_rpc_handle = group->get_subgroup<PFoo>();

/*
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 0...");
        cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
        foo_rpc_handle.ordered_query<Foo::READ_STATE>();
*/
        cout << "Reading PFoo's state just to allow node 1's message to be delivered" << endl;
        pfoo_rpc_handle.ordered_query<PFoo::READ_STATE>();
    }
    if(node_id == 1) {
//        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
//        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
        Replicated<PFoo>& pfoo_rpc_handle = group->get_subgroup<PFoo>();
        int new_value = 3;
/*
        cout << "Changing Foo's state to " << new_value << endl;
        derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_query<Foo::CHANGE_STATE>(new_value);
        decltype(results)::ReplyMap& replies = results.get();
        cout << "Got a reply map!" << endl;
        for(auto& reply_pair : replies) {
            cout << "Reply from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
        }
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 1...");
*/
        cout << "Changing PFoo's state to " << new_value << endl;
        derecho::rpc::QueryResults<bool> resultx = pfoo_rpc_handle.ordered_query<PFoo::CHANGE_STATE>(new_value);
        decltype(resultx)::ReplyMap& repliex = resultx.get();
        cout << "Got a reply map!" << endl;
        for(auto& reply_pair : repliex) {
            cout << "Replyx from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
        }
 
    }
    if(node_id == 2) {
/*
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        cout << "Reading Foo's state from the group" << endl;
        derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_query<Foo::READ_STATE>();
        for(auto& reply_pair : foo_results.get()) {
            cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
        }
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 2...");
        cout << "Printing log from Bar" << endl;
        derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.ordered_query<Bar::PRINT>();
        for(auto& reply_pair : bar_results.get()) {
            cout << "Node " << reply_pair.first << " says the log is: " << reply_pair.second.get() << endl;
        }
        cout << "Clearing Bar's log" << endl;
        bar_rpc_handle.ordered_send<Bar::CLEAR>();
*/    }

    if(node_id == 3) {
/*
        Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>();
        cout << "Waiting for a 'Ken' value to appear in the cache..." << endl;
        bool found = false;
        while(!found) {
            derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_query<Cache::CONTAINS>("Ken");
            derecho::rpc::QueryResults<bool>::ReplyMap& replies = results.get();
            //Fold "&&" over the results to see if they're all true
            bool contains_accum = true;
            for(auto& reply_pair : replies) {
                bool contains_result = reply_pair.second.get();
                cout << std::boolalpha << "  Reply from node " << reply_pair.first << ": " << contains_result << endl;
                contains_accum = contains_accum && contains_result;
            }
            found = contains_accum;
        }
        cout << "..found!" << endl;
        derecho::rpc::QueryResults<std::string> results = cache_rpc_handle.ordered_query<Cache::GET>("Ken");
        for(auto& reply_pair : results.get()) {
            cout << "Node " << reply_pair.first << " had Ken = " << reply_pair.second.get() << endl;
        }
*/
    }
    if(node_id == 4) {
/*
        Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>();
        cout << "Putting Ken = Birman in the cache" << endl;
        //Do it twice just to send more messages, so that the "contains" and "get" calls can go through
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Birman");
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Birman");
        derecho::node_id_t p2p_target = 2;
        cout << "Reading Foo's state from node " << p2p_target << endl;
        ExternalCaller<Foo>& p2p_foo_handle = group->get_nonmember_subgroup<Foo>();
        derecho::rpc::QueryResults<int> foo_results = p2p_foo_handle.p2p_query<Foo::READ_STATE>(p2p_target);
        int response = foo_results.get().get(p2p_target);
        cout << "  Response: " << response << endl;
*/
    }
    if(node_id == 5) {
/*
        Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>();
        cout << "Putting Ken = Woodberry in the cache" << endl;
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
*/
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
