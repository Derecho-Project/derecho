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

/**
 * Example replicated object, containing some serializable state and providing
 * two RPC methods. In order to be serialized it must extend ByteRepresentable.
 */
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

class Bar : public mutils::ByteRepresentable {
    std::string log;

public:
    void append(const std::string& words) {
        log += words;
    }
    void clear() {
        log.clear();
    }
    std::string print() {
        return log;
    }
    enum Functions { APPEND,
                     CLEAR,
                     PRINT };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<APPEND>(&Bar::append),
                               derecho::rpc::tag<CLEAR>(&Bar::clear),
                               derecho::rpc::tag<PRINT>(&Bar::print));
    }

    DEFAULT_SERIALIZATION_SUPPORT(Bar, log);
    Bar(const std::string& s = "") : log(s) {}
};

class Cache : public mutils::ByteRepresentable {
    std::map<std::string, std::string> cache_map;

public:
    void put(const std::string& key, const std::string& value) {
        cache_map[key] = value;
    }
    std::string get(const std::string& key) {
        return cache_map[key];
    }
    bool contains(const std::string& key) {
        return cache_map.find(key) != cache_map.end();
    }
    bool invalidate(const std::string& key) {
        auto key_pos = cache_map.find(key);
        if(key_pos == cache_map.end()) {
            return false;
        }
        cache_map.erase(key_pos);
        return true;
    }
    enum Functions { PUT,
                     GET,
                     CONTAINS,
                     INVALIDATE };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<PUT>(&Cache::put),
                               derecho::rpc::tag<GET>(&Cache::get),
                               derecho::rpc::tag<CONTAINS>(&Cache::contains),
                               derecho::rpc::tag<INVALIDATE>(&Cache::invalidate));
    }

    Cache() : cache_map() {}
    /**
     * This constructor is required by default serialization support, in order
     * to reconstruct an object after deserialization.
     * @param cache_map The state of the cache.
     */
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
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

    derecho::message_callback stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    //Since this is just a test, assume there will always be 6 members with IDs 0-5
    //Assign Foo and Bar to a subgroup containing 0, 1, and 2, and Cache to a subgroup containing 3, 4, and 5
    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(Foo)), [](const derecho::View& curr_view) {
                  if(curr_view.num_members < 3) {
                      std::cout << "Foo function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
                  subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2}));
                  return subgroup_vector;
              }},
             {std::type_index(typeid(Bar)), [](const derecho::View& curr_view) {
                  if(curr_view.num_members < 3) {
                      std::cout << "Bar function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  subgroup_vector[0].emplace_back(curr_view.make_subview({0, 1, 2}));
                  return subgroup_vector;
              }},
             {std::type_index(typeid(Cache)), [](const derecho::View& curr_view) {
                  if(curr_view.num_members < 6) {
                      std::cout << "Cache function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  subgroup_vector[0].emplace_back(curr_view.make_subview({3, 4, 5}));
                  return subgroup_vector;
              }}}};

    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state
    auto foo_factory = []() { return std::make_unique<Foo>(-1); };
    auto bar_factory = []() { return std::make_unique<Bar>(); };
    auto cache_factory = []() { return std::make_unique<Cache>(); };

    std::unique_ptr<derecho::Group<Foo, Bar, Cache>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<Foo, Bar, Cache>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, 12345,
                foo_factory, bar_factory, cache_factory);
    } else {
        group = std::make_unique<derecho::Group<Foo, Bar, Cache>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, 12345,
                foo_factory, bar_factory, cache_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    //Keep attempting to get a subgroup pointer to see if the group is "adequately provisioned"
    bool inadequately_provisioned = true;
    while(inadequately_provisioned) {
        try {
            if(node_id < 3) {
                group->get_subgroup<Foo>();
            } else {
                group->get_subgroup<Cache>();
            }
            inadequately_provisioned = false;
        } catch(derecho::subgroup_provisioning_exception& e) {
            inadequately_provisioned = true;
        }
    }

    cout << "All members have joined, subgroups are provisioned" << endl;

    if(node_id == 0) {
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 0...");
        cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
        foo_rpc_handle.ordered_query<Foo::READ_STATE>();
    }
    if(node_id == 1) {
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
        int new_value = 3;
        cout << "Changing Foo's state to " << new_value << endl;
        derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_query<Foo::CHANGE_STATE>(new_value);
        decltype(results)::ReplyMap& replies = results.get();
        cout << "Got a reply map!" << endl;
        for(auto& reply_pair : replies) {
            cout << "Reply from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
        }
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 1...");
    }
    if(node_id == 2) {
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
    }

    if(node_id == 3) {
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
    }
    if(node_id == 4) {
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

    }
    if(node_id == 5) {
        Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>();
        cout << "Putting Ken = Woodberry in the cache" << endl;
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
