/**
 * @file typed_subgroup_test.cpp
 *
 * @date Mar 1, 2017
 * @author edward
 */

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <map>

#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>

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

    enum Functions { READ_STATE,
                     CHANGE_STATE };

    static auto register_functions(derecho::rpc::RPCManager& m, std::unique_ptr<Foo>* ptr) {
        assert(ptr);
        return m.setup_rpc_class(ptr, derecho::rpc::wrap<READ_STATE>(&Foo::read_state),
                                 derecho::rpc::wrap<CHANGE_STATE>(&Foo::change_state));
    }

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

    static auto register_functions(derecho::rpc::RPCManager& m, std::unique_ptr<Bar>* ptr) {
        assert(ptr);
        return m.setup_rpc_class(ptr, derecho::rpc::wrap<APPEND>(&Bar::append),
                                 derecho::rpc::wrap<CLEAR>(&Bar::clear),
                                 derecho::rpc::wrap<PRINT>(&Bar::print));
    }

    DEFAULT_SERIALIZATION_SUPPORT(Bar, log);
    //Constructor for deserialization only
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

    static auto register_functions(derecho::rpc::RPCManager& m, std::unique_ptr<Cache>* ptr) {
        assert(ptr);
        return m.setup_rpc_class(ptr, derecho::rpc::wrap<PUT>(&Cache::put),
                                 derecho::rpc::wrap<GET>(&Cache::get),
                                 derecho::rpc::wrap<CONTAINS>(&Cache::contains),
                                 derecho::rpc::wrap<INVALIDATE>(&Cache::invalidate));
    }

    Cache() : cache_map() {}
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};

using std::cout;
using std::endl;
using derecho::Replicated;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::message_callback stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    derecho::SubgroupInfo subgroup_info{{
        {std::type_index(typeid(Foo)), [](const derecho::View& curr_view) {
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
            std::cout << "Cache function found enough members! curr_view = " << curr_view.debug_string() << std::endl;
            derecho::subgroup_shard_layout_t subgroup_vector(1);
            subgroup_vector[0].emplace_back(curr_view.make_subview({3, 4, 5}));
            return subgroup_vector;
        }}
    }};

    auto foo_factory = []() {return std::make_unique<Foo>(-1); };
    auto bar_factory = []() {return std::make_unique<Bar>(); };
    auto cache_factory = []() {return std::make_unique<Cache>(); };

    std::unique_ptr<derecho::Group<Foo, Bar, Cache>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<Foo, Bar, Cache>>(
            my_ip, callback_set, subgroup_info, derecho_params,
            std::vector<derecho::view_upcall_t>{}, 12345,
            foo_factory, bar_factory, cache_factory);
    } else {
        group = std::make_unique<derecho::Group<Foo, Bar, Cache>>(
            node_id, my_ip, leader_ip, callback_set, subgroup_info,
            std::vector<derecho::view_upcall_t>{}, 12345,
            foo_factory, bar_factory, cache_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    bool inadequately_provisioned = true;
    while(inadequately_provisioned) {
        try {
            //Any get_subgroup should fail if the View has inadequately_provisioned set
            group->get_subgroup<Foo>();
            inadequately_provisioned = false;
        } catch(derecho::subgroup_provisioning_exception& e) {
            inadequately_provisioned = true;
        }
    }

    cout << "All members have joined, subgroups are provisioned" << endl;

    Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
    Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>();
    Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>();

    if(node_id == 0) {
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<Bar::APPEND>("Write from 0...");
        cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
        foo_rpc_handle.ordered_query<Foo::READ_STATE>();
    }
    if(node_id == 1) {
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
        derecho::rpc::QueryResults<std::string> results =
            cache_rpc_handle.ordered_query<Cache::GET>("Ken");
        for(auto& reply_pair : results.get()) {
            cout << "Node " << reply_pair.first << " had Ken = " << reply_pair.second.get() << endl;
        }
    }
    if(node_id == 4) {
        cout << "Putting Ken = Birman in the cache" << endl;
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Birman");
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Birman");
    }
    if(node_id == 5) {
        cout << "Putting Ken = Woodberry in the cache" << endl;
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
        cache_rpc_handle.ordered_send<Cache::PUT>("Ken", "Woodberry");
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
