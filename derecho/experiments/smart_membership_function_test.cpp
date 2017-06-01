/**
 * @file smart_membership_function_test.cpp
 *
 * @date May 9, 2017
 * @author edward
 */

#include <algorithm>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <tuple>
#include <typeindex>
#include <vector>

#include "derecho/derecho.h"
#include "initialize.h"

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
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};

class LoadBalancer : public mutils::ByteRepresentable {
    std::vector<std::pair<std::string, std::string>> key_ranges_by_shard;

public:
    //I can't think of any RPC methods this class needs, but it can't be a Replicated Object without an RPC method
    void dummy() {}

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<0>(&LoadBalancer::dummy));
    }

    LoadBalancer() : LoadBalancer({{"a", "i"}, {"j", "r"}, {"s", "z"}}) {}
    LoadBalancer(const std::vector<std::pair<std::string, std::string>>& key_ranges_by_shard)
            : key_ranges_by_shard(key_ranges_by_shard) {}

    DEFAULT_SERIALIZATION_SUPPORT(LoadBalancer, key_ranges_by_shard);
};

struct SubgroupAllocationState {
    std::list<derecho::node_id_t> unassigned_members;
    std::list<std::type_index> subgroups_completed;
};

class LoadBalancerAllocator {
    std::shared_ptr<SubgroupAllocationState> current_allocation_state;
    std::unique_ptr<derecho::subgroup_shard_layout_t> previous_assignment;
    const uint total_subgroup_functions;

public:
    LoadBalancerAllocator(const std::shared_ptr<SubgroupAllocationState>& assignment_state, uint total_subgroup_functions)
            : current_allocation_state(assignment_state), total_subgroup_functions(total_subgroup_functions) {}
    //This should never be copied, but it must be copied once to store it inside a std::function (d'oh)
    LoadBalancerAllocator(const LoadBalancerAllocator& copy)
            : current_allocation_state(copy.current_allocation_state),
              /* subgroup_shard_layout_t is copyable, so do the obvious thing and copy it. */
              previous_assignment(copy.previous_assignment ? std::make_unique<derecho::subgroup_shard_layout_t>(*copy.previous_assignment)
                                                           : nullptr),
              total_subgroup_functions(copy.total_subgroup_functions) {}
    LoadBalancerAllocator(LoadBalancerAllocator&&) = default;

    /* Attempts to assign the first 3 members of the group to a single LoadBalancer subgroup,
     * retaining any nodes that were already assigned to this subgroup in the previous view
     */
    derecho::subgroup_shard_layout_t operator()(const derecho::View& curr_view) {
        //If this is the first allocator to be called, initialize allocation state
        if(current_allocation_state->subgroups_completed.empty()) {
            current_allocation_state->unassigned_members.assign(curr_view.members.begin(), curr_view.members.end());
        }
        if(previous_assignment) {
            std::cout << "LoadBalancer had a previous assignment" << std::endl;
            //Modify the previous assignment to replace failed members and return a copy of it
            for(std::size_t subgroup_rank = 0;
                subgroup_rank < (*previous_assignment)[0][0].members.size();
                ++subgroup_rank) {
                if(curr_view.rank_of((*previous_assignment)[0][0].members[subgroup_rank]) == -1) {
                    //rank_of == -1 means the node is not in the current view
                    if(current_allocation_state->unassigned_members.empty()) {
                        throw derecho::subgroup_provisioning_exception();
                    }
                    //Take from the end of unassigned_members, since that's where new nodes go
                    //(I really hope I don't accidentally steal a node that was in Cache's previous assignment...)
                    derecho::node_id_t new_member = current_allocation_state->unassigned_members.back();
                    current_allocation_state->unassigned_members.pop_back();
                    std::cout << "LB: Replacing failed member " << (*previous_assignment)[0][0].members[subgroup_rank] << " with node " << new_member << " in rank " << subgroup_rank << std::endl;
                    (*previous_assignment)[0][0].members[subgroup_rank] = new_member;
                    (*previous_assignment)[0][0].member_ips[subgroup_rank] = curr_view.member_ips[curr_view.rank_of(new_member)];
                } else {
                    std::cout << "LB: Keeping node " << (*previous_assignment)[0][0].members[subgroup_rank] << " in rank " << subgroup_rank << std::endl;
                    //Keep the node and delete it from unassigned_members
                    current_allocation_state->unassigned_members.remove(
                            (*previous_assignment)[0][0].members[subgroup_rank]);
                }
            }
            //These will be initialized from scratch by the calling ViewManager
            (*previous_assignment)[0][0].joined.clear();
            (*previous_assignment)[0][0].departed.clear();
        } else {
            if(current_allocation_state->unassigned_members.size() < 3) {
                throw derecho::subgroup_provisioning_exception();
            }
            previous_assignment = std::make_unique<derecho::subgroup_shard_layout_t>(1);
            //Consume 3 elements from the front of the unassigned_members list (this should really be a 1-liner)
            std::vector<derecho::node_id_t> desired_nodes;
            for(int count = 0; count < 3; ++count) {
                desired_nodes.emplace_back(current_allocation_state->unassigned_members.front());
                current_allocation_state->unassigned_members.pop_front();
            }
            (*previous_assignment)[0].emplace_back(curr_view.make_subview(desired_nodes));
            std::cout << "LoadBalancer created a new assignment: " << desired_nodes << std::endl;
        }
        current_allocation_state->subgroups_completed.emplace_back(std::type_index(typeid(LoadBalancer)));
        //If this is the last function to be called, reset current_allocation_state for next time
        if(current_allocation_state->subgroups_completed.size() == total_subgroup_functions) {
            current_allocation_state->subgroups_completed.clear();
            current_allocation_state->unassigned_members.clear();
        }
        return *previous_assignment;
    }
};

class CacheAllocator {
    std::shared_ptr<SubgroupAllocationState> current_allocation_state;
    std::unique_ptr<derecho::subgroup_shard_layout_t> previous_assignment;
    const uint total_subgroup_functions;

public:
    const unsigned int NUM_SHARDS = 2;
    const unsigned int NODES_PER_SHARD = 2;

    CacheAllocator(const std::shared_ptr<SubgroupAllocationState>& assignment_state, uint total_subgroup_functions)
            : current_allocation_state(assignment_state), total_subgroup_functions(total_subgroup_functions) {}
    CacheAllocator(const CacheAllocator& copy)
            : current_allocation_state(copy.current_allocation_state),
              /* subgroup_shard_layout_t is copyable, so do the obvious thing and copy it. */
              previous_assignment(copy.previous_assignment ? std::make_unique<derecho::subgroup_shard_layout_t>(*copy.previous_assignment)
                                                           : nullptr),
              total_subgroup_functions(copy.total_subgroup_functions) {}
    CacheAllocator(CacheAllocator&&) = default;

    derecho::subgroup_shard_layout_t operator()(const derecho::View& curr_view) {
        //If this is the first allocator to be called, initialize allocation state
        if(current_allocation_state->subgroups_completed.empty()) {
            current_allocation_state->unassigned_members.assign(curr_view.members.begin(), curr_view.members.end());
        }
        if(previous_assignment) {
            std::cout << "Cache had a previous assignment" << std::endl;
            for(std::size_t shard_num = 0; shard_num < NUM_SHARDS; ++shard_num) {
                for(std::size_t r = 0; r < NODES_PER_SHARD; ++r) {
                    if(curr_view.rank_of((*previous_assignment)[0][shard_num].members[r]) == -1) {
                        //This node is not in the current view, so take a new one from the end of unassigned_members
                        if(current_allocation_state->unassigned_members.empty()) {
                            throw derecho::subgroup_provisioning_exception();
                        }
                        derecho::node_id_t new_member = current_allocation_state->unassigned_members.back();
                        current_allocation_state->unassigned_members.pop_back();
                        std::cout << "Cache: Shard " << shard_num << ": replacing failed member " << (*previous_assignment)[0][shard_num].members[r] << " with node " << new_member << " in rank " << r << std::endl;
                        (*previous_assignment)[0][shard_num].members[r] = new_member;
                        (*previous_assignment)[0][shard_num].member_ips[r] = curr_view.member_ips[curr_view.rank_of(new_member)];
                    } else {
                        std::cout << "Cache: Shard " << shard_num << ": keeping node " << (*previous_assignment)[0][shard_num].members[r] << " in rank " << r << std::endl;
                        //Keep the node and delete it from unassigned_members
                        current_allocation_state->unassigned_members.remove(
                                (*previous_assignment)[0][shard_num].members[r]);
                    }
                }
                //These will be initialized from scratch by the calling ViewManager
                (*previous_assignment)[0][shard_num].joined.clear();
                (*previous_assignment)[0][shard_num].departed.clear();
            }
        } else {
            std::cout << "Cache creating a new assignment..." << std::endl;
            auto unassigned_members_iter = current_allocation_state->unassigned_members.begin();
            if(std::find(current_allocation_state->subgroups_completed.begin(),
                         current_allocation_state->subgroups_completed.end(),
                         std::type_index(typeid(LoadBalancer)))
               == current_allocation_state->subgroups_completed.end()) {
                //LoadBalancerAllocator has not been run yet, so leave it the first three nodes
                if(current_allocation_state->unassigned_members.size() - 3 < NUM_SHARDS * NODES_PER_SHARD) {
                    throw derecho::subgroup_provisioning_exception();
                }
                std::advance(unassigned_members_iter, 3);

            } else if(current_allocation_state->unassigned_members.size() < NUM_SHARDS * NODES_PER_SHARD) {
                throw derecho::subgroup_provisioning_exception();
            }

            previous_assignment = std::make_unique<derecho::subgroup_shard_layout_t>(1);
            for(std::size_t shard_num = 0; shard_num < NUM_SHARDS; ++shard_num) {
                std::vector<derecho::node_id_t> desired_nodes;
                for(unsigned int count = 0; count < NODES_PER_SHARD; ++count) {
                    desired_nodes.emplace_back(*unassigned_members_iter);
                    unassigned_members_iter = current_allocation_state->unassigned_members.erase(unassigned_members_iter);
                }
                (*previous_assignment)[0].emplace_back(curr_view.make_subview(desired_nodes));
                std::cout << "Cache: Assigned nodes " << desired_nodes << " to shard " << shard_num << std::endl;
            }
        }
        current_allocation_state->subgroups_completed.emplace_back(std::type_index(typeid(Cache)));
        //If this is the last function to be called, reset current_allocation_state for next time
        if(current_allocation_state->subgroups_completed.size() == total_subgroup_functions) {
            current_allocation_state->subgroups_completed.clear();
            current_allocation_state->unassigned_members.clear();
        }
        return *previous_assignment;
    }
};

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
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::message_callback stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    auto load_balancer_factory = []() { return std::make_unique<LoadBalancer>(); };
    auto cache_factory = []() { return std::make_unique<Cache>(); };

    auto allocation_state = std::make_shared<SubgroupAllocationState>();
    derecho::SubgroupInfo subgroup_info{{{std::type_index(typeid(LoadBalancer)), LoadBalancerAllocator(allocation_state, 2)},
                                         {std::type_index(typeid(Cache)), CacheAllocator(allocation_state, 2)}}};

    std::unique_ptr<derecho::Group<LoadBalancer, Cache>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<LoadBalancer, Cache>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho_gms_port,
                load_balancer_factory, cache_factory);
    } else {
        group = std::make_unique<derecho::Group<LoadBalancer, Cache>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho_gms_port,
                load_balancer_factory, cache_factory);
    }
    cout << "Finished constructing/joining Group" << endl;

    //Keep attempting to get a subgroup pointer to see if the group is "adequately provisioned"
    bool inadequately_provisioned = true;
    while(inadequately_provisioned) {
        try {
            if(node_id < 3) {
                group->get_subgroup<LoadBalancer>();
            } else {
                group->get_subgroup<Cache>();
            }
            inadequately_provisioned = false;
        } catch(derecho::subgroup_provisioning_exception& e) {
            inadequately_provisioned = true;
        }
    }

    cout << "All members have joined, subgroups are provisioned" << endl;

    if(node_id == 1) {
        derecho::ExternalCaller<Cache>& cache_handle = group->get_nonmember_subgroup<Cache>();
        derecho::node_id_t who = 3;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        derecho::rpc::QueryResults<std::string> cache_results = cache_handle.p2p_query<Cache::GET>(who, "6");
        std::string response = cache_results.get().get(who);
        cout << " Response from node " << who << ":" << response << endl;
    }
    if(node_id > 2) {
        derecho::Replicated<Cache>& cache_handle = group->get_subgroup<Cache>();
        std::stringstream string_builder;
        string_builder << "Node " << node_id << "'s things";
        cache_handle.ordered_send<Cache::PUT>(std::to_string(node_id), string_builder.str());
    }

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
