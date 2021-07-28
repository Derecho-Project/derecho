#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>

#include "subgroup_function_tester.hpp"
#include <derecho/core/detail/derecho_internal.hpp>

derecho::IpAndPorts ip_and_ports_generator() {
    static int invocation_count = 0;
    std::stringstream string_generator;
    string_generator << "192.168.1." << invocation_count;
    ++invocation_count;
    return {string_generator.str(), 35465, 35465, 35465, 35465, 35465};
}

//We're really just testing the allocation functions, so each "subgroup" will be a dummy Replicated type
struct TestType1 {};
struct TestType2 {};
struct TestType3 {};
struct TestType4 {};
struct TestType5 {};
struct TestType6 {};

void test_fixed_allocation_functions() {
    using derecho::CrossProductPolicy;
    using derecho::DefaultSubgroupAllocator;
    using derecho::SubgroupAllocationPolicy;
    //Reduce the verbosity of specifying "ordered" for three custom subgroups
    std::vector<derecho::Mode> three_ordered(3, derecho::Mode::ORDERED);
    std::vector<std::string> three_default_profiles(3, "default");

    SubgroupAllocationPolicy sharded_policy = derecho::one_subgroup_policy(derecho::fixed_even_shards(5, 3));
    SubgroupAllocationPolicy unsharded_policy = derecho::one_subgroup_policy(derecho::fixed_even_shards(1, 5));
    SubgroupAllocationPolicy uneven_sharded_policy = derecho::one_subgroup_policy(
            derecho::custom_shards_policy({2, 5, 3}, {2, 5, 3}, three_ordered, three_default_profiles));
    SubgroupAllocationPolicy multiple_copies_policy = derecho::identical_subgroups_policy(
            2, derecho::fixed_even_shards(3, 4));
    SubgroupAllocationPolicy multiple_subgroups_policy{3, false, {derecho::fixed_even_shards(3, 3), derecho::custom_shards_policy({4, 3, 4}, {4, 3, 4}, three_ordered, three_default_profiles), derecho::fixed_even_shards(2, 2)}};

    //This will create subgroups that are the cross product of the "uneven_sharded_policy" and "sharded_policy" groups
    CrossProductPolicy uneven_to_even_cp{
            {std::type_index(typeid(TestType3)), 0},
            {std::type_index(typeid(TestType1)), 0}};

    derecho::SubgroupInfo test_fixed_subgroups(
            DefaultSubgroupAllocator({{std::type_index(typeid(TestType1)), sharded_policy},
                                      {std::type_index(typeid(TestType2)), unsharded_policy},
                                      {std::type_index(typeid(TestType3)), uneven_sharded_policy},
                                      {std::type_index(typeid(TestType4)), multiple_copies_policy},
                                      {std::type_index(typeid(TestType5)), multiple_subgroups_policy},
                                      {std::type_index(typeid(TestType6)), uneven_to_even_cp}}));

    std::vector<std::type_index> subgroup_type_order = {std::type_index(typeid(TestType1)),
                                                        std::type_index(typeid(TestType2)),
                                                        std::type_index(typeid(TestType3)),
                                                        std::type_index(typeid(TestType4)),
                                                        std::type_index(typeid(TestType5)),
                                                        std::type_index(typeid(TestType6))};
    std::vector<node_id_t> members(100);
    std::iota(members.begin(), members.end(), 0);
    std::vector<derecho::IpAndPorts> member_ips_and_ports(members.size());
    std::generate(member_ips_and_ports.begin(), member_ips_and_ports.end(), ip_and_ports_generator);
    std::vector<char> none_failed(members.size(), 0);
    auto curr_view = std::make_unique<derecho::View>(0, members, member_ips_and_ports, none_failed,
                                                     std::vector<node_id_t>{}, std::vector<node_id_t>{},
                                                     0, 0, subgroup_type_order);

    rls_default_info("TEST 1: Initial allocation");
    derecho::test_provision_subgroups(test_fixed_subgroups, nullptr, *curr_view);

    std::set<int> ranks_to_fail{1, 3, 17, 38, 40};
    rls_default_info("TEST 2: Failing some nodes that are in subgroups: {}", ranks_to_fail);
    std::unique_ptr<derecho::View> prev_view(std::move(curr_view));
    curr_view = derecho::make_next_view(*prev_view, ranks_to_fail, {}, {});

    derecho::test_provision_subgroups(test_fixed_subgroups, prev_view, *curr_view);

    std::set<int> more_ranks_to_fail{13, 20, 59, 78, 89};
    rls_default_info("TEST 3: Failing nodes both before and after the pointer. Ranks are {}", more_ranks_to_fail);
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, more_ranks_to_fail, {}, {});

    derecho::test_provision_subgroups(test_fixed_subgroups, prev_view, *curr_view);

    //There are now 90 members left, so fail ranks 39-89
    std::vector<int> range_39_to_89(50);
    std::iota(range_39_to_89.begin(), range_39_to_89.end(), 39);
    std::set<int> lots_of_members_to_fail(range_39_to_89.begin(), range_39_to_89.end());
    rls_default_info("TEST 4: Failing 50 nodes so the next view is inadequate");
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, lots_of_members_to_fail, {}, {});

    derecho::test_provision_subgroups(test_fixed_subgroups, prev_view, *curr_view);

    std::vector<node_id_t> new_members(40);
    std::iota(new_members.begin(), new_members.end(), 100);
    std::vector<derecho::IpAndPorts> new_member_ips_and_ports(new_members.size());
    std::generate(new_member_ips_and_ports.begin(), new_member_ips_and_ports.end(), ip_and_ports_generator);
    rls_default_info("TEST 5: Adding new members 100-140");
    //Since an inadequate view will never be installed, keep the same prev_view from before the failures
    curr_view = derecho::make_next_view(*prev_view, lots_of_members_to_fail, new_members, new_member_ips_and_ports);

    derecho::test_provision_subgroups(test_fixed_subgroups, prev_view, *curr_view);
}

void test_flexible_allocation_functions() {
    using derecho::DefaultSubgroupAllocator;
    using derecho::SubgroupAllocationPolicy;

    //Reduce the verbosity of specifying "ordered" for three custom subgroups
    std::vector<derecho::Mode> three_ordered(3, derecho::Mode::ORDERED);
    std::vector<std::string> three_default_profiles(3, "default");

    SubgroupAllocationPolicy flexible_shards_policy = derecho::one_subgroup_policy(
            derecho::flexible_even_shards(5, 2, 3));
    SubgroupAllocationPolicy uneven_flexible_shards = derecho::one_subgroup_policy(
            derecho::custom_shards_policy({2, 5, 3}, {3, 6, 5}, three_ordered, three_default_profiles));
    SubgroupAllocationPolicy multiple_copies_flexible = derecho::identical_subgroups_policy(
            2, derecho::flexible_even_shards(3, 4, 5));
    SubgroupAllocationPolicy multiple_fault_tolerant_subgroups{3, false, {derecho::flexible_even_shards(3, 2, 4), derecho::custom_shards_policy({4, 3, 4}, {5, 4, 5}, three_ordered, three_default_profiles), derecho::flexible_even_shards(2, 2, 4)}};

    derecho::SubgroupInfo test_flexible_subgroups(
            DefaultSubgroupAllocator({{std::type_index(typeid(TestType1)), flexible_shards_policy},
                                      {std::type_index(typeid(TestType2)), uneven_flexible_shards},
                                      {std::type_index(typeid(TestType3)), multiple_copies_flexible},
                                      {std::type_index(typeid(TestType4)), multiple_fault_tolerant_subgroups}}));

    std::vector<std::type_index> subgroup_type_order = {std::type_index(typeid(TestType1)),
                                                        std::type_index(typeid(TestType2)),
                                                        std::type_index(typeid(TestType3)),
                                                        std::type_index(typeid(TestType4))};
    std::vector<node_id_t> members(100);
    std::iota(members.begin(), members.end(), 0);
    std::vector<derecho::IpAndPorts> member_ips_and_ports(members.size());
    std::generate(member_ips_and_ports.begin(), member_ips_and_ports.end(), ip_and_ports_generator);
    std::vector<char> none_failed(members.size(), 0);
    auto curr_view = std::make_unique<derecho::View>(0, members, member_ips_and_ports, none_failed,
                                                     std::vector<node_id_t>{}, std::vector<node_id_t>{},
                                                     0, 0, subgroup_type_order);
    rls_default_info("Now testing flexible subgroup allocation");
    rls_default_info("TEST 6: Initial allocation");
    derecho::test_provision_subgroups(test_flexible_subgroups, nullptr, *curr_view);

    std::set<int> flexible_ranks_to_fail{3, 6, 31, 45, 57};
    rls_default_info("TEST 7: Failing some nodes that are in subgroups: {}", flexible_ranks_to_fail);
    std::unique_ptr<derecho::View> prev_view(std::move(curr_view));
    curr_view = derecho::make_next_view(*prev_view, flexible_ranks_to_fail, {}, {});
    derecho::test_provision_subgroups(test_flexible_subgroups, prev_view, *curr_view);

    std::set<int> flexible_ranks_to_fail_2{7, 8, 17, 18, 40, 41, 51, 61, 62};
    rls_default_info("TEST 8: Failing more nodes so that shards must shrink. Ranks are: {}", flexible_ranks_to_fail_2);
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, flexible_ranks_to_fail_2, {}, {});
    derecho::test_provision_subgroups(test_flexible_subgroups, prev_view, *curr_view);

    std::vector<node_id_t> new_members(40);
    std::iota(new_members.begin(), new_members.end(), 100);
    std::vector<derecho::IpAndPorts> new_member_ips_and_ports(new_members.size());
    std::generate(new_member_ips_and_ports.begin(), new_member_ips_and_ports.end(), ip_and_ports_generator);
    rls_default_info("TEST 9: Adding new members 100-140 so shards can re-expand.");
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, {}, new_members, new_member_ips_and_ports);
    derecho::test_provision_subgroups(test_flexible_subgroups, prev_view, *curr_view);
}

void test_json_layout() {
    const char* json_layout_string =
            R"|([
    {
        "type_alias":   "TestType1",
        "layout":       [
                            {
                                "min_nodes_by_shard": ["2"],
                                "max_nodes_by_shard": ["3"],
                                "reserved_node_ids_by_shard": [["*1", "2", "3"]],
                                "delivery_modes_by_shard": ["Ordered"],
                                "profiles_by_shard": ["Default"]
                            }
                        ]
    },
    {
        "type_alias":   "TestType2",
        "layout":       [
                            {
                                "min_nodes_by_shard": ["2"],
                                "max_nodes_by_shard": ["3"],
                                "reserved_node_ids_by_shard": [["2", "3", "4"]],
                                "delivery_modes_by_shard": ["Ordered"],
                                "profiles_by_shard": ["Default"]
                            }
                        ]
    }
])|";

    derecho::SubgroupInfo test_json_overlapping(
            derecho::make_subgroup_allocator<TestType1, TestType2>(nlohmann::json::parse(json_layout_string)));

    std::vector<std::type_index> subgroup_type_order = {std::type_index(typeid(TestType1)),
                                                        std::type_index(typeid(TestType2))};

    std::vector<node_id_t> members(3);
    std::iota(members.begin(), members.end(), 0);
    std::vector<derecho::IpAndPorts> ips_and_ports(3);
    std::generate(ips_and_ports.begin(), ips_and_ports.end(), ip_and_ports_generator);
    std::vector<char> none_failed(3, 0);
    auto curr_view = std::make_unique<derecho::View>(0, members, ips_and_ports, none_failed,
                                                     std::vector<node_id_t>{}, std::vector<node_id_t>{},
                                                     0, 0, subgroup_type_order);

    rls_default_info("Now testing JSON-based allocation with overlapping reserved nodes");
    rls_default_info("TEST 10: Initial allocation");
    derecho::test_provision_subgroups(test_json_overlapping, nullptr, *curr_view);

    std::vector<node_id_t> new_members_in_reservation{3, 4};
    std::vector<derecho::IpAndPorts> new_member_ips_and_ports(new_members_in_reservation.size());
    std::generate(new_member_ips_and_ports.begin(), new_member_ips_and_ports.end(), ip_and_ports_generator);
    rls_default_info("TEST 11: Nodes 3 and 4 join, which are in a reserved node list");
    std::unique_ptr<derecho::View> prev_view(std::move(curr_view));
    curr_view = derecho::make_next_view(*prev_view, {}, new_members_in_reservation, new_member_ips_and_ports);
    derecho::test_provision_subgroups(test_json_overlapping, prev_view, *curr_view);

    std::set<int> ranks_to_fail{0, 2};
    rls_default_info("TEST 12: Nodes 0 and 2 fail; 2 is in both reserved node lists");
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, ranks_to_fail, {}, {});
    dbg_default_debug("New view has members: {}", curr_view->members);
    derecho::test_provision_subgroups(test_json_overlapping, prev_view, *curr_view);

    std::vector<node_id_t> new_members_outside_reservation{5, 6};
    std::vector<derecho::IpAndPorts> ips_and_ports_2(new_members_outside_reservation.size());
    std::generate(ips_and_ports_2.begin(), ips_and_ports_2.end(), ip_and_ports_generator);
    rls_default_info("TEST 13: Nodes 5 and 6 join");
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, {}, new_members_outside_reservation, ips_and_ports_2);
    dbg_default_debug("New view has members: {}", curr_view->members);
    derecho::test_provision_subgroups(test_json_overlapping, prev_view, *curr_view);

    std::set<int> ranks_to_fail_2{1, 3};
    std::vector<node_id_t> node_rejoined{2};
    std::vector<derecho::IpAndPorts> node_2_ip{ips_and_ports[2]};
    rls_default_info("TEST 14: Nodes 3 and 5 fail, node 2 rejoins");
    prev_view.swap(curr_view);
    curr_view = derecho::make_next_view(*prev_view, ranks_to_fail_2, node_rejoined, node_2_ip);
    dbg_default_debug("New view has members: {}", curr_view->members);
    derecho::test_provision_subgroups(test_json_overlapping, prev_view, *curr_view);
}

int main(int argc, char* argv[]) {

    test_fixed_allocation_functions();
    test_flexible_allocation_functions();
    test_json_layout();

    return 0;
}

namespace derecho {

void print_subgroup_layout(const subgroup_shard_layout_t& layout) {
    std::stringstream string_builder;
    for(std::size_t subgroup_num = 0; subgroup_num < layout.size(); ++subgroup_num) {
        string_builder << "Subgroup " << subgroup_num << ": ";
        for(std::size_t shard_num = 0; shard_num < layout[subgroup_num].size(); ++shard_num) {
            string_builder << layout[subgroup_num][shard_num].members
                           << "|"
                           << layout[subgroup_num][shard_num].is_sender
                           << ", ";
        }
        string_builder << "\b\b";
        rls_default_info(string_builder.str());
    }
}

void print_allocations(const std::map<std::type_index, subgroup_shard_layout_t>& subgroup_allocations) {
    for(const auto& subgroup_type_allocation : subgroup_allocations) {
        rls_default_info("Subgroup type {} got assignment: ", subgroup_type_allocation.first.name());
        print_subgroup_layout(subgroup_type_allocation.second);
    }
}

void test_provision_subgroups(const SubgroupInfo& subgroup_info,
                              const std::unique_ptr<View>& prev_view,
                              View& curr_view) {
    int32_t initial_next_unassigned_rank = curr_view.next_unassigned_rank;
    curr_view.subgroup_shard_views.clear();
    curr_view.subgroup_ids_by_type_id.clear();
    rls_default_info("View has there members: {}", curr_view.members);
    std::map<std::type_index, subgroup_shard_layout_t> subgroup_allocations;
    try {
        auto temp = subgroup_info.subgroup_membership_function(curr_view.subgroup_type_order,
                                                               prev_view, curr_view);
        //Hack to ensure RVO works even though subgroup_allocations had to be declared outside this scope
        subgroup_allocations = std::move(temp);
    } catch(subgroup_provisioning_exception& ex) {
        // Mark the view as inadequate and roll back everything done by allocation functions
        curr_view.is_adequately_provisioned = false;
        curr_view.next_unassigned_rank = initial_next_unassigned_rank;
        curr_view.subgroup_shard_views.clear();
        curr_view.subgroup_ids_by_type_id.clear();
        rls_default_info ("Got a subgroup_provisioning_exception, marking View inadequate");
        return;
    }
    print_allocations(subgroup_allocations);
    //Go through subgroup_allocations and initialize curr_view
    for(subgroup_type_id_t subgroup_type_id = 0;
        subgroup_type_id < curr_view.subgroup_type_order.size();
        ++subgroup_type_id) {
        const std::type_index& subgroup_type = curr_view.subgroup_type_order[subgroup_type_id];
        subgroup_shard_layout_t& curr_type_subviews = subgroup_allocations[subgroup_type];
        std::size_t num_subgroups = curr_type_subviews.size();
        curr_view.subgroup_ids_by_type_id.emplace(subgroup_type_id, std::vector<subgroup_id_t>(num_subgroups));
        for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
            // Assign this (type, index) pair a new unique subgroup ID
            subgroup_id_t curr_subgroup_id = curr_view.subgroup_shard_views.size();
            curr_view.subgroup_ids_by_type_id[subgroup_type_id][subgroup_index] = curr_subgroup_id;
            uint32_t num_shards = curr_type_subviews[subgroup_index].size();
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                SubView& shard_view = curr_type_subviews[subgroup_index][shard_num];
                shard_view.my_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
                if(shard_view.my_rank != -1) {
                    // Initialize my_subgroups
                    curr_view.my_subgroups[curr_subgroup_id] = shard_num;
                }
                if(prev_view) {
                    // Initialize this shard's SubView.joined and SubView.departed
                    subgroup_id_t prev_subgroup_id = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)
                                                             .at(subgroup_index);
                    SubView& prev_shard_view = prev_view->subgroup_shard_views[prev_subgroup_id][shard_num];
                    shard_view.init_joined_departed(prev_shard_view);
                }
            }  // for(shard_num)
            /* Pull this shard->SubView mapping out of the subgroup allocation
             * and save it under its subgroup ID (which was subgroup_shard_views.size()).
             * This deletes it from the subgroup_shard_layout_t's outer vector. */
            curr_view.subgroup_shard_views.emplace_back(std::move(
                    subgroup_allocations[subgroup_type][subgroup_index]));
        }  //for(subgroup_index)
    }
}

std::unique_ptr<View> make_next_view(const View& curr_view,
                                     const std::set<int>& leave_ranks,
                                     const std::vector<node_id_t>& joiner_ids,
                                     const std::vector<IpAndPorts>& joiner_ips_and_ports) {
    int next_num_members = curr_view.num_members - leave_ranks.size() + joiner_ids.size();
    std::vector<node_id_t> joined, members(next_num_members), departed;
    std::vector<char> failed(next_num_members);
    std::vector<IpAndPorts> member_ips_and_ports(next_num_members);
    int next_unassigned_rank = curr_view.next_unassigned_rank;
    for(std::size_t i = 0; i < joiner_ids.size(); ++i) {
        joined.emplace_back(joiner_ids[i]);
        //New members go at the end of the members list, but it may shrink in the new view
        int new_member_rank = curr_view.num_members - leave_ranks.size() + i;
        members[new_member_rank] = joiner_ids[i];
        member_ips_and_ports[new_member_rank] = joiner_ips_and_ports[i];
    }
    for(const auto& leaver_rank : leave_ranks) {
        departed.emplace_back(curr_view.members[leaver_rank]);
        //Decrement next_unassigned_rank for every failure, unless the failure wasn't in a subgroup anyway
        if(leaver_rank <= curr_view.next_unassigned_rank) {
            next_unassigned_rank--;
        }
    }
    //Copy member information, excluding the members that have failed
    int new_rank = 0;
    for(int old_rank = 0; old_rank < curr_view.num_members; old_rank++) {
        //This is why leave_ranks needs to be a set
        if(leave_ranks.find(old_rank) == leave_ranks.end()) {
            members[new_rank] = curr_view.members[old_rank];
            member_ips_and_ports[new_rank] = curr_view.member_ips_and_ports[old_rank];
            failed[new_rank] = curr_view.failed[old_rank];
            ++new_rank;
        }
    }

    //Initialize my_rank in next_view
    int32_t my_new_rank = -1;
    node_id_t myID = curr_view.members[curr_view.my_rank];
    for(int i = 0; i < next_num_members; ++i) {
        if(members[i] == myID) {
            my_new_rank = i;
            break;
        }
    }
    return std::make_unique<View>(curr_view.vid + 1, members, member_ips_and_ports, failed,
                                  joined, departed, my_new_rank, next_unassigned_rank,
                                  curr_view.subgroup_type_order);
}

} /* namespace derecho */
