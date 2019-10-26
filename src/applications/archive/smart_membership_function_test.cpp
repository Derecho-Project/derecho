#include <algorithm>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <tuple>
#include <typeindex>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <derecho/conf/conf.hpp>

/*
 * The Eclipse CDT parser crashes if it tries to expand the REGISTER_RPC_FUNCTIONS
 * macro, probably because there are too many layers of variadic argument expansion.
 * This definition makes the RPC macros no-ops when the CDT parser tries to expand
 * them, which allows it to continue syntax-highlighting the rest of the file.
 */
#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

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
    REGISTER_RPC_FUNCTIONS(Cache, put, get, contains, invalidate);

    Cache() : cache_map() {}
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};

class LoadBalancer : public mutils::ByteRepresentable {
    std::vector<std::pair<std::string, std::string>> key_ranges_by_shard;

public:
    //I can't think of any RPC methods this class needs, but it can't be a Replicated Object without an RPC method
    void dummy() {}

    REGISTER_RPC_FUNCTIONS(LoadBalancer, dummy);

    LoadBalancer() : LoadBalancer({{"a", "i"}, {"j", "r"}, {"s", "z"}}) {}
    LoadBalancer(const std::vector<std::pair<std::string, std::string>>& key_ranges_by_shard)
            : key_ranges_by_shard(key_ranges_by_shard) {}

    DEFAULT_SERIALIZATION_SUPPORT(LoadBalancer, key_ranges_by_shard);
};

using std::cout;
using std::endl;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    auto load_balancer_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<LoadBalancer>(); };
    auto cache_factory = [](persistent::PersistentRegistry*, derecho::subgroup_id_t) { return std::make_unique<Cache>(); };

    derecho::SubgroupAllocationPolicy load_balancer_policy = derecho::one_subgroup_policy(derecho::fixed_even_shards(1, 3));
    derecho::SubgroupAllocationPolicy cache_policy = derecho::one_subgroup_policy(derecho::fixed_even_shards(3, 3));
    derecho::SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator({
        {std::type_index(typeid(LoadBalancer)), load_balancer_policy},
        {std::type_index(typeid(Cache)), cache_policy}
    }));

    derecho::Group<LoadBalancer, Cache> group({},
					      subgroup_info, nullptr,
					      std::vector<derecho::view_upcall_t> {},
					      load_balancer_factory, cache_factory);
    cout << "Finished constructing/joining Group" << endl;
    uint32_t node_rank = group.get_my_rank();

    if(node_rank == 1) {
        derecho::ExternalCaller<Cache>& cache_handle = group.get_nonmember_subgroup<Cache>();
        node_id_t who = 3;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        derecho::rpc::QueryResults<std::string> cache_results = cache_handle.p2p_send<RPC_NAME(get)>(who, "6");
        std::string response = cache_results.get().get(who);
        cout << " Response from node " << who << ":" << response << endl;
    }
    if(node_rank > 2) {
        derecho::Replicated<Cache>& cache_handle = group.get_subgroup<Cache>();
        std::stringstream string_builder;
        string_builder << "Node " << node_rank << "'s things";
        cache_handle.ordered_send<RPC_NAME(put)>(std::to_string(node_rank), string_builder.str());
    }

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
