/**
 * @file subgroup_functions.hpp
 */

#pragma once

#include <derecho/config.h>
#include <derecho/utils/logger.hpp>
#include "derecho_exception.hpp"
#include "derecho_modes.hpp"
#include "detail/derecho_internal.hpp"
#include "subgroup_info.hpp"

#include <nlohmann/json.hpp>

#include <exception>
#include <memory>
#include <set>
#include <variant>

using json = nlohmann::json;

namespace derecho {

/**
 * A copy constructor for objects owned by unique_ptr. Does the obvious thing
 * and invokes the copy constructor of the object being pointed to, or returns
 * nullptr if the unique_ptr is empty.
 * @param to_copy A unique_ptr to the object to copy
 * @return A new object in a new unique_ptr that is a copy of the old object.
 */
template <typename T>
std::unique_ptr<T> deep_pointer_copy(const std::unique_ptr<T>& to_copy) {
    if(to_copy) {
        return std::make_unique<T>(*to_copy);
    } else {
        return nullptr;
    }
}

/*
 * String constants for the names of configuration profile fields that
 * the default subgroup allocator will look up
 */
constexpr char num_shards_profile_field[] = "num_shards";
constexpr char min_nodes_profile_field[] = "min_nodes";
constexpr char max_nodes_profile_field[] = "max_nodes";
/* It would be really nice if we could group these together in an enumerated class
 * called ProfileFields or something, but there's no way to do that with strings. */

/*
 * String constants for the names of JSON object fields that the JSON-based
 * default subgroup allocator will look up
 */
constexpr char json_layout_field[] = "layout";
constexpr char json_type_alias_field[] = "type_alias";
constexpr char min_nodes_by_shard_field[] = "min_nodes_by_shard";
constexpr char max_nodes_by_shard_field[] = "max_nodes_by_shard";
constexpr char reserved_node_ids_by_shard_field[] = "reserved_node_ids_by_shard";
constexpr char reserved_node_is_sender_tag = '*';
constexpr char delivery_modes_by_shard_field[] = "delivery_modes_by_shard";
constexpr char delivery_mode_ordered[] = "Ordered";
constexpr char delivery_mode_raw[] = "Raw";
constexpr char profiles_by_shard_field[] = "profiles_by_shard";

/**
 * A simple implementation of shard_view_generator_t that creates a single,
 * un-sharded subgroup containing all the members of curr_view for every subgroup
 * type in the list. This is best used when there is only one subgroup type.
 */
subgroup_allocation_map_t one_subgroup_entire_view(const std::vector<std::type_index>& subgroup_type_order,
                                                   const std::unique_ptr<View>& prev_view, View& curr_view);
/**
 * A simple implementation of shard_view_generator_t that returns a single,
 * un-sharded subgroup in Unordered (Raw) mode containing all the members of
 * curr_view for every type in the list. This is best used when there is only
 * one subgroup type.
 */
subgroup_allocation_map_t one_subgroup_entire_view_raw(const std::vector<std::type_index>& subgroup_type_order,
                                                       const std::unique_ptr<View>& prev_view, View& curr_view);

/**
 * A data structure defining the parameters of the default subgroup allocation
 * function for a single subgroup; it specifies how one subgroup is divided
 * into shards.
 */
struct ShardAllocationPolicy {
    /** The number of shards; set to 1 for a non-sharded subgroup */
    int num_shards;
    /** Whether all shards should contain the same number of members. */
    bool even_shards;
    /** If even_shards is true, this is the minimum number of nodes per shard.
     * (Ignored if even_shards is false). */
    int min_nodes_per_shard;
    /** If even_shards is true, this is the maximum number of nodes per shard. */
    int max_nodes_per_shard;
    /** If even_shards is true, this is the delivery mode that will be used for
     * every shard. (Ignored if even_shards is false). */
    Mode shards_mode;
    /** If even_shards is true, this is the profile that will be used for every
     * shard. (Ignore if even_shards is false). */
    std::string shards_profile;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the minimum number of members it should have.
     * (Ignored if even_shards is true). */
    std::vector<int> min_num_nodes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating the maximum number of members it should have. */
    std::vector<int> max_num_nodes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating which delivery mode it should use. (Ignored if even_shards is
     * true). */
    std::vector<Mode> modes_by_shard;
    /** If even_shards is false, this will contain an entry for each shard
     * indicating which profile it should use. (Ignored if even_shards is
     * true). */
    std::vector<std::string> profiles_by_shard;
    /**
     * Only used when even_shards is false.
     * For each shard, this stores a list of node IDs reserved for it. When a
     * new node is added to the View with an ID on the list, it will always be
     * added to its dedicated shard. A node ID can be reserved by more than one
     * shard, as long as they are in different subgroups; this will make the
     * subgroups overlap (colocate).
     */
    std::vector<std::set<node_id_t>> reserved_node_ids_by_shard;
    /**
     * Only used with 'reserved_node_ids_by_shard'.
     * For each shard, this stores a list of node IDs that will perform send.
     * This list must be a subset of the reserved nodes specified in
     * 'reserved_node_ids_by_shard'. If 'reserved_sender_ids_by_shard' is empty,
     * Then, all nodes in the shard, no matter if they are specified in the
     * reserved pool or not, can send. Otherwise, only the nodes specified in
     * the reserved pool can send. If none of the live nodes is from the
     * reserved pool, this shard will have no senders.
     */
    std::vector<std::set<node_id_t>> reserved_sender_ids_by_shard;
};

/**
 * A data structure defining the parameters of the default subgroup allocation
 * function for a single subgroup type.
 */
struct SubgroupAllocationPolicy {
    /** The number of subgroups of the same Replicated type to create */
    int num_subgroups;
    /** Whether all subgroups of this type will have an identical shard layout */
    bool identical_subgroups;
    /** If identical_subgroups is true, contains a single entry with the allocation
     * policy for all subgroups of this type. If identical_subgroups is false,
     * contains an entry for each subgroup describing that subgroup's shards. */
    std::vector<ShardAllocationPolicy> shard_policy_by_subgroup;
    /** The senders contains the node ids of senders for each of the subgroup.
     * - if senders is empty, all nodes are senders;
     * - if identical_subgroups is true, senders contains a single entry for all subgroups;
     * - otherwise, the senders specifies the sender lists for each of subgroup of this type.
     */
    std::vector<std::set<uint32_t>> senders;
};

/**
 * An alternate type of subgroup allocation policy for subgroup types whose
 * membership will be defined as a cross-product of other subgroups. Each node
 * in the source subgroup will be placed in T subgroups, one for each shard in
 * the target subgroup (the target subgroup has T shards). Thus, if there are S
 * members in the source subgroup, and T shards in the target subgroup, S * T
 * subgroups of a single type will be created. The nodes in the source subgroup
 * will be marked as the only senders in these subgroups. A node that has rank
 * i within the source subgroup can send a multicast to shard j of the target
 * subgroup by selecting the cross-product subgroup at index (i * T + j).
 */
struct CrossProductPolicy {
    /** The (type, index) pair identifying the "source" subgroup of the cross-product.
     * Each member of this subgroup will be a sender in T subgroups, where T is the
     * number of shards in the target subgroup. */
    std::pair<std::type_index, uint32_t> source_subgroup;
    /** The (type, index) pair identifying the "target" subgroup of the cross-product.
     * Each shard in this subgroup will have all of its members assigned to S subgroups
     * as receivers, where S is the number of members in the source subgroup. */
    std::pair<std::type_index, uint32_t> target_subgroup;
};

/**
 * A type alias for a std::variant containing one of the possible subgroup policies.
 */
using SubgroupPolicyVariant = std::variant<SubgroupAllocationPolicy, CrossProductPolicy>;

/* Helper functions that construct ShardAllocationPolicy values for common cases. */

/**
 * Returns a ShardAllocationPolicy that specifies num_shards "flexible" or
 * fault-tolerant shards, each of which has the same minimum number of nodes
 * and maximum number of nodes
 * @param num_shards The number of shards to request in this policy
 * @param min_nodes_per_shard The minimum number of nodes that each shard can have
 * @param max_nodes_per_shard The maximum number of nodes that each shard can have
 * @param profile An optional configuration profile string to associate with every
 * shard in this subgroup
 * @return A ShardAllocationPolicy value with these parameters
 */
ShardAllocationPolicy flexible_even_shards(int num_shards, int min_nodes_per_shard,
                                           int max_nodes_per_shard,
                                           const std::string& profile = "DEFAULT");

/**
 * Returns a ShardAllocationPolicy that specifies a set number of "flexible"
 * shards, each of which has the same minimum and maximum number of nodes,
 * using the values of the constants num_shards, min_nodes, and max_nodes in
 * the section of the configuration file identified by the profile string.
 * @param profile A profile string identifying the configuration of this
 * subgroup
 * @return A ShardAllocationPolicy value constructed based on this
 * configuration profile
 */
ShardAllocationPolicy flexible_even_shards(const std::string& profile);

/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard; each shard must have
 * exactly nodes_per_shard members.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @param profile An optional configuration profile string to associate with
 * every shard in this subgroup
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy fixed_even_shards(int num_shards, int nodes_per_shard,
                                        const std::string& profile = "DEFAULT");

/**
 * Returns a ShardAllocationPolicy that specifies num_shards shards with
 * the same fixed number of nodes in each shard, and every shard running in
 * "raw" delivery mode.
 * @param num_shards The number of shards to request in this policy.
 * @param nodes_per_shard The number of nodes per shard to request.
 * @param profile An optional configuration profile string to associate with
 * every shard in this subgroup
 * @return A ShardAllocationPolicy value with these parameters.
 */
ShardAllocationPolicy raw_fixed_even_shards(int num_shards, int nodes_per_shard,
                                            const std::string& profile = "DEFAULT");
/**
 * Returns a ShardAllocationPolicy for a subgroup that has a different number of
 * members in each shard, and possibly has each shard in a different delivery mode.
 * Note that the parameter vectors must all be the same length.
 * @param min_nodes_by_shard A vector specifying the minimum number of nodes for each
 * shard; the ith shard must have at least min_nodes_by_shard[i] members.
 * @param max_nodes_by_shard A vector specifying the maximum number of nodes for each
 * shard; the ith shard can have up to max_nodes_by_shard[i] members.
 * @param delivery_modes_by_shard A vector specifying the delivery mode (Raw or
 * Ordered) for each shard, in the same order as the other vectors.
 * @param profiles_by_shard A vector specifying the profile (defined in configuration
 * file) for each shard, in the same order as the other vectors.
 * @return A ShardAllocationPolicy that specifies these shard sizes and modes.
 */
ShardAllocationPolicy custom_shards_policy(const std::vector<int>& min_nodes_by_shard,
                                           const std::vector<int>& max_nodes_by_shard,
                                           const std::vector<Mode>& delivery_modes_by_shard,
                                           const std::vector<std::string>& profiles_by_shard);

/**
 * Returns a ShardAllocationPolicy for a subgroup that has a different number of
 * members in each shard, and possibly has each shard in a different delivery mode.
 * In this function, the minimum and maximum number of members in each shard is
 * specified by the min_nodes and max_nodes constants in each shard's configuration
 * profile.
 * @param delivery_modes_by_shard A vector specifying the delivery mode (Raw or
 * Ordered) for each shard
 * @param profiles_by_shard A vector specifying the configuration profile to use
 * for each shard
 * @return A ShardAllocationPolicy that specifies the shard sizes and modes found
 * in the configuration profiles
 */
ShardAllocationPolicy custom_shard_policy(const std::vector<Mode>& delivery_modes_by_shard,
                                          const std::vector<std::string>& profiles_by_shard);

/**
 * Returns a SubgroupAllocationPolicy for a replicated type that only has a
 * single subgroup. The ShardAllocationPolicy argument can be the result of
 * one of the ShardAllocationPolicy helper functions.
 * @param policy The allocation policy to use for the single subgroup.
 * @return A SubgroupAllocationPolicy for a single-subgroup type.
 */
SubgroupAllocationPolicy one_subgroup_policy(const ShardAllocationPolicy& policy);

/**
 * Returns a SubgroupAllocationPolicy for a replicated type that needs n
 * subgroups with identical sharding policies.
 * @param num_subgroups The number of subgroups to create.
 * @param subgroup_policy The policy to use for sharding each subgroup.
 * @return A SubgroupAllocationPolicy for a replicated type with num_subgroups
 * copies of the same subgroup.
 */
SubgroupAllocationPolicy identical_subgroups_policy(int num_subgroups, const ShardAllocationPolicy& subgroup_policy);

/**
 * Functor of type shard_view_generator_t that implements the default subgroup
 * allocation algorithm, parameterized based on a policy for each subgroup type
 * (i.e. Replicated Object type). Its operator() will throw a
 * subgroup_provisioning_exception if there are not enough nodes in the current
 * view to populate all of the subgroups and shards.
 */
class DefaultSubgroupAllocator {
protected:
    /**
     * The entry for each type of subgroup is either a SubgroupAllocationPolicy
     * if that type should use the standard "partitioning" allocator, or a
     * CrossProductPolicy if that type should use the "cross-product" allocator
     * instead.
     */
    std::map<std::type_index, SubgroupPolicyVariant> policies;

    /**
     * The union set of reserved_node_ids from all shards.
     */
    std::set<node_id_t> all_reserved_node_ids;

    /**
     * Determines how many members each shard can have in the current view, based
     * on each shard's policy (minimum and maximum number of nodes) and the size
     * of the current view. This function first assigns the minimum number of
     * nodes to each shard, then evenly increments every shard's size by 1 (in
     * order of subgroup_type_order and in order of shard number within each
     * subgroup) until either all shards are at their maximum size or all of the
     * View's members are accounted for. It throws a subgroup_provisioning_exception
     * if the View doesn't have enough members for even the minimum-size
     * allocation.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The previous View, if there is one. This is used to
     * ensure that every shard can keep the non-failed nodes it had in the
     * previous View, even if the even-incrementing rule would assign it fewer
     * nodes in this View.
     * @param curr_view The current View
     * @return A map from subgroup type -> subgroup index -> shard number
     * -> number of nodes in that shard
     */
    std::map<std::type_index, std::vector<std::vector<uint32_t>>> compute_standard_shard_sizes(
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<View>& prev_view,
            const View& curr_view) const;

    /**
     * Creates and returns an initial membership allocation for a single
     * subgroup type, based on the input map of shard sizes.
     * @param subgroup_type The subgroup type to allocate members for
     * @param curr_view The current view, whose next_unassigned_rank will be updated
     * @param shard_sizes The map of membership sizes for every subgroup and shard
     * @param curr_members
     * @param curr_member_set
     * @return A subgroup layout for this subgroup type
     */
    subgroup_shard_layout_t allocate_standard_subgroup_type(
            const std::type_index subgroup_type,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes,
            const std::vector<node_id_t>& curr_members,
            const std::set<node_id_t>& curr_member_set) const;

    /**
     * Creates and returns a new membership allocation for a single subgroup
     * type, based on its previous allocation and its newly-assigned sizes.
     * @param subgroup_type The subgroup type to allocate members for
     * @param subgroup_type_id The numeric "type ID" for this subgroup type
     * (its position in the subgroup_type_order list)
     * @param prev_view The previous View, now known to be non-null
     * @param curr_view The current View, whose next_unassigned_rank will be updated
     * @param shard_sizes The map of membership sizes for every subgroup and shard in curr_view
     * @param surviving_member_set
     * @param added_member_set
     * @param curr_members
     * @param curr_member_set
     * @return A subgroup layout for this subgroup type.
     */
    subgroup_shard_layout_t update_standard_subgroup_type(
            const std::type_index subgroup_type,
            const subgroup_type_id_t subgroup_type_id,
            const std::unique_ptr<View>& prev_view,
            View& curr_view,
            const std::map<std::type_index, std::vector<std::vector<uint32_t>>>& shard_sizes,
            const std::set<node_id_t>& surviving_member_set,
            const std::set<node_id_t>& added_member_set,
            const std::vector<node_id_t>& curr_members,
            const std::set<node_id_t>& curr_member_set) const;

    /**
     * Helper function that implements the subgroup allocation algorithm for all
     * "standard" (non-cross-product) subgroups. It creates entries for those
     * subgroups in the out-parameter subgroup_layouts.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The same previous view passed in to the operator() function
     * @param curr_view The same current view passed in to the operator() function
     * @param subgroup_layouts The map of subgroup types to subgroup layouts that operator()
     * will end up returning; this function will fill in the entries for some of the types.
     */
    void compute_standard_memberships(const std::vector<std::type_index>& subgroup_type_order,
                                      const std::unique_ptr<View>& prev_view,
                                      View& curr_view,
                                      subgroup_allocation_map_t& subgroup_layouts) const;

    /**
     * Helper function that implements the subgroup allocation algorithm for all
     * cross-product subgroups. It must be run second so that it can refer to the allocations
     * created for the "standard" subgroups. It creates entries for the cross-product
     * subgroups in the out-parameter subgroup_layouts.
     * @param subgroup_type_order The same subgroup type order passed in to the operator() function
     * @param prev_view The same previous view passed in to the operator() function
     * @param curr_view The same current view passed in to the operator() function
     * @param subgroup_layouts The map of subgroup types to subgroup layouts that operator()
     * will end up returning; this function will fill in the entries for some of the types.
     */
    void compute_cross_product_memberships(const std::vector<std::type_index>& subgroup_type_order,
                                           const std::unique_ptr<View>& prev_view,
                                           View& curr_view,
                                           subgroup_allocation_map_t& subgroup_layouts) const;

public:
    /**
     * Constructs a subgroup allocator from a map of subgroup policies.
     * @param policies_by_subgroup_type A map containing one entry for each
     * subgroup type, whose value is the allocation policy to use for that
     * subgroup type (either a SubgroupAllocationPolicy or a CrossProductPolicy)
     */
    DefaultSubgroupAllocator(const std::map<std::type_index, SubgroupPolicyVariant>&
                                     policies_by_subgroup_type)
            : policies(policies_by_subgroup_type) {}
    /**
     * Constructs a subgroup allocator with policies that include reserved node
     * IDs. In this case the allocator must be initialized with the set of all
     * reserved node IDs used by any policy in the map.
     * @param policies_by_subgroup_type A map containing one entry for each
     * subgroup type, whose value is the allocation policy to use for that
     * subgroup type (either a SubgroupAllocationPolicy or a CrossProductPolicy)
     * @param all_reserved_node_ids The set of all reserved node IDs used by any
     * policy in the map.
     */
    DefaultSubgroupAllocator(const std::map<std::type_index, SubgroupPolicyVariant>&
                                     policies_by_subgroup_type,
                             const std::set<node_id_t>& all_reserved_node_ids)
            : policies(policies_by_subgroup_type),
              all_reserved_node_ids(all_reserved_node_ids) {}

    /**
     * Constructs a subgroup allocator from a vector of subgroup types and a
     * JSON object containing layout policy information for each subgroup type.
     * This assumes that the JSON object is an array with one entry for each
     * subgroup type, in the same order as the types in the vector.
     * @param subgroup_types A vector of subgroup types (as type_indexes)
     * @param layout_array A JSON object containing a policy entry for each subgroup type
     */
    DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types, const json& layout_array);

    /**
     * Constructs a subgroup allocator from a vector of subgroup types and a
     * path to a file containing a JSON string with layout information for each
     * type.
     * @param subgroup_types A vector of subgroup types (as type_indexes)
     * @param json_file_path A path to a file containing a JSON string
     */
    DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types, const std::string& json_file_path);

    /**
     * Constructs a subgroup allocator from a vector of subgroup types, assuming
     * that a JSON layout object has been configured for these subgroup types.
     * This will use either the json_layout or json_layout_file config option
     * (whichever one is present) to load a JSON object, and assume that it is
     * an array with one entry for each subgroup type in the same order as the
     * types in the vector.
     * @param subgroup_types A vector of subgroup types (as type_indexes)
     */
    DefaultSubgroupAllocator(std::vector<std::type_index> subgroup_types);

    /**
     * Copy constructor
     */
    DefaultSubgroupAllocator(const DefaultSubgroupAllocator& to_copy)
            : policies(to_copy.policies),
              all_reserved_node_ids(to_copy.all_reserved_node_ids) {}
    /**
     * Move constructor
     */
    DefaultSubgroupAllocator(DefaultSubgroupAllocator&&) = default;

    subgroup_allocation_map_t operator()(const std::vector<std::type_index>& subgroup_type_order,
                                         const std::unique_ptr<View>& prev_view,
                                         View& curr_view) const;
};

/*
 * Since constructors can't take template parameters, these free functions
 * allow you to construct a DefaultSubgroupAllocator by specifying the
 * subgroup types as template parameters rather than type_indexes. This is
 * modeled after the same workaround used by std::make_unique<T>
 */

/**
 * Constructs a subgroup allocator using information in the Derecho config
 * file, as long as the template parameters are the subgroup types in the
 * correct order. This will use either the json_layout or json_layout_file
 * config option (whichever one is present) to load a JSON object, then
 * assume that it is an array with one policy entry for each subgroup type,
 * in the same order as the template parameters.
 * @tparam The subgroup types, in the same order as the layout policies in
 * the JSON object.
 */
template <typename... ReplicatedTypes>
DefaultSubgroupAllocator make_subgroup_allocator() {
    return DefaultSubgroupAllocator(std::vector<std::type_index>{std::type_index(typeid(ReplicatedTypes))...});
}

/**
 * Constructs a subgroup allocator from a JSON object describing layout
 * policies for each subgroup type.
 * @param json_file_path A path to a file containing a JSON string
 * @tparam The subgroup types, in the same order as the layout policies
 * in the JSON object
 */
template <typename... ReplicatedTypes>
DefaultSubgroupAllocator make_subgroup_allocator(const std::string& json_file_path) {
    return DefaultSubgroupAllocator({std::type_index(typeid(ReplicatedTypes))...}, json_file_path);
}

/**
 * Constructs a subgroup allocator from a JSON object describing layout
 * policies for each subgroup type.
 * @param layout A JSON object that is an array with one entry for each
 * subgroup type
 * @tparam The subgroup types, in the same order as the layout policies
 * in the JSON object
 */
template <typename... ReplicatedTypes>
DefaultSubgroupAllocator make_subgroup_allocator(const json& layout) {
    return DefaultSubgroupAllocator({std::type_index(typeid(ReplicatedTypes))...}, layout);
}

/**
 * Generate a single-type subgroup allocation policy from a JSON object
 * @param jconf A subgroup configuration represented in json format.
 * @param all_reserved_node_ids A set that holds the union of all reserved node_ids.
 * @return SubgroupAllocationPolicy for that subgroup
 */
SubgroupAllocationPolicy parse_json_subgroup_policy(const json&, std::set<node_id_t>&);

/**
 * TODO: If we just need to check shards within one subgroup, this function is redundant.
 * Make sure that no shards inside a subgroup reserve same node_ids. Shards in
 * different subgroups of one same type or from different types can share nodes,
 * and this why we use the reserved_node_id feature.
 * For example, we can assign 2 subgroups for type "PersistentCascadeStoreWithStringKey"
 * to store data and model respectively for an ML application, and actually reserve
 * the same node_ids for shards in this two subgroup. This way the data and the model
 * coexist in the same node, thus delivering performance gains.
 * @param dsa_map the subgroup allocation map derived from json configuration.
 */
void check_reserved_node_id_pool(const std::map<std::type_index, SubgroupPolicyVariant>& dsa_map);

}  // namespace derecho
