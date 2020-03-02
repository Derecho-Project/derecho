/**
 * @file view.h
 * @brief: Contains the definition of the View class
 */

#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include "detail/derecho_internal.hpp"
#include "derecho_modes.hpp"
#include "detail/derecho_sst.hpp"
#include "detail/multicast_group.hpp"
#include <derecho/sst/sst.hpp>
#include <derecho/mutils-serialization/SerializationMacros.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace derecho {
enum PORT_TYPE { GMS = 1,
                 RPC,
                 SST,
                 RDMC,
                 EXTERNAL };

/**
 * The subset of a View associated with a single shard, or a single subgroup if
 * the subgroup is non-sharded.
 */
class SubView : public mutils::ByteRepresentable {
public:
    /** Operation mode, raw mode does not do stability and delivery */
    Mode mode;
    /** Node IDs of members in this subgroup/shard, indexed by their order in the SST */
    std::vector<node_id_t> members;
    /** vector selecting the senders, 0 for non-sender, non-0 for sender*/
    /** integers instead of booleans due to the serialization issue :-/ */
    std::vector<int> is_sender;
    /** IP addresses and ports of members in this subgroup/shard, with the same indices as members. */
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>> member_ips_and_ports;
    /** List of IDs of nodes that joined since the previous view, if any. */
    std::vector<node_id_t> joined;
    /** List of IDs of nodes that left since the previous view, if any. */
    std::vector<node_id_t> departed;
    /** The rank of this node within the subgroup/shard, or -1 if this node is
     * not a member of the subgroup/shard. This is initialized by ViewManager, not
     * the subgroup allocation functions that create SubViews. */
    int32_t my_rank;
    /** Settings for the subview */
    const std::string profile;
    /** Looks up the sub-view rank of a node ID. Returns -1 if
     * that node ID is not a member of this subgroup/shard. */
    int rank_of(const node_id_t& who) const;
    /** Looks up the sender rank of a given member. Returns -1 if the member isn't a sender */
    int sender_rank_of(uint32_t rank) const;
    /** returns the number of senders in the subview */
    uint32_t num_senders() const;
    /** Creates an empty new SubView with num_members members.
     * The vectors will have room for num_members elements. */
    SubView(int32_t num_members);

    DEFAULT_SERIALIZATION_SUPPORT(SubView, mode, members, is_sender,
                                  member_ips_and_ports, joined, departed, profile);
    SubView(Mode mode, const std::vector<node_id_t>& members,
            std::vector<int> is_sender,
            const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
            const std::vector<node_id_t>& joined,
            const std::vector<node_id_t>& departed,
            const std::string &profile)
            : mode(mode),
              members(members),
              is_sender(is_sender),
              member_ips_and_ports(member_ips_and_ports),
              joined(joined),
              departed(departed),
              my_rank(-1),
              profile(profile) {}

    SubView(Mode mode, const std::vector<node_id_t>& members,
            std::vector<int> is_sender,
            const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
            std::string profile);

    /**
     * Initialization helper method that initializes the joined and departed lists
     * given the previous View's version of this SubView. This should be called
     * after using the fewer-parameters constructor to finish setting up the SubView.
     * @param previous_subview The previous SubView to compare against
     */
    void init_joined_departed(const SubView& previous_subview);
};

class View : public mutils::ByteRepresentable {
public:
    /** Sequential view ID: 0, 1, ... */
    const int32_t vid;
    /** Node IDs of members in the current view, indexed by their SST rank. */
    const std::vector<node_id_t> members;
    /** IP addresses and ports (gms, rpc, sst, rdmc in order) of members in the current view, indexed by their SST rank. */
    const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>> member_ips_and_ports;
    /** failed[i] is true if members[i] is considered to have failed.
     * Once a member is failed, it will be removed from the members list in a future view. */
    std::vector<char> failed;  //Note: std::vector<bool> is broken, so we pretend these char values are C-style booleans
    /** Number of current outstanding failures in this view. After
     * transitioning to a new view that excludes a failed member, this count
     * will decrease by one. */
    int32_t num_failed;
    /** List of IDs of nodes that joined since the previous view, if any. */
    std::vector<node_id_t> joined;
    /** List of IDs of nodes that left since the previous view, if any. */
    std::vector<node_id_t> departed;
    /** Number of members in this view */
    const int32_t num_members;
    /** The rank of this node (as returned by rank_of()) */
    int32_t my_rank;
    /** Set to false during MulticastGroup setup if a subgroup membership function
     * throws a subgroup_provisioning_exception. If false, no subgroup operations will
     * work in this View. */
    bool is_adequately_provisioned = true;
    /** The rank of the lowest-ranked member that is not assigned to a subgroup
     * in this View. Members with this rank or higher can be assumed to be
     * available to assign to any subgroup and will not appear in any SubView. */
    int32_t next_unassigned_rank;
    /** RDMC manager object used for sending multicasts */
    std::unique_ptr<MulticastGroup> multicast_group;
    /** Pointer to the SST instance used by the GMS in this View */
    std::shared_ptr<DerechoSST> gmsSST;
    /** The order of subgroup types as they were declared in the Group's template
     * parameters. This never changes at runtime, but it is stored here so that
     * the subgroup allocation function can read it. A subgroup type's ID is its
     * position in this vector. */
    std::vector<std::type_index> subgroup_type_order;
    /** Maps the (type, index) pairs used by users to identify subgroups to the
     * internal subgroup IDs generated by ViewManager during SST setup. 
     * The order of ids in the vector follows the order in which the user created
     * those subgroups of the same type.
     */
    std::map<subgroup_type_id_t, std::vector<subgroup_id_t>> subgroup_ids_by_type_id;
    /** Maps subgroup ID -> shard number -> SubView for that subgroup/shard.
     * Note that this contains an entry for every subgroup and shard, even those
     * that the current node does not belong to. */
    std::vector<std::vector<SubView>> subgroup_shard_views;
    /** Lists the (subgroup ID, shard num) pairs that this node is a member of */
    std::map<subgroup_id_t, uint32_t> my_subgroups;
    /** Reverse index of members[]; maps node ID -> SST rank */
    std::map<node_id_t, uint32_t> node_id_to_rank;

    /**
     * Constructs a SubView containing the provided subset of this View's
     * members. This is helpful in writing subgroup-membership functions.
     * @param with_members The node IDs that will be the SubView's members vector
     * @return A SubView containing those members, the corresponding member IPs,
     * and the subsets of joined[] and departed[] that intersect with those members
     * @throws subgroup_provisioning_exception if any of the requested members
     * are not actually in this View's members vector.
     */
    SubView make_subview(const std::vector<node_id_t>& with_members, const Mode mode = Mode::ORDERED, const std::vector<int>& is_sender = {}, std::string profile = "default") const;

    /** Looks up the SST rank of an IP address. Returns -1 if that IP is not a member of this view. */
    int rank_of(const std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>& who) const;
    /** Looks up the SST rank of a node ID. Returns -1 if that node ID is not a member of this view. */
    int rank_of(const node_id_t& who) const;
    /** Returns the rank of this View's leader, based on failed[]. */
    int find_rank_of_leader() const;
    /** @return rank_of_leader() == my_rank */
    bool i_am_leader() const;
    /** Wedges the view, which means wedging both SST and DerechoGroup. */
    void wedge();
    /** Checks to see if this view has been wedged. */
    bool is_wedged();

    /** Computes the within-shard rank of a particular shard's leader, based on failed[].
     * This is not a member of SubView because it needs access to failed[], but it returns
     * a SubView rank, not an SST rank in this View. */
    int subview_rank_of_shard_leader(subgroup_id_t subgroup_id, uint32_t shard_index) const;

    /** Builds a human-readable string representing the state of the view.
     *  Used for debugging only.*/
    std::string debug_string() const;

    DEFAULT_SERIALIZATION_SUPPORT(View, vid, members, member_ips_and_ports,
                                  failed, num_failed, joined, departed,
                                  num_members, next_unassigned_rank,
                                  subgroup_ids_by_type_id, subgroup_shard_views, my_subgroups);

    /**
     * Constructor used by deserialization: constructs a View given the values of its serialized fields.
     * Warning: subgroup_type_order cannot be serialized, so it must be manually set by the receiving
     * ViewManager after deserialization (the receiving ViewManager will always know this value).
     */
    View(const int32_t vid, const std::vector<node_id_t>& members,
         const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
         const std::vector<char>& failed, const int32_t num_failed,
         const std::vector<node_id_t>& joined,
         const std::vector<node_id_t>& departed, const int32_t num_members,
         const int32_t next_unassigned_rank,
         const std::map<subgroup_type_id_t, std::vector<subgroup_id_t>>& subgroup_ids_by_type_id,
         const std::vector<std::vector<SubView>>& subgroup_shard_views,
         const std::map<subgroup_id_t, uint32_t>& my_subgroups);

    /** Standard constructor for making a new View */
    View(const int32_t vid, const std::vector<node_id_t>& members,
         const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t>>& member_ips_and_ports,
         const std::vector<char>& failed,
         const std::vector<node_id_t>& joined,
         const std::vector<node_id_t>& departed, const int32_t my_rank,
         const int32_t next_unassigned_rank,
         const std::vector<std::type_index>& subgroup_type_order);
};

}  // namespace derecho
