/**
 * @file view.h
 * @brief: Contains the definition of the View class
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <mutex>
#include <iostream>

#include "derecho_group.h"
#include "derecho_sst.h"
#include "sst/sst.h"
#include <mutils-serialization/SerializationMacros.hpp>
#include <mutils-serialization/SerializationSupport.hpp>

namespace derecho {

template <typename handlersType>
class View : public mutils::ByteRepresentable {
public:
    /** Sequential view ID: 0, 1, ... */
    int32_t vid;
    /** Node IDs of members in the current view, indexed by their SST rank. */
    std::vector<node_id_t> members;
    /** IP addresses of members in the current view, indexed by their SST rank. */
    std::vector<ip_addr> member_ips;
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
    int32_t num_members;
    /** The rank of this node (as returned by rank_of()) */
    int32_t my_rank;
    /** RDMC manager object containing one RDMC group for each sender */
    std::unique_ptr<DerechoGroup<handlersType>> derecho_group;
    /** Pointer to the SST instance used by the GMS in this View */
    std::shared_ptr<DerechoSST> gmsSST;

    bool i_know_i_am_leader = false;  // I am the leader (and know it)

    /** Creates a completely empty new View. Vectors such as members will
     * be empty (size 0), so the client will need to resize them. */
    View();
    /** Creates an empty new View with num_members members.
     * The vectors will have room for num_members elements. */
    View(int32_t num_members);
    /** Announces a new view to the application using this group
     * (or at least that's the idea; this still needs to be implemented,
     * probably with some sort of callback system). */
    void announce_new_view(const View& Vc);

    int rank_of(const ip_addr& who) const;
    /** Looks up the SST rank of a node ID. Returns -1 if that node ID is not a member of this view. */
    int rank_of(const node_id_t& who) const;
    int rank_of_leader() const;

    bool i_am_leader() const;
    /** Determines whether this node is the new leader after a view change. */
    bool i_am_new_leader();
    /** Merges changes lists from other SST rows into this node's SST row. */
    void merge_changes();
    /** Wedges the view, which means wedging both SST and DerechoGroup. */
    void wedge();

    /** Prints a human-readable string representing the state of the view.
     *  Used for debugging only.*/
    std::string debug_string() const;

    DEFAULT_SERIALIZATION_SUPPORT(View, vid, members, member_ips, failed, num_failed, num_members, my_rank);

    /** Constructor used by deserialization: constructs a View given the values of its serialized fields. */
    View(const int32_t vid, const std::vector<node_id_t>& members, const std::vector<ip_addr>& member_ips,
         const std::vector<char>& failed, const int32_t nFailed, const int32_t num_members, const int32_t my_rank)
            : vid(vid),
              members(members),
              member_ips(member_ips),
              failed(failed),
              num_failed(nFailed),
              num_members(num_members),
              my_rank(my_rank) {}
};

/**
 * Custom implementation of load_object for Views. The View from the swap file
 * will be used if it is newer than the View from view_file_name (according to
 * VID), since this means a crash occurred before the swap file could be renamed.
 * @param view_file_name The name of the file to read for a serialized View
 * @return A new View constructed with the data in the file
 */
template <typename handlersType>
std::unique_ptr<View<handlersType>> load_view(const std::string& view_file_name);

/**
 * Prints a plaintext representation of the View to an output stream. This is
 * not interchangeable with the serialization library, but can be used to create
 * a log file parseable by standard bash tools.
 * @param stream The output stream
 * @param view The View to print
 * @return The output stream
 */
template <typename handlersType>
std::ostream& operator<<(std::ostream& stream, const View<handlersType>& view);

/**
 * Parses the plaintext representation created by operator<< and modifies the View
 * argument to contain the view it represents.
 */
template <typename handlersType>
std::istream& operator>>(std::istream& stream, View<handlersType>& view);
}

#include "view_impl.h"
