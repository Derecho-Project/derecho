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
#include "mutils-serialization/SerializationMacros.hpp"
#include "mutils-serialization/SerializationSupport.hpp"
#include "max_members.h"

namespace derecho {

template <typename handlersType>
class View : public mutils::ByteRepresentable {
public:
    /** Upper bound on the number of members that will ever be in any one view.
     */
    static constexpr int MAX_MEMBERS = ::MAX_MEMBERS;

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
    int32_t nFailed;
    /** ID of the node that joined or departed since the prior view; null if this is the first view */
    std::shared_ptr<node_id_t> who;
    /** Number of members in this view */
    int32_t num_members;
    /** For member p, returns rankOf(p) */
    int32_t my_rank;
    /** RDMC manager object containing one RDMC group for each sender */
    std::unique_ptr<DerechoGroup<handlersType>> derecho_group;

    std::shared_ptr<DerechoSST> gmsSST;

    /** Creates a completely empty new View. Vectors such as members will
     * be empty (size 0), so the client will need to resize them. */
    View();
    /** Creates an empty new View with num_members members.
     * The vectors will have room for num_members elements. */
    View(int32_t num_members);
    void newView(const View& Vc);
    /** When constructing a View piecemeal, call this after num_members has been set. */
    void init_vectors();

    int rank_of(const ip_addr& who) const;
    int rank_of(const node_id_t& who) const;
    int rank_of_leader() const;

    bool IKnowIAmLeader = false;  // I am the leader (and know it)

    bool IAmLeader() const;
    /** Determines whether this node is the new leader after a view change. */
    bool IAmTheNewLeader();
    /** Merges changes lists from other SST rows into this node's SST row. */
    void merge_changes();
    /** Wedges the view, which means wedging both SST and DerechoGroup. */
    void wedge();

    /** Returns a pointer to the (IP address of the) member who recently joined,
     * or null if the most recent change was not a join. */
    std::shared_ptr<node_id_t> Joined() const;
    /** Returns a pointer to the (IP address of the) member who recently departed,
     * or null if the most recent change was not a departure. */
    std::shared_ptr<node_id_t> Departed() const;

    /** Prints a human-readable string representing the state of the view.
     *  Used for debugging only.*/
    std::string ToString() const;

    DEFAULT_SERIALIZATION_SUPPORT(View, vid, members, member_ips, failed, nFailed, num_members, my_rank);

    /** Constructor used by deserialization: constructs a View given the values of its serialized fields. */
    View(const int32_t vid, const std::vector<node_id_t>& members, const std::vector<ip_addr>& member_ips,
         const std::vector<char>& failed, const int32_t nFailed, const int32_t num_members, const int32_t my_rank)
            : vid(vid),
              members(members),
              member_ips(member_ips),
              failed(failed),
              nFailed(nFailed),
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
