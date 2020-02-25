#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../subgroup_info.hpp"
#include "../view.hpp"
#include "derecho_internal.hpp"

#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <spdlog/spdlog.h>

namespace derecho {
/**
 * Represents the data needed to log a "ragged trim" decision to disk. There
 * will be one of these per subgroup that a node belongs to, because each
 * subgroup decides its ragged edge cleanup separately.
 */
struct RaggedTrim : public mutils::ByteRepresentable {
    subgroup_id_t subgroup_id;
    uint32_t shard_num;
    int vid;
    int32_t leader_id;  //Signed instead of unsigned so it can have the special value -1
    std::vector<int32_t> max_received_by_sender;
    RaggedTrim(subgroup_id_t subgroup_id, uint32_t shard_num, int vid,
               int32_t leader_id, std::vector<int32_t> max_received_by_sender)
            : subgroup_id(subgroup_id), shard_num(shard_num), vid(vid), leader_id(leader_id), max_received_by_sender(max_received_by_sender) {}
    DEFAULT_SERIALIZATION_SUPPORT(RaggedTrim, subgroup_id, shard_num, vid, leader_id, max_received_by_sender);
};

/**
 * A type-safe set of messages that can be sent during two-phase commit
 */
enum class CommitMessage {
    PREPARE,  //!< PREPARE
    COMMIT,   //!< COMMIT
    ABORT,    //!< ABORT
    ACK       //!< ACK
};

/**
 * Builds a filename to use for a RaggedTrim logged to disk using its subgroup and shard IDs.
 */
inline std::string ragged_trim_filename(subgroup_id_t subgroup_num, uint32_t shard_num) {
    std::ostringstream string_builder;
    string_builder << "RaggedTrim_" << subgroup_num << "_" << shard_num;
    return string_builder.str();
}

/** List of logged ragged trim states, indexed by (subgroup ID, shard num), stored by pointer */
using ragged_trim_map_t = std::map<subgroup_id_t, std::map<uint32_t, std::unique_ptr<RaggedTrim>>>;

struct RestartState {
    /** List of logged ragged trim states recovered from the last known View,
     * either read locally from this node's logs or received from the restart
     * leader. */
    ragged_trim_map_t logged_ragged_trim;
    /** Map from (subgroup ID, shard num) to ID of the "restart leader" for that
     * shard, which is the node with the longest persistent log for that shard's
     * replicated state. */
    std::vector<std::vector<int64_t>> restart_shard_leaders;
    void load_ragged_trim(const View& curr_view);
    /**
     * Computes the persistent version corresponding to a ragged trim proposal,
     * i.e. the version number that will be persisted if these updates are committed.
     * @param view_id The VID of the current View (since it's part of the version number)
     * @param max_received_by_sender The ragged trim proposal for a single shard,
     * corresponding to a single Replicated Object
     * @return The persistent version number that the object will have if this
     * ragged trim is delivered
     */
    static persistent::version_t ragged_trim_to_latest_version(const int32_t view_id,
                                                               const std::vector<int32_t>& max_received_by_sender);
};

class RestartLeaderState {
private:
    /** Takes ownership of ViewManager's curr_view pointer, because
     * await_quroum() might replace curr_view with a newer view discovered
     * on a restarting node. */
    std::unique_ptr<View> curr_view;
    /** Mutable reference to RestartState, since this class needs to update
     * the restart state stored in ViewManager. */
    RestartState& restart_state;
    const SubgroupInfo& subgroup_info;

    std::unique_ptr<View> restart_view;
    std::map<node_id_t, tcp::socket> waiting_join_sockets;
    std::map<node_id_t, std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>> rejoined_node_ips_and_ports;
    std::set<node_id_t> members_sent_restart_view;
    std::set<node_id_t> rejoined_node_ids;
    std::set<node_id_t> last_known_view_members;
    /**
     * Map from (subgroup ID, shard num) to the latest persistent log version
     * currently known for that shard
     */
    std::vector<std::vector<persistent::version_t>> longest_log_versions;
    /**
     * Map from (subgroup ID, shard num) to the ID of the node on which the
     * longest known log for that shard resides. An entry will be -1 if no log
     * for that shard has yet been found, or if the subgroup is non-persistent
     * and there is no logged state at all. This is the same format as
     * ViewManager::prior_view_shard_leaders, in which an entry of -1 indicates
     * a shard with no prior state to transfer.
     */
    std::vector<std::vector<int64_t>> nodes_with_longest_log;
    const node_id_t my_id;

    /**
     * Helper method for await_quorum that processes the logged View and
     * RaggedTrims from a single rejoining node. This may update curr_view or
     * logged_ragged_trim if the joiner has newer information.
     * @param joiner_id The ID of the rejoining node
     * @param client_socket The TCP socket connected to the rejoining node
     */
    void receive_joiner_logs(const node_id_t& joiner_id, tcp::socket& client_socket);

    /**
     * Recomputes the restart view based on the current set of nodes that have
     * rejoined (in waiting_join_sockets and rejoined_node_ids). This just ties
     * together update_curr_and_next_restart_view and make_subgroup_maps.
     * @return True if the restart view would be adequate, false if it would be
     * inadequate.
     */
    bool compute_restart_view();

    /**
     * Updates curr_view and makes a new next_view based on the current set of
     * rejoining nodes during total restart.
     * @return The next view that will be installed if the restart continues at this point
     */
    std::unique_ptr<View> update_curr_and_next_restart_view();

public:
    static const int RESTART_LEADER_TIMEOUT = 2000;
    RestartLeaderState(std::unique_ptr<View> _curr_view, RestartState& restart_state,
                       const SubgroupInfo& subgroup_info,
                       const node_id_t my_id);
    /**
     * Waits for nodes to rejoin at this node, updating the last known View and
     * RaggedTrim (and corresponding longest-log information) as each node connects,
     * until there is a quorum of nodes from the last known View and a new View
     * can be installed that is adequately provisioned.
     * @param server_socket The TCP socket to listen for rejoining nodes on
     */
    void await_quorum(tcp::connection_listener& server_socket);

    /**
     * Checks to see whether the leader has achieved a restart quorum, which
     * may involve recomputing the restart view if the minimum number of nodes
     * have rejoined.
     * @return True if there is a restart quorum, false if there is not
     */
    bool has_restart_quorum();

    /**
     * Repeatedly attempts to send a new restart view, recomputing it on each
     * failure, until either there is no longer a restart quorum or the view was
     * sent successfully to everyone.
     * @return True if the view was sent successfully, false if the quorum was lost
     */
    bool resend_view_until_quorum_lost();

    /**
     * Sends the currently-computed restart view, the current ragged trim, the
     * current location of the longest logs (the "shard leaders"), and the
     * DerechoParams to all members who are currently ready to restart.
     * @return -1 if all sends were successful; the ID of a node that has failed
     * if sending the View to a node failed.
     */
    int64_t send_restart_view();

    /**
     * Sends an Abort message to all nodes that have previously been sent the
     * restart View, indicating that they must go back to waiting for a new View.
     */
    void send_abort();

    /**
     * Sends a Prepare message to all members who are currently ready to restart;
     * this checks for failures one more time before committing.
     * @return -1 if all sends were successful; the ID of a node that has failed
     * if sending a Prepare message failed.
     */
    int64_t send_prepare();

    /**
     * Sends a Commit message to all members of the restart view, then closes the
     * TCP sockets connected to them.
     */
    void send_commit();
    /** Read the curr_view (last known view) managed by RestartLeaderState. Only used for debugging. */
    const View& get_curr_view() const { return *curr_view; }
    /** Read the current restart view managed by RestartLeaderState. */
    const View& get_restart_view() const { return *restart_view; }
    /** Remove and return the restart view managed by RestartLeaderState;
     * this will take ownership back to the caller (ViewManager). */
    std::unique_ptr<View> take_restart_view() { return std::move(restart_view); }

    void print_longest_logs() const;

    /**
     * Constructs the next view from the current view and a list of joining
     * nodes, by ID and IP address. This is slightly different from the standard
     * ViewManager::make_next_view because it gets explicit inputs rather than
     * examining the SST, and assumes that all nodes marked failed in curr_view
     * will be removed (instead of removing only the "accepted changes").
     * @param curr_view The current view, including the list of failed members
     * to remove
     * @param joiner_ids The list of joining node IDs
     * @param joiner_ips_and_ports The list of IP addresses and ports for the joining nodes
     * @param logger
     * @return A View object for the next view
     */
    static std::unique_ptr<View> make_next_view(const std::unique_ptr<View>& curr_view,
                                                const std::vector<node_id_t>& joiner_ids,
                                                const std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>>& joiner_ips_and_ports);
    /**
     * @return true if the set of node IDs includes at least one member of each
     * subgroup in the given View.
     */
    static bool contains_at_least_one_member_per_subgroup(std::set<node_id_t> rejoined_node_ids,
                                                          const View& last_view);
};

} /* namespace derecho */
