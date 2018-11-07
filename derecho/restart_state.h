#pragma once

#include <cstdint>
#include <memory>
#include <map>
#include <string>
#include <sstream>
#include <vector>

#include "derecho_internal.h"
#include "view.h"
#include "subgroup_info.h"

#include <mutils-serialization/SerializationSupport.hpp>
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
    int32_t leader_id; //Signed instead of unsigned so it can have the special value -1
    std::vector<int32_t> max_received_by_sender;
    RaggedTrim(subgroup_id_t subgroup_id, uint32_t shard_num, int vid,
               int32_t leader_id, std::vector<int32_t> max_received_by_sender)
    : subgroup_id(subgroup_id), shard_num(shard_num), vid(vid),
      leader_id(leader_id), max_received_by_sender(max_received_by_sender) {}
    DEFAULT_SERIALIZATION_SUPPORT(RaggedTrim, subgroup_id, shard_num, vid, leader_id, max_received_by_sender);
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
    whenlog(std::shared_ptr<spdlog::logger> logger;)
    /** Takes ownership of ViewManager's curr_view pointer, because
     * await_quroum() might replace curr_view with a newer view discovered
     * on a restarting node. */
    std::unique_ptr<View> curr_view;
    /** Mutable reference to RestartState, since this class needs to update
     * the restart state stored in ViewManager. */
    RestartState& restart_state;
    std::map<subgroup_id_t, SubgroupSettings>& restart_subgroup_settings;
    uint32_t& restart_num_received_size;
    const SubgroupInfo& subgroup_info;

    std::unique_ptr<View> restart_view;
    std::map<node_id_t, tcp::socket> waiting_join_sockets;
    std::set<node_id_t> members_sent_restart_view;
    std::set<node_id_t> rejoined_node_ids;
    std::set<node_id_t> last_known_view_members;
    std::vector<std::vector<persistent::version_t>> longest_log_versions;
    std::vector<std::vector<int64_t>> nodes_with_longest_log;
    const node_id_t my_id;
public:
    static const int RESTART_LEADER_TIMEOUT = 300000;
    RestartLeaderState(std::unique_ptr<View> _curr_view, RestartState& restart_state,
                       std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings_map,
                       uint32_t& num_received_size,
                       const SubgroupInfo& subgroup_info,
                       const node_id_t my_id);
    void await_quorum(tcp::connection_listener& server_socket);
    /**
     * Recomputes the restart view based on the current set of nodes that have
     * rejoined (in waiting_join_sockets and rejoined_node_ids).
     * @return True if the group is now ready to restart, false if it is not
     * (because, e.g. the restart view would be inadquate).
     */
    bool compute_restart_view();
    /**
     * Sends the currently-computed restart view and the provided DerechoParams
     * to all members who are currently ready to restart.
     * @param derecho_params The DerechoParams to send to each restarting node
     * @return -1 if all sends were successful; the ID of a node that has failed
     * if sending the View to a node failed.
     */
    int64_t send_restart_view(const DerechoParams& derecho_params);
    /**
     * Sends a Boolean value to all nodes that have previously been sent the
     * restart view, indicating whether they can commit this view or must
     * discard it because some node has failed.
     * @param commit
     */
    void confirm_restart_view(const bool commit);
    /** Read the curr_view (last known view) managed by RestartLeaderState. Only used for debugging. */
    const View& get_curr_view() const { return *curr_view; }
    /** Read the current restart view managed by RestartLeaderState. */
    const View& get_restart_view() const { return *restart_view; }
    /** Remove and return the restart view managed by RestartLeaderState;
     * this will take ownership back to the caller (ViewManager). */
    std::unique_ptr<View> take_restart_view() { return std::move(restart_view); }

    void print_longest_logs() const;
    /**
     * Updates curr_view and makes a new next_view based on the current set of
     * rejoining nodes during total restart. This is only used by the restart
     * leader during total restart.
     * @param waiting_join_sockets The set of connections to restarting nodes
     * @param rejoined_node_ids The IDs of those nodes
     * @return The next view that will be installed if the restart continues at this point
     */
    std::unique_ptr<View> update_curr_and_next_restart_view(const std::map<node_id_t, tcp::socket>& waiting_join_sockets,
                                                            const std::set<node_id_t>& rejoined_node_ids);
    /**
     * Constructs the next view from the current view and a list of joining
     * nodes, by ID and IP address. This version is only used by the restart
     * leader during total restart, and assumes that all nodes marked failed
     * in curr_view will be removed.
     * @param curr_view The current view, including the list of failed members
     * to remove
     * @param joiner_ids
     * @param joiner_ips
     * @param logger
     * @return A View object for the next view
     */
    static std::unique_ptr<View> make_next_view(const std::unique_ptr<View>& curr_view,
                                                const std::vector<node_id_t>& joiner_ids,
                                                const std::vector<ip_addr>& joiner_ips whenlog(,
                                                std::shared_ptr<spdlog::logger> logger));
    /**
     * @return true if the set of node IDs includes at least one member of each
     * subgroup in the given View.
     */
    static bool contains_at_least_one_member_per_subgroup(std::set<node_id_t> rejoined_node_ids, const View& last_view);
};

} /* namespace derecho */

