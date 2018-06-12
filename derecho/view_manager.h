/**
 * @file ViewManager.h
 *
 * @date Feb 6, 2017
 */
#pragma once

#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "derecho_internal.h"
#include "derecho_ports.h"
#include "locked_reference.h"
#include "multicast_group.h"
#include "subgroup_info.h"
#include "view.h"

#include "tcp/tcp.h"
#include <spdlog/spdlog.h>
#include <mutils-serialization/SerializationSupport.hpp>

namespace derecho {

template <typename T>
class Replicated;
template <typename T>
class ExternalCaller;

class ReplicatedObject;

namespace rpc {
class RPCManager;
}

/**
 * Represents the data needed to log a "ragged trim" decision to disk. There
 * will be one of these per subgroup that a node belongs to, because each
 * subgroup decides its ragged edge cleanup separately.
 */
struct RaggedTrim : public mutils::ByteRepresentable {
    subgroup_id_t subgroup_id;
    uint32_t shard_num;
    int vid;
    node_id_t leader_id;
    std::vector<int32_t> max_received_by_sender;
    RaggedTrim(subgroup_id_t subgroup_id, uint32_t shard_num, int vid,
               node_id_t leader_id, std::vector<int32_t> max_received_by_sender)
    : subgroup_id(subgroup_id), shard_num(shard_num), vid(vid),
      leader_id(leader_id), max_received_by_sender(max_received_by_sender) {}
    DEFAULT_SERIALIZATION_SUPPORT(RaggedTrim, subgroup_id, shard_num, vid, leader_id, max_received_by_sender);
};

/**
 * A little helper class that implements a threadsafe queue by requiring all
 * clients to lock a mutex before accessing the queue.
 */
template <typename T>
class LockedQueue {
private:
    using unique_lock_t = std::unique_lock<std::mutex>;
    std::mutex mutex;
    std::list<T> underlying_list;

public:
    struct LockedListAccess {
    private:
        unique_lock_t lock;

    public:
        std::list<T>& access;
        LockedListAccess(std::mutex& m, std::list<T>& a) : lock(m), access(a){};
    };
    LockedListAccess locked() {
        return LockedListAccess{mutex, underlying_list};
    }
};

/**
 * A set of status codes the group leader can respond with upon initially
 * receiving a connection request from a new node.
 */
enum class JoinResponseCode {
    OK,            //!< OK The new member can proceed to join as normal.
    TOTAL_RESTART, //!< TOTAL_RESTART The group is currently restarting from a total failure, so the new member should send its logged view and ragged trim
    ID_IN_USE,     //!< ID_IN_USE The node's ID is already listed as a member of the current view, so it can't join.
    LEADER_REDIRECT//!< LEADER_REDIRECT This node is not actually the leader and can't accept a join.
};

/**
 * Bundles together a JoinResponseCode and the leader's node ID, which it also
 * needs to send to the new node that wants to join.
 */
struct JoinResponse {
    JoinResponseCode code;
    node_id_t leader_id;
};

template <typename T>
using SharedLockedReference = LockedReference<std::shared_lock<std::shared_timed_mutex>, T>;

using view_upcall_t = std::function<void(const View&)>;

class ViewManager {
private:
    using pred_handle = sst::Predicates<DerechoSST>::pred_handle;

    using initialize_rpc_objects_t = std::function<void(node_id_t, const View&, const std::vector<std::vector<int64_t>>&)>;

    //Allow RPCManager and Replicated to access curr_view and view_mutex directly
    friend class rpc::RPCManager;
    template <typename T>
    friend class Replicated;
    template <typename T>
    friend class ExternalCaller;
    template <typename... T>
    friend class PersistenceManager;

    std::shared_ptr<spdlog::logger> logger;

    /** The port that this instance of the GMS communicates on. */
    const int gms_port;

    /** Controls access to curr_view. Read-only accesses should acquire a
     * shared_lock, while view changes acquire a unique_lock. */
    std::shared_timed_mutex view_mutex;
    /** Notified when curr_view changes (i.e. we are finished with a pending view change).*/
    std::condition_variable_any view_change_cv;

    /** The current View, containing the state of the managed group.
     *  Must be a pointer so we can re-assign it, but will never be null.*/
    std::unique_ptr<View> curr_view;
    /** May hold a pointer to the partially-constructed next view, if we are
     *  in the process of transitioning to a new view. */
    std::unique_ptr<View> next_view;

    /** Contains client sockets for pending joins that have not yet been handled.*/
    LockedQueue<tcp::socket> pending_join_sockets;

    /** Contains old Views that need to be cleaned up*/
    std::queue<std::unique_ptr<View>> old_views;
    std::mutex old_views_mutex;
    std::condition_variable old_views_cv;

    /** The sockets connected to clients that will join in the next view, if any */
    std::list<tcp::socket> proposed_join_sockets;
    /** The node ID that has been assigned to the client that is currently joining, if any. */
    node_id_t joining_client_id;
    /** A cached copy of the last known value of this node's suspected[] array.
     * Helps the SST predicate detect when there's been a change to suspected[].*/
    std::vector<bool> last_suspected;

    tcp::connection_listener server_socket;
    /** A flag to signal background threads to shut down; set to true when the group is destroyed. */
    std::atomic<bool> thread_shutdown;
    /** The background thread that listens for clients connecting on our server socket. */
    std::thread client_listener_thread;
    std::thread old_view_cleanup_thread;

    //Handles for all the predicates the GMS registered with the current view's SST.
    pred_handle suspected_changed_handle;
    pred_handle start_join_handle;
    pred_handle reject_join_handle;
    pred_handle change_commit_ready_handle;
    pred_handle leader_proposed_handle;
    pred_handle leader_committed_handle;

    /** Functions to be called whenever the view changes, to report the
     * new view to some other component. */
    std::vector<view_upcall_t> view_upcalls;
    //Parameters stored here, in case we need them again after construction
    const SubgroupInfo subgroup_info;
    DerechoParams derecho_params;

    /** The same set of TCP sockets used by Group and RPCManager. */
    std::shared_ptr<tcp::tcp_connections> group_member_sockets;

    using ReplicatedObjectReferenceMap = std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>;
    /**
     * A type-erased list of references to the Replicated<T> objects in
     * this group, indexed by their subgroup ID. The actual objects live in the
     * Group<ReplicatedTypes...> that owns this ViewManager, and the abstract
     * ReplicatedObject interface only provides functions for the object state
     * management tasks that ViewManager needs to do. This list also lives in
     * the Group, where it is updated as replicated objects are added and
     * destroyed, so ViewManager has only a reference to it.
     */
    ReplicatedObjectReferenceMap& subgroup_objects;
    /** A function that will be called to initialize replicated objects
     * after transitioning to a new view. This transfers control back to
     * Group because the objects' constructors are only known by Group. */
    initialize_rpc_objects_t initialize_subgroup_objects;
    /** List of logged ragged trim states, indexed by (subgroup ID, shard num),
     * that have been recovered from the last view-change before a total crash.
     * Used only during total restart; empty if the group started up normally. */
    std::map<subgroup_id_t, std::map<uint32_t, std::unique_ptr<RaggedTrim>>> logged_ragged_trim;

    std::vector<std::vector<int64_t>> restart_shard_leaders;

    /** Sends a joining node the new view that has been constructed to include it.*/
    void commit_join(const View& new_view,
                     tcp::socket& client_socket);

    bool has_pending_join() { return pending_join_sockets.locked().access.size() > 0; }

    /**
     * Assuming this node is the leader, handles a join request from a client.
     * @return True if the join succeeded, false if it failed because the
     *         client's ID was already in use.
     */
    bool receive_join(tcp::socket& client_socket);

    /**
     * Helper for joining an existing group; receives the View and parameters from the leader.
     * @return True if the leader informed this node that it is in total restart mode, false otherwise
     */
    bool receive_configuration(node_id_t my_id, tcp::socket& leader_connection);

    // View-management triggers
    /** Called when there is a new failure suspicion. Updates the suspected[]
     * array and, for the leader, proposes new views to exclude failed members. */
    void new_suspicion(DerechoSST& gmsSST);
    /** Runs only on the group leader; proposes new views to include new members. */
    void leader_start_join(DerechoSST& gmsSST);
    /** Runs on non-leaders to redirect confused new members to the current leader. */
    void redirect_join_attempt(DerechoSST& gmsSST);
    /** Runs only on the group leader and updates num_committed when all non-failed
     * members have acked a proposed view change. */
    void leader_commit_change(DerechoSST& gmsSST);
    /** Updates num_acked to acknowledge a proposed change when the leader increments
     * num_changes. Mostly intended for non-leaders, but also runs on the leader. */
    void acknowledge_proposed_change(DerechoSST& gmsSST);
    /** Runs when at least one membership change has been committed by the leader, and
     * wedges the current view in preparation for a new view. Ends by awaiting the
     * "meta-wedged" state and registers start_view_change() to trigger when meta-wedged
     * is true. */
    void start_meta_wedge(DerechoSST& gmsSST);
    /** Runs when all live nodes have reported they have wedged the current view
     * (meta-wedged), and does ragged edge cleanup to finalize the terminated epoch.
     * Determines if the next view will be adequate, and only proceeds to start a view change if it will be. */
    void terminate_epoch(std::shared_ptr<std::map<subgroup_id_t, SubgroupSettings>> next_subgroup_settings,
                         uint32_t next_num_received_size,
                         DerechoSST& gmsSST);
    /** Finishes installing the new view, assuming it is adequately provisioned.
     * Sends the new view and necessary Replicated Object state to new members,
     * sets up the new SST and MulticastGroup instances, and calls the new-view upcalls. */
    void finish_view_change(std::shared_ptr<std::map<subgroup_id_t, uint32_t>> follower_subgroups_and_shards,
                            std::shared_ptr<std::map<subgroup_id_t, SubgroupSettings>> next_subgroup_settings,
                            uint32_t next_num_received_size,
                            DerechoSST& gmsSST);

    /** Helper method for completing view changes; determines whether this node
     * needs to send Replicated Object state to each node that just joined, and then
     * sends the state if necessary. */
    void send_objects_to_new_members(const std::vector<std::vector<int64_t>>& old_shard_leaders);

    /** Sends a single subgroup's replicated object to a new member after a view change. */
    void send_subgroup_object(subgroup_id_t subgroup_id, node_id_t new_node_id);

    // Static helper methods that implement chunks of view-management functionality
    static void deliver_in_order(const View& Vc, const int shard_leader_rank,
                                 const subgroup_id_t subgroup_num, const uint32_t nReceived_offset,
                                 const std::vector<node_id_t>& shard_members, uint num_shard_senders,
                                 std::shared_ptr<spdlog::logger> logger);
    static void leader_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                           const uint32_t num_received_offset,
                                           const std::vector<node_id_t>& shard_members,
                                           uint num_shard_senders,
                                           std::shared_ptr<spdlog::logger> logger,
                                           const std::vector<node_id_t>& next_view_members);
    static void follower_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                             uint shard_leader_rank,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members,
                                             uint num_shard_senders,
                                             std::shared_ptr<spdlog::logger> logger);

    static bool suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old);
    static void copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old);
    static bool changes_contains(const DerechoSST& gmsSST, const node_id_t q);
    static int min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed);

    /**
     * Constructs the next view from the current view and the set of committed
     * changes in the SST.
     * @param curr_view The current view, which the proposed changes are relative to
     * @param gmsSST The SST containing the proposed/committed changes
     * @param logger A logger for printing out debug information
     * @return A View object for the next view
     */
    static std::unique_ptr<View> make_next_view(const std::unique_ptr<View>& curr_view,
                                                const DerechoSST& gmsSST,
                                                std::shared_ptr<spdlog::logger> logger);
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
                                                const std::vector<ip_addr>& joiner_ips,
                                                std::shared_ptr<spdlog::logger> logger);

    /**
     * Updates curr_view and makes a new next_view based on the current set of
     * rejoining nodes during total restart.
     * @param waiting_join_sockets The set of connections to restarting nodes
     * @param rejoined_node_ids The IDs of those nodes
     * @return The next view that will be installed if the restart continues at this point
     */
    std::unique_ptr<View> update_curr_and_next_restart_view(const std::map<node_id_t, tcp::socket>& waiting_join_sockets,
                                                            const std::set<node_id_t>& rejoined_node_ids);
    //Setup/constructor helpers
    /** Constructor helper method to encapsulate spawning the background threads. */
    void create_threads();
    /** Constructor helper method to encapsulate creating all the predicates. */
    void register_predicates();
    /** Constructor helper that reads logged ragged trim information from disk,
     * called only if there is also a logged view on disk from a previous failed group. */
    void load_ragged_trim();
    /** Constructor helper for the leader when it first starts; waits for enough
     * new nodes to join to make the first view adequately provisioned. */
    void await_first_view(const node_id_t my_id,
                          std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                          uint32_t& num_received_size);
    /** Constructor helper for the leader when it is restarting from complete failure;
     * waits for a majority of nodes from the last known view to join. */
    void await_rejoining_nodes(const node_id_t my_id,
                               std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                               uint32_t& num_received_size);

    /** Helper function for total restart mode: Uses the RaggedTrim values
     * in logged_ragged_trim to truncate any persistent logs that have a
     * persisted version later than the last committed version in the RaggedTrim. */
    void truncate_persistent_logs();

    /** Performs one-time global initialization of RDMC and SST, using the current view's membership. */
    void initialize_rdmc_sst();

    /**
     * Creates the SST and MulticastGroup for the first time, using the current view's member list.
     * @param callbacks The custom callbacks to supply to the MulticastGroup
     * @param derecho_params The initial DerechoParams to supply to the MulticastGroup
     * @param subgroup_settings The subgroup settings map to supply to the MulticastGroup
     * @param num_received_size The size of the num_received field in the SST (derived from subgroup_settings)
     */
    void construct_multicast_group(CallbackSet callbacks,
                                   const DerechoParams& derecho_params,
                                   const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                   const uint32_t num_received_size);

    /** Sets up the SST and MulticastGroup for a new view, based on the settings in the current view,
     * and copies over the SST data from the current view. */
    void transition_multicast_group(const std::map<subgroup_id_t, SubgroupSettings>& new_subgroup_settings,
                                    const uint32_t new_num_received_size);
    /**
     * Initializes the current View with subgroup information, and creates the
     * subgroup-settings map that MulticastGroup's constructor needs based on
     * this information. If the current View is inadequate based on the subgroup
     * allocation functions, it will be marked as inadequate and no subgroup
     * settings will be provided.
     * @param prev_view The previous View, which may be null if the current view is the first one
     * @param curr_view A mutable reference to the current View, which will have its SubViews initialized
     * @param subgroup_settings A mutable reference to the subgroup settings map, which will be filled out
     * @return num_received_size for the SST based on the computed subgroup membership
     */
    uint32_t make_subgroup_maps(const std::unique_ptr<View>& prev_view,
                                View& curr_view,
                                std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings);

    /**
     * Creates the subgroup-settings map that MulticastGroup's constructor needs
     * (and the num_received_size for the SST) based on the subgroup information
     * already in curr_view. Also reinitializes curr_view's my_subgroups to
     * indicate which subgroups this node belongs to. This function is only used
     * during total restart, when a joining node receives a View that already
     * has subgroup_shard_views populated.
     * @param curr_view A mutable reference to the current View, which will have its
     * my_subgroups corrected
     * @param subgroup_settings A mutable reference to the subgroup settings map,
     * which will be filled in by this function
     * @return num_received_size for the SST based on the current View's subgroup membership
     */
    uint32_t derive_subgroup_settings(View& curr_view,
                                      std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings);
    /** The persistence request func is from persistence manager*/
    persistence_manager_callbacks_t persistence_manager_callbacks;

    /**
     * Recomputes num_received_size (the length of the num_received column in
     * the SST) for an existing provisioned View, without re-running the
     * subgroup membership functions. Used in total restart to set up an SST
     * when all you have is a logged View.
     * @param view The View to compute num_received_size for, based on its SubViews
     * @return The length to provide to DerechoSST for num_received_size
     */
    static uint32_t compute_num_received_size(const View& view);
    /** Constructs a map from node ID -> IP address from the parallel vectors in the given View. */
    static std::map<node_id_t, ip_addr> make_member_ips_map(const View& view);
    /**
     * Constructs a map from subgroup type -> index -> shard -> node ID of that shard's leader.
     * If a shard has no leader in the current view (because it has no members), the vector will
     * contain -1 instead of a node ID
     */
    static std::map<std::type_index, std::vector<std::vector<int64_t>>> make_shard_leaders_map(const View& view);
    /**
     * Translates the old shard leaders' indices from (type, index) pairs to new subgroup IDs.
     * An entry in this vector will have value -1 if there was no old leader for that shard.
     */
    static std::vector<std::vector<int64_t>> translate_types_to_ids(
            const std::map<std::type_index, std::vector<std::vector<int64_t>>>& old_shard_leaders_by_type,
            const View& new_view);

public:

    static const int RESTART_LEADER_TIMEOUT = 300000;
    /**
     * Constructor for a new group where this node is the GMS leader.
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions defining subgroup membership
     * for this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
     * @param object_reference_map A mutable reference to the list of
     * ReplicatedObject references in Group, so that ViewManager can access it
     * while Group manages the list
     * @param _persistence_manager_callbacks The persistence manager callbacks.
     * @param _view_upcalls Any extra View Upcalls to be called when a view
     * changes.
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    ViewManager(const node_id_t my_id,
                const ip_addr my_ip,
                CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                const DerechoParams& derecho_params,
                ReplicatedObjectReferenceMap& object_reference_map,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = derecho_gms_port);

    /**
     * Constructor for joining an existing group, assuming the caller has already
     * opened a socket to the group's leader.
     * @param my_id The node ID of this node
     * @param leader_connection A Socket connected to the leader on its
     * group-management service port.
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions defining subgroup membership
     * in this group. Must be the same as the SubgroupInfo used to set up the
     * leader.
     * @param object_reference_map A mutable reference to the list of
     * ReplicatedObject references in Group, so that ViewManager can access it
     * while Group manages the list
     * @param _persistence_manager_callbacks The persistence manager callbacks
     * @param _view_upcalls Any extra View Upcalls to be called when a view
     * changes.
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    ViewManager(const node_id_t my_id,
                tcp::socket& leader_connection,
                CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                ReplicatedObjectReferenceMap& object_reference_map,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = derecho_gms_port);

    ~ViewManager();

    /**
     * Completes first-time setup of the ViewManager, including synchronizing
     * the initial SST and delivering the first new-view upcalls. This must be
     * separate from the constructor to resolve the circular dependency of SST
     * synchronization.
     * @param group_tcp_sockets The TCP connection pool that is shared with
     * Group and RPCManager. This also must be set after the constructor to
     * resolve a circular dependency: The RPC port for these sockets comes from
     * DerechoParams, which is received from the leader in the constructor.
     */
    void finish_setup(const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets);

    /** Starts predicate evaluation in the current view's SST. Call this only
     * when all other setup has been done for the managed Derecho group.
     * @param old_shard_leaders_for_restart The list of shard leaders from the
     * previous view, which may have just been received from the leader if this
     * node is not the leader. A parameter only needed if the group is doing
     * total restart. This is an ugly hack, there must be a better way. */
    void start(const std::unique_ptr<std::vector<std::vector<int64_t>>>& old_shard_leaders_for_restart);

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Creates and returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();
    /** Gets a pointer into the managed DerechoGroup's send buffer, at a
     * position where there are at least payload_size bytes remaining in the
     * buffer. The returned pointer can be used to write a message into the
     * buffer. */
    char* get_sendbuffer_ptr(subgroup_id_t subgroup_num, long long unsigned int payload_size,
                             int pause_sending_turns = 0, bool cooked_send = false,
                             bool null_send = false);
    /** Instructs the managed DerechoGroup's to send the next message. This
     * returns immediately; the send is scheduled to happen some time in the future. */
    void send(subgroup_id_t subgroup_num);

    const uint64_t compute_global_stability_frontier(subgroup_id_t subgroup_num);

    /**
     * @return a reference to the current View, wrapped in a container that
     * holds a read-lock on it. This is mostly here to make it easier for
     * the Group that contains this ViewManager to set things up.
     */
    SharedLockedReference<View> get_current_view();

    /** Gets a read-only reference to the DerechoParams settings,
     * in case other components need to see them after construction time. */
    const DerechoParams& get_derecho_params() const { return derecho_params; }

    /** Adds another function to the set of "view upcalls," which are called
     * when the view changes to notify another component of the new view. */
    void add_view_upcall(const view_upcall_t& upcall);

    /** Reports to the GMS that the given node has failed. */
    void report_failure(const node_id_t who);
    /** Waits until all members of the group have called this function. */
    void barrier_sync();

    /**
     * Registers a function that will initialize all the RPC objects at this node,
     * given a new view and a list of the shard leaders in the previous view (needed
     * to download object state). ViewManger will call it after it has installed a new
     * view.
     */
    void register_initialize_objects_upcall(initialize_rpc_objects_t upcall) {
        initialize_subgroup_objects = std::move(upcall);
    }

    void debug_print_status() const;
};

} /* namespace derecho */
