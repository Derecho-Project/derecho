/**
 * @file ViewManager.h
 *
 * @date Feb 6, 2017
 */
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "conf/conf.hpp"
#include "derecho_internal.h"
#include "locked_reference.h"
#include "multicast_group.h"
#include "restart_state.h"
#include "subgroup_info.h"
#include "view.h"

#include <mutils-serialization/SerializationSupport.hpp>
#include <spdlog/spdlog.h>

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
    OK,              //!< OK The new member can proceed to join as normal.
    TOTAL_RESTART,   //!< TOTAL_RESTART The group is currently restarting from a total failure, so the new member should send its logged view and ragged trim
    ID_IN_USE,       //!< ID_IN_USE The node's ID is already listed as a member of the current view, so it can't join.
    LEADER_REDIRECT  //!< LEADER_REDIRECT This node is not actually the leader and can't accept a join.
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

    friend class RestartLeaderState;

    whenlog(std::shared_ptr<spdlog::logger> logger;);

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
    /** The subgroup membership function, which will be called whenever the view changes. */
    const SubgroupInfo subgroup_info;
    /** Indicates the order that the subgroups should be provisioned;
     * set by Group to be the same order as its template parameters. */
    std::vector<std::type_index> subgroup_type_order;
    //Parameters stored here, in case we need them again after construction
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

    /** State related to restarting, such as the current logged ragged trim;
     * null if this node is not currently doing a total restart. */
    std::unique_ptr<RestartState> restart_state;

    /** The persistence request func is from persistence manager*/
    persistence_manager_callbacks_t persistence_manager_callbacks;

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

    /**
     * Helper for total restart mode that re-initializes TCP connections (in the
     * tcp_connections pool) to all of the "current" members of curr_view. This
     * is needed because Group's update_tcp_connections_callback will only
     * initialize TCP connections with nodes in the joiners list, but after
     * total restart even nodes that are not "joining" the new view will need
     * their TCP connections initialized.
     */
    void restart_existing_tcp_connections(node_id_t my_id);

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

    /* -- Static helper methods that implement chunks of view-management functionality -- */
    static void deliver_in_order(const View& Vc, const int shard_leader_rank,
                                 const subgroup_id_t subgroup_num, const uint32_t nReceived_offset,
                                 const std::vector<node_id_t>& shard_members, uint num_shard_senders whenlog(, std::shared_ptr<spdlog::logger> logger));
    static void leader_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                           const uint32_t num_received_offset,
                                           const std::vector<node_id_t>& shard_members,
                                           uint num_shard_senders,
                                           whenlog(std::shared_ptr<spdlog::logger> logger, )
                                           const std::vector<node_id_t>& next_view_members);
    static void follower_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                             uint shard_leader_rank,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members,
                                             uint num_shard_senders
                                             whenlog(, std::shared_ptr<spdlog::logger> logger));

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
                                                const DerechoSST& gmsSST
                                                whenlog(, std::shared_ptr<spdlog::logger> logger));

    /* ---------------------------------------------------------------------------------- */

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
    void truncate_persistent_logs(const ragged_trim_map_t& logged_ragged_trim);

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
                                   const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                   const uint32_t num_received_size);

    /** Sets up the SST and MulticastGroup for a new view, based on the settings in the current view,
     * and copies over the SST data from the current view. */
    void transition_multicast_group(const std::map<subgroup_id_t, SubgroupSettings>& new_subgroup_settings,
                                    const uint32_t new_num_received_size);
    /**
     * Initializes curr_view with subgroup information based on the membership
     * functions in subgroup_info, and creates the subgroup-settings map that
     * MulticastGroup's constructor needs based on this information. If curr_view
     * would be inadequate based on the subgroup allocation functions, it will
     * be marked as inadequate and no subgroup settings will be provided.
     * @param subgroup_info The SubgroupInfo (containing subgroup membership
     * functions) to use to provision subgroups
     * @param prev_view The previous View, which may be null if the current view
     * is the first one
     * @param curr_view A mutable reference to the current View, which will have
     * its SubViews initialized
     * @param subgroup_settings A mutable reference to the subgroup settings map,
     * which will be filled out
     * @return num_received_size for the SST based on the computed subgroup membership
     */
    static uint32_t make_subgroup_maps(const SubgroupInfo& subgroup_info,
                                       const std::unique_ptr<View>& prev_view,
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
    static uint32_t derive_subgroup_settings(View& curr_view,
                                             std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings);

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
    template <PORT_TYPE port_index>
    static std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>
    make_member_ips_and_ports_map(const View& view) {
        std::map<node_id_t, std::pair<ip_addr_t, uint16_t>> member_ips_and_ports_map;
        size_t num_members = view.members.size();
        for(uint i = 0; i < num_members; ++i) {
            if(!view.failed[i]) {
                member_ips_and_ports_map[view.members[i]] = std::pair<ip_addr_t, uint16_t>{std::get<0>(view.member_ips_and_ports[i]), std::get<port_index>(view.member_ips_and_ports[i])};
            }
        }
        return member_ips_and_ports_map;
    }
    /**
     * Constructs a vector mapping subgroup ID in the new view -> shard number
     * -> node ID of that shard's leader in the old view. If a shard had no
     * leader in the old view, or is a RawObject shard (which does not do state
     * transfer), the "node ID" for that shard will be -1.
     */
    static std::vector<std::vector<int64_t>> old_shard_leaders_by_new_ids(const View& curr_view, const View& next_view);

public:
    /**
     * Constructor for a new group where this node is the GMS leader.
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions defining subgroup membership
     * for this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
     * @param group_tcp_sockets The pool of TCP connections to each group member
     * that is shared with Group.
     * @param object_reference_map A mutable reference to the list of
     * ReplicatedObject references in Group, so that ViewManager can access it
     * while Group manages the list
     * @param _persistence_manager_callbacks The persistence manager callbacks.
     * @param _view_upcalls Any extra View Upcalls to be called when a view
     * changes.
     */
    ViewManager(CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                const std::vector<std::type_index>& subgroup_type_order,
                const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
                ReplicatedObjectReferenceMap& object_reference_map,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {});

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
     * @param group_tcp_sockets The pool of TCP connections to each group member
     * that is shared with Group.
     * @param object_reference_map A mutable reference to the list of
     * ReplicatedObject references in Group, so that ViewManager can access it
     * while Group manages the list
     * @param _persistence_manager_callbacks The persistence manager callbacks
     * @param _view_upcalls Any extra View Upcalls to be called when a view
     * changes.
     */
    ViewManager(tcp::socket& leader_connection,
                CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                const std::vector<std::type_index>& subgroup_type_order,
                const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
                ReplicatedObjectReferenceMap& object_reference_map,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {});

    ~ViewManager();

    /**
     * Completes first-time setup of the ViewManager, including synchronizing
     * the initial SST and delivering the first new-view upcalls. This must be
     * separate from the constructor to resolve the circular dependency of SST
     * synchronization. This also provides a convenient way to give the
     * constructor a "return value" to hand back to its caller.
     * @return A copy of restart_shard_leaders, which was computed in the
     * constructor if we're in total restart mode, or an empty vector if we're
     * not in total restart mode. The Group leader constructor will need this information.
     */
    std::vector<std::vector<int64_t>> finish_setup();

    /**
     * An extra setup method only needed during total restart. Sends Replicated
     * Object data (most importantly, the persistent logs) to all members of a
     * shard if this node is listed as that shard's leader. This does nothing if
     * the node is not in total restart mode, but it must be called anyway
     * because only ViewManager knows if we are doing total restart.
     * @param restart_shard_leaders The list of shard leaders for total restart
     * received from the restart leader; these are the nodes with the longest logs.
     */
    void send_logs_if_total_restart(const std::unique_ptr<std::vector<std::vector<int64_t>>>& restart_shard_leaders);

    /** Starts predicate evaluation in the current view's SST. Call this only
     * when all other setup has been done for the Derecho group. */
    void start();

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();
    /** Returns the order of this node in the sequence of members of the group */
    int32_t get_my_rank();
    /** Returns a vector of vectors listing the members of a single subgroup
     * (identified by type and index), organized by shard number. */
    std::vector<std::vector<node_id_t>> get_subgroup_members(subgroup_type_id_t subgroup_type, uint32_t subgroup_index);
    /** Instructs the managed DerechoGroup's to send the next message. This
     * returns immediately in sending through RDMC; the send is scheduled to happen some time in the future.
     * if sending through SST, the RDMA write is issued in this call*/
    void send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
              const std::function<void(char* buf)>& msg_generator, bool cooked_send = false);

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
