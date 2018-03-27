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
#include "spdlog/spdlog.h"
#include "subgroup_info.h"
#include "tcp/tcp.h"
#include "view.h"

#include "mutils-serialization/SerializationSupport.hpp"

namespace derecho {

template <typename T>
class Replicated;
template <typename T>
class ExternalCaller;

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

template <typename T>
using SharedLockedReference = LockedReference<std::shared_lock<std::shared_timed_mutex>, T>;

using view_upcall_t = std::function<void(const View&)>;

class ViewManager {
private:
    using pred_handle = sst::Predicates<DerechoSST>::pred_handle;

    using send_object_upcall_t = std::function<void(subgroup_id_t, node_id_t)>;
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
    pred_handle change_commit_ready_handle;
    pred_handle leader_proposed_handle;
    pred_handle leader_committed_handle;

    /** Functions to be called whenever the view changes, to report the
     * new view to some other component. */
    std::vector<view_upcall_t> view_upcalls;
    //Parameters stored here, in case we need them again after construction
    const SubgroupInfo subgroup_info;
    DerechoParams derecho_params;

    /** A function that will be called to send replicated objects to a new
     * member of a subgroup after a view change. This abstracts away the RPC
     * functionality, which ViewManager shouldn't need to know about. */
    send_object_upcall_t send_subgroup_object;
    /** A function that will be called to initialize replicated objects
     * after transitioning to a new view, in the case where the previous
     * view was inadequately provisioned. */
    initialize_rpc_objects_t initialize_subgroup_objects;

    /** Sends a joining node the new view that has been constructed to include it.*/
    void commit_join(const View& new_view,
                     tcp::socket& client_socket);

    bool has_pending_join() { return pending_join_sockets.locked().access.size() > 0; }

    /** Assuming this node is the leader, handles a join request from a client.*/
    void receive_join(tcp::socket& client_socket);

    /** Helper for joining an existing group; receives the View and parameters from the leader. */
    void receive_configuration(node_id_t my_id, tcp::socket& leader_connection);

    // View-management triggers
    /** Called when there is a new failure suspicion. Updates the suspected[]
     * array and, for the leader, proposes new views to exclude failed members. */
    void new_suspicion(DerechoSST& gmsSST);
    /** Runs only on the group leader; proposes new views to include new members. */
    void leader_start_join(DerechoSST& gmsSST);
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
    /**  and finishes installing the new view. */
    void finish_view_change(std::shared_ptr<std::map<subgroup_id_t, uint32_t>> follower_subgroups_and_shards,
                            std::shared_ptr<std::map<subgroup_id_t, SubgroupSettings>> next_subgroup_settings,
                            uint32_t next_num_received_size,
                            DerechoSST& gmsSST);

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

    static std::unique_ptr<View> make_next_view(const std::unique_ptr<View>& curr_view,
                                                const DerechoSST& gmsSST,
                                                std::shared_ptr<spdlog::logger> logger);

    /** Constructor helper method to encapsulate spawning the background threads. */
    void create_threads();
    /** Constructor helper method to encapsulate creating all the predicates. */
    void register_predicates();
    /** Constructor helper for the leader when it first starts; waits for enough
     * new nodes to join to make the first view adequately provisioned. */
    void await_first_view(const node_id_t my_id,
                          std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                          uint32_t& num_received_size);
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

    /** The persistence request func is from persistence manager*/
    persistence_manager_callbacks_t persistence_manager_callbacks;

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
    /**
     * Constructor for a new group where this node is the GMS leader.
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param subgroup_info The set of functions defining subgroup membership
     * for this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
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
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = derecho_gms_port);

    /**
     * Constructor for recovering a failed node by loading its View from log
     * files.
     * @param recovery_filename The base name of the set of recovery files to
     * use (extensions will be added automatically)
     * @param my_id The node ID of the node executing this code
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions to use for message
     * delivery events once the group has been re-joined
     * @param _persistence_manager_callbacks
     * @param derecho_params (Optional) If set, and this node is the leader of
     * the restarting group, a new set of Derecho parameters to configure the
     * group with. Otherwise, these parameters will be read from the logfile or
     * copied from the existing group leader.
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    ViewManager(const std::string& recovery_filename,
                const node_id_t my_id,
                const ip_addr my_ip,
                CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                const DerechoParams& derecho_params = DerechoParams(0, 0),
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = derecho_gms_port);

    ~ViewManager();

    /** Completes first-time setup of the ViewManager, including synchronizing
     * the initial SST and delivering the first new-view upcalls. This must be
     * separate from the constructor to resolve the circular dependency of SST
     * synchronization. */
    void finish_setup();

    /** Starts predicate evaluation in the current view's SST. Call this only
     * when all other setup has been done for the managed Derecho group. */
    void start();

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

    /** Adds another function to the set of "view upcalls," which are called
     * when the view changes to notify another component of the new view. */
    void add_view_upcall(const view_upcall_t& upcall);

    /** Reports to the GMS that the given node has failed. */
    void report_failure(const node_id_t who);
    /** Waits until all members of the group have called this function. */
    void barrier_sync();

    /**
     * Registers a function that will send serializable object state from this node
     * to a new node in a specified subgroup and shard. ViewManager will call it when
     * it has installed a new view that adds a member to a shard for which this node
     * is the leader.
     */
    void register_send_object_upcall(send_object_upcall_t upcall) {
        send_subgroup_object = std::move(upcall);
    }

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

/**
 * Base case for functional_append, with one argument.
 */
template <typename T>
std::vector<T> functional_append(const std::vector<T>& original, const T& item) {
    std::vector<T> appended_vec(original);
    appended_vec.emplace_back(item);
    return appended_vec;
}

/**
 * Returns a new std::vector value that is equal to the parameter std::vector
 * with the rest of the arguments appended. Adds some missing functionality to
 * std::vector: the ability to append to a const vector without taking a
 * several-line detour to call the void emplace_back() method.
 * @param original The vector that should be the prefix of the new vector
 * @param first_item The first element to append to the original vector
 * @param rest_items The rest of the elements to append to the original vector
 * @return A new vector (by value), containing a copy of original plus all the
 * elements given as arguments.
 */
template <typename T, typename... RestArgs>
std::vector<T> functional_append(const std::vector<T>& original, const T& first_item, RestArgs... rest_items) {
    std::vector<T> appended_vec = functional_append(original, first_item);
    return functional_append(appended_vec, rest_items...);
}

} /* namespace derecho */
