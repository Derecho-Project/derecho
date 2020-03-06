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

#include "../subgroup_info.hpp"
#include "../view.hpp"
#include "derecho_internal.hpp"
#include "locked_reference.hpp"
#include "multicast_group.hpp"
#include "restart_state.hpp"
#include <derecho/conf/conf.hpp>

#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <spdlog/spdlog.h>

namespace derecho {

/*--- Forward declarations ---*/

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
 * A simple POD message that the group leader sends back to a new node in
 * response to a JoinRequest. Includes a JoinResponseCode and the leader's
 * node ID.
 */
struct JoinResponse {
    JoinResponseCode code;
    node_id_t leader_id;
};

/**
 * A simple POD message that new nodes send to the group leader to indicate that
 * they want to join the group.
 */
struct JoinRequest {
    node_id_t joiner_id;
    bool is_external;
};

/**
 * A set of status codes that an external client can send to any member of the
 * group indicating the type of request it is making. External clients send this
 * after sending a JoinRequest with is_external=true.
 */
enum class ExternalClientRequest {
    GET_VIEW,      //!< GET_VIEW The external client wants to download the current View
    ESTABLISH_P2P  //!< ESTABLISH_P2P The external client wants to set up a P2P connection with this node
};

template <typename T>
using SharedLockedReference = LockedReference<std::shared_lock<std::shared_timed_mutex>, T>;

using view_upcall_t = std::function<void(const View&)>;

/** Type of a 2-dimensional vector used to store potential node IDs, or -1 */
using vector_int64_2d = std::vector<std::vector<int64_t>>;

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

    friend class PersistenceManager;

    friend class RestartLeaderState;

    /**
     * Mutex to protect the curr_view pointer. Non-SST-predicate threads that
     * access the current View through the pointer should acquire a shared_lock;
     * the view change predicates will acquire a unique_lock before swapping the
     * pointer. */
    std::shared_timed_mutex view_mutex;
    /** Notified when curr_view changes (i.e. we are finished with a pending view change).*/
    std::condition_variable_any view_change_cv;

    /** The current View, containing the state of the managed group.
     *  Must be a pointer so we can re-assign it, but will never be null.*/
    std::unique_ptr<View> curr_view;
    /** May hold a pointer to the partially-constructed next view, if we are
     *  in the process of transitioning to a new view. */
    std::unique_ptr<View> next_view;

    /** contains client sockets for pending requests that have not yet been handled.*/
    LockedQueue<tcp::socket> pending_new_sockets;
    /** On the leader node, contains client sockets for pending joins that have not yet been handled.*/
    std::list<std::pair<node_id_t, tcp::socket>> pending_join_sockets;
    /** The sockets connected to clients that will join in the next view, if any */
    std::list<std::pair<node_id_t, tcp::socket>> proposed_join_sockets;

    /** Contains old Views that need to be cleaned up. */
    std::queue<std::unique_ptr<View>> old_views;
    std::mutex old_views_mutex;
    std::condition_variable old_views_cv;

    /** A cached copy of the last known value of this node's suspected[] array.
     * Helps the SST predicate detect when there's been a change to suspected[].*/
    std::vector<bool> last_suspected;

    /** The TCP socket the leader uses to listen for joining clients */
    tcp::connection_listener server_socket;
    /** A flag to signal background threads to shut down; set to true when the group is destroyed. */
    std::atomic<bool> thread_shutdown;
    /** The background thread that listens for clients connecting on our server socket. */
    std::thread client_listener_thread;
    std::thread old_view_cleanup_thread;

    /**
     * A user-configurable option that disables the checks for partitioning events.
     * It defaults to false, because disabling them is unsafe, but some users might
     * want to do this for testing purposes.
     */
    const bool disable_partitioning_safety;

    //Handles for all the predicates the GMS registered with the current view's SST.
    pred_handle leader_suspicion_handle;
    pred_handle follower_suspicion_handle;
    pred_handle start_join_handle;
    pred_handle new_sockets_handle;
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

    /** The same set of TCP sockets used by Group and RPCManager. */
    std::shared_ptr<tcp::tcp_connections> tcp_sockets;

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
    /**
     * True if any of the Replicated<T> objects in this group have a Persistent<T>
     * field, false if none of them do
     */
    const bool any_persistent_objects;

    /** Set to true in the constructor if this node must do a total restart
     * before completing group setup; false otherwise. */
    bool in_total_restart;

    /** If this node is the restart leader and currently doing a total restart,
     * this object contains state related to the restart, including curr_view.
     * Otherwise this will be a null pointer. */
    std::unique_ptr<RestartLeaderState> restart_leader_state_machine;

    /** If this node is currently doing a total restart, this object contains
     * state related to restarting, such as the current logged ragged trim.
     * Otherwise this will be a null pointer. */
    std::unique_ptr<RestartState> restart_state;

    /**
     * True if this node is the current leader and is fully active (i.e. has
     * finished "waking up"), false otherwise.
     */
    bool active_leader;

    /** The persistence request func is from persistence manager*/
    persistence_manager_callbacks_t persistence_manager_callbacks;

    /**
     * A 2-dimensional vector, indexed by (subgroup ID -> shard number),
     * containing the ID of the node in each shard that was its leader
     * in the prior view, or -1 if that shard had no state in the prior view.
     * Only used for state transfer during initial startup and total restart,
     * may be empty otherwise.
     */
    std::vector<std::vector<int64_t>> prior_view_shard_leaders;

    /**
     * On a graceful exit, nodes will be agree to leave at some point, where
     * the view manager should stop throw exception on "failure". Set 
     * 'bSilence' to keep the view manager calm on detecting intended node
     * "failure."
     */
    std::atomic<bool> bSilent = false;

    std::function<void(const std::vector<uint32_t>&)> add_external_connection_upcall;

    bool has_pending_new() { return pending_new_sockets.locked().access.size() > 0; }
    bool has_pending_join() { return pending_join_sockets.size() > 0; }

    /* ---------------------------- View-management triggers ---------------------------- */
    /**
     * Called on non-leaders when there is a new failure suspicion. Updates the
     * suspected[] and failed[] arrays but does not propose any changes.
     */
    void new_suspicion(DerechoSST& gmsSST);
    /**
     * A gateway that handles any socket connections, exchanges version code,
     * reads JoinRequest and then decides whether to propose changes, redirect
     * to leader, or handle as an external connection request.
     */
    void process_new_sockets();
    /**
     * Runs only on the group leader; called whenever there is either a new
     * suspicion or a new join attempt, and proposes a batch of changes to
     * add and remove members. This always wedges the current view.
     */
    void propose_changes(DerechoSST& gmsSST);

    /** Runs on non-leaders to redirect confused new members to the current leader. */
    void redirect_join_attempt(tcp::socket& client_socket);
    /** Handles join request from external clients. */
    void external_join_handler(tcp::socket& client_socket, const node_id_t& joiner_id);
    /**
     * Runs once on a node that becomes a leader due to a failure. Searches for
     * and re-proposes changes proposed by prior leaders, as well as suspicions
     * noticed by this node before it became the leader.
     */
    void new_leader_takeover(DerechoSST& gmsSST);
    /**
     * Runs only on the group leader and updates num_committed when all non-failed
     * members have acked a proposed view change.
     */
    void leader_commit_change(DerechoSST& gmsSST);
    /**
     * Updates num_acked to acknowledge a proposed change when the leader increments
     * num_changes. Mostly intended for non-leaders, but also runs on the leader.
     */
    void acknowledge_proposed_change(DerechoSST& gmsSST);
    /**
     * Runs when at least one membership change has been committed by the leader, and
     * wedges the current view in preparation for a new view. Ends by awaiting the
     * "meta-wedged" state and registers terminate_epoch() to trigger when meta-wedged
     * is true.
     */
    void start_meta_wedge(DerechoSST& gmsSST);
    /**
     * Runs when all live nodes have reported they have wedged the current view
     * (meta-wedged), and starts ragged edge cleanup to finalize the terminated epoch.
     * Determines if the next view will be adequate, and only proceeds to start a
     * view change if it will be.
     */
    void terminate_epoch(DerechoSST& gmsSST);
    /**
     * Runs when the leader nodes of each subgroup have finished ragged edge
     * cleanup. Echoes the global_min they have posted in the SST to
     * acknowledge it.
     * @param follower_subgroups_and_shards A list of subgroups this node is a
     * non-leader in, and the corresponding shard number for this node
     */
    void echo_ragged_trim(std::shared_ptr<std::map<subgroup_id_t, uint32_t>> follower_subgroups_and_shards,
                          DerechoSST& gmsSST);
    /**
     * Delivers messages that were marked deliverable by the ragged trim and
     * proceeds to finish_view_change() when this is done. Runs after every
     * non-leader node has echoed the subgroup leaders' ragged trims.
     */
    void deliver_ragged_trim(DerechoSST& gmsSST);
    /**
     * Finishes installing the new view, assuming it is adequately provisioned.
     * Sends the new view and necessary Replicated Object state to new members,
     * sets up the new SST and MulticastGroup instances, and calls the new-view
     * upcalls.
     */
    void finish_view_change(DerechoSST& gmsSST);

    /* ---------------------------------------------------------------------------------- */
    /* ------------------- Helper methods for view-management triggers ------------------ */

    /**
     * Assuming this node is the leader, handles a join request from a client.
     * @param client_socket A TCP socket connected to the joining client
     * @return True if the join succeeded, false if it failed because the
     *         client's ID was already in use.
     */
    bool receive_join(DerechoSST& gmsSST, const node_id_t joiner_id, tcp::socket& client_socket);

    /**
     * Assuming the suspected[] array in the SST has changed, searches through
     * it to find new suspicions, marks the suspected nodes as failed in the
     * current View, and wedges the current View.
     * @return A list of the SST ranks corresponding to nodes that have just
     * been marked as failed (i.e. the new suspicions)
     */
    std::vector<int> process_suspicions(DerechoSST& gmsSST);
    /**
     * Updates the TCP connections pool to reflect the joined and departed
     * members in a new view. Removes connections to departed members, and
     * initializes new connections to joined members.
     * @param new_view The new view that is about to be installed.
     */
    void update_tcp_connections(const View& new_view);

    /** Helper method for completing view changes; determines whether this node
     * needs to send Replicated Object state to each node that just joined, and then
     * sends the state if necessary. */
    void send_objects_to_new_members(const View& new_view, const vector_int64_2d& old_shard_leaders);

    /** Sends a single subgroup's replicated object to a new member after a view change. */
    void send_subgroup_object(subgroup_id_t subgroup_id, node_id_t new_node_id);

    /** Sends a joining node the new view that has been constructed to include it.*/
    void send_view(const View& new_view, tcp::socket& client_socket);

    /**
     * Reads the global_min values for the specified subgroup (and the shard
     * that this node belongs to) from the SST, creates a ragged trim vector
     * with these values, and persists the ragged trim to disk
     * @param shard_leader_rank The rank of the leader node in this node's shard
     * of the specified subgroup
     * @param subgroup_num The subgroup ID to compute the ragged trim for
     * @param num_received_offset The offset into the SST's num_received field
     * that corresponds to the specified subgroup's entries in it
     * @param num_shard_senders The number of nodes in that shard that are active
     * senders in the current epoch
     */
    void log_ragged_trim(const int shard_leader_rank,
                         const subgroup_id_t subgroup_num,
                         const uint32_t num_received_offset,
                         const uint num_shard_senders);
    /**
     * Reads the global_min for the specified subgroup from the SST (assuming it
     * has been computed already) and tells the current View's MulticastGroup to
     * deliver messages up to the global_min (i.e. the computed ragged trim).
     * @param shard_leader_rank The rank of the leader node in this node's shard
     * of the specified subgroup
     * @param subgroup_num The subgroup ID to deliver messages in
     * @param num_received_offset The offset into the SST's num_received field
     * that corresponds to the specified subgroup's entries in it
     * @param shard_members The IDs of the members of this node's shard in the
     * specified subgroup
     * @param num_shard_senders The number of nodes in that shard that are active
     * senders in the current epoch
     */
    void deliver_in_order(const int shard_leader_rank,
                          const subgroup_id_t subgroup_num, const uint32_t num_received_offset,
                          const std::vector<node_id_t>& shard_members, uint num_shard_senders);
    /**
     * Implements the Ragged Edge Cleanup algorithm for a subgroup/shard leader,
     * operating on the shard that this node is a member of. This computes the
     * last safely-deliverable message from each sender in the shard and places
     * it in this node's SST row in the global_min field.
     * @param subgroup_num The subgroup ID of the subgroup to do cleanup on
     * @param num_received_offset The offset into the SST's num_received field
     * that corresponds to the specified subgroup's entries in it
     * @param shard_members The IDs of the members of this node's shard in the
     * specified subgroup
     * @param num_shard_senders The number of nodes in that shard that are active
     * senders in the current epoch
     */
    void leader_ragged_edge_cleanup(const subgroup_id_t subgroup_num,
                                    const uint32_t num_received_offset,
                                    const std::vector<node_id_t>& shard_members,
                                    uint num_shard_senders);
    /**
     * Implements the Ragged Edge Cleanup algorithm for a non-leader node in a
     * subgroup. This simply waits for the leader to write a value to global_min
     * and then copies and uses it.
     * @param subgroup_num The subgroup ID of the subgroup to do cleanup on
     * @param shard_leader_rank The rank of the leader node in this node's shard
     * of the specified subgroup
     * @param num_received_offset The offset into the SST's num_received field
     * that corresponds to the specified subgroup's entries in it
     * @param num_shard_senders The number of nodes in that shard that are active
     * senders in the current epoch
     */
    void follower_ragged_edge_cleanup(const subgroup_id_t subgroup_num,
                                      uint shard_leader_rank,
                                      const uint32_t num_received_offset,
                                      uint num_shard_senders);

    /* -- Static helper methods that implement chunks of view-management functionality -- */
    static bool suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old);
    static void copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old);
    static bool changes_contains(const DerechoSST& gmsSST, const node_id_t q);
    static bool changes_includes_end_of_view(const DerechoSST& gmsSST, const int rank_of_leader);
    static int min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed);
    static bool previous_leaders_suspected(const DerechoSST& gmsSST, const View& curr_view);

    /**
     * Searches backwards from this node's row in the SST to lower-ranked rows,
     * looking for proposed changes not in this node's changes list, assuming
     * this node is the current leader and the lower-ranked rows are failed
     * prior leaders. If a lower-ranked row has more changes, or different
     * changes, copies that node's changes array to the local row.
     * @param gmsSST
     * @return True if there was a prior leader with changes to copy, false if
     * no prior proposals were found.
     */
    static bool copy_prior_leader_proposals(DerechoSST& gmsSST);

    /**
     * Constructs the next view from the current view and the set of committed
     * changes in the SST.
     * @param curr_view The current view, which the proposed changes are relative to
     * @param gmsSST The SST containing the proposed/committed changes
     * @param logger A logger for printing out debug information
     * @return A View object for the next view
     */
    static std::unique_ptr<View> make_next_view(const std::unique_ptr<View>& curr_view,
                                                const DerechoSST& gmsSST);

    /* ---------------------------------------------------------------------------------- */

    /* ------------------------ Setup/constructor helpers ------------------------------- */

    /** Constructor helper method to encapsulate spawning the background threads. */
    void create_threads();
    /** Constructor helper method to encapsulate creating all the predicates. */
    void register_predicates();
    /** Constructor helper that reads logged ragged trim information from disk,
     * called only if there is also a logged view on disk from a previous failed group. */
    void load_ragged_trim();
    /** Constructor helper for the leader when it first starts; waits for enough
     * new nodes to join to make the first view adequately provisioned. */
    void await_first_view(const node_id_t my_id);

    /** Constructor helper for non-leader nodes; encapsulates receiving and
     * deserializing a View, DerechoParams, and state-transfer leaders (old
     * shard leaders) from the leader. */
    void receive_view_and_leaders(const node_id_t my_id, tcp::socket& leader_connection);

    /** Performs one-time global initialization of RDMC and SST, using the current view's membership. */
    void initialize_rdmc_sst();
    /**
     * Helper for joining an existing group; receives the View and parameters from the leader.
     */
    void receive_initial_view(const node_id_t my_id, tcp::socket& leader_connection);

    /**
     * Constructor helper that initializes TCP connections (for state transfer)
     * to the members of initial_view in ascending rank order. Assumes that no TCP
     * connections have been set up yet.
     * @param initial_view The View to use for membership
     */
    void setup_initial_tcp_connections(const View& initial_view, const node_id_t my_id);

    /**
     * Another setup helper for joining nodes; re-initializes the TCP connections
     * list to reflect the current list of members in initial_view, assuming that the
     * first view was aborted and a new one has been sent.
     * @param initial_view The View whose membership the TCP connections should be
     * updated to reflect
     */
    void reinit_tcp_connections(const View& initial_view, const node_id_t my_id);
    /**
     * Creates the SST and MulticastGroup for the first time, using the current view's member list.
     * @param callbacks The custom callbacks to supply to the MulticastGroup
     * @param subgroup_settings The subgroup settings map to supply to the MulticastGroup
     * @param num_received_size The size of the num_received field in the SST (derived from subgroup_settings)
     */
    void construct_multicast_group(CallbackSet callbacks,
                                   const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                   const uint32_t num_received_size,
                                   const uint32_t slot_size);

    /**
     * Sets up the SST and MulticastGroup for a new view, based on the settings in the current view,
     * and copies over the SST data from the current view.
     * @param new_subgroup_settings The subgroup settings map to supply to the MulticastGroup;
     * this needs to change to account for the new subgroup/shard membership in the new view
     * @param new_num_received_size The size of the num_recieved field in the new SST
     */
    void transition_multicast_group(const std::map<subgroup_id_t, SubgroupSettings>& new_subgroup_settings,
                                    const uint32_t new_num_received_size,
                                    const uint32_t new_slot_size);
    /**
     * Initializes curr_view with subgroup information based on the membership
     * functions in subgroup_info. If curr_view would be inadequate based on
     * the subgroup allocation functions, it will be marked as inadequate.
     * @param subgroup_info The SubgroupInfo (containing subgroup membership
     * functions) to use to provision subgroups
     * @param prev_view The previous View, which may be null if the current view
     * is the first one
     * @param curr_view A mutable reference to the current View, which will have
     * its SubViews initialized
     */
    static void make_subgroup_maps(const SubgroupInfo& subgroup_info,
                                   const std::unique_ptr<View>& prev_view,
                                   View& curr_view);

    /**
     * Creates the subgroup-settings map that MulticastGroup's constructor needs
     * (and the num_received_size for the SST) based on the subgroup information
     * already in curr_view. Also reinitializes curr_view's my_subgroups to
     * indicate which subgroups this node belongs to.
     * @param curr_view A mutable reference to the current View, which will have its
     * my_subgroups corrected
     * @param subgroup_settings A mutable reference to the subgroup settings map,
     * which will be filled in by this function
     * @return num_received_size and slot_size for the SST based on the current View's subgroup membership
     */
    std::pair<uint32_t, uint32_t> derive_subgroup_settings(View& curr_view,
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

    /**
     * Constructs a map from node ID -> (IP address, port) for a specific port from
     * the members and member_ips_and_ports vectors in the given View.
     */
    static std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>
    make_member_ips_and_ports_map(const View& view, const PortType port);
    /**
     * Constructs a vector mapping subgroup ID in the new view -> shard number
     * -> node ID of that shard's leader in the old view. If a shard had no
     * leader in the old view, or is a RawObject shard (which does not do state
     * transfer), the "node ID" for that shard will be -1.
     */
    static vector_int64_2d old_shard_leaders_by_new_ids(const View& curr_view, const View& next_view);

    /**
     * A little convenience method that receives a 2-dimensional vector using
     * our standard network protocol, which first sends the buffer size and then
     * a serialized buffer.
     * @param socket The socket to read from
     * @return A 2-dimensional vector of ValueType
     * @tparam ValueType The type of values in the vector; this assumes ValueType
     * can be serialized by mutils
     */
    template <typename ValueType>
    static std::unique_ptr<std::vector<std::vector<ValueType>>> receive_vector2d(tcp::socket& socket) {
        std::size_t buffer_size;
        socket.read(buffer_size);
        if(buffer_size == 0) {
            return std::make_unique<std::vector<std::vector<ValueType>>>();
        }
        char buffer[buffer_size];
        socket.read(buffer, buffer_size);
        return mutils::from_bytes<std::vector<std::vector<ValueType>>>(nullptr, buffer);
    }

public:
    /**
     * Constructor for a new group where this node is the GMS leader.
     * @param my_ip The IP address of the node executing this code
     * @param subgroup_info The set of functions defining subgroup membership
     * for this group.
     * @param group_tcp_sockets The pool of TCP connections to each group member
     * that is shared with Group.
     * @param object_reference_map A mutable reference to the list of
     * ReplicatedObject references in Group, so that ViewManager can access it
     * while Group manages the list
     * @param _persistence_manager_callbacks The persistence manager callbacks.
     * @param _view_upcalls Any extra View Upcalls to be called when a view
     * changes.
     */
    ViewManager(const SubgroupInfo& subgroup_info,
                const std::vector<std::type_index>& subgroup_type_order,
                const bool any_persistent_objects,
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
                const SubgroupInfo& subgroup_info,
                const std::vector<std::type_index>& subgroup_type_order,
                const bool any_persistent_objects,
                const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
                ReplicatedObjectReferenceMap& object_reference_map,
                const persistence_manager_callbacks_t& _persistence_manager_callbacks,
                std::vector<view_upcall_t> _view_upcalls = {});

    ~ViewManager();

    /**
     * Setup method for the leader when it is restarting from complete failure:
     * waits for a restart quorum of nodes from the last known view to join.
     */
    void await_rejoining_nodes(const node_id_t my_id);

    /**
     * Setup method for non-leader nodes: checks whether the initial View received
     * in the constructor gets committed by the leader, and if not, waits for another
     * initial View to be sent.
     * @param leader_connection
     * @return True if the initial View was committed, false if it was aborted
     * and a new View has been received.
     */
    bool check_view_committed(tcp::socket& leader_connection);

    /**
     * Setup method for the leader node in total restart mode: sends a Prepare
     * message to all non-leader nodes indicating that state transfer has finished.
     * Also checks to see whether any non-leader nodes have failed in the meantime.
     * This function simply does nothing if this node is not in total restart mode.
     * @param leader_has_quorum A mutable reference to a bool that will be set to
     * False if the leader realizes it no longer has a restart quorum due to
     * failures.
     * @return True if all non-leader nodes are still alive, False if there was
     * a failure.
     */
    bool leader_prepare_initial_view(bool& leader_has_quorum);

    /**
     * Setup method for the leader node: sends a commit message to all non-leader
     * nodes indicating that it is safe to use the initial View.
     */
    void leader_commit_initial_view();

    /**
     * An extra setup step only needed during total restart; truncates the
     * persistent logs of this node to conform to the ragged trim decided
     * on by the restart leader. This function does nothing if this node is not
     * in total restart mode.
     */
    void truncate_logs();

    /**
     * An extra setup method only needed during total restart. Sends Replicated
     * Object data (most importantly, the persistent logs) to all members of a
     * shard if this node is listed as that shard's leader. This function does
     * nothing if this node is not in total restart mode.
     */
    void send_logs();

    /**
     * Sets up RDMA sessions for the multicast groups within this group. This
     * should only be called once the initial view is committed by the leader.
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     */
    void initialize_multicast_groups(CallbackSet callbacks);

    /**
     * Completes first-time setup of the ViewManager, including synchronizing
     * the initial SST and delivering the first new-view upcalls. This assumes
     * the initial view has been committed and initialize_multicast_groups has
     * finished.
     */
    void finish_setup();

    void register_add_external_connection_upcall(const std::function<void(const std::vector<uint32_t>&)>& upcall) {
        add_external_connection_upcall = upcall;
    }

    /**
     * Starts predicate evaluation in the current view's SST. Call this only
     * when all other setup has been done for the Derecho group.
     */
    void start();

    /**
     * @return The list of shard leaders in the previous view that this node
     * received along with curr_view when it joined the group. Needed by Group
     * to complete state transfer.
     */
    const vector_int64_2d& get_old_shard_leaders() const { return prior_view_shard_leaders; }

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();
    /** Returns the order of this node in the sequence of members of the group */
    int32_t get_my_rank();
    /** Returns a vector of vectors listing the members of a single subgroup
     * (identified by type and index), organized by shard number. */
    std::vector<std::vector<node_id_t>> get_subgroup_members(subgroup_type_id_t subgroup_type, uint32_t subgroup_index);
    std::size_t get_number_of_shards_in_subgroup(subgroup_type_id_t subgroup_type, uint32_t subgroup_index);
    /**
     * If this node is a member of the given subgroup (identified by its type
     * and index), returns the number of the shard this node belongs to.
     * Otherwise, returns -1.
     */
    int32_t get_my_shard(subgroup_type_id_t subgroup_type, uint32_t subgroup_index);
    /** Instructs the managed DerechoGroup's to send the next message. This
     * returns immediately in sending through RDMC; the send is scheduled to happen some time in the future.
     * if sending through SST, the RDMA write is issued in this call*/
    void send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
              const std::function<void(char* buf)>& msg_generator, bool cooked_send = false);

    const uint64_t compute_global_stability_frontier(subgroup_id_t subgroup_num);

    /**
     * @return a reference to the current View, wrapped in a container that
     * holds a read-lock on the View pointer. This allows the Group that
     * contains this ViewManager to look at the current View (and set it up
     * during construction) without creating an unsafe interleaving with
     * View changes.
     */
    SharedLockedReference<View> get_current_view();

    /**
     * An ugly workaround function needed only during initial setup during a
     * total restart. The Group constructor needs to read the members of the
     * currently-proposed initial View in order to construct Replicated Objects,
     * but on the restart leader the initial View is stored in restart_leader_state_machine,
     * not curr_view.
     * @return A reference to the initial View to use to set up Replicated Objects,
     * which is either the "current view" on a joining node or the "restart view" on
     * the restart leader.
     */
    SharedLockedReference<const View> get_current_or_restart_view();

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

    /**
     * stop complaining about node failures.
     */
    void silence();

    void debug_print_status() const;

    // UGLY - IMPROVE LATER
    std::map<subgroup_id_t, uint64_t> max_payload_sizes;
    std::map<subgroup_id_t, uint64_t> get_max_payload_sizes();
    // max of max_payload_sizes
    uint64_t view_max_rpc_reply_payload_size = 0;
    uint32_t view_max_rpc_window_size = 0;
};

} /* namespace derecho */
