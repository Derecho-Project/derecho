#ifndef MANAGED_GROUP_H_
#define MANAGED_GROUP_H_

#include <chrono>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "logger.h"
#include "rdmc/connection.h"
#include "view.h"

namespace derecho {

/**
 * Base exception class for all exceptions raised by Derecho.
 */
struct derecho_exception : public std::exception {
public:
    const std::string message;
    derecho_exception(const std::string& message) : message(message) {}

    const char* what() const noexcept { return message.c_str(); }
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
 * A wrapper for DerechoGroup that adds the group management service (GMS)
 * features. All members of a group should create instances of ManagedGroup
 * (instead of creating DerechoGroup directly) in order to enable the GMS.
 */
template <typename dispatcherType>
class ManagedGroup {
private:
    using pred_handle =
        typename View<dispatcherType>::DerechoSST::Predicates::pred_handle;

    using view_upcall_t = std::function<void(vector<node_id_t> new_members,
                                             vector<node_id_t> old_members)>;
    static constexpr int MAX_MEMBERS = View<dispatcherType>::MAX_MEMBERS;

    /** Contains client sockets for all pending joins, except the current one.*/
    LockedQueue<tcp::socket> pending_joins;

    /** Contains old Views that need to be cleaned up*/
    std::queue<std::unique_ptr<View<dispatcherType>>> old_views;
    std::mutex old_views_mutex;
    std::condition_variable old_views_cv;

    /** The socket connected to the client that is currently joining, if any */
    tcp::socket joining_client_socket;
    /** The node ID that has been assigned to the client that is currently joining, if any. */
    node_id_t joining_client_id;
    /** A cached copy of the last known value of this node's suspected[] array.
     * Helps the SST predicate detect when there's been a change to suspected[].*/
    std::vector<bool> last_suspected;

    /** The port that this instance of the GMS communicates on. */
    const int gms_port;

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

    /** Name of the file to use to persist the current view (and other parameters) to disk. */
    std::string view_file_name;

    /** Lock this before accessing curr_view, since it's shared by multiple threads */
    std::mutex view_mutex;
    /** Notified when curr_view changes (i.e. we are finished with a pending view change).*/
    std::condition_variable view_change_cv;

    /** The current View, containing the state of the managed group.
     *  Must be a pointer so we can re-assign it.*/
    std::unique_ptr<View<dispatcherType>> curr_view;
    /** May hold a pointer to the partially-constructed next view, if we are
     *  in the process of transitioning to a new view. */
    std::unique_ptr<View<dispatcherType>> next_view;

    dispatcherType dispatchers;
    std::vector<view_upcall_t> view_upcalls;

    DerechoParams derecho_params;
    /** Sends a joining node the new view that has been constructed to include it.*/
    void commit_join(const View<dispatcherType>& new_view,
                     tcp::socket& client_socket);

    bool has_pending_join() { return pending_joins.locked().access.size() > 0; }

    /** Assuming this node is the leader, handles a join request from a client.*/
    void receive_join(tcp::socket& client_socket);

    /** Starts a new Derecho group with this node as the only member, and initializes the GMS. */
    std::unique_ptr<View<dispatcherType>> start_group(const node_id_t my_id, const ip_addr my_ip);
    /** Joins an existing Derecho group, initializing this object to participate in its GMS. */
    std::unique_ptr<View<dispatcherType>> join_existing(const node_id_t my_id, const ip_addr& leader_ip, const int leader_port);

    // Ken's helper methods
    void deliver_in_order(const View<dispatcherType>& Vc, int Leader);
    void ragged_edge_cleanup(View<dispatcherType>& Vc);
    void leader_ragged_edge_cleanup(View<dispatcherType>& Vc);
    void follower_ragged_edge_cleanup(View<dispatcherType>& Vc);

    static bool suspected_not_equal(const typename View<dispatcherType>::DerechoSST& gmsSST, const std::vector<bool>& old);
    static void copy_suspected(const typename View<dispatcherType>::DerechoSST& gmsSST, std::vector<bool>& old);
    static bool changes_contains(const typename View<dispatcherType>::DerechoSST& gmsSST, const node_id_t q);
    static int min_acked(const typename View<dispatcherType>::DerechoSST& gmsSST, const std::vector<char>& failed);

    /** Constructor helper method to encapsulate spawning the background threads. */
    void create_threads();
    /** Constructor helper method to encapsulate creating all the predicates. */
    void register_predicates();

    /** Creates the SST and derecho_group for the current view, using the current view's member list.
     * The parameters are all the possible parameters for constructing derecho_group. */
    void setup_derecho(std::vector<MessageBuffer>& message_buffers,
                       CallbackSet callbacks,
                       const DerechoParams& derecho_params);
    /** Sets up the SST and derecho_group for a new view, based on the settings in the current view
     * (and copying over the SST data from the current view). */
    void transition_sst_and_rdmc(View<dispatcherType>& newView, int whichFailed);

public:
    /**
     * Constructor, starts or joins a managed Derecho group.
     * The parameters after leader_id are the parameters for the DerechoGroup
     * that should be constructed for communications within this managed group.
     * If a filename is specified, the group will run in persistent mode and log
     * all messages to disk.
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     * @param global_ip_map A map specifying all of the potential members in the
     * group, mapping their node IDs to their IP addresses
     * @param my_id The node ID of the node executing this code
     * @param leader_id The node ID of the GMS leader
     */
    ManagedGroup(const ip_addr my_ip,
                 dispatcherType _dispatchers,
                 CallbackSet callbacks,
                 const DerechoParams derecho_params,
                 std::vector<view_upcall_t> _view_upcalls = {},
                 const int gms_port = 12345);

    ManagedGroup(const node_id_t my_id,
                 const ip_addr my_ip,
                 const node_id_t leader_id,
                 const ip_addr leader_ip,
                 dispatcherType _dispatchers,
                 CallbackSet callbacks,
                 std::vector<view_upcall_t> _view_upcalls = {},
                 const int gms_port = 12345);
    /**
     * Constructor that re-starts a failed group member from log files.
     * It assumes the local ".paxosstate" file already contains the last known
     * view, obtained from a quorum of members, and that any messages missing
     * from the local log have already been appended from the longest log of a
     * member of the last known view. (This can be accomplished by running the
     * script log_recovery_helper.sh). Does NOT currently attempt to replay
     * completion events for missing messages that were transferred over from
     * another member's log.
     * The last 5 parameters are the callbacks and DerechoGroup parameters
     * to use for sending messages once recovery is complete.
     * @param recovery_filename The base name of the set of recovery files to
     * use (extensions will be added automatically)
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     * @param my_id The node ID of the node executing this code
     * @param _max_payload_size
     * @param stability_callbacks
     * @param _block_size
     * @param _window_size
     * @param _type
     */
    ManagedGroup(const std::string& recovery_filename,
                 const node_id_t my_id,
                 const ip_addr my_ip,
                 dispatcherType _dispatchers,
                 CallbackSet callbacks,
                 DerechoParams derecho_params,
                 std::vector<view_upcall_t> _view_upcalls = {},
                 const int gms_port = 12345);

    ~ManagedGroup();

    void rdmc_sst_setup();
    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Creates and returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();
    /** Gets a pointer into the managed DerechoGroup's send buffer, at a
     * position where there are at least payload_size bytes remaining in the
     * buffer. The returned pointer can be used to write a message into the
     * buffer. (Analogous to DerechoGroup::get_position) */
    char* get_sendbuffer_ptr(long long unsigned int payload_size,
                             int pause_sending_turns = 0, bool cooked_send = false);
    /** Instructs the managed DerechoGroup to send the next message. This
     * returns immediately; the send is scheduled to happen some time in the future. */
    void send();
    template <typename IdClass, unsigned long long tag, typename... Args>
    void orderedSend(const vector<node_id_t>& nodes, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    void orderedSend(Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto orderedQuery(const vector<node_id_t>& nodes, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto orderedQuery(Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    void p2pSend(node_id_t dest_node, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto p2pQuery(node_id_t dest_node, Args&&... args);
    /** Reports to the GMS that the given node has failed. */
    void report_failure(const node_id_t who);
    /** Waits until all members of the group have called this function. */
    void barrier_sync();
    void debug_print_status() const;
    static void log_event(const std::string& event_text) {
        util::debug_log().log_event(event_text);
    }
    static void log_event(const std::stringstream& event_text) {
        util::debug_log().log_event(event_text);
    }
    void print_log(std::ostream& output_dest) const;
    std::map<node_id_t, ip_addr> get_member_ips_map(std::vector<node_id_t>& members, std::vector<ip_addr>& member_ips, std::vector<char> failed);
};

} /* namespace derecho */

#include "managed_group_impl.h"

#endif /* MANAGED_GROUP_H_ */
