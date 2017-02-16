/**
 * @file ViewManager.h
 *
 * @date Feb 6, 2017
 * @author edward
 */
#pragma once

#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "view.h"
#include "tcp/tcp.h"
#include "logger.h"

#include "mutils-serialization/SerializationSupport.hpp"

namespace derecho {

template <typename T>
class Replicated;

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

class ViewManager {
private:
    using pred_handle = sst::Predicates<DerechoSST>::pred_handle;

    using view_upcall_t = std::function<void(std::vector<node_id_t> new_members,
                                             std::vector<node_id_t> old_members)>;

    using send_objects_upcall_t = std::function<void(tcp::socket&)>;

    //Allow RPCManager and Replicated to access curr_view and view_mutex directly
    friend class rpc::RPCManager;
    template<typename T>
    friend class Replicated;


    /** The port that this instance of the GMS communicates on. */
    const int gms_port;

    /** Lock this before accessing curr_view, since it's shared by multiple threads */
    std::mutex view_mutex;
    /** Notified when curr_view changes (i.e. we are finished with a pending view change).*/
    std::condition_variable view_change_cv;

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

    /** Name of the file to use to persist the current view to disk. */
    std::string view_file_name;


    /** Functions to be called whenever the view changes, to report the
     * new view to some other component. */
    std::vector<view_upcall_t> view_upcalls;
    const SubgroupInfo subgroup_info;
    DerechoParams derecho_params;

    send_objects_upcall_t send_subgroup_objects;

    /** Sends a joining node the new view that has been constructed to include it.*/
    void commit_join(const View& new_view,
                     tcp::socket& client_socket);

    bool has_pending_join() { return pending_join_sockets.locked().access.size() > 0; }

    /** Assuming this node is the leader, handles a join request from a client.*/
    void receive_join(tcp::socket& client_socket);

    /** Helper for joining an existing group; receives the View and parameters from the leader. */
    void receive_configuration(node_id_t my_id, tcp::socket& leader_connection);

    // Ken's helper methods
    void deliver_in_order(const View& Vc, const int shard_leader_rank,
                          const uint32_t subgroup_num, const uint32_t nReceived_offset,
                          const std::vector<node_id_t>& shard_members);
    void ragged_edge_cleanup(View& Vc);
    void leader_ragged_edge_cleanup(View& Vc, const uint32_t subgroup_num,
                                    const uint32_t num_received_offset,
                                    const std::vector<node_id_t>& shard_members);
    void follower_ragged_edge_cleanup(View& Vc, const uint32_t subgroup_num,
                                      const uint32_t num_received_offset,
                                      const std::vector<node_id_t>& shard_members);

    static bool suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old);
    static void copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old);
    static bool changes_contains(const DerechoSST& gmsSST, const node_id_t q);
    static int min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed);

    /** Constructor helper method to encapsulate spawning the background threads. */
    void create_threads();
    /** Constructor helper method to encapsulate creating all the predicates. */
    void register_predicates();
    /** Constructor helper called when creating a new group; waits for a new
     * member to join, then sends it the view. */
    void await_second_member(const node_id_t my_id);

    /** Creates the SST and MulticastGroup for the current view, using the current view's member list.
     * The parameters are all the possible parameters for constructing MulticastGroup. */
    void setup_derecho(CallbackSet callbacks,
                       const DerechoParams& derecho_params);
    /** Sets up the SST and MulticastGroup for a new view, based on the settings in the current view
     * (and copying over the SST data from the current view). */
    void transition_sst_and_rdmc(View& newView);
    void rdmc_sst_setup();
    uint32_t calc_num_received_size(std::vector<uint32_t> members);
    uint32_t calc_total_num_subgroups() const;
    static std::map<node_id_t, ip_addr> make_member_ips_map(const View& view);
public:
    /**
     * Constructor for a new group where this node is the GMS leader.
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
     * @param _view_upcalls
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    ViewManager(const ip_addr my_ip,
          CallbackSet callbacks,
          const SubgroupInfo& subgroup_info,
          const DerechoParams& derecho_params,
          std::vector<view_upcall_t> _view_upcalls = {},
          const int gms_port = 12345);

    /**
     * Constructor for joining an existing group, assuming the caller has already
     * contacted the group's leader and received a View and DerechoParams.
     * @param existing_config A pair containing the current view and DerechoParams
     * (by pointer). Awkwardly must be a pair because these are returned from the
     * join_existing function and there's no way to "unpack" a pair into a
     * parameter list.
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param _view_upcalls
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    ViewManager(const node_id_t my_id,
                tcp::socket& leader_connection,
                CallbackSet callbacks,
                const SubgroupInfo& subgroup_info,
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = 12345);

    /**
     * Constructor for recovering a failed node by loading its View from log
     * files.
     * @param recovery_filename The base name of the set of recovery files to
     * use (extensions will be added automatically)
     * @param my_id The node ID of the node executing this code
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions to use for message
     * delivery events once the group has been re-joined
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
                std::experimental::optional<DerechoParams> _derecho_params = std::experimental::optional<DerechoParams>{},
                std::vector<view_upcall_t> _view_upcalls = {},
                const int gms_port = 12345);

    ~ViewManager();

    /** Finishes initializing the ViewManager and starts the GMS (i.e. starts evaluating predicates). */
    void start();

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Creates and returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();
    /** Gets a pointer into the managed DerechoGroup's send buffer, at a
     * position where there are at least payload_size bytes remaining in the
     * buffer. The returned pointer can be used to write a message into the
     * buffer. (Analogous to MulticastGroup::get_position) */
    char* get_sendbuffer_ptr(uint32_t subgroup_num, long long unsigned int payload_size,
                             int pause_sending_turns = 0, bool cooked_send = false);
    /** Instructs the managed MulticastGroup to send the next message. This
     * returns immediately; the send is scheduled to happen some time in the future. */
    void send(uint32_t subgroup_num);

    /**
     * @return a reference to the current View, to make it easier for the
     * Group that contains this ViewManager to set things up.
     */
    View& get_current_view();

    /** Adds another function to the set of "view upcalls," which are called
     * when the view changes to notify another component of the new view. */
    void add_view_upcall(const view_upcall_t& upcall);

    /** Reports to the GMS that the given node has failed. */
    void report_failure(const node_id_t who);
    /** Waits until all members of the group have called this function. */
    void barrier_sync();


    void register_send_objects_upcall(send_objects_upcall_t upcall) {
        send_subgroup_objects = std::move(upcall);
    }

    void debug_print_status() const;
    static void log_event(const std::string& event_text) {
        util::debug_log().log_event(event_text);
    }
    static void log_event(const std::stringstream& event_text) {
        util::debug_log().log_event(event_text);
    }
    void print_log(std::ostream& output_dest) const;


};

} /* namespace derecho */

