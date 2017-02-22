#pragma once


#include <chrono>
#include <ctime>
#include <cstdint>
#include <experimental/optional>
#include <exception>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>
#include <iostream>

#include "tcp/tcp.h"

#include "logger.h"
#include "replicated.h"
#include "rpc_manager.h"
#include "view_manager.h"
#include "derecho_exception.h"
#include "subgroup_info.h"
#include "raw_subgroup.h"

#include "mutils-containers/TypeMap2.hpp"
#include "mutils-containers/KindMap.hpp"

namespace derecho {

/**
 * The top-level object for creating a Derecho group. This implements the group
 * management service (GMS) features and contains a MulticastGroup instance that
 * manages the actual sending and tracking of messages within the group.
 * @tparam ReplicatedObjects The types of user-provided objects that will represent
 * state and RPC functions for subgroups of this group.
 */
template <typename... ReplicatedObjects>
class Group {
private:
    using pred_handle = sst::Predicates<DerechoSST>::pred_handle;

    //The type of a map from subgroup index -> Replicated<T>
    template<typename T>
    using replicated_index_map = std::map<uint32_t, Replicated<T>>;

    /** Contains all state related to managing Views, including the
     * ManagedGroup and SST (since those change when the view changes). */
    ViewManager view_manager;
    /** Contains all state related to receiving and handling RPC function
     * calls for any Replicated objects implemented by this group. */
    rpc::RPCManager rpc_manager;
    /** Maps a type to the Factory for that type. */
    mutils::KindMap<Factory, ReplicatedObjects...> factories;
    /** Maps each type T to a map of <index, Replicated<T>> for that type's
     * subgroup(s). If this node is not a member of a subgroup for a type, the
     * Replicated<T> will be invalid/empty. If this node is a member of a subgroup,
     * the Replicated<T> will refer to the one shard that this node belongs to. */
    mutils::KindMap<replicated_index_map, ReplicatedObjects...> replicated_objects;
    /** The map of <index, RawSubgroup> for the subgroups of type RawObject. */
    std::map<uint32_t, RawSubgroup> raw_subgroups;

    /* get_subgroup is actually implemented in these two methods. This is an
     * ugly hack to allow us to specialize get_subgroup<RawObject> to behave differently than
     * get_subgroup<T>. The unnecessary unused parameter is for overload selection. */
    template<typename SubgroupType>
    Replicated<SubgroupType>& get_subgroup(SubgroupType*, uint32_t subgroup_index);
    RawSubgroup& get_subgroup(RawObject*, uint32_t subgroup_index);

    /** Constructor helper that wires together the component objects of Group. */
    void set_up_components();

    /**
     * Constructor helper that constructs RawSubgroup objects for each subgroup
     * of type RawObject; called to initialize the raw_subgroups map.
     * @param my_id The Node ID of this node
     * @param subgroup_info The structure describing subgroup membership
     * @return A map containing a RawSubgroup for each "raw" subgroup the user
     * requested.
     */
    std::map<uint32_t, RawSubgroup> construct_raw_subgroups(node_id_t my_id, const SubgroupInfo& subgroup_info);

    /** Base case for construct_objects template. */
	template<typename...>
    void construct_objects(node_id_t my_id, const SubgroupInfo& subgroup_info){}

    /**
     * Constructor helper that unpacks the template parameter pack. Constructs
     * Replicated<T> wrappers for each object being replicated, and saves each
     * Factory<T> in a map so they can be used again if more subgroups are
     * added when the group expands.
     * @param my_id The Node ID of this node
     * @param subgroup_info The structure describing subgroup membership
     * @param curr_factory The current Factory<ReplicatedObject> being considered
     * @param rest_factories The rest of the template parameter pack
     */
    template<typename FirstType, typename... RestTypes>
    void construct_objects(node_id_t my_id, const SubgroupInfo& subgroup_info,
                           Factory<FirstType> curr_factory, Factory<RestTypes>... rest_factories) {
        factories.template get<FirstType>() = curr_factory;
        std::vector<node_id_t> members(view_manager.get_current_view().members);
        std::type_index subgroup_type(typeid(FirstType));
        uint32_t subgroups_of_type = subgroup_info.num_subgroups.at(subgroup_type);
        for(uint32_t subgroup_index = 0; subgroup_index < subgroups_of_type; ++subgroup_index){
            //Find out if this node is in any shard of this subgroup
            bool in_subgroup = false;
            uint32_t num_shards = subgroup_info.num_shards.at({subgroup_type, subgroup_index});
            for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
                std::vector<node_id_t> members = subgroup_info.subgroup_membership(
                        view_manager.get_current_view(), subgroup_type,
                        subgroup_index, shard_num);
                //"If this node is in subgroup_membership() for this shard"
                if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                     in_subgroup = true;
                     subgroup_id_t subgroup_id = view_manager.get_subgroup_ids_by_type()
                             .at({subgroup_type, subgroup_index});
                     replicated_objects.template get<FirstType>().emplace(subgroup_index,
                             Replicated<FirstType>(my_id, subgroup_id, rpc_manager, curr_factory));
                     break; //This node can be in at most one shard
                }
            }
            if(!in_subgroup) {
                //Put a default-constructed Replicated() in the map
                replicated_objects.template get<FirstType>()[subgroup_index];
            }
        }

        construct_objects<RestTypes...>(my_id, subgroup_info, rest_factories...);
    }

    /**
     * Delegate constructor for joining an existing managed group, called after
     * the entry-point constructor constructs a socket that connects to the leader.
     * @param my_id The node ID of the node running this code
     * @param leader_connection A socket connected to the existing group's leader
     * @param callbacks
     * @param subgroup_info
     * @param _view_upcalls
     * @param gms_port
     * @param factories
     */
    Group(const node_id_t my_id,
          tcp::socket leader_connection,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls,
          const int gms_port,
          Factory<ReplicatedObjects>... factories);

public:
    /**
     * Constructor that starts a new managed Derecho group with this node as
     * the leader (ID 0). The DerechoParams will be passed through to construct
     * the  underlying DerechoGroup. If they specify a filename, the group will
     * run in persistent mode and log all messages to disk.
     * @param my_ip The IP address of the node executing this code
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param derecho_params The assorted configuration parameters for this
     * Derecho group instance, such as message size and logfile name
     * @param _view_upcalls
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     *
     */
    Group(const ip_addr my_ip,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          const DerechoParams& derecho_params,
          std::vector<view_upcall_t> _view_upcalls = {},
          const int gms_port = 12345,
          Factory<ReplicatedObjects>... factories);

    /**
     * Constructor that joins an existing managed Derecho group. The parameters
     * normally set by DerechoParams will be initialized by copying them from
     * the existing group's leader.
     * @param my_id The node ID of the node running this code
     * @param my_ip The IP address of the node running this code
     * @param leader_id The node ID of the existing group's leader
     * @param leader_ip The IP address of the existing group's leader
     * @param callbacks The set of callback functions for message delivery
     * events in this group.
     * @param _view_upcalls
     * @param gms_port The port to contact other group members on when sending
     * group-management messages
     */
    Group(const node_id_t my_id,
          const ip_addr my_ip,
          const ip_addr leader_ip,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls = {},
          const int gms_port = 12345,
          Factory<ReplicatedObjects>... factories);
    /**
     * Constructor that re-starts a failed group member from log files.
     * It assumes the local ".paxosstate" file already contains the last known
     * view, obtained from a quorum of members, and that any messages missing
     * from the local log have already been appended from the longest log of a
     * member of the last known view. (This can be accomplished by running the
     * script log_recovery_helper.sh). Does NOT currently attempt to replay
     * completion events for missing messages that were transferred over from
     * another member's log.
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
    Group(const std::string& recovery_filename,
          const node_id_t my_id,
          const ip_addr my_ip,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::experimental::optional<DerechoParams> _derecho_params = std::experimental::optional<DerechoParams>{},
          std::vector<view_upcall_t> _view_upcalls = {},
          const int gms_port = 12345,
          Factory<ReplicatedObjects>... factories);

    ~Group();

    /**
     * Gets the "handle" for the subgroup of the specified type and index, which
     * is either a Replicated<T> or a RawSubgroup. If this node is a member of
     * the desired subgroup, the Replicated<T> will contain the replicated
     * state of an object of type T and be usable to send multicasts to this node's
     * shard of the subgroup. If this node is not a member of the subgroup, it
     * will be an invalid/empty Replicated<T>.
     * @param subgroup_index The index of the subgroup within the set of
     * subgroups that replicate the same type of object. Defaults to 0, so
     * if there is only one subgroup of type T, it can be retrieved with
     * get_subgroup<T>();
     * @tparam SubgroupType The object type identifying the subgroup
     * @return A reference to either a Replicated<SubgroupType> or a RawSubgroup
     * for this subgroup
     */
    template<typename SubgroupType>
    auto& get_subgroup(uint32_t subgroup_index = 0);

    /**
     * Serializes and sends the state of all replicated objects that represent
     * subgroups this node is a member of. (This sends the state of the object
     * itself, not the Replicated<T> that wraps it).
     * @param receiver_socket The socket that should receive the serialized
     * objects.
     */
    void send_objects(tcp::socket& receiver_socket);

    /**
     * Updates the state of all replicated objects that correspond to subgroups
     * this node is a member of, replacing them with the objects received over
     * the given TCP socket.
     * @param sender_socket The socket that is sending serialized objects to
     * this node.
     */
    void receive_objects(tcp::socket& sender_socket);

    /** Causes this node to cleanly leave the group by setting itself to "failed." */
    void leave();
    /** Creates and returns a vector listing the nodes that are currently members of the group. */
    std::vector<node_id_t> get_members();

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
};

} /* namespace derecho */

#include "group_impl.h"
