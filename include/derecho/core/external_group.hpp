#pragma once

#include "detail/connection_manager.hpp"
#include "detail/p2p_connection_manager.hpp"
#include "group.hpp"
#include "view.hpp"
#include "notification.hpp"
#include "derecho/conf/conf.hpp"

#include <functional>
#include <iostream>
#include <unordered_map>
#include <memory>
#include <map>
#include <thread>
#include <mutex>
#include <queue>
#include <exception>

namespace derecho {

template <typename... ReplicatedTypes>
class ExternalGroupClient;

using namespace rpc;

/**
 * This class represents a "handle" for communicating with a specific type of
 * subgroup using its RPC functions. It can be used to send P2P RPC messages to
 * a node in that subgroup using the P2P connections in ExternalGroupClient.
 */
template <typename T, typename ExternalGroupType>
class ExternalClientCaller {
private:
    /** The ID of this node */
    const node_id_t node_id;
    /** The internally-generated subgroup ID of the subgroup that this ExternalClientCaller will contact. */
    subgroup_id_t subgroup_id;
    /** A reference to the ExternalGroupClient that this ExternalClientCaller will use to send P2P messages */
    ExternalGroupType& group_client;
    /** The actual implementation of ExternalCaller, which has lots of ugly template parameters */
    std::unique_ptr<rpc::RemoteInvokerFor<T>> wrapped_this;

    std::unordered_map<node_id_t, std::unique_ptr<T>> support_map;
    std::unordered_map<node_id_t, std::unique_ptr<rpc::RemoteInvocableOf<T>>> remote_invocable_ptr_map;

public:
    /**
     * Constructs an ExternalClientCaller that can communicate with members of
     * a specific subgroup, identified by its subgroup type and subgroup ID.
     * @param type_id A number uniquely identifying the type of the subgroup
     * (i.e. the subgroup type's index in the group's template parameters)
     * @param nid The "node ID" of this external client. Should match the ID in
     * ExternalGroupClient.
     * @param subgroup_id The ID of the particular subgroup that this client
     * will communicate with
     * @param group_client A reference back to the ExternalGroupClient that
     * created this ExternalClientCaller
     */
    ExternalClientCaller(subgroup_type_id_t type_id, node_id_t nid, subgroup_id_t subgroup_id, ExternalGroupType& group_client);

    ExternalClientCaller(ExternalClientCaller&&) = default;
    ExternalClientCaller(const ExternalClientCaller&) = delete;

    void register_notification(std::function<void(const derecho::Bytes&)>, node_id_t nid);
    void add_p2p_connections(node_id_t dest_node);
    /**
     * Sends a peer-to-peer message to a single member of the subgroup that
     * this ExternalClientCaller connects to, invoking the RPC function
     * identified by the FunctionTag template parameter.
     * @param dest_node The ID of the node that the P2P message should be sent to
     * @param args The arguments to the RPC function being invoked
     * @return An instance of rpc::QueryResults<Ret>, where Ret is the return type
     * of the RPC function being invoked
     */
    template <rpc::FunctionTag tag, typename... Args>
    auto p2p_send(node_id_t dest_node, Args&&... args);
};

/**
 * This class acts as an external (non-group-member) client for a Derecho group
 * with the specified subgroup types. It maintains a local copy of the group's
 * current View and a set of P2P RDMA connections to some of the group's
 * members - specifically, the members that it has recently communicated with.
 * It also runs a P2P listening thread to listen for responses to the messages
 * it sends to group members.
 *
 * @tparam ReplicatedTypes A list of subgroup types that matches the Derecho
 * group this client will contact. To communicate with a Group<A, B, C>, you
 * must construct an ExternalGroupClient<A, B, C>
 */
template <typename... ReplicatedTypes>
class ExternalGroupClient {
private:
    template <typename T, typename ExternalGroupType>
    friend class ExternalClientCaller;
    const node_id_t my_id;
    std::unique_ptr<View> prev_view;
    std::unique_ptr<View> curr_view;
    std::unique_ptr<sst::P2PConnectionManager> p2p_connections;
    std::unique_ptr<std::map<rpc::Opcode, rpc::receive_fun_t>> receivers;
    std::map<subgroup_id_t, std::list<std::weak_ptr<AbstractPendingResults>>> fulfilled_pending_results;
    std::map<subgroup_id_t, uint64_t> max_payload_sizes;

    template <typename T>
    using external_caller_index_map = std::map<uint32_t, ExternalClientCaller<T, ExternalGroupClient<ReplicatedTypes...>>>;
    mutils::KindMap<external_caller_index_map, ReplicatedTypes...> external_callers;

    /**
     * requests a new view from group member nid
     * if nid is -1, then request a view from CONF_DERECHO_LEADER_IP
     * defined in derecho.cfg
     */
    bool get_view(const node_id_t nid);
    void clean_up();
    volatile char* get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type);
    void finish_p2p_send(node_id_t dest_id, subgroup_id_t dest_subgroup_id, std::weak_ptr<AbstractPendingResults> pending_results_handle);
    uint32_t get_index_of_type(const std::type_info& ti) const;


    /** ======================== copy/paste from rpc_manager ======================== **/
    std::atomic<bool> thread_shutdown{false};
    std::thread rpc_listener_thread;
    /** p2p send and queries are queued in fifo worker */
    std::thread request_worker_thread;
    struct p2p_req {
        node_id_t sender_id;
        char* msg_buf;
        uint32_t buffer_size;
        p2p_req() : sender_id(0),
                    msg_buf(nullptr) {}
        p2p_req(node_id_t _sender_id,
                 char* _msg_buf) : sender_id(_sender_id),
                                   msg_buf(_msg_buf) {}
    };
    std::queue<p2p_req> p2p_request_queue;
    std::mutex request_queue_mutex;
    std::condition_variable request_queue_cv;
    mutils::RemoteDeserialization_v rdv;
    void p2p_receive_loop();
    void p2p_request_worker();
    void p2p_message_handler(node_id_t sender_id, char* msg_buf);
    std::exception_ptr receive_message(const rpc::Opcode& indx, const node_id_t& received_from,
                                       char const* const buf, std::size_t payload_size,
                                       const std::function<char*(int)>& out_alloc);
    /** ======================== copy/paste from rpc_manager ======================== **/

public:
    ExternalGroupClient(std::vector<DeserializationContext*> deserialization_contexts = {});
    virtual ~ExternalGroupClient();

    /**
     * Get a handle for external client calls to a specific subgroup.
     * @tparam SubgroupType         The type of the interested subgroup
     * @param subgroup_index        The index of the interested subgroup
     * @return      An external client caller handle for the given subgroup
     */
    template <typename SubgroupType>
    ExternalClientCaller<SubgroupType, ExternalGroupClient<ReplicatedTypes...>>& get_subgroup_caller(uint32_t subgroup_index = 0);
    /**
     * Pull a new view from derecho members
     * @return      true for success, false for failure.
     */
    bool update_view();
    /**
     * Get local node id
     * @return node id
     */
    inline node_id_t get_my_id() const { return this->my_id; }
    /**
     * Get all members in the top level group.
     * @return      A vector including the node ids of all members.
     */
    std::vector<node_id_t> get_members() const;
    /**
     * Get members in a shard.
     * @param subgroup_id   The subgroup id
     * @param shard_num     The shard number in subgroup specified by 'subgroup_id'
     * @return      A vector including the node ids of all members in the specified shard.
     */
    std::vector<node_id_t> get_shard_members(uint32_t subgroup_id, uint32_t shard_num) const;
    /**
     * Get members in a shard.
     * @tparam SubgroupType     The type of the subgroup containing the shard.
     * @param subgroup_index    The index of the subgroup of type 'SubgroupType'
     * @param shard_num         The shard number in subgroup specified by 'SubgroupType' and 'subgroup_index'
     * @return      A vector including the node ids of all members in the specified shard.
     */
    template <typename SubgroupType>
    std::vector<node_id_t> get_shard_members(uint32_t subgroup_index, uint32_t shard_num) const;
    /**
     * Get the index of a type
     * @tparam SubgroupType     The subgroup type
     * @return      The index of the type in ReplicatedTypes... list, start from 0.
     */
    template <typename SubgroupType>
    uint32_t get_index_of_type() const;
    /**
     * Get the number of subgroups of a type.
     * @tparam SubgroupType The type of the subgroup.
     * @return      The number of subgroups of type 'SubgroupType', whose indexes start from 0.
     */
    template <typename SubgroupType>
    uint32_t get_number_of_subgroups() const;
    /**
     * Get the number of shards in a subgroup.
     * @param subgroup_id   The subgroup id
     * @return      The number of shards in the specified subgroup.
     */
    uint32_t get_number_of_shards(uint32_t subgroup_id) const;
    /**
     * Get the number of shards in a subgroup.
     * @tparam SubgroupType     The type of the subgroup containing the shard
     * @param subgroup_index    The index of the subgroup of type 'SubgroupType'
     * @return      The number of shards in the specified subgroup.
     */
    template <typename SubgroupType>
    uint32_t get_number_of_shards(uint32_t subgroup_index = 0) const;
};
}  // namespace derecho

#include "detail/external_group_impl.hpp"
