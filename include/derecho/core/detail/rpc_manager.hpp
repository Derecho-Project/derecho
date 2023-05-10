/**
 * @file rpc_manager.h
 *
 * @date Feb 7, 2017
 */

#pragma once

#include "../derecho_type_definitions.hpp"
#include "../view.hpp"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "derecho/persistent/Persistent.hpp"
#include "derecho/utils/logger.hpp"
#include "derecho_internal.hpp"
#include "p2p_connection_manager.hpp"
#include "remote_invocable.hpp"
#include "rpc_utils.hpp"

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace derecho {

/* --- Forward declarations to break circular include dependencies --- */
template <typename T>
class Replicated;
template <typename T>
class PeerCaller;
template <typename T>
class ExternalClientCallback;

class ViewManager;

/**
 * The Deserialization Interface to be implemented by user applications.
 */
using DeserializationContext = mutils::RemoteDeserializationContext;

namespace rpc {

/**
 * Given a subgroup ID and a list of functions, constructs a
 * RemoteInvokerForClass for the type of object given by the template
 * parameter, with its receive functions registered to this RPCManager.
 * @param type_id A number uniquely identifying the type of the object (in
 * practice, this is the index of UserProvidedClass within the template
 * parameters of the containing Group).
 * @param instance_id A number uniquely identifying the subgroup to which
 * RPC invocations for this object should be sent.
 * @param funs A tuple of "partially wrapped" pointer-to-member-functions
 * (the return type of rpc::tag<>(), which is called by the client), one for
 * each method of UserProvidedClass that should be an RPC function
 * @return The RemoteInvokerForClass that can call a remote UserProvidedClass,
 * by pointer
 * @tparam UserProvidedClass The type of the object being wrapped with a
 * RemoteInvokerForClass
 * @tparam FunctionTuple The type of the tuple of partial_wrapped<> structs
 */
template <typename UserProvidedClass, typename FunctionTuple>
auto make_remote_invoker(const node_id_t nid, uint32_t type_id, uint32_t instance_id, FunctionTuple funs, std::map<Opcode, receive_fun_t>& receivers) {
    return mutils::callFunc([&](const auto&... unpacked_functions) {
        // Supply the template parameters for build_remote_invoker_for_class by
        // asking bind_to_instance for the type of the wrapped<> that corresponds to each partial_wrapped<>
        return build_remote_invoker_for_class<UserProvidedClass,
                                              decltype(bind_to_instance(std::declval<std::unique_ptr<UserProvidedClass>*>(),
                                                                        unpacked_functions))...>(nid, type_id,
                                                                                                 instance_id, receivers);
    },
                            funs);
}

class RPCManager {
    static_assert(std::is_trivially_copyable<Opcode>::value, "Oh no! Opcode is not trivially copyable!");
    /** The ID of the node this RPCManager is running on. */
    const node_id_t nid;
    /** A pointer to the logger for the RPC module, which lives in a global static registry. */
    std::shared_ptr<spdlog::logger> rpc_logger;
    /** A map from FunctionIDs to RPC functions, either the "server" stubs that receive
     * remote calls to invoke functions, or the "client" stubs that receive responses
     * from the targets of an earlier remote call.
     * Note that a FunctionID is (class ID, subgroup ID, Function Tag). */
    std::unique_ptr<std::map<Opcode, receive_fun_t>> receivers;
    /** An emtpy DeserializationManager, in case we need it later. */
    // mutils::DeserializationManager dsm{{}};
    // Weijia: I prefer the deserialization context vector.
    mutils::RemoteDeserialization_v rdv;

    template <typename T>
    friend class ::derecho::Replicated;  // Give only Replicated access to view_manager
    template <typename T>
    friend class ::derecho::PeerCaller;
    template <typename T>
    friend class ::derecho::ExternalClientCallback;
    ViewManager& view_manager;

    /**
     * Manages an RDMA connection to each member of the group, and to each
     * external client that has not yet failed or disconnected. These are used
     * for receiving P2P request messages and sending replies (to both P2P and
     * ordered RPC messages).
     */
    std::unique_ptr<sst::P2PConnectionManager> connections;

    /**
     * Contains all the node IDs that currently correspond to external clients,
     * rather than Derecho group members. Needed only because the external
     * clients are treated differently when they fail.
     */
    std::set<node_id_t> external_client_ids;

    /**
     * This mutex guards all the pending_results maps.
     * Technically it's only needed between two of them at a time, so it would
     * be possible to achieve more concurrency with more fine-grained locks.
     */
    std::mutex pending_results_mutex;
    /**
     * This condition variable is to resolve a race condition in using
     * pending_results_to_fulfill and results_pending_local_persistence
     */
    std::condition_variable pending_results_cv;
    /**
     * For each subgroup, contains a queue of PendingResults references that still
     * need to have their ReplyMaps fulfilled. The queue is in the same order as
     * the PendingResults' corresponding RPC messages were sent, so the front of
     * the queue corresponds to the oldest in-flight RPC message (i.e. the next
     * one to be received in rpc_message_handler()). Note that the PendingResults
     * objects themselves live in the RemoteInvocableClass that sent the message.
     */
    std::map<subgroup_id_t, std::queue<std::weak_ptr<AbstractPendingResults>>> pending_results_to_fulfill;
    /**
     * For each subgroup, contains a map from version number to the PendingResults
     * for that version's RPC call (i.e., a set of PendingResults indexed by
     * version number). These RPC messages have been delivered locally but RPCManager
     * still needs to use the PendingResults to report that persistence has finished.
     */
    std::map<subgroup_id_t, std::map<persistent::version_t, std::weak_ptr<AbstractPendingResults>>> results_awaiting_local_persistence;
    /**
     * For each subgroup, contains a map from version number to the PendingResults
     * for that version's RPC call (i.e., a set of PendingResults indexed by
     * version number). These RPC messages have been persisted locally but RPCManager
     * still needs to use the PendingResults to report that global persistence has finished.
     */
    std::map<subgroup_id_t, std::map<persistent::version_t, std::weak_ptr<AbstractPendingResults>>> results_awaiting_global_persistence;
    /**
     * For each subgroup, contains a map from version number to the PendingResults
     * for that version's RPC call (i.e., a set of PendingResults indexed by
     * version number). These RPC messages have finished global persistence but
     * were sent to subgroups with signatures enabled, so RPCManager still
     * needs to use the PendingResults to report that the signature
     * verification has finished.
     */
    std::map<subgroup_id_t, std::map<persistent::version_t, std::weak_ptr<AbstractPendingResults>>> results_awaiting_signature;
    /**
     * For each subgroup, contains a list of PendingResults references for RPC
     * messages that have completed all of their promise events (ReplyMap has
     * been fulfilled and there is no persistence to track) and are only being
     * kept around in case RPCManager needs to notify them of a removed node on
     * a View change. Note that if View changes are rare, each list can grow
     * very large, since it will not be garbage-collected at any time other than
     * a View change.
     */
    std::map<subgroup_id_t, std::list<std::weak_ptr<AbstractPendingResults>>> completed_pending_results;

    bool thread_start = false;
    /** Mutex for thread_start_cv. */
    std::mutex thread_start_mutex;
    /** Notified when the P2P listening thread should start. */
    std::condition_variable thread_start_cv;
    std::atomic<bool> thread_shutdown{false};
    /** The thread that listens for incoming P2P RPC calls; implemented by p2p_receive_loop() */
    std::thread rpc_listener_thread;
    /** The maximum busy wait time in millisecond before sleep */
    const uint64_t busy_wait_before_sleep_ms;
    /** The thread that processes P2P requests in FIFO order; implemented by p2p_request_worker() */
    std::thread request_worker_thread;
    /** A simple struct representing a P2P request message.
     *  Encapsulates the parameters to a p2p_message_handler call. */
    struct p2p_req {
        node_id_t sender_id;
        uint8_t* msg_buf;
        p2p_req() : sender_id(0),
                    msg_buf(nullptr) {}
        p2p_req(node_id_t _sender_id,
                uint8_t* _msg_buf)
                : sender_id(_sender_id),
                  msg_buf(_msg_buf) {}
    };
    /** P2P requests that need to be handled by the worker thread. */
    std::queue<p2p_req> p2p_request_queue;
    std::mutex request_queue_mutex;
    /** Notified when the request worker thread has work to do. */
    std::condition_variable request_queue_cv;

    /** The caller id of the latest rpc */
    static thread_local node_id_t rpc_caller_id;

    /** Listens for P2P RPC calls over the RDMA P2P connections and handles them. */
    void p2p_receive_loop();

    /** Handles non-cascading P2P Send requests in FIFO order. */
    void p2p_request_worker();

    /**
     * Handler to be called by p2p_receive_loop each time it receives a
     * peer-to-peer message over an RDMA P2P connection.
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A pointer to a buffer containing the message
     */
    void p2p_message_handler(node_id_t sender_id, uint8_t* msg_buf);

    /**
     * Reports to the view manager that the given node has failed if it's an
     * internal member, or removes its global SST connection if it's an external member.
     */
    void report_failure(const node_id_t who);

    /**
     * Processes an RPC message for any of the functions managed by this RPCManager,
     * using the opcode to forward it to the correct function for execution.
     * @param indx The function opcode for this RPC message, which should
     * correspond to either a "call" or "response" function of some RemoteInvocable
     * @param received_from The ID of the node that sent the message
     * @param buf The buffer containing the message
     * @param payload_size The size of the message in bytes
     * @param out_alloc A function that can allocate a buffer for the response
     * to this message.
     * @return A pointer to the exception caused by invoking this RPC function,
     * if the message was an RPC function call and the function threw an exception.
     */
    std::exception_ptr receive_message(const Opcode& indx, const node_id_t& received_from,
                                       uint8_t const* const buf, std::size_t payload_size,
                                       const std::function<uint8_t*(int)>& out_alloc);

    /**
     * Entry point for receiving a single RPC message for a function managed by
     * this RPCManager. Parses the header of the message to retrieve the opcode
     * and message size, then calls receive_message().
     * @param buf The buffer containing the message
     * @param size The size of the buffer
     * @param out_alloc A function that can allocate a buffer for the response
     * to this message
     * @return A pointer to the exception caused by invoking this RPC function,
     * if the message was an RPC function call and the function threw an exception.
     */
    std::exception_ptr parse_and_receive(uint8_t* buf, std::size_t size,
                                         const std::function<uint8_t*(int)>& out_alloc);

public:
    RPCManager(ViewManager& group_view_manager,
               const std::vector<DeserializationContext*>& deserialization_context)
            : nid(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
              rpc_logger(LoggerFactory::createIfAbsent(LoggerFactory::RPC_LOGGER_NAME, getConfString(CONF_LOGGER_RPC_LOG_LEVEL))),
              receivers(new std::decay_t<decltype(*receivers)>()),
              view_manager(group_view_manager),
              busy_wait_before_sleep_ms(getConfUInt64(CONF_DERECHO_P2P_LOOP_BUSY_WAIT_BEFORE_SLEEP_MS)) {
        RpcLoggerPtr::initialize();
        for(const auto& deserialization_context_ptr : deserialization_context) {
            rdv.push_back(deserialization_context_ptr);
        }
        rpc_listener_thread = std::thread(&RPCManager::p2p_receive_loop, this);
    }

    ~RPCManager();

    /**
     * Post-constructor setup method that creates the set of P2P connections
     * (i.e. the P2PConnectionManager object).
     */
    void create_connections();

    /**
     * Sets up a new P2P connection to an external client. Normally, P2P connections
     * are set up in the new-view callback, but external clients can join at any time.
     */
    void add_external_connection(node_id_t node_id);

    /**
     * Starts the thread that listens for incoming P2P RPC requests over the RDMA P2P
     * connections.
     */
    void start_listening();
    /**
     * Given a pointer to an object and a list of its methods, constructs a
     * RemoteInvocableClass for that object with its receive functions
     * registered to this RPCManager.
     * @param cls A raw pointer(??) to a pointer to the object being set up as
     * a RemoteInvocableClass
     * @param type_id A number uniquely identifying the type of the object (in
     * practice, this is the index of UserProvidedClass within the template
     * parameters of the containing Group).
     * @param instance_id A number uniquely identifying the object, corresponding
     * to the subgroup that will be receiving RPC invocations for it (in practice,
     * this is the subgroup ID).
     * @param funs A tuple of "partially wrapped" pointer-to-member-functions
     * (the return type of rpc::tag<>(), which is called by the client), one for
     * each method of UserProvidedClass that should be an RPC function
     * @return The RemoteInvocableClass that wraps UserProvidedClass, by pointer
     * @tparam UserProvidedClass The type of the object being wrapped with a
     * RemoteInvocableClass
     * @tparam FunctionTuple The type of the tuple of partial_wrapped<> structs
     */
    template <typename UserProvidedClass, typename FunctionTuple>
    auto make_remote_invocable_class(std::unique_ptr<UserProvidedClass>* cls, uint32_t type_id, uint32_t instance_id, FunctionTuple funs) {
        // FunctionTuple is a std::tuple of partial_wrapped<Tag, Ret, UserProvidedClass, Args>,
        // which is the result of the user calling tag<Tag>(&UserProvidedClass::method) on each RPC method
        // Use callFunc to unpack the tuple into a variadic parameter pack for build_remoteinvocableclass
        return mutils::callFunc([&](const auto&... unpacked_functions) {
            return build_remote_invocable_class<UserProvidedClass>(nid, type_id, instance_id, *receivers,
                                                                   bind_to_instance(cls, unpacked_functions)...);
        },
                                funs);
    }

    void destroy_remote_invocable_class(uint32_t instance_id);

    /**
     * Callback for new-view events that updates internal state in response to
     * joins or leaves. Specifically, forms new RDMA connections for P2P RPC
     * calls, and updates "pending results" (futures for RPC calls) to report
     * failures for nodes that were removed in the new view.
     * @param new_view The new view that was just installed.
     */
    void new_view_callback(const View& new_view);

    /**
     * Handler to be called by MulticastGroup when it receives a message that
     * appears to be a "cooked send" RPC message. Parses the message and
     * delivers it to the appropriate RPC function registered with this RPCManager,
     * then sends a reply to the sender if one is needed.
     * @param subgroup_id The internal subgroup number of the subgroup this
     * message was received in
     * @param sender_id The ID of the node that sent the message
     * @param version The persistent version number assigned to the message
     * @param timestamp The timestamp (in microseconds) assigned to the message
     * @param msg_buf A buffer containing the message
     * @param buffer_size The size of the message in the buffer, in bytes
     */
    void rpc_message_handler(subgroup_id_t subgroup_id, node_id_t sender_id,
                             persistent::version_t version,
                             uint64_t timestamp,
                             uint8_t* msg_buf, uint32_t buffer_size);

    /**
     * Callback to be called by PersistenceManager when it has finished
     * persisting a version. This will deliver "local persistence done" events
     * to the PendingResults objects of all RPC messages with version numbers
     * lower than the provided version.
     * @param subgroup_id The subgroup in which persistence has finished for a
     * version
     * @param version The latest version number that PersistenceManager has
     * finished persisting in that subgroup
     */
    void notify_persistence_finished(subgroup_id_t subgroup_id, persistent::version_t version);

    /**
     * Callback to be called by MulticastGroup when it detects that a version
     * has reached global persistence. This will deliver "global persistence
     * done" events to the PendingResults objects of all RPC messages with
     * version numbers lower than the provided version.
     * @param subgroup_id The subgroup in which persistence has finished for a
     * version
     * @param version The latest version number that PersistenceManager has
     * finished persisting on all replicas in that subgroup
     */
    void notify_global_persistence_finished(subgroup_id_t subgroup_id, persistent::version_t version);

    /**
     * Callback to be called by MulticastGroup when it detects that a version
     * has been (globally) verified. This will deliver "signature verified"
     * events to the PendingResults objects of all RPC messages with version
     * numbers lower than the provided version (assuming they have already
     * received their global persistence notification).
     * @param subgroup_id The subgroup in which global verification has finished
     * for a version
     * @param version The latest version number that PersistenceManager has
     * finished verifying on all replicas in that subgroup
     */
    void notify_verification_finished(subgroup_id_t subgroup_id, persistent::version_t version);

    /**
     * Notifies RPCManager that an (ordered) RPC message was just sent, which
     * registers the "promise object" pointed to by pending_results_handle for
     * delivering reply and persistence events.
     * @param subgroup_id The subgroup in which the message was sent.
     * @param pending_results_handle A non-owning pointer to the "promise object"
     * created by RemoteInvoker for this send.
     */
    void register_rpc_results(subgroup_id_t subgroup_id, std::weak_ptr<AbstractPendingResults> pending_results_handle);

    /**
     * Retrieves a buffer for sending P2P messages from the RPCManager's pool of
     * P2P RDMA connections. After filling it with data, the next call to
     * send_p2p_message will send it.
     * @param dest_id The ID of the node that the P2P message will be sent to
     * @param type The type of P2P message that will be sent
     */
    sst::P2PBufferHandle get_sendbuffer_ptr(uint32_t dest_id, sst::MESSAGE_TYPE type);

    /**
     * Sends the P2P message buffer with the specified sequence number over an RDMA
     * connection to the specified node, and registers the "promise object" pointed
     * to by pending_results_handle in case RPCManager needs to deliver a node_removed_from_group_exception.
     * @param dest_node The node to send the message to
     * @param dest_subgroup_id The subgroup ID of the subgroup that node is in
     * @param sequence_num The sequence number of the message buffer, as returned by get_sendbuffer_ptr
     * @param pending_results_handle A non-owning pointer to the "promise object"
     * created by RemoteInvoker for this send.
     */
    void send_p2p_message(node_id_t dest_node, subgroup_id_t dest_subgroup_id, uint64_t sequence_num,
                          std::weak_ptr<AbstractPendingResults> pending_results_handle);
    /**
     * Get the id of the latest rpc caller.
     */
    static node_id_t get_rpc_caller_id();
    /**
     * write to remote OOB memory
     * @param remote_node       remote node id
     * @param iov               gather of local memory regions
     * @param iovcnt
     * @param remote_dest_addr  the address of the remote memory region
     * @param rkey              the access key for remote memory
     * @param size              the size of the remote memory region
     * @throw                   derecho::derecho_exception on error
     */
    void oob_remote_write(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_dest_addr, uint64_t rkey, size_t size);
    /**
     * read from remote OOB memory
     * @param remote_node       remote node id
     * @param iov               scatter of local memory regions
     * @param iovcnt
     * @param remote_src_addr   the address of the remote memory region
     * @param rkey              the access key for remote memory
     * @param size              the size of the remote memory region
     * @throw                   derecho::derecho_exception on error
     */
    void oob_remote_read(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_src_addr, uint64_t rkey, size_t size);
};

// Now that RPCManager is finished being declared, we can declare these convenience types
//(the declarations should really live in remote_invocable.h, but they depend on RPCManager existing)
template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<RPCManager>()
                                                         .make_remote_invocable_class(std::declval<std::unique_ptr<T>*>(),
                                                                                      std::declval<uint32_t>(),
                                                                                      std::declval<uint32_t>(),
                                                                                      T::register_functions()))>;

template <typename T>
using RemoteInvokerFor = std::decay_t<decltype(*make_remote_invoker<T>(std::declval<node_id_t>(),
                                                                       std::declval<uint32_t>(),
                                                                       std::declval<uint32_t>(),
                                                                       T::register_functions(),
                                                                       std::declval<std::map<Opcode, receive_fun_t>&>()))>;

// test if the current thread is in an RPC handler to tell if we are sending a cascading RPC message.
bool in_rpc_handler();

}  // namespace rpc
}  // namespace derecho
