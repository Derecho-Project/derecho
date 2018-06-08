/**
 * @file rpc_manager.h
 *
 * @date Feb 7, 2017
 */

#pragma once

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "derecho_internal.h"
#include "mutils-serialization/SerializationSupport.hpp"
#include "p2p_connections.h"
#include "remote_invocable.h"
#include "rpc_utils.h"
#include "view.h"
#include "view_manager.h"

namespace derecho {

template <typename T>
class Replicated;
template <typename T>
class ExternalCaller;

namespace rpc {

class RPCManager {
    static_assert(std::is_trivially_copyable<Opcode>::value, "Oh no! Opcode is not trivially copyable!");
    /** The ID of the node this RPCManager is running on. */
    const node_id_t nid;
    /** A map from FunctionIDs to RPC functions, either the "server" stubs that receive
     * remote calls to invoke functions, or the "client" stubs that receive responses
     * from the targets of an earlier remote call.
     * Note that a FunctionID is (class ID, subgroup ID, Function Tag). */
    std::unique_ptr<std::map<Opcode, receive_fun_t>> receivers;
    /** An emtpy DeserializationManager, in case we need it later. */
    // mutils::DeserializationManager dsm{{}};
    // Weijia: I prefer the deserialization context vector.
    mutils::RemoteDeserialization_v rdv{};

    std::shared_ptr<spdlog::logger> logger;

    template <typename T>
    friend class ::derecho::Replicated;  //Give only Replicated access to view_manager
    template <typename T>
    friend class ::derecho::ExternalCaller;
    ViewManager& view_manager;

    tcp::tcp_connections tcp_connections;

    /** Contains an RDMA connection to each member of the group. */
    std::unique_ptr<sst::P2PConnections> connections;

    std::mutex p2p_connections_mutex;
    std::mutex pending_results_mutex;
    std::queue<std::reference_wrapper<PendingBase>> toFulfillQueue;
    std::list<std::reference_wrapper<PendingBase>> fulfilledList;

    /** This is not accessed outside invocations of rpc_message_handler,
     * it's just a member so it won't be newly allocated every time. */
    std::unique_ptr<char[]> replySendBuffer;

    bool thread_start = false;
    /** Mutex for thread_start_cv. */
    std::mutex thread_start_mutex;
    /** Notified when the P2P listening thread should start. */
    std::condition_variable thread_start_cv;
    std::atomic<bool> thread_shutdown{false};
    std::thread rpc_thread;

    /** Listens for P2P RPC calls over the RDMA P2P connections and handles them. */
    void p2p_receive_loop();

    /**
     * Handler to be called by rpc_process_loop each time it receives a
     * peer-to-peer message over an RDMA P2P connection.
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param buffer_size The size of the buffer, in bytes
     */
    void p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size);

public:
    RPCManager(node_id_t node_id, ViewManager& group_view_manager)
            : nid(node_id),
              receivers(new std::decay_t<decltype(*receivers)>()),
              logger(spdlog::get("debug_log")),
              view_manager(group_view_manager),
              //Connections initially only contains the local node. Other nodes are added in the new view callback
              tcp_connections(node_id, std::map<node_id_t, ip_addr>(),
                          group_view_manager.derecho_params.rpc_port),
              connections(std::make_unique<sst::P2PConnections>(sst::P2PParams{node_id, {node_id}, group_view_manager.derecho_params.window_size, group_view_manager.derecho_params.max_payload_size})),
              replySendBuffer(new char[group_view_manager.derecho_params.max_payload_size]) {
        rpc_thread = std::thread(&RPCManager::p2p_receive_loop, this);
    }

    ~RPCManager();

    /**
     * Starts the thread that listens for incoming P2P RPC requests over the RDMA P2P
     * connections. This should only be called after Group's constructor has
     * finished receiving Replicated Object state, since that process uses the
     * same TCP sockets that this thread will use for RPC requests.
     */
    void start_listening();
    /**
     * Given a pointer to an object and a list of its methods, constructs a
     * RemoteInvocableClass for that object with its receive functions
     * registered to this RPCManager.
     * @param cls A raw pointer(??) to a pointer to the object being set up as
     * a RemoteInvocableClass
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
    auto make_remote_invocable_class(std::unique_ptr<UserProvidedClass>* cls, uint32_t instance_id, FunctionTuple funs) {
        //FunctionTuple is a std::tuple of partial_wrapped<Tag, Ret, UserProvidedClass, Args>,
        //which is the result of the user calling tag<Tag>(&UserProvidedClass::method) on each RPC method
        //Use callFunc to unpack the tuple into a variadic parameter pack for build_remoteinvocableclass
        return mutils::callFunc([&](const auto&... unpacked_functions) {
            return build_remote_invocable_class<UserProvidedClass>(nid, instance_id, *receivers,
                                                                   bind_to_instance(cls, unpacked_functions)...);
        },
                                funs);
    }

    /**
     * Given a subgroup ID and a list of functions, constructs a
     * RemoteInvokerForClass for the type of object given by the template
     * parameter, with its receive functions registered to this RPCManager.
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
    auto make_remote_invoker(uint32_t instance_id, FunctionTuple funs) {
        return mutils::callFunc([&](const auto&... unpacked_functions) {
            //Supply the template parameters for build_remote_invoker_for_class by
            //asking bind_to_instance for the type of the wrapped<> that corresponds to each partial_wrapped<>
            return build_remote_invoker_for_class<UserProvidedClass,
                                                  decltype(bind_to_instance(
                                                          std::declval<std::unique_ptr<UserProvidedClass>*>(),
                                                          unpacked_functions))...>(
                    nid, instance_id, *receivers);
        },
                                funs);
    }

    /**
     * Callback for new-view events that updates internal state in response to
     * joins or leaves. Specifically, forms new RDMA connections for P2P RPC
     * calls, and updates "pending results" (futures for RPC calls) to report
     * failures for nodes that were removed in the new view.
     * @param new_view The new view that was just installed.
     */
    void new_view_callback(const View& new_view);

    /**
     * Handles an RPC message for any of the functions managed by this RPCManager,
     * using the opcode to forward it to the correct function.
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
    std::exception_ptr handle_receive(
            const Opcode& indx, const node_id_t& received_from, char const* const buf,
            std::size_t payload_size, const std::function<char*(int)>& out_alloc);

    /**
     * Alternative handler for RPC messages received for functions managed by
     * this RPCManager. Parses the header of the message to retrieve the opcode
     * and message size before forwarding the call to the other handle_receive().
     * @param buf The buffer containing the message
     * @param size The size of the buffer
     * @param out_alloc A function that can allocate a buffer for the response
     * to this message
     * @return A pointer to the exception caused by invoking this RPC function,
     * if the message was an RPC function call and the function threw an exception.
     */
    std::exception_ptr handle_receive(
            char* buf, std::size_t size,
            const std::function<char*(int)>& out_alloc);

    /**
     * Handler to be called by MulticastGroup when it receives a message that
     * appears to be a "cooked send" RPC message. Parses the message and
     * delivers it to the appropriate RPC function registered with this RPCManager.
     * @param subgroup_id The internal subgroup number of the subgroup this
     * message was received in
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param payload_size The size of the message in the buffer, in bytes
     */
    void rpc_message_handler(subgroup_id_t subgroup_id, node_id_t sender_id, char* msg_buf, uint32_t payload_size);

    /**
     * Returns a LockedReference to the TCP socket connected to the specified
     * node. This allows other Derecho components to re-use RPCManager's
     * connection pool without causing a race condition.
     * @param node The ID of the node to get a TCP connection to
     * @return A LockedReference containing a reference to that node's socket.
     */
    LockedReference<std::unique_lock<std::mutex>, tcp::socket> get_socket(node_id_t node);

    /**
     * Writes the "list of destination nodes" header field into the given
     * buffer, in preparation for sending an RPC message.
     * @param dest_nodes The list of destination nodes
     * @param buffer The buffer in which to write the header
     * @param max_payload_size Out parameter: the maximum size of a payload
     * that can be written to this buffer after the header has been written.
     * @return The size of the header.
     */
    int populate_nodelist_header(const std::vector<node_id_t>& dest_nodes, char* buffer,
                                 std::size_t& max_payload_size);

    /**
     * Sends the next message in the MulticastGroup's send buffer (which is
     * assumed to be an RPC message prepared by earlier functions) and registers
     * the "promise object" in pending_results_handle to await replies.
     * @param dest_nodes The list of node IDs the message is being sent to
     * @param pending_results_handle A reference to the "promise object" in the
     * send_return for this send.
     * @return True if the send was successful, false if the current view is wedged
     */
    bool finish_rpc_send(uint32_t subgroup_id, const std::vector<node_id_t>& dest_nodes, PendingBase& pending_results_handle);

  /**
   * called by replicated.h for sending a p2p send/query
   */
  volatile char* get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type);
  
    /**
     * Sends the message in msg_buf to the node identified by dest_node over a
     * TCP connection, and registers the "promise object" in pending_results_handle
     * to await its reply.
     * @param dest_node The node to send the message to
     * @param msg_buf A buffer containing the message
     * @param size The size of the message, in bytes
     * @param pending_results_handle A reference to the "promise object" in the
     * send_return for this send.
     */
  void finish_p2p_send(node_id_t dest_node, PendingBase& pending_results_handle);
};

//Now that RPCManager is finished being declared, we can declare these convenience types
//(the declarations should really live in remote_invocable.h, but they depend on RPCManager existing)
template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<RPCManager>()
                                                         .make_remote_invocable_class(std::declval<std::unique_ptr<T>*>(),
                                                                                      std::declval<uint32_t>(),
                                                                                      T::register_functions()))>;

template <typename T>
using RemoteInvokerFor = std::decay_t<decltype(*std::declval<RPCManager>()
                                                        .make_remote_invoker<T>(std::declval<uint32_t>(),
                                                                                T::register_functions()))>;
}
}
