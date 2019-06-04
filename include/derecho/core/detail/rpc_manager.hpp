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

#include "../derecho_type_definitions.hpp"
#include "../view.hpp"
#include "derecho_internal.hpp"
#include "p2p_connections.hpp"
#include "remote_invocable.hpp"
#include "rpc_utils.hpp"
#include "view_manager.hpp"
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/utils/logger.hpp>

namespace derecho {

template <typename T>
class Replicated;
template <typename T>
class ExternalCaller;

/**
 * The Deserialization Interface to be implemented by user applications.
 */
struct IDeserializationContext : public mutils::RemoteDeserializationContext {};

namespace rpc {

using PendingBase_ref = std::reference_wrapper<PendingBase>;

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
    mutils::RemoteDeserialization_v rdv;

    template <typename T>
    friend class ::derecho::Replicated;  //Give only Replicated access to view_manager
    template <typename T>
    friend class ::derecho::ExternalCaller;
    ViewManager& view_manager;

    /** Contains an RDMA connection to each member of the group. */
    std::unique_ptr<sst::P2PConnections> connections;

    /**
     * This provides mutual exclusion between the P2P listening thread
     * and the view-change thread, guarding the P2P connections pointer.
     */
    std::mutex p2p_connections_mutex;
    /** This mutex guards both toFulfillQueue and fulfilledList. */
    std::mutex pending_results_mutex;
    /** This condition variable is to resolve a race condition in using ToFulfillQueue and fulfilledList */
    std::condition_variable pending_results_cv;
    //Both maps contain one list of PendingResults references per subgroup
    std::map<subgroup_id_t, std::queue<PendingBase_ref>> pending_results_to_fulfill;
    std::map<subgroup_id_t, std::list<PendingBase_ref>> fulfilled_pending_results;

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
    /** p2p send and queries are queued in fifo worker */
    std::thread fifo_worker_thread;
    struct fifo_req {
        node_id_t sender_id;
        char* msg_buf;
        uint32_t buffer_size;
        fifo_req() : sender_id(0),
                     msg_buf(nullptr),
                     buffer_size(0) {}
        fifo_req(node_id_t _sender_id,
                 char* _msg_buf,
                 uint32_t _buffer_size) : sender_id(_sender_id),
                                          msg_buf(_msg_buf),
                                          buffer_size(_buffer_size) {}
    };
    std::queue<fifo_req> fifo_queue;
    std::mutex fifo_queue_mutex;
    std::condition_variable fifo_queue_cv;

    /** Listens for P2P RPC calls over the RDMA P2P connections and handles them. */
    void p2p_receive_loop();

    /** Handle Non-cascading P2P Send and P2P Queries in fifo*/
    void fifo_worker();

    /**
     * Handler to be called by rpc_process_loop each time it receives a
     * peer-to-peer message over an RDMA P2P connection.
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param buffer_size The size of the buffer, in bytes
     */
    void p2p_message_handler(node_id_t sender_id, char* msg_buf, uint32_t buffer_size);

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
                                       char const* const buf, std::size_t payload_size,
                                       const std::function<char*(int)>& out_alloc);

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
    std::exception_ptr parse_and_receive(char* buf, std::size_t size,
                                         const std::function<char*(int)>& out_alloc);

public:
    RPCManager(ViewManager& group_view_manager,
               IDeserializationContext* deserialization_context_ptr)
            // mutils::RemoteDeserializationContext_p deserialization_context_ptr = nullptr)
            : nid(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
              receivers(new std::decay_t<decltype(*receivers)>()),
              view_manager(group_view_manager),
              connections(std::make_unique<sst::P2PConnections>(sst::P2PParams{nid, {nid}, group_view_manager.view_max_window_size, group_view_manager.view_max_payload_size})),
              replySendBuffer(new char[group_view_manager.view_max_payload_size + sizeof(header)]) {
        if(deserialization_context_ptr != nullptr) {
            rdv.push_back(deserialization_context_ptr);
        }
        rpc_thread = std::thread(&RPCManager::p2p_receive_loop, this);
    }

    ~RPCManager();

    void create_connections();

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
        //FunctionTuple is a std::tuple of partial_wrapped<Tag, Ret, UserProvidedClass, Args>,
        //which is the result of the user calling tag<Tag>(&UserProvidedClass::method) on each RPC method
        //Use callFunc to unpack the tuple into a variadic parameter pack for build_remoteinvocableclass
        return mutils::callFunc([&](const auto&... unpacked_functions) {
            return build_remote_invocable_class<UserProvidedClass>(nid, type_id, instance_id, *receivers,
                                                                   bind_to_instance(cls, unpacked_functions)...);
        },
                                funs);
    }

    void destroy_remote_invocable_class(uint32_t instance_id);

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
    auto make_remote_invoker(uint32_t type_id, uint32_t instance_id, FunctionTuple funs) {
        return mutils::callFunc([&](const auto&... unpacked_functions) {
            //Supply the template parameters for build_remote_invoker_for_class by
            //asking bind_to_instance for the type of the wrapped<> that corresponds to each partial_wrapped<>
            return build_remote_invoker_for_class<UserProvidedClass,
                                                  decltype(bind_to_instance(std::declval<std::unique_ptr<UserProvidedClass>*>(),
                                                                            unpacked_functions))...>(nid, type_id,
                                                                                                     instance_id, *receivers);
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
     * Handler to be called by MulticastGroup when it receives a message that
     * appears to be a "cooked send" RPC message. Parses the message and
     * delivers it to the appropriate RPC function registered with this RPCManager,
     * then sends a reply to the sender if one is needed.
     * @param subgroup_id The internal subgroup number of the subgroup this
     * message was received in
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param buffer_size The size of the message in the buffer, in bytes
     */
    void rpc_message_handler(subgroup_id_t subgroup_id, node_id_t sender_id,
                             char* msg_buf, uint32_t buffer_size);

    /**
     * Sends the next message in the MulticastGroup's send buffer (which is
     * assumed to be an RPC message prepared by earlier functions) and registers
     * the "promise object" in pending_results_handle to await replies.
     * @param subgroup_id The subgroup in which this message is being sent
     * @param pending_results_handle A reference to the "promise object" in the
     * send_return for this send.
     * @return True if the send was successful, false if the current view is wedged
     */
    bool finish_rpc_send(subgroup_id_t subgroup_id, PendingBase& pending_results_handle);

    /**
     * Retrieves a buffer for sending P2P messages from the RPCManager's pool of
     * P2P RDMA connections. After filling it with data, the next call to
     * finish_p2p_send will send it.
     * @param dest_id The ID of the node that the P2P message will be sent to
     * @param type The type of P2P message that will be sent
     */
    volatile char* get_sendbuffer_ptr(uint32_t dest_id, sst::REQUEST_TYPE type);

    /**
     * Sends the next P2P message buffer over an RDMA connection to the specified node,
     * and registers the "promise object" in pending_results_handle to await its reply.
     * @param dest_node The node to send the message to
     * @param dest_subgroup_id The subgroup ID of the subgroup that node is in
     * @param pending_results_handle A reference to the "promise object" in the
     * send_return for this send.
     */
    void finish_p2p_send(node_id_t dest_node, subgroup_id_t dest_subgroup_id, PendingBase& pending_results_handle);
};

//Now that RPCManager is finished being declared, we can declare these convenience types
//(the declarations should really live in remote_invocable.h, but they depend on RPCManager existing)
template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<RPCManager>()
                                                         .make_remote_invocable_class(std::declval<std::unique_ptr<T>*>(),
                                                                                      std::declval<uint32_t>(),
                                                                                      std::declval<uint32_t>(),
                                                                                      T::register_functions()))>;

template <typename T>
using RemoteInvokerFor = std::decay_t<decltype(*std::declval<RPCManager>()
                                                        .make_remote_invoker<T>(std::declval<uint32_t>(),
                                                                                std::declval<uint32_t>(),
                                                                                T::register_functions()))>;

// test if the current thread is in an RPC handler to tell if we are sending a cascading RPC message.
bool in_rpc_handler();

}  // namespace rpc
}  // namespace derecho
