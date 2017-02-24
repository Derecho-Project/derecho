/**
 * @file rpc_manager.h
 *
 * @date Feb 7, 2017
 * @author edward
 */

#pragma once

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "rpc_utils.h"
#include "remote_invocable.h"
#include "view_manager.h"
#include "view.h"
#include "mutils-serialization/SerializationSupport.hpp"

namespace derecho {

template<typename T>
class Replicated;

namespace rpc {


class RPCManager {
    /** The ID of the node this RPCManager is running on. */
    const node_id_t nid;
    /** A map from opcodes to RPC functions, either the "server" stubs that receive
     * remote calls to invoke functions, or the "client" stubs that receive responses
     * from the targets of an earlier remote call. */
    std::unique_ptr<std::map<Opcode, receive_fun_t>> receivers;
    /** An emtpy DeserializationManager, in case we need it later. */
    mutils::DeserializationManager dsm{{}};

    template<typename T>
    friend class ::derecho::Replicated; //Give only Replicated access to view_manager
    ViewManager& view_manager;

    /** Contains a TCP connection to each member of the group. */
    tcp::tcp_connections connections;

    std::mutex pending_results_mutex;
    std::queue<std::reference_wrapper<PendingBase>> toFulfillQueue;
    std::list<std::reference_wrapper<PendingBase>> fulfilledList;

    /** This is not accessed outside invocations of cooked_send_callback,
     * it's just a member so it won't be newly allocated every time. */
    std::unique_ptr<char[]> replySendBuffer;

    std::atomic<bool> thread_shutdown{false};
    std::thread rpc_thread;


    /** Listens for P2P RPC calls over the TCP connections and handles them. */
    void rpc_process_loop();


    /**
     * Handler to be called by rpc_process_loop each time it receives a
     * peer-to-peer message over a TCP connection.
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param buffer_size The size of the buffer, in bytes
     */
    void p2p_message_handler(int32_t sender_id, char* msg_buf, uint32_t buffer_size);


public:

    RPCManager(node_id_t node_id, ViewManager& group_view_manager) :
        nid(node_id),
        receivers(new std::decay_t<decltype(*receivers)>()),
        view_manager(group_view_manager),
        connections(node_id, std::map<node_id_t, ip_addr>(),
                    group_view_manager.derecho_params.rpc_port),
        replySendBuffer(new char[group_view_manager.derecho_params.max_payload_size]) {
            rpc_thread = std::thread(&RPCManager::rpc_process_loop, this);
    }


    ~RPCManager();
    /**
     * Given a pointer to an object and a list of its methods, constructs a
     * RemoteInvocableClass for that object that can be registered with this
     * RpcManager.
     * @param cls A raw pointer(??) to a pointer to the object being set up as
     * a RemoteInvocableClass
     * @param f A variable-length list of pointer-to-member-functions, one for
     * each method of NewClass that should be an RPC function
     * @return The RemoteInvocableClass that wraps NewClass, by pointer
     * @tparam NewClass The type of the object being wrapped with a
     * RemoteInvocableClass
     * @tparam NewFuns The types of the member function pointers
     */
    template <class NewClass, typename... NewFuns>
    auto setup_rpc_class(std::unique_ptr<NewClass>* cls, NewFuns... f) {
        //NewFuns must be of type Ret (NewClass::*) (Args...)
        //or of type wrapped<opcode,Ret,Args...>
        return build_remoteinvocableclass<NewClass>(nid, *receivers, wrap(cls, wrap(f))...);
    }

    /**
     * Callback for new-view events that updates internal state in response to
     * joins or leaves. Specifically, forms new TCP connections for P2P RPC
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
     * @param sender_id The ID of the node that sent the message
     * @param msg_buf A buffer containing the message
     * @param payload_size The size of the message in the buffer, in bytes
     */
    void rpc_message_handler(node_id_t sender_id, char* msg_buf, uint32_t payload_size);

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
     */
    void finish_rpc_send(uint32_t subgroup_id, const std::vector<node_id_t>& dest_nodes, PendingBase& pending_results_handle);

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
    void finish_p2p_send(node_id_t dest_node, char* msg_buf, std::size_t size, PendingBase& pending_results_handle);

};


}
}


