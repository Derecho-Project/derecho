#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <assert.h>
#include <condition_variable>
#include <experimental/optional>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <ostream>
#include <list>
#include <set>
#include <tuple>
#include <vector>

#include "connection_manager.h"
#include "derecho_caller.h"
#include "derecho_row.h"
#include "filewriter.h"
#include "mutils-serialization/SerializationMacros.hpp"
#include "mutils-serialization/SerializationSupport.hpp"
#include "rdmc/rdmc.h"
#include "sst/sst.h"

namespace derecho {

/** Alias for the type of std::function that is used for message delivery event callbacks. */
using message_callback = std::function<void(int, long long int, char*, long long int)>;

/**
 * Bundles together a set of callback functions for message delivery events.
 * These will be invoked by DerechoGroup to hand control back to the client
 * when it needs to handle message delivery.
 */
struct CallbackSet {
    message_callback global_stability_callback;
    message_callback local_persistence_callback = nullptr;
};

struct DerechoParams : public mutils::ByteRepresentable {
    long long unsigned int max_payload_size;
    long long unsigned int block_size;
    std::string filename = std::string();
    unsigned int window_size = 3;
    unsigned int timeout_ms = 1;
    rdmc::send_algorithm type = rdmc::BINOMIAL_SEND;
    uint32_t rpc_port = 12487;

    DerechoParams(long long unsigned int max_payload_size,
                  long long unsigned int block_size,
                  std::string filename = std::string(),
                  unsigned int window_size = 3,
                  unsigned int timeout_ms = 1,
                  rdmc::send_algorithm type = rdmc::BINOMIAL_SEND,
                  uint32_t rpc_port = 12487)
            : max_payload_size(max_payload_size),
              block_size(block_size),
              filename(filename),
              window_size(window_size),
              timeout_ms(timeout_ms),
              type(type),
              rpc_port(rpc_port) {
    }

    DEFAULT_SERIALIZATION_SUPPORT(DerechoParams, max_payload_size, block_size, filename, window_size, timeout_ms, type, rpc_port);
};

struct __attribute__((__packed__)) header {
    uint32_t header_size;
    uint32_t pause_sending_turns;
    bool cooked_send;
};

class PendingBase {
public:
    virtual void fulfill_map(const std::vector<node_id_t>&) {
        assert(false);
    }
    virtual void set_exception_for_removed_node(const node_id_t&) {
        assert(false);
    }
};

template <class Ret>
class Pending : public PendingBase {
    PendingResults<Ret>& pending;

public:
    Pending(PendingResults<Ret>& _pending) : pending(_pending) {}
    void fulfill_map(const std::vector<node_id_t>& nodes) {
        who_t who;
        for(auto n : nodes) {
            who.push_back(Node_id(n));
        }
        pending.fulfill_map(who);
    }
    void set_exception_for_removed_node(const node_id_t& removed_id) {
        pending.set_exception_for_removed_node(removed_id);
    }
};

//   template <>
//   class Pending<void> : public PendingBase {
//     PendingResults<void>& pending;

// public:
//     Pending(PendingResults<Ret>& _pending) : pending(_pending) {}
//     void fulfill_map(const std::vector<node_id_t>& nodes) {
//         who_t who;
//         for(auto n : nodes) {
//             who.push_back(Node_id(n));
//         }
//         pending.fulfill_map(who);
//     }
// };

template <class T>
auto createPending(PendingResults<T>& pending) {
    return std::make_unique<Pending<T>>(pending);
};

/**
 * Represents a block of memory used to store a message. This object contains
 * both the array of bytes in which the message is stored and the corresponding
 * RDMA memory region (which has registered that array of bytes as its buffer).
 * This is a move-only type, since memory regions can't be copied.
 */
struct MessageBuffer {
    std::unique_ptr<char[]> buffer;
    std::shared_ptr<rdma::memory_region> mr;

    MessageBuffer() {}
    MessageBuffer(size_t size) {
        if(size != 0) {
            buffer = std::unique_ptr<char[]>(new char[size]);
            mr = std::make_shared<rdma::memory_region>(buffer.get(), size);
        }
    }
    MessageBuffer(const MessageBuffer&) = delete;
    MessageBuffer(MessageBuffer&&) = default;
    MessageBuffer& operator=(const MessageBuffer&) = delete;
    MessageBuffer& operator=(MessageBuffer&&) = default;
};

struct Message {
    /** The rank of the message's sender within this group. */
    int sender_rank;
    /** The message's index (relative to other messages sent by that sender). */
    long long int index;
    /** The message's size in bytes. */
    long long unsigned int size;
    /** The MessageBuffer that contains the message's body. */
    MessageBuffer message_buffer;
};

/**
 * SST row state variables needed to track message completion status in this
 * group.
 */
struct MessageTrackingRow {
    /** Sequence numbers are interpreted like a row-major pair:
     * (sender, index) becomes sender + num_members * index.
     * Since the global order is round-robin, the correct global order of
     * messages becomes a consecutive sequence of these numbers: with 4
     * senders, we expect to receive (0,0), (1,0), (2,0), (3,0), (0,1),
     * (1,1), ... which is 0, 1, 2, 3, 4, 5, ....
     *
     * This variable is the highest sequence number that has been received
     * in-order by this node; if a node updates seq_num, it has received all
     * messages up to seq_num in the global round-robin order. */
    long long int seq_num;
    /** This represents the highest sequence number that has been received
     * by every node, as observed by this node. If a node updates stable_num,
     * then it believes that all messages up to stable_num in the global
     * round-robin order have been received by every node. */
    long long int stable_num;
    /** This represents the highest sequence number that has been delivered
     * at this node. Messages are only delievered once stable, so it must be
     * at least stable_num. */
    long long int delivered_num;
};

/** combines sst and rdmc to give an abstraction of a group where anyone can send */
template <typename dispatcherType>
class DerechoGroup {
private:
    /** vector of member id's */
    std::vector<node_id_t> members;
    /**  number of members */
    const int num_members;
    /** index of the local node in the members vector, which should also be its row index in the SST */
    const int member_index;
    /** Block size used for message transfer.
     * we keep it simple; one block size for messages from all senders */
    const long long unsigned int block_size;
    // maximum size of any message that can be sent
    const long long unsigned int max_msg_size;
    /** Send algorithm for constructing a multicast from point-to-point unicast.
     *  Binomial pipeline by default. */
    const rdmc::send_algorithm type;
    const unsigned int window_size;
    const CallbackSet callbacks;
    dispatcherType dispatchers;
    tcp::tcp_connections connections;
    std::queue<std::unique_ptr<PendingBase>> toFulfillQueue;
    std::list<std::unique_ptr<PendingBase>> fulfilledList;
    std::mutex pending_results_mutex;
    /** Offset to add to member ranks to form RDMC group numbers. */
    const uint16_t rdmc_group_num_offset;
    /** false if RDMC groups haven't been created successfully */
    bool rdmc_groups_created = false;
    unsigned int total_message_buffers;
    /** Stores message buffers not currently in use. Protected by
     * msg_state_mtx */
    std::vector<MessageBuffer> free_message_buffers;
    std::unique_ptr<char[]> p2pBuffer;
    std::unique_ptr<char[]> deliveryBuffer;

    // int send_slot;
    // vector<int> recv_slots;
    // /** buffers to store incoming/outgoing messages */
    // std::vector<std::unique_ptr<char[]> > buffers;
    // /** memory regions wrapping the buffers for RDMA ops */
    // std::vector<std::shared_ptr<rdma::memory_region> > mrs;

    /** Index to be used the next time get_position is called.
     * When next_message is not none, then next_message.index = future_message_index-1 */
    long long int future_message_index = 0;

    /** next_message is the message that will be sent when send is called the next time.
     * It is boost::none when there is no message to send. */
    std::experimental::optional<Message> next_send;
    /** Messages that are ready to be sent, but must wait until the current send finishes. */
    std::queue<Message> pending_sends;
    /** The message that is currently being sent out using RDMC, or boost::none otherwise. */
    std::experimental::optional<Message> current_send;

    /** Messages that are currently being received. */
    std::map<long long int, Message> current_receives;

    /** Messages that have finished sending/receiving but aren't yet globally stable */
    std::map<long long int, Message> locally_stable_messages;
    /** Messages that are currently being written to persistent storage */
    std::map<long long int, Message> non_persistent_messages;

    long long int next_message_to_deliver = 0;
    std::mutex msg_state_mtx;
    std::condition_variable sender_cv;

    /** The time, in milliseconds, that a sender can wait to send a message before it is considered failed. */
    unsigned int sender_timeout;

    /** Indicates that the group is being destroyed. */
    std::atomic<bool> thread_shutdown{false};
    /** The background thread that sends messages with RDMC. */
    std::thread sender_thread;

    std::thread timeout_thread;
    std::thread rpc_thread;

    /** The SST, shared between this group and its GMS. */
    std::shared_ptr<DerechoSST> sst;

    using pred_handle = typename sst::Predicates<DerechoSST>::pred_handle;
    pred_handle stability_pred_handle;
    pred_handle delivery_pred_handle;
    pred_handle sender_pred_handle;

    std::unique_ptr<FileWriter> file_writer;

    /** Continuously waits for a new pending send, then sends it. This function
     * implements the sender thread. */
    void send_loop();

    /** Checks for failures when a sender reaches its timeout. This function
     * implements the timeout thread. */
    void check_failures_loop();

    std::function<void(persistence::message)> make_file_written_callback();
    bool create_rdmc_groups();
    void initialize_sst_row();
    void register_predicates();

    void deliver_message(Message& msg);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto derechoCallerSend(const std::vector<node_id_t>& nodes, char* buf, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto tcpSend(node_id_t dest_node, Args&&... args);
    // private get_position - used for cooked send

public:
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    DerechoGroup(
        std::vector<node_id_t> _members, node_id_t my_node_id,
        std::shared_ptr<DerechoSST> _sst,
        std::vector<MessageBuffer>& free_message_buffers,
        dispatcherType _dispatchers,
        CallbackSet callbacks,
        const DerechoParams derecho_params,
        std::map<node_id_t, std::string> ip_addrs,
        std::vector<char> already_failed = {});
    /** Constructor to initialize a new derecho_group from an old one,
     * preserving the same settings but providing a new list of members. */
    DerechoGroup(
        std::vector<node_id_t> _members, node_id_t my_node_id,
        std::shared_ptr<DerechoSST> _sst,
        DerechoGroup&& old_group, std::map<node_id_t, std::string> ip_addrs,
        std::vector<char> already_failed = {}, uint32_t rpc_port = 12487);
    ~DerechoGroup();

    void deliver_messages_upto(const std::vector<long long int>& max_indices_for_senders);
    /** get a pointer into the buffer, to write data into it before sending */
    char* get_position(long long unsigned int payload_size,
                       int pause_sending_turns = 0, bool cooked_send = false);
    /** Note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
     * This still allows making multiple send calls without acknowledgement; at a single point in time, however,
     * there is only one message per sender in the RDMC pipeline */
    bool send();
    template <typename IdClass, unsigned long long tag, typename... Args>
    void orderedSend(const std::vector<node_id_t>& nodes, char* buf, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    void orderedSend(char* buf, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto orderedQuery(const std::vector<node_id_t>& nodes, char* buf, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto orderedQuery(char* buf, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    void p2pSend(node_id_t dest_node, Args&&... args);
    template <typename IdClass, unsigned long long tag, typename... Args>
    auto p2pQuery(node_id_t dest_node, Args&&... args);
    void send_objects(tcp::socket& new_member_socket);
    void rpc_process_loop();
    void set_exceptions_for_removed_nodes(
        std::vector<node_id_t> removed_members);
    /** Stops all sending and receiving in this group, in preparation for shutting it down. */
    void wedge();
    /** Debugging function; prints the current state of the SST to stdout. */
    void debug_print();
    static long long unsigned int compute_max_msg_size(
        const long long unsigned int max_payload_size,
        const long long unsigned int block_size);
};
}  // namespace derecho

#include "derecho_group_impl.h"

#endif /* DERECHO_GROUP_H */
