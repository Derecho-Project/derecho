#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>

/**
 * This object, representing arbitrary binary data, was (mostly) copied and
 * pasted from the Cascade code.
 */
class Blob : public mutils::ByteRepresentable {
private:
    //True when this Blob wraps a byte buffer within a serialized message and doesn't own the bytes
    bool is_temporary;

public:
    uint8_t* bytes;
    std::size_t size;

    // constructor - copy to own the data
    Blob(const uint8_t* const b, const decltype(size) s);

    // copy constructor - copy to own the data
    Blob(const Blob& other);

    // move constructor - accept the memory from another object
    Blob(Blob&& other);

    // default constructor - no data at all
    Blob();

    // destructor
    virtual ~Blob();

    // move evaluator:
    Blob& operator=(Blob&& other);

    // copy evaluator:
    Blob& operator=(const Blob& other);

    // serialization/deserialization supports
    std::size_t to_bytes(uint8_t* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const uint8_t* const buffer);

    static mutils::context_ptr<Blob> from_bytes_noalloc(mutils::DeserializationManager* ctx,
                                                        const uint8_t* const buffer);

    static mutils::context_ptr<const Blob> from_bytes_noalloc_const(mutils::DeserializationManager* m,
                                                                    const uint8_t* const buffer);

private:
    //Non-owning constructor, used only by from_bytes_noalloc
    Blob(uint8_t* buffer, std::size_t size, bool temporary);
};

/**
 * Enumerates the types of events a client could be interested in receiving
 * notifications about from the storage node.
 */
enum class ClientCallbackType {
    LOCAL_PERSISTENCE,      // Local persistence is finished for an update
    GLOBAL_PERSISTENCE,     // Global persistence is finished for an update
    SIGNATURE_VERIFICATION  //Signature verification is finished for an update
};

std::ostream& operator<<(std::ostream& os, const ClientCallbackType& cb_type);

/**
 * Structure used by StorageNode to pass callback requests from the
 * register_callback RPC function to the callback-sending thread.
 */
struct CallbackRequest {
    ClientCallbackType callback_type;
    node_id_t client;
    persistent::version_t version;
};

class StorageNode : public mutils::ByteRepresentable,
                    public derecho::PersistsFields,
                    public derecho::GroupReference {
    using derecho::GroupReference::group;
    persistent::Persistent<Blob> object_log;
    /** A local cache of QueryResults from ordered_update() calls. Not part of the replicated state. */
    mutable std::map<persistent::version_t, derecho::rpc::QueryResults<void>> update_results;
    const derecho::subgroup_id_t my_subgroup_id;

    std::thread callback_sending_thread;
    std::atomic<bool> thread_shutdown;
    mutable std::queue<CallbackRequest> callback_request_queue;
    /** This mutex guards both update_results and callback_request_queue */
    mutable std::mutex callback_thread_mutex;
    mutable std::condition_variable request_queue_nonempty;

public:
    StorageNode(persistent::PersistentRegistry* pr, derecho::subgroup_id_t my_subgroup_id);
    StorageNode(persistent::Persistent<Blob>& other_log, derecho::subgroup_id_t subgroup_id);
    ~StorageNode();
    /**
     * P2P-callable function that creates a new log entry with the provided data.
     * @return The version assigned to the new log entry, and the timestamp assigned to the new log entry
    */
    std::pair<persistent::version_t, uint64_t> update(node_id_t sender_id,
                                                      uint32_t update_counter,
                                                      const Blob& new_data) const;

    /** Actual implementation of update, only callable from within the subgroup as an ordered send. */
    void ordered_update(const Blob& new_data);

    /**
     * Retrieves the data in the log at a specific version number. P2P-callable.
     */
    Blob get(const persistent::version_t& version) const;
    /**
     * P2P-callable function that lets another node request a callback when a particular
     * update (identified by its version) has reached a particular state (locally/globally
     * persisted, signed)
     */
    void register_callback(node_id_t client_node_id, const ClientCallbackType& callback_type, persistent::version_t version) const;
    /**
     * Function that implements the callback-checking thread. This thread waits for
     * updates to finish persisting and then sends callbacks to clients who requested
     * them, to avoid blocking the P2P RPC thread.
     */
    void callback_thread_function();

    DEFAULT_SERIALIZATION_SUPPORT(StorageNode, object_log, my_subgroup_id);
    REGISTER_RPC_FUNCTIONS(StorageNode, ORDERED_TARGETS(ordered_update),
                           P2P_TARGETS(update, get, register_callback));
};

/**
 * Structure used by the client node to pass callback notifications from its
 * P2P RPC-receiving thread to its callback-handling thread.
 */
struct CallbackEvent {
    ClientCallbackType callback_type;
    persistent::version_t version;
    derecho::subgroup_id_t sending_subgroup;
};

/**
 * The interface that internal or external clients must implement (as an RPC
 * function) in order to receive notification messages from Derecho nodes.
 */
class NotificationSupport {
public:
    /**
     * A P2P-callable RPC function that is invoked when a Derecho node wants to
     * deliver a notification callback to this client.
     */
    virtual void receive_callback(const ClientCallbackType& callback_type,
                                  persistent::version_t version,
                                  derecho::subgroup_id_t sending_subgroup) const = 0;
};

class InternalClientNode : public mutils::ByteRepresentable,
                           public derecho::GroupReference,
                           public NotificationSupport {
    using derecho::GroupReference::group;

    /**
     * A function that this node should run in response to receiving a
     * "global persistence finished" notification from a storage node
     */
    mutable derecho::persistence_callback_t global_persistence_callback;

    std::thread client_callbacks_thread;
    std::atomic<bool> thread_shutdown;
    /**
     * A queue for passing callback events from the RPC callback method (on the
     * P2P thread) to a separate thread to handle them.
     */
    mutable std::queue<CallbackEvent> callback_event_queue;
    /**
     * A mutex and associated condition variable for signaling the callback
     * event thread that a new callback has been received by the P2P RPC thread.
     */
    mutable std::mutex event_queue_mutex;
    mutable std::condition_variable event_queue_nonempty;

public:
    InternalClientNode(const derecho::persistence_callback_t& global_persistence_callback);
    virtual ~InternalClientNode();

    /**
     * Sends an update to a StorageNode (using a P2P RPC) and registers interest in a
     * persistence callback for that update's version.
     * @return The version and timestamp assigned to the update by the StorageNode
     */
    std::pair<persistent::version_t, uint64_t> submit_update(uint32_t update_counter,
                                                             const Blob& new_data) const;

    virtual void receive_callback(const ClientCallbackType& callback_type,
                                  persistent::version_t version,
                                  derecho::subgroup_id_t sending_subgroup) const;

    /**
     * Function that implements the callback-handling thread. This thread actually
     * executes the user-provided callback function, so that it doesn't block the
     * P2P RPC thread.
     */
    void callback_thread_function();

    //ClientNode really shouldn't be replicated or copied, but it needs to implement
    //the ByteRepresentable interface anyway
    std::size_t to_bytes(uint8_t* v) const { return 0; };

    std::size_t bytes_size() const { return 0; };

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {};

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<InternalClientNode> from_bytes(mutils::DeserializationManager*, const uint8_t* const v) {
        return std::make_unique<InternalClientNode>(derecho::persistence_callback_t{});
    };

    REGISTER_RPC_FUNCTIONS(InternalClientNode, P2P_TARGETS(receive_callback, submit_update));
};
