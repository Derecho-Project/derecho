#pragma once

#include <derecho/core/derecho.hpp>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

enum NotificationMessageType : uint64_t {
    LOCAL_PERSISTENCE = 1,
    GLOBAL_PERSISTENCE
};

struct NotificationRequest {
    NotificationMessageType notification_type;
    node_id_t client_id;
    persistent::version_t version;
};

/**
 * A simple persistent-storage subgroup for a Derecho group that can send notifications
 * to external clients when it finishes persisting their updates. An external client must
 * first request a notification for a particular version number; then its notify() method
 * will be invoked by the StorageNode when that version has finished persisting locally
 * or globally.
 */
class StorageNode : public mutils::ByteRepresentable,
                    public derecho::PersistsFields,
                    public derecho::GroupReference,
                    public derecho::NotificationSupport {
    using derecho::GroupReference::group;
    persistent::Persistent<derecho::Bytes> object_log;
    /** A local cache of QueryResults from ordered_update() calls. Not part of the replicated state. */
    mutable std::map<persistent::version_t, derecho::rpc::QueryResults<void>> update_results;
    const derecho::subgroup_id_t my_subgroup_id;

    std::thread notification_sending_thread;
    std::atomic<bool> thread_shutdown;
    mutable std::queue<NotificationRequest> notification_request_queue;
    /** This mutex guards both update_results and notification_request_queue */
    mutable std::mutex notification_thread_mutex;
    mutable std::condition_variable request_queue_nonempty;

public:
    StorageNode(persistent::PersistentRegistry* pr, derecho::subgroup_id_t my_subgroup_id);
    StorageNode(persistent::Persistent<derecho::Bytes>& other_log, derecho::subgroup_id_t subgroup_id);
    ~StorageNode();
    /**
     * P2P-callable function that creates a new log entry with the provided data.
     * @return The version assigned to the new log entry, and the timestamp assigned to the new log entry
    */
    std::pair<persistent::version_t, uint64_t> update(uint32_t update_counter,
                                                      const derecho::Bytes& new_data) const;

    /** Actual implementation of update, only callable from within the subgroup as an ordered send. */
    void ordered_update(const derecho::Bytes& new_data);

    /**
     * Retrieves the data in the log at a specific version number. P2P-callable.
     */
    derecho::Bytes get(const persistent::version_t& version) const;
    /**
     * P2P-callable function that lets another node request a notification when a particular
     * update (identified by its version) has reached a particular state (locally/globally
     * persisted, signed)
     */
    void request_notification(node_id_t client_node_id, NotificationMessageType notification_type,
                              persistent::version_t version) const;
    /**
     * Function that implements the notification-checking thread. This thread waits for
     * updates to finish persisting and then sends notifications to clients who requested
     * them, to avoid blocking the P2P RPC thread.
     */
    void notification_thread_function();

    /** Notification function, which will be called on external clients of this subgroup. */
    void notify(const derecho::NotificationMessage& msg) const { derecho::NotificationSupport::notify(msg); }

    DEFAULT_SERIALIZATION_SUPPORT(StorageNode, object_log, my_subgroup_id);
    REGISTER_RPC_FUNCTIONS(StorageNode, ORDERED_TARGETS(ordered_update),
                           P2P_TARGETS(update, get, request_notification, notify));
};
