/**
 * @file persistence_manager.hpp
 *
 * @date Jun 20, 2017
 */
#pragma once

#include <derecho/config.h>
#include <derecho/openssl/signature.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/utils/logger.hpp>
#include "derecho_internal.hpp"
#include "replicated_interface.hpp"

#include <atomic>
#include <chrono>
#include <errno.h>
#include <list>
#include <queue>
#include <semaphore.h>
#include <thread>

namespace derecho {

/* Forward declaration to break circular include dependency between ViewManager
 * and PersistenceManager. */
class ViewManager;

/**
 * PersistenceManager is responsible for persisting all the data in a group.
 */
class PersistenceManager {
public:
    struct ThreadRequest {
        subgroup_id_t subgroup_id;
        persistent::version_t version;
    };

private:
    /** Pointer to the persistence-module logger */
    std::shared_ptr<spdlog::logger> persistence_logger;
    /** Thread handle for the persistence thread */
    std::thread persist_thread;
    /** Thread handle for the verification thread */
    std::thread verify_thread;
    /**
     * A flag to signal the PersistenceManager's threads to shutdown; set to
     * true when the group is destroyed.
     */
    std::atomic<bool> thread_shutdown;
    /**
     * A semaphore that counts the number of persistence requests available for
     * the persistence thread to handle
     */
    sem_t persistence_request_sem;
    /**
     * A semaphore that counts the number of verification requests available for
     * the verification thread to handle
     */
    sem_t verification_request_sem;
    /**
     * Queue of requests for the persistence thread, which is shared with other
     * threads (e.g. the predicates thread) so they can make requests
     */
    std::queue<ThreadRequest> persistence_request_queue;
    /**
     * Queue of requests for the verification thread, which is shared with the
     * predicates thread so it can make requests.
     */
    std::queue<ThreadRequest> verify_request_queue;
    /** A test-and-set lock guarding the persistence request queue */
    std::atomic_flag prq_lock = ATOMIC_FLAG_INIT;
    /** A test-and-set lock guarding the verification request queue */
    std::atomic_flag vrq_lock = ATOMIC_FLAG_INIT;
    /**
     * The latest version that has been persisted successfully in each subgroup
     * (indexed by subgroup number). Updated each time a persistence request completes.
     * This is equal to this node's row of the SST field persisted_num, but cached
     * in non-SST memory to more easily check if a persistence request is obsolete.
     */
    std::vector<persistent::version_t> last_persisted_version;
    /**
     * The latest version that has been signed and verified in each subgroup,
     * indexed by subgroup number. This is equal to this node's row of the SST field
     * verified_num but cached in non-SST memory, analogous to last_persisted_version.
     * Remains at -1 for all subgroups if signatures are not enabled in this group.
     */
    std::vector<persistent::version_t> last_verified_version;
    /** The size of a signature (which is a constant), or 0 if signatures are disabled. */
    std::size_t signature_size;
    /**
     * The persistence callback(s), which will be called to notify clients that
     * a particular version has finished persisting locally (on this node).
     */
    std::list<persistence_callback_t> persistence_callbacks;
    /** Reference to the ReplicatedObjects map in the Group that owns this PersistenceManager. */
    std::map<subgroup_id_t, ReplicatedObject*>& objects_by_subgroup_id;
    /**
     * Pointer to the ViewManager in the Group that owns this PersistenceManager.
     * This is needed to access the SST in the current View (for updating signatures and
     * persisted_num), but must be initialized after construction since ViewManager
     * also needs a reference to PersistenceManager.
     */
    ViewManager* view_manager;
    /**
     * Handles a single persistence request that was previously enqueued by
     * post_persist_request. Ensures the specified subgroup persists its
     * persistent fields, if any, up through the current version, and adds
     * signatures to each persisted version if signatures are enabled. Then
     * calls local persistence callbacks and updates the relevant SST columns
     * for this subgroup. Note that the parameter version is only a hint as to
     * what the subgroup's latest version was at the time of the request; this
     * method may actually persist a later version due to Persistent<T>'s
     * batching, or a slightly earlier version if the latest version has no
     * persistent state (i.e. corresponds to a null message).
     *
     * @param subgroup_id The subgroup in which to persist data.
     * @param version The current version in this subgroup at the time the
     * request was posted.
     */
    void handle_persist_request(subgroup_id_t subgroup_id, persistent::version_t version);
    /**
     * Handles a single verification request that was previously enqueued by
     * post_verification_request. Checks that other replicas have the same
     * signature for the current version, then updates the relevant SST columns.
     * Note that the version parameter has no effect on which version is
     * verified; it is only used to determine if the request is already obsolete
     * because a later version has already been verified.
     *
     * @param subgroup_id The subgroup in which to verify signatures.
     * @param version The current version in this subgroup at the time the
     * request was posted.
     */
    void handle_verify_request(subgroup_id_t subgroup_id, persistent::version_t version);

public:
    /**
     * Constructor.
     * @param objects_map A reference to the objects_by_subgroup_id from Group.
     * @param any_signed_objects True if at least one of the replicated objects
     * uses a Persistent<T> field with signatures enabled, false otherwise
     * @param user_persistence_callback The user-provided persistence callback
     * function to call when new versions are done persisting
     */
    PersistenceManager(
            std::map<subgroup_id_t, ReplicatedObject*>& objects_map,
            bool any_signed_objects,
            const persistence_callback_t& user_persistence_callback);

    /**
     * Custom destructor needed to clean up the semaphore
     */
    virtual ~PersistenceManager();

    /** Initializes the ViewManager pointer. Must be called before start(). */
    void set_view_manager(ViewManager& view_manager);

    /** Adds another function to the list of persistence callbacks, which are
     * called when a version finishes persisting locally. */
    void add_persistence_callback(const persistence_callback_t& callback);

    //This method is probably unnecessary since ViewManager should have other ways of determining the signature size.
    /** @return the size of a signature on an update in this group. */
    std::size_t get_signature_size() const;

    /** Start the persistence and verification threads. */
    void start();

    /**
     * Ask the persistence manager to persist any unpersisted log entries in
     * a particular subgroup. Called by MulticastGroup when at least one non-null
     * message has been delivered in that subgroup.
     *
     * @param subgroup_id The subgroup in which to persist persistent state.
     * @param version The subgroup's latest version number at the time the
     * request was posted.
     */
    void post_persist_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version);

    /**
     * Ask the persistence manager to verify a new version that has been
     * persisted by other nodes in a particular subgroup. If verification
     * succeeds, this will update verified_num in the SST.
     * @param subgroup_id The subgroup in which to verify the other members'
     * signatures
     * @param version The minimum version number that has been persisted by all
     * the other members of this node's shard in the subgroup.
     */
    void post_verify_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version);

    /** make a version */
    void make_version(const subgroup_id_t& subgroup_id,
                      const persistent::version_t& version, const HLC& mhlc);

    /**
     * Shutdown the threads
     * @param   wait    Whether to wait until both threads have finished
     */
    void shutdown(bool wait);
};
}  // namespace derecho
