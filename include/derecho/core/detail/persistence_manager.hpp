/**
 * @file persistence_manager.h
 *
 * @date Jun 20, 2017
 */
#pragma once

#include <atomic>
#include <chrono>
#include <errno.h>
#include <queue>
#include <semaphore.h>
#include <thread>

#include "derecho_internal.hpp"
#include "replicated_interface.hpp"
#include <derecho/openssl/signature.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/utils/logger.hpp>

namespace derecho {

/* Forward declaration to break circular include dependency between ViewManager
 * and PersistenceManager. */
class ViewManager;

/**
 * PersistenceManager is responsible for persisting all the data in a group.
 */
class PersistenceManager {
public:
    enum class RequestType {
        PERSIST,
        VERIFY
    };
    struct ThreadRequest {
        RequestType operation;
        subgroup_id_t subgroup_id;
        persistent::version_t version;
    };
private:
    /** Thread handle */
    std::thread persist_thread;
    /**
     * A flag to signal the persistent thread to shutdown; set to true when the
     * group is destroyed.
     */
    std::atomic<bool> thread_shutdown;
    /**
     * A semaphore that counts the number of persistence requests available for
     * the persistence thread to handle
     */
    sem_t persistence_request_sem;
    /**
     * Queue of requests for the persistence thread, which is shared with other
     * threads (e.g. the predicates thread) so they can make requests
     */
    std::queue<ThreadRequest> persistence_request_queue;
    /** A test-and-set lock guarding the persistence request queue */
    std::atomic_flag prq_lock = ATOMIC_FLAG_INIT;
    /**
     * The latest version that has been persisted successfully in each subgroup
     * (indexed by subgroup number). Updated each time a persistence request completes.
     */
    std::vector<persistent::version_t> last_persisted_version;
    /**
     * The Verifier to use for verifying other replicas' signatures over
     * persistent log entries, if signatures are enabled. This will be null if
     * signatures are disabled.
     */
    std::unique_ptr<openssl::Verifier> signature_verifier;
    /** The size of a signature (which is a constant), or 0 if signatures are disabled. */
    std::size_t signature_size;
    /** The global persistence callback */
    persistence_callback_t persistence_callback;
    /** Reference to the ReplicatedObjects map in the Group that owns this PersistenceManager. */
    std::map<subgroup_id_t, ReplicatedObject*>& objects_by_subgroup_id;
    /**
     * Pointer to the ViewManager in the Group that owns this PersistenceManager.
     * This is needed to access the SST in the current View (for updating signatures and
     * persisted_num), but must be initialized after construction since ViewManager
     * also needs a reference to PersistenceManager.
     */
    ViewManager* view_manager;
    /** Helper function that handles a single persistence request */
    void handle_persist_request(subgroup_id_t subgroup_id, persistent::version_t version);
    /** Helper function that handles a single verification request */
    void handle_verify_request(subgroup_id_t subgroup_id, persistent::version_t version);
public:
    /**
     * Constructor.
     * @param objects_map A reference to the objects_by_subgroup_id from Group.
     * @param any_signed_objects True if at least one of the replicated objects
     * uses a Persistent<T> field with signatures enabled, false otherwise
     * @param _persistence_callback The persistence callback function to call when
     * new versions are done persisting
     */
    PersistenceManager(
            std::map<subgroup_id_t, ReplicatedObject*>& objects_map,
            bool any_signed_objects,
            const persistence_callback_t& _persistence_callback);

    /**
     * Custom destructor needed to clean up the semaphore
     */
    virtual ~PersistenceManager();

    /** Initializes the ViewManager pointer. Must be called before start(). */
    void set_view_manager(ViewManager& view_manager);

    //This method is probably unnecessary since ViewManager should have other ways of determining the signature size.
    /** @return the size of a signature on an update in this group. */
    std::size_t get_signature_size() const;

    /** Start the persistent thread. */
    void start();

    /** post a persistence request */
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

    /** shutdown the thread
     * @wait - wait till the thread finished or not.
     */
    void shutdown(bool wait);
};
}  // namespace derecho
