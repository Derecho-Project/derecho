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
#include "public_key_store.hpp"
#include "replicated_interface.hpp"
#include <derecho/openssl/signature.hpp>
#include <derecho/persistent/PersistentTypenames.hpp>
#include <derecho/utils/logger.hpp>

namespace derecho {

/* Forward declaration to break circular include dependency between ViewManager
 * and PersistenceManager. */
class ViewManager;

using persistence_request_t = std::tuple<subgroup_id_t, persistent::version_t>;

/**
 * PersistenceManager is responsible for persisting all the data in a group.
 */
class PersistenceManager {
private:
    /** Thread handle */
    std::thread persist_thread;
    /** A flag to singal the persistent thread to shutdown; set to true when the group is destroyed. */
    std::atomic<bool> thread_shutdown;
    /** The semaphore for persistence request the persistent thread */
    sem_t persistence_request_sem;
    /** a queue for the requests */
    std::queue<persistence_request_t> persistence_request_queue;
    /** lock for persistence request queue */
    std::atomic_flag prq_lock = ATOMIC_FLAG_INIT;
    /** Shared pointer to the PublicKeyStore that keeps track of public keys for each node, if signatures are enabled. */
    std::shared_ptr<PublicKeyStore> node_public_keys;
    /** The size of a signature, determined at startup by reading the local private key. 0 if signatures are disabled. */
    std::size_t signature_size;
    /** persistence callback */
    persistence_callback_t persistence_callback;
    /** Reference to the ReplicatedObjects map in the Group that owns this PersistenceManager. */
    std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>& objects_by_subgroup_id;
    /**
     * Pointer to the ViewManager in the Group that owns this PersistenceManager.
     * This is needed to access the SST in the current View (for updating signatures and
     * persisted_num), but must be initialized after construction since ViewManager
     * also needs a reference to PersistenceManager.
     */
    ViewManager* view_manager;

public:
    /**
     * Constructor.
     * @param public_key_store A shared pointer to the PublicKeyStore for the group,
     * if signatures are enabled
     * @param objects_map A reference to the objects_by_subgroup_id from Group.
     * @param _persistence_callback The persistence callback function to call when
     * new versions are done persisting
     */
    PersistenceManager(
            std::shared_ptr<PublicKeyStore> public_key_store,
            std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>& objects_map,
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

    /** make a version */
    void make_version(const subgroup_id_t& subgroup_id,
                      const persistent::version_t& version, const HLC& mhlc);

    /** shutdown the thread
     * @wait - wait till the thread finished or not.
     */
    void shutdown(bool wait);
};
}  // namespace derecho
