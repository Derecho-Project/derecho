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

#include "../replicated.hpp"
#include "derecho_internal.hpp"
#include "view_manager.hpp"
#include <derecho/persistent/Persistent.hpp>
#include <derecho/utils/logger.hpp>

namespace derecho {

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

    /** persistence callback */
    persistence_callback_t persistence_callback;
    /** Replicated Objects handle: TODO:make it safer */
    std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>& objects_by_subgroup_id;
    /** View Manager pointer. Need to access the SST for the purpose of updating persisted_num*/
    ViewManager* view_manager;

public:
    /** Constructor
     * @param objects_map reference to the objects_by_subgroup_id from Group.
     */
    PersistenceManager(
            std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>& objects_map,
            const persistence_callback_t& _persistence_callback);


    /** default Destructor
     */
    virtual ~PersistenceManager();

    void set_view_manager(ViewManager& view_manager);

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

    /** get the persistence callbacks. The multicast_group object will use this to notify
     *  the persistence thread about it.
     */
    persistence_manager_callbacks_t get_callbacks();
};
}  // namespace derecho
