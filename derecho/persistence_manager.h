/**
 * @file persistence_manager.h
 *
 * @date Jun 20, 2017
 * @author Weijia
 */
#pragma once

#include <atomic>
#include <chrono>
#include <errno.h>
#include <queue>
#include <semaphore.h>
#include <thread>

#include "derecho_internal.h"
#include "replicated.h"

#include "mutils-containers/KindMap.hpp"
#include "persistent/Persistent.hpp"
#include "spdlog/spdlog.h"

namespace derecho {

template <typename T>
using replicated_index_map = std::map<uint32_t, Replicated<T>>;
using persistence_request_t = std::tuple<subgroup_id_t, persistent::version_t>;

/**
   * PersistenceManager is responsible for persisting all the data in a group.
   */
template <typename... ReplicatedTypes>
class PersistenceManager {
private:
    /** logger */
    std::shared_ptr<spdlog::logger> logger;

    /** Thread handle */
    std::thread persist_thread;
    /** A flag to singal the persistent thread to shutdown; set to true when the group is destroyed. */
    std::atomic<bool> thread_shutdown;
    /** The semaphore for persistence request the persistent thread */
    sem_t persistence_request_sem;
    /** a queue for the requests */
    std::queue<persistence_request_t> persistence_request_queue;

    /** persistence callback */
    persistence_callback_t persistence_callback;
    /** Replicated Objects handle: TODO:make it safer */
    mutils::KindMap<replicated_index_map, ReplicatedTypes...> *replicated_objects;
    /** View Manager pointer. Need to access the SST for the purpose of updating persisted_num*/
    ViewManager *view_manager;

public:
    /** Constructor
     * @param pro pointer to the replicated_objects.
     */
    PersistenceManager(
            mutils::KindMap<replicated_index_map, ReplicatedTypes...> *pro,
            const persistence_callback_t &_persistence_callback)
            : logger(spdlog::get("debug_log")),
              thread_shutdown(false),
              persistence_callback(_persistence_callback),
              replicated_objects(pro) {
        // initialize semaphore
        if(sem_init(&persistence_request_sem, 1, 0) != 0) {
            throw derecho_exception("Cannot initialize persistent_request_sem:errno=" + std::to_string(errno));
        }
    }

    /** default Constructor
     */
    PersistenceManager(const persistence_callback_t &_persistence_callback) : PersistenceManager(nullptr, _persistence_callback) {
    }

    /** default Destructor
     */
    virtual ~PersistenceManager() {
        sem_destroy(&persistence_request_sem);
    }

    /**
     * Set the 'replicated_objects' in case we can't get the replicated_object
     * 
     */
    template <typename... Types>
    typename std::enable_if<0 == sizeof...(Types), void>::type
    set_objects(mutils::KindMap<replicated_index_map> *pro) {
        //we don't need it for Raw Subgroups.
        this->replicated_objects = nullptr;
    }

    template <typename... Types>
    typename std::enable_if<!(0 == sizeof...(Types)), void>::type
    set_objects(mutils::KindMap<replicated_index_map, Types...> *pro) {
        this->replicated_objects = pro;
    }

    void set_view_manager(ViewManager *view_manager) {
        this->view_manager = view_manager;
    }

    /** Start the persistent thread. */
    void start() {
        //skip for raw subgroups -- NO, DON'T
        // if(replicated_objects == nullptr) return;

        this->persist_thread = std::thread{[this]() {
            std::cout << "The persist thread started" << std::endl;
            do {
                // wait for semaphore
                sem_wait(&persistence_request_sem);
                if(this->persistence_request_queue.empty()) {
                    continue;
                }

                subgroup_id_t subgroup_id = std::get<0>(persistence_request_queue.front());
                persistent::version_t version = std::get<1>(persistence_request_queue.front());
                persistence_request_queue.pop();

                // persist

                try {
                    this->replicated_objects->for_each([&](auto *pkey, replicated_index_map<auto> &map) {
                        auto search = map.find(subgroup_id);
                        if(search != map.end()) {
                            search->second.persist(version);
                        }
                    });
                    // read lock the view
                    std::shared_lock<std::shared_timed_mutex> read_lock(view_manager->view_mutex);
                    // update the persisted_num in SST

                    View &Vc = *view_manager->curr_view;
                    Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id] = version;
                    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                                   (char *)std::addressof(Vc.gmsSST->persisted_num[0][subgroup_id]) - Vc.gmsSST->getBaseAddress(),
                                   sizeof(long long int));
                } catch(uint64_t exp) {
                    SPDLOG_DEBUG(logger, "exception on persist():subgroup={},ver={},exp={}.", subgroup_id, version, exp);
                    std::cout << "exception on persistent:subgroup=" << subgroup_id << ",ver=" << version << "exception=0x" << std::hex << exp << std::endl;
                }

                // callback
                if(this->persistence_callback != nullptr) {
                    this->persistence_callback(subgroup_id, version);
                }

            } while(!this->thread_shutdown || !this->persistence_request_queue.empty());
            std::cout << "The persist thread is exiting" << std::endl;
        }};
    }

    /** post a persistence request */
    void post_persist_request(const subgroup_id_t &subgroup_id, const persistent::version_t &version) {
        // request enqueue
        persistence_request_queue.push(std::make_tuple(subgroup_id, version));
        // post semaphore
        sem_post(&persistence_request_sem);
    }

    /** make a version */
    void make_version(const subgroup_id_t &subgroup_id,
                      const persistent::version_t &version, const HLC &mhlc) {
        // find the corresponding Replicated<T> in replicated_objects
        this->replicated_objects->for_each([&](auto *pkey, replicated_index_map<auto> &map) {
            // make a version
            auto search = map.find(subgroup_id);
            if(search != map.end()) {
                search->second.make_version(version, mhlc);
            }
        });
    }

    /** shutdown the thread 
     * @wait - wait till the thread finished or not.
     */
    void shutdown(bool wait) {
        if(replicated_objects == nullptr) return;  //skip for raw subgroups

        thread_shutdown = true;
        sem_post(&persistence_request_sem);  // kick the persistence thread in case it is sleeping

        if(wait) {
            this->persist_thread.join();
        }
    }

    /** get the persistence callbacks. The multicast_group object will use this to notify
     *  the persistence thread about it.
     */
    persistence_manager_callbacks_t get_callbacks() {
        return std::make_tuple(
                [this](const subgroup_id_t &subgroup_id,
                       const persistent::version_t &ver,
                       const HLC &mhlc) {
                    this->make_version(subgroup_id, ver, mhlc);
                },
                [this](const subgroup_id_t &subgroup_id,
                       const persistent::version_t &ver) {
                    this->post_persist_request(subgroup_id, ver);
                });
    }
};
}
