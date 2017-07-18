/**
 * @file persistence_manager.h
 *
 * @date Jun 20, 2017
 * @author Weijia
 */
#pragma once

#include <chrono>
#include <thread>
#include <atomic>

#include "replicated.h"

#include "spdlog/spdlog.h"
#include "mutils-containers/KindMap.hpp"
#include "persistent/Persistent.hpp"

#define PERSISTENCE_INTERVAL_MILLISECONDS (500)

namespace derecho {

  template <typename T>
  using replicated_index_map = std::map<uint32_t, Replicated<T>>;

  using post_persistence_request_func_t = std::function<void(subgroup_id_t,message_id_t)>;

  /**
   * PersistenceManager is responsible for persisting all the data in a group.
   */
  template<typename... ReplicatedTypes>
  class PersistenceManager {
  private:
    /** logger */
    std::shared_ptr<spdlog::logger> logger;

    /** Thread handle */
    std::thread persist_thread;
    /** A flag to singal the persistent thread to shutdown; set to true when the group is destroyed. */
    std::atomic<bool> thread_shutdown;
    /** A flag to singal the persistent thread to do persistent work; */
    std::atomic<bool> pending_request;

    /** Replicated Objects handle: TODO:make it safer */
    mutils::KindMap<replicated_index_map, ReplicatedTypes...> *replicated_objects;
 
  public:
    /** Constructor
     * @param pro pointer to the replicated_objects.
     */
    PersistenceManager(
      mutils::KindMap<replicated_index_map, ReplicatedTypes...> *pro
      )
      : logger(spdlog::get("debug_log")),
        thread_shutdown(false),
        pending_request(false),
        replicated_objects(pro) {
    }

    /** default Constructor
     */
    PersistenceManager():PersistenceManager(nullptr){
    }

    /**
     * Set the 'replicated_objects' in case we can't get the replicated_object
     * 
     */
    template <typename...Types>
    typename std::enable_if<0 == sizeof...(Types),void>::type
    set_objects(mutils::KindMap<replicated_index_map> *pro) {
      //we don't need it for Raw Subgroups.
      this->replicated_objects = nullptr;
    }

    template <typename...Types>
    typename std::enable_if<!(0 == sizeof...(Types)),void>::type
    set_objects(mutils::KindMap<replicated_index_map, Types...> *pro) {
      this->replicated_objects = pro;
    }

    /** Start the persistent thread. */
    void start() {
      if (replicated_objects == nullptr) return; //skip for raw subgroups

      this->persist_thread = std::thread{[this](){
        do{
          bool expected = true;
          if(this->pending_request.compare_exchange_strong(expected,false)) {
            // if there is some pending request for persistence:
            // go through the replicated object and perform persistent
            this->replicated_objects->for_each([&](auto *pkey, replicated_index_map<auto> & map){
              for(auto it=map.begin();it != map.end(); ++it) {
                // iterate through all subgroups
                // it->first is the subgroup index
                // it->second is the corresponding Replicated<T>
                // T = decltype(*pkey)
                it->second.persist();
              } 
            });
          } else {
            // sleep for PERSISTENCE_INTERVAL_MILLISECONDS milliseconds.
            std::this_thread::sleep_for(std::chrono::milliseconds(PERSISTENCE_INTERVAL_MILLISECONDS));
          }
        } while(!this->thread_shutdown || this->pending_request);
      }};
    }

    /** post a persistence request */
    void post_request(subgroup_id_t subgroup_id, message_id_t message_id) {
      // find the corresponding Replicated<T> in replicated_objects
      this->replicated_objects->for_each([&](auto *pkey, replicated_index_map<auto> & map){
        // make a version
        auto search = map.find(subgroup_id);
        if (search != map.end()) {
          search->second.make_version(message_id);
          // post a request by setting the flag
          pending_request = true;
        }
      });
    }

    /** shutdown the thread 
     * @wait - wait till the thread finished or not.
     */
    void shutdown(bool wait) {
      if (replicated_objects == nullptr) return; //skip for raw subgroups

      thread_shutdown = true;
      if (wait) {
        this->persist_thread.join();
      }
    }
  };
}
