/**
 * @file sst_impl.hpp
 *
 * @date Oct 28, 2016
 */

#pragma once

#include <derecho/config.h>
#include "../sst.hpp"

#include "../predicates.hpp"
#include <derecho/utils/time.h>
#include "poll_utils.hpp"
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sys/time.h>
#include <thread>
#include <time.h>
#include <vector>

namespace sst {

/**
 * Destructor for the SST object; sets thread_shutdown to true and waits for
 * background threads to exit cleanly.
 */
template <typename DerivedSST>
SST<DerivedSST>::~SST() {
    thread_shutdown = true;
    for(auto& thread : background_threads) {
        if(thread.joinable()) thread.join();
    }

    if(rows != nullptr) {
        delete[](const_cast<uint8_t*>(rows));
    }
}

/**
 * This simply unblocks the background thread that runs the predicate evaluation
 * loop. It must be called at some point after the the constructor in order for
 * any registered predicates to trigger.
 */
template <typename DerivedSST>
void SST<DerivedSST>::start_predicate_evaluation() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}

/**
 * This function is run in a detached background thread to detect predicate
 * events. It continuously evaluates predicates one by one, and runs the
 * trigger functions for each predicate that fires. In addition, it
 * continuously evaluates named functions one by one, and updates the local
 * row's observed values of those functions.
 */
template <typename DerivedSST>
void SST<DerivedSST>::detect() {
    pthread_setname_np(pthread_self(), "sst_detect");
    if(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    uint64_t last_time_ms = get_walltime() / INT64_1E6;

    while(!thread_shutdown) {
        bool predicate_fired = false;
        // Take the predicate lock before reading the predicate lists
        std::unique_lock<std::mutex> predicates_lock(predicates.predicate_mutex);

        // one time predicates need to be evaluated only until they become true
        for(auto& pred : predicates.one_time_predicates) {
            if(pred != nullptr && (pred->first(*derived_this) == true)) {
                predicate_fired = true;
                // Copy the trigger pointer locally, so it can continue running without
                // segfaulting even if this predicate gets deleted when we unlock predicates_lock
                std::shared_ptr<typename Predicates<DerivedSST>::trig> trigger(pred->second);
                predicates_lock.unlock();
                (*trigger)(*derived_this);
                predicates_lock.lock();
                // erase the predicate as it was just found to be true
                pred.reset();
            }
        }

        // recurrent predicates are evaluated each time they are found to be true
        for(auto& pred : predicates.recurrent_predicates) {
            if(pred != nullptr && (pred->first(*derived_this) == true)) {
                predicate_fired = true;
                std::shared_ptr<typename Predicates<DerivedSST>::trig> trigger(pred->second);
                predicates_lock.unlock();
                (*trigger)(*derived_this);
                predicates_lock.lock();
            }
        }

        // transition predicates are only evaluated when they change from false to true
        // We need to use iterators here because we need to iterate over two lists in parallel
        auto pred_it = predicates.transition_predicates.begin();
        auto pred_state_it = predicates.transition_predicate_states.begin();
        while(pred_it != predicates.transition_predicates.end()) {
            if(*pred_it != nullptr) {
                //*pred_state_it is the previous state of the predicate at *pred_it
                bool curr_pred_state = (*pred_it)->first(*derived_this);
                if(curr_pred_state == true && *pred_state_it == false) {
                    predicate_fired = true;
                    std::shared_ptr<typename Predicates<DerivedSST>::trig> trigger(
                            (*pred_it)->second);
                    predicates_lock.unlock();
                    (*trigger)(*derived_this);
                    predicates_lock.lock();
                }
                *pred_state_it = curr_pred_state;

                ++pred_it;
                ++pred_state_it;
            }
        }

        if(predicate_fired) {
            // update last time
            last_time_ms = get_walltime() / INT64_1E6;
        } else {
            // check if the system has been inactive for enough time to induce sleep
            uint64_t time_elapsed_in_ms = ( get_walltime() / INT64_1E6 ) - last_time_ms;
            if(time_elapsed_in_ms > 100) {
                predicates_lock.unlock();
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
                predicates_lock.lock();
            }
        }
        //Still to do: Clean up deleted predicates
    }
}

template <typename DerivedSST>
void SST<DerivedSST>::put(const std::vector<uint32_t> receiver_ranks, size_t offset, size_t size) {
    assert(offset + size <= rowLen);
    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        res_vec[index]->post_remote_write(offset, size);
    }
    return;
}

template <typename DerivedSST>
void SST<DerivedSST>::put_with_completion(const std::vector<uint32_t> receiver_ranks, size_t offset, size_t size) {
    assert(offset + size <= rowLen);
    unsigned int num_writes_posted = 0;
    std::vector<bool> posted_write_to(num_members, false);

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t ce_idx = util::polling_data.get_index(tid);

    util::polling_data.set_waiting(tid);
#ifdef USE_VERBS_API
    verbs_sender_ctxt ce_ctxt[receiver_ranks.size()];
#else
    lf_completion_entry_ctxt ce_ctxt[receiver_ranks.size()];
#endif
    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        ce_ctxt[index].set_remote_id(res_vec[index]->remote_id);
        ce_ctxt[index].set_ce_idx(ce_idx);
        res_vec[index]->post_remote_write_with_completion(&ce_ctxt[index], offset, size);
        posted_write_to[index] = true;
        num_writes_posted++;
    }

    // track which nodes respond successfully
    std::vector<bool> polled_successfully_from(num_members, false);

    std::vector<uint32_t> failed_node_indexes;

    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;

    // wait for completions for a while but eventually give up on it
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

    // poll for a single completion for each write request submitted
    for(auto index : receiver_ranks) {
        // didn't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        std::optional<int32_t> result;
        while(true) {
            result = util::polling_data.get_completion_entry(tid,res_vec[index]->remote_id);
            if (result) {
                break;
            }
            gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
            if((cur_time_msec - start_time_msec) >= poll_cq_timeout_ms) {
                break;
            }
        }
        if (result && result.value() == 1) {
            polled_successfully_from[index] = true;
        } else {
            failed_node_indexes.push_back(index);
        }
    }

    util::polling_data.reset_waiting(tid);

    for(auto index : failed_node_indexes) {
        freeze(index);
    }
}

template <typename DerivedSST>
void SST<DerivedSST>::freeze(uint32_t row_index) {
    // If some other node reported this one as failed,
    // don't attempt to freeze your own row
    if(row_index == my_index) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(freeze_mutex);
        if(row_is_frozen[row_index]) {
            return;
        }
        row_is_frozen[row_index] = true;
    }
    num_frozen++;
    res_vec[row_index]->report_failure();
    //We can't delete from res_vec here because it creates a race with put(),
    //but maybe marking the resource object as "failed" is good enough.
    //    res_vec[row_index].reset();
    if(failure_upcall) {
        failure_upcall(members[row_index]);
    }
}

/**
 * Exchanges a single byte of data with each member of the SST group over the
 * TCP (not RDMA) connection, in descending order of the members' node ranks.
 * This creates a synchronization barrier, since the TCP reads are blocking,
 * and should be called after SST initialization to ensure all nodes have
 * finished initializing their local SST code.
 */
template <typename DerivedSST>
void SST<DerivedSST>::sync_with_members() const {
    unsigned int node_id, sst_index;
    for(auto const& id_index : members_by_id) {
        std::tie(node_id, sst_index) = id_index;
        if(sst_index != my_index && !row_is_frozen[sst_index]) {
            sync(node_id);
        }
    }
}

/**
 * Same as before but syncs with only a subset of the members
 */
template <typename DerivedSST>
void SST<DerivedSST>::sync_with_members(std::vector<uint32_t> row_indices) const {
    for(auto const& row_index : row_indices) {
        if(row_index == my_index) {
            continue;
        }
        if(!row_is_frozen[row_index]) {
            sync(members[row_index]);
        }
    }
}
}  // namespace sst
