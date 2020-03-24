/**
 * @file sst_impl.h
 *
 * @date Oct 28, 2016
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sys/time.h>
#include <thread>
#include <time.h>
#include <vector>

#include "poll_utils.hpp"
#include "../predicates.hpp"
#include "../sst.hpp"

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
        delete[](const_cast<char*>(rows));
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
    struct timespec last_time, cur_time;
    clock_gettime(CLOCK_REALTIME, &last_time);

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
            clock_gettime(CLOCK_REALTIME, &last_time);
        } else {
            clock_gettime(CLOCK_REALTIME, &cur_time);
            // check if the system has been inactive for enough time to induce sleep
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3
                                        + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
            if(time_elapsed_in_ms > 1) {
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
    struct verbs_sender_ctxt sctxt[receiver_ranks.size()];
#else
    struct lf_sender_ctxt sctxt[receiver_ranks.size()];
#endif
    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        sctxt[index].set_remote_id(index);
        sctxt[index].set_ce_idx(ce_idx);
        res_vec[index]->post_remote_write_with_completion(&sctxt[index], offset, size);
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
    for(unsigned int index = 0; index < num_writes_posted; ++index) {
        std::optional<std::pair<int32_t, int32_t>> ce;

        while(true) {
            // check if polling result is available
            ce = util::polling_data.get_completion_entry(tid);
            if(ce) {
                break;
            }
            gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
            if((cur_time_msec - start_time_msec) >= poll_cq_timeout_ms) {
                break;
            }
        }
        // if waiting for a completion entry timed out
        if(!ce) {
            // mark all nodes that have not yet responded as failed
            for(unsigned int index2 = 0; index2 < num_members; ++index2) {
                if(!posted_write_to[index2] || polled_successfully_from[index2]) {
                    continue;
                }
                failed_node_indexes.push_back(index2);
            }
            break;
        }

        auto ce_v = ce.value();
        int remote_id = ce_v.first;
        int result = ce_v.second;
        if(result == 1) {
            polled_successfully_from[remote_id] = true;
        } else if(result == -1) {
            if(!row_is_frozen[index]) {
                failed_node_indexes.push_back(remote_id);
            }
        }
    }

    util::polling_data.reset_waiting(tid);

    for(auto index : failed_node_indexes) {
        freeze(index);
    }
}

template <typename DerivedSST>
void SST<DerivedSST>::freeze(int row_index) {
    {
        std::lock_guard<std::mutex> lock(freeze_mutex);
        if(row_is_frozen[row_index]) {
            return;
        }
        row_is_frozen[row_index] = true;
    }
    num_frozen++;
    //BUG: deleting from res_vec here creates a race with put(), which blindly
    //dereferences res_vec[index] after checking fow_is_frozen
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
