/**
 * @file sst_impl.h
 *
 * @date Oct 28, 2016
 * @author edward
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <time.h>
#include <thread>
#include <vector>

#include "predicates.h"
#include "sst.h"

namespace sst {

/**
 * Destructor for the SST object; sets thread_shutdown to true and waits for
 * background threads to exit cleanly.
 */
template <typename DerivedSST>
SST<DerivedSST>::~SST() {
    if(rows != nullptr) {
        delete[](const_cast<char*>(rows));
    }

    thread_shutdown = true;
    for(auto& thread : background_threads) {
        if(thread.joinable()) thread.join();
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
            double time_elapsed_in_ms = (cur_time.tv_sec - last_time.tv_sec) * 1e3 + (cur_time.tv_nsec - last_time.tv_nsec) / 1e6;
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
void SST<DerivedSST>::put(std::vector<uint32_t> receiver_ranks, long long int offset, long long int size) {
    unsigned int num_writes_posted = 0;
    std::vector<bool> posted_write_to(num_members, false);

    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        res_vec[index]->post_remote_write(offset, size);
        posted_write_to[index] = true;
        num_writes_posted++;
    }

    // track which nodes haven't failed yet
    std::vector<bool> polled_successfully_from(num_members, false);

    // poll for surviving number of rows
    for(unsigned int index = 0; index < num_writes_posted; ++index) {
        // poll for completion
        auto p = verbs_poll_completion();
        int qp_num = p.first;
        int result = p.second;
        if(result == 1) {
            int index = qp_num_to_index[qp_num];
            // assert(posted_write_to[index]);
            polled_successfully_from[index] = true;
        } else if(result == -1) {
            int index = qp_num_to_index[qp_num];
            if(!row_is_frozen[index]) {
                std::cerr << "Poll completion error in QP " << qp_num
                          << ". Freezing row " << index << std::endl;
                freeze(index);
                return;
            }
        } else if(result == 0) {
            // find some node that hasn't been polled yet and report it
            for(unsigned int index2 = 0; index2 < num_members; ++index2) {
                if(!posted_write_to[index2] ||
                   polled_successfully_from[index2]) {
                    continue;
                }

                std::cout << "Reporting failure on row " << index2
                          << " even though it didn't fail directly" << std::endl;
                freeze(index2);
                return;
            }
        }
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
    res_vec[row_index].reset();
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
}
