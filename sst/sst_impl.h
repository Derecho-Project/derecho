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

#include "poll_utils.h"
#include "predicates.h"
// #include "rdmc/util.h"
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
void SST<DerivedSST>::put(const std::vector<uint32_t>& receiver_ranks, long long int offset, long long int size) {
    // DERECHO_LOG(-1, -1, "start_put");
    // int num_called = 0;
    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        res_vec[index]->post_remote_write(0, offset, size);
	// num_called++;
    }
    // DERECHO_LOG(num_called, -1, "end_put");
    return;
}

template <typename DerivedSST>
void SST<DerivedSST>::put_with_completion(const std::vector<uint32_t>& receiver_ranks, long long int offset, long long int size) {
    unsigned int num_writes_posted = 0;
    std::vector<bool> posted_write_to(num_members, false);

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);

    util::polling_data.set_waiting(tid);

    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == my_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        res_vec[index]->post_remote_write_with_completion(id, offset, size);
        posted_write_to[index] = true;
        num_writes_posted++;
    }

    // track which nodes haven't failed yet
    std::vector<bool> polled_successfully_from(num_members, false);

    std::vector<uint32_t> failed_node_indexes;

    /** Completion Queue poll timeout in millisec */
    const int MAX_POLL_CQ_TIMEOUT = 2000;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;

    // wait for completion for a while before giving up of doing it ..
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

    // poll for surviving number of rows
    for(unsigned int index = 0; index < num_writes_posted; ++index) {
        std::experimental::optional<std::pair<int32_t, int32_t>> ce;

        while(true) {
            // check if polling result is available
            ce = util::polling_data.get_completion_entry(tid);
            if(ce) {
                break;
            }
            gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
            if((cur_time_msec - start_time_msec) >= MAX_POLL_CQ_TIMEOUT) {
                break;
            }
        }
        // if waiting for a completion entry timed out
        if(!ce) {
            // find some node that hasn't been polled yet and report it
            for(unsigned int index2 = 0; index2 < num_members; ++index2) {
                if(!posted_write_to[index2] || polled_successfully_from[index2]) {
                    continue;
                }

                std::cout << "Reporting failure on row " << index2
                          << " due to a missing poll completion" << std::endl;
                failed_node_indexes.push_back(index2);
            }
            continue;
        }

        auto ce_v = ce.value();
        int qp_num = ce_v.first;
        int result = ce_v.second;
        if(result == 1) {
            int index = qp_num_to_index[qp_num];
            polled_successfully_from[index] = true;
        } else if(result == -1) {
            int index = qp_num_to_index[qp_num];
            if(!row_is_frozen[index]) {
                std::cerr << "Poll completion error in QP " << qp_num
                          << ". Freezing row " << index << std::endl;
                failed_node_indexes.push_back(index);
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
}
