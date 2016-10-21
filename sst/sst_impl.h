#ifndef SST_IMPL_H
#define SST_IMPL_H

// This will be included at the bottom of sst.h

#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>

#include "predicates.h"
#include "sst.h"

namespace sst {

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
SST<Row, ImplMode, NameEnum, RowExtras>::SST(
    const vector<uint32_t> &_members, uint32_t my_node_id,
    std::pair<decltype(named_functions), std::vector<row_predicate_updater_t>>
        row_preds,
    failure_upcall_t _failure_upcall, std::vector<char> already_failed,
    bool start_predicate_thread)
        : named_functions(row_preds.first),
          members(_members.size()),
          num_members(_members.size()),
          table(new InternalRow[_members.size()]),
          failure_upcall(_failure_upcall),
          row_predicate_updater_functions(row_preds.second),
          res_vec(num_members),
          background_threads(),
          thread_shutdown(false),
          thread_start(start_predicate_thread),
          predicates(*(new Predicates())) {
    // copy members and figure out the member_index
    for(uint32_t i = 0; i < num_members; ++i) {
        members[i] = _members[i];
        if(members[i] == my_node_id) {
            member_index = i;
        }
    }

    if(already_failed.size()) {
        assert(already_failed.size() == num_members);
        for(char c : already_failed) {
            if(c) {
                row_is_frozen.push_back(true);
            } else {
                row_is_frozen.push_back(false);
            }
        }
    } else {
        row_is_frozen.resize(num_members, false);
    }

    // sort members descending by node rank, while keeping track of their
    // specified index in the SST
    for(unsigned int sst_index = 0; sst_index < num_members; ++sst_index) {
        members_by_rank[members[sst_index]] = sst_index;
    }

    // initialize each element of res_vec
    unsigned int node_rank, sst_index;
    for(auto const &rank_index : members_by_rank) {
        std::tie(node_rank, sst_index) = rank_index;
        char *write_addr, *read_addr;
        if(ImplMode == Mode::Reads) {
            write_addr = (char *)&(table[member_index]);
            read_addr = (char *)&(table[sst_index]);
        } else {
            write_addr = (char *)&(table[sst_index]);
            read_addr = (char *)&(table[member_index]);
        }
        int size = sizeof(table[0]);
        if(sst_index != member_index) {
            if(row_is_frozen[sst_index]) {
                continue;
            }
            // exchange lkey and addr of the table via tcp for enabling rdma
            // reads
            res_vec[sst_index] = std::make_unique<resources>(
                node_rank, write_addr, read_addr, size, size);
            // update qp_num_to_index
            qp_num_to_index[res_vec[sst_index].get()->qp->qp_num] = sst_index;
        }
    }

    if(ImplMode == Mode::Reads) {
        // create the reader and the detector thread
        thread reader(&SST::read, this);
        background_threads.push_back(std::move(reader));
    }
    thread detector(&SST::detect, this);
    background_threads.push_back(std::move(detector));

    cout << "Initialized SST and Started Threads" << endl;
}

/**
 * Destructor for the state table; sets thread_shutdown to true and waits for
 * background threads to exit cleanly.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
SST<Row, ImplMode, NameEnum, RowExtras>::~SST() {
    thread_shutdown = true;
    for(auto &thread : background_threads) {
        if(thread.joinable()) thread.join();
    }
    // Even though predicates is a reference, we actually created it with an
    // unmanaged new
    delete &predicates;
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::delete_all_predicates() {
    predicates.clear();
}

/**
 * This simply unblocks the background thread that runs the predicate evaluation
 * loop. It must be called at some point after the the constructor in order for
 * any registered predicates to trigger.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::start_predicate_evaluation() {
    std::lock_guard<std::mutex> lock(thread_start_mutex);
    thread_start = true;
    thread_start_cv.notify_all();
}
/**
 * Although a mutable reference is returned, only the local row should be
 * modified through this function. Modifications to remote rows will not be
 * propagated to other nodes and may be overwritten at any time when the SST
 * system updates those remote rows.
 *
 * @param index The index of the row to access.
 * @return A reference to the row structure stored at the requested row.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
volatile typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
SST<Row, ImplMode, NameEnum, RowExtras>::get(unsigned int index) {
    // check that the index is within range
    assert(index >= 0 && index < num_members);

    // return the table entry
    return table[index];
}

/**
 * Even the local row will be immutable when accessed through this method.
 *
 * @param index The index of the row to access.
 * @return A reference to the row structure stored at the requested row.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
const volatile typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
SST<Row, ImplMode, NameEnum, RowExtras>::get(unsigned int index) const {
    assert(index >= 0 && index < num_members);
    return table[index];
}

/**
 * Simply calls the const get function.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
const volatile typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
    SST<Row, ImplMode, NameEnum, RowExtras>::
    operator[](unsigned int index) const {
    return get(index);
}

/**
 * Simply calls the get function.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
volatile typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
    SST<Row, ImplMode, NameEnum, RowExtras>::
    operator[](unsigned int index) {
    return get(index);
}

/**
 * @return The number of rows in the table.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
int SST<Row, ImplMode, NameEnum, RowExtras>::get_num_rows() const {
    return num_members;
}

/**
 * This is the index of the local node, i.e. the node on which this code is
 * running, with respect to the group.
 *`sst_instance[sst_instance.get_local_index()]`
 * will always returna reference to the local node's row.
 *
 * @return The index of the local row.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
int SST<Row, ImplMode, NameEnum, RowExtras>::get_local_index() const {
    return member_index;
}

/**
 * This is a deep copy of the table that can be used for predicate evaluation,
 * which will no longer be affected by remote nodes updating their rows.
 *
 * @return A copy of all the SST's rows in their current state.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
std::unique_ptr<typename SST<Row, ImplMode, NameEnum, RowExtras>::SST_Snapshot>
SST<Row, ImplMode, NameEnum, RowExtras>::get_snapshot() const {
    return std::make_unique<SST_Snapshot>(table, num_members);
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::freeze(int index) {
    {
        std::lock_guard<std::mutex> lock(freeze_mutex);
        if(row_is_frozen[index]) {
            return;
        }
        row_is_frozen[index] = true;
    }
    num_frozen++;
    res_vec[index].reset();
    if(failure_upcall) {
        failure_upcall(members[index]);
    }
}

/**
 * Exchanges a single byte of data with each member of the SST group over the
 * TCP (not RDMA) connection, in descending order of the members' node ranks.
 * This creates a synchronization barrier, since the TCP reads are blocking,
 * and should be called after SST initialization to ensure all nodes have
 * finished initializing their local SST code.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::sync_with_members() const {
    unsigned int node_rank, sst_index;
    for(auto const &rank_index : members_by_rank) {
        std::tie(node_rank, sst_index) = rank_index;
        if(sst_index != member_index && !row_is_frozen[sst_index]) {
            sync(node_rank);
        }
    }
}

/**
 * If this SST is in Writes mode, this function does nothing.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::refresh_table() {
    assert(ImplMode == Mode::Reads);
    for(unsigned int index = 0; index < num_members; ++index) {
        // don't read own row or a frozen row
        if(index == member_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA read on the owner of the row
        res_vec[index]->post_remote_read(sizeof(table[0]));
    }
    // track which nodes haven't failed yet
    vector<bool> polled_successfully(num_members, false);
    // poll for one less than number of rows
    for(unsigned int index = 0; index < num_members - num_frozen - 1; ++index) {
        // poll for completion
        auto p = verbs_poll_completion();
        int qp_num = p.first;
        int result = p.second;
        if(result == 1) {
            polled_successfully[qp_num_to_index[qp_num]] = true;
        } else if(result == -1) {
            int index = qp_num_to_index[qp_num];
            if(!row_is_frozen[index]) {
                freeze(index);
                return;
            }
        } else if(result == 0) {
            // find some node that hasn't been polled yet and report it
            for(unsigned int index = 0; index < num_members; ++index) {
                if(index == member_index || row_is_frozen[index] ||
                   polled_successfully[index] == true) {
                    continue;
                }
                freeze(index);
                return;
            }
        }
    }
}

/**
 * If this SST is in Reads mode, this function is run in a detached background
 * thread to continuously keep the local SST table updated. If this SST is in
 * Writes mode, this function does nothing.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::read() {
    if(ImplMode == Mode::Reads) {
        while(!thread_shutdown) {
            refresh_table();
        }
        cout << "Reader thread shutting down" << endl;
    }
}

/**
 * This function is run in a detached background thread to detect predicate
 * events. It continuously evaluates predicates one by one, and runs the
 * trigger functions for each predicate that fires. In addition, it
 * continuously evaluates named functions one by one, and updates the local
 * row's observed values of those functions.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::detect() {
    if(!thread_start) {
        std::unique_lock<std::mutex> lock(thread_start_mutex);
        thread_start_cv.wait(lock, [this]() { return thread_start; });
    }
    while(!thread_shutdown) {
        // Take the predicate lock before reading the predicate lists
        std::unique_lock<std::mutex> predicates_lock(
            predicates.predicate_mutex);

        // update intermediate results for Row Predicates
        for(auto &f : row_predicate_updater_functions) {
            f(*this);
        }

        // evolving predicates trigger, then evolve
        for(std::size_t i = 0; i < predicates.evolving_preds.size(); ++i) {
            if(predicates.evolving_preds.at(i)) {
                if(predicates.evolving_preds.at(i)->first(*this)) {
                    // take predicate out of list
                    auto pred_pair = std::move(predicates.evolving_preds[i]);
                    // evaluate triggers on predicate
                    for(auto &trig : predicates.evolving_triggers.at(i)) {
                        trig(*this, pred_pair->second);
                    }
                    // evolve predicate
                    predicates.evolving_preds[i].reset(
                        new std::pair<function<bool(const SST &)>, int>{
                            (*predicates.evolvers.at(i))(*this,
                                                         pred_pair->second),
                            pred_pair->second + 1});
                }
            }
        }

        // one time predicates need to be evaluated only until they become true
        for(auto &pred : predicates.one_time_predicates) {
            if(pred != nullptr && (pred->first(*this) == true)) {
                // Copy the trigger pointer locally, so it can continue running
                // without
                // segfaulting
                // even if this predicate gets deleted when we unlock
                // predicates_lock
                std::shared_ptr<typename Predicates::trig> trigger(
                    pred->second);
                predicates_lock.unlock();
                (*trigger)(*this);
                predicates_lock.lock();
                // erase the predicate as it was just found to be true
                pred.reset();
            }
        }

        // recurrent predicates are evaluated each time they are found to be
        // true
        for(auto &pred : predicates.recurrent_predicates) {
            if(pred != nullptr && (pred->first(*this) == true)) {
                std::shared_ptr<typename Predicates::trig> trigger(
                    pred->second);
                predicates_lock.unlock();
                (*trigger)(*this);
                predicates_lock.lock();
            }
        }

        // transition predicates are only evaluated when they change from false
        // to
        // true
        // We need to use iterators here because we need to iterate over two
        // lists
        // in parallel
        auto pred_it = predicates.transition_predicates.begin();
        auto pred_state_it = predicates.transition_predicate_states.begin();
        while(pred_it != predicates.transition_predicates.end()) {
            if(*pred_it != nullptr) {
                //*pred_state_it is the previous state of the predicate at
                //*pred_it
                bool curr_pred_state = (*pred_it)->first(*this);
                if(curr_pred_state == true && *pred_state_it == false) {
                    std::shared_ptr<typename Predicates::trig> trigger(
                        (*pred_it)->second);
                    predicates_lock.unlock();
                    (*trigger)(*this);
                    predicates_lock.lock();
                }
                *pred_state_it = curr_pred_state;

                ++pred_it;
                ++pred_state_it;
            }
        }

        // TODO: clean up deleted predicates
        // The code below doesn't work, because the user might be holding a
        // handle
        // to a one-time predicate that we just deleted
        //        pred_it = predicates.one_time_predicates.begin();
        //        while (pred_it != predicates.one_time_predicates.end()) {
        //            if(*pred_it == nullptr) {
        //                pred_it =
        //                predicates.one_time_predicates.erase(pred_it);
        //            } else {
        //                pred_it++;
        //            }
        //        }
        //        pred_it = predicates.recurrent_predicates.begin();
        //        while (pred_it != predicates.recurrent_predicates.end()) {
        //            if(*pred_it == nullptr) {
        //                pred_it =
        //                predicates.recurrent_predicates.erase(pred_it);
        //            } else {
        //                pred_it++;
        //            }
        //        }
        //        pred_it = predicates.transition_predicates.begin();
        //        pred_state_it =
        //        predicates.transition_predicate_states.begin();
        //        while (pred_it != predicates.transition_predicates.end()) {
        //            if(*pred_it == nullptr) {
        //                pred_it =
        //                predicates.transition_predicates.erase(pred_it);
        //                pred_state_it =
        //                predicates.transition_predicate_states.erase(pred_state_it);
        //            } else {
        //                pred_it++;
        //                pred_state_it++;
        //            }
        //        }
    }

    cout << "Predicate detection thread shutting down" << endl;
}

/**
 * This writes the entire local row, using a one-sided RDMA write, to all of
 * the other members of the SST group. If this SST is in Reads mode, this
 * function does nothing.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::put() {
    vector<uint32_t> indices(num_members);
    iota(indices.begin(), indices.end(), 0);
    put(indices, 0, sizeof(table[0]));
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::put(
    vector<uint32_t> receiver_ranks) {
    put(receiver_ranks, 0, sizeof(table[0]));
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::put(long long int offset,
                                                  long long int size) {
    vector<uint32_t> indices(num_members);
    iota(indices.begin(), indices.end(), 0);
    put(indices, offset, size);
}

/**
* This can be used to write only a single state variable to the remote nodes,
* instead of the enitre row, if only that variable has changed. To get the
* correct offset and size, use `offsetof` and `sizeof`. For example, if the
* Row type is `RowType` and the variable to write is RowType::item, use
*
*     sst_instance.put(offsetof(RowType, item), sizeof(item));
*
* If this SST is in Reads mode, this function does nothing.
*
* @param offset The offset, within the Row structure, of the region of the
* row to write
* @param size The number of bytes to write, starting at the offset.
*/
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::put(
    vector<uint32_t> receiver_ranks, long long int offset, long long int size) {
    assert(ImplMode == Mode::Writes);
    vector<bool> posted_write_to(num_members, false);
    uint num_writes_posted = 0;
    for(auto index : receiver_ranks) {
        // don't write to yourself or a frozen row
        if(index == member_index || row_is_frozen[index]) {
            continue;
        }
        // perform a remote RDMA write on the owner of the row
        res_vec[index]->post_remote_write(offset, size);
        posted_write_to[index] = true;
        num_writes_posted++;
    }
    // track which nodes haven't failed yet
    vector<bool> polled_successfully_from(num_members, false);
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
                cout << "Poll completion error in QP " << qp_num
                     << ". Freezing row " << index << endl;
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
                // if (index2 != 0) {
                //   cout << "Number of writes posted = " <<
                //   remote_writes_posted << endl;
                //   cout << "num_frozen = " << num_frozen << endl;

                //   cout << "Writes posted to " << endl;
                //   for (auto n : writes_posted_to) {
                //     cout << n <<  " ";
                //   }
                //   cout << endl;

                //   cout << "Polled successfully from " << endl;
                //   for (uint i = 0; i < num_members; ++i) {
                //     if (polled_successfully[i]) {
                //       cout << members[i] << " ";
                //     }
                //   }
                //   cout << endl;
                // }
                cout << "Reporting failure on row " << index2
                     << " even though it didn't fail directly" << endl;
                freeze(index2);
                return;
            }
        }
    }
}

// SST_Snapshot implementation

/**
 * @param _table A reference to the SST's current internal state table
 * @param _num_members The number of members (rows) in the SST
 * @param _named_functions A reference to the SST's list of named functions
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
SST<Row, ImplMode, NameEnum, RowExtras>::SST_Snapshot::SST_Snapshot(
    const unique_ptr<volatile InternalRow[]> &_table, int _num_members)
        : num_members(_num_members), table(new InternalRow[num_members]) {
    std::memcpy(const_cast<InternalRow *>(table.get()),
                const_cast<const InternalRow *>(_table.get()),
                num_members * sizeof(InternalRow));
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
SST<Row, ImplMode, NameEnum, RowExtras>::SST_Snapshot::SST_Snapshot(
    const SST_Snapshot &to_copy)
        : num_members(to_copy.num_members), table(new InternalRow[num_members]) {
    std::memcpy(table.get(), to_copy.table.get(),
                num_members * sizeof(InternalRow));
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
const typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
SST<Row, ImplMode, NameEnum, RowExtras>::SST_Snapshot::get(int index) const {
    assert(index >= 0 && index < num_members);
    return table[index];
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
const typename SST<Row, ImplMode, NameEnum, RowExtras>::InternalRow &
    SST<Row, ImplMode, NameEnum, RowExtras>::SST_Snapshot::
    operator[](int index) const {
    return get(index);
}

} /* namespace sst */

#endif /* SST_H */
