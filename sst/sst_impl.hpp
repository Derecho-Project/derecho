#pragma once

#include "sst.hpp"

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
template <typename T>
SSTField<T>::SSTField() : _SSTField(sizeof(T)) {
}

template <typename T>
volatile T& SSTField<T>::operator[](const size_t row_index) const {
    return ((T&)base[row_index * row_length]);
}

template <typename T>
SSTFieldVector<T>::SSTFieldVector(size_t _size) : _SSTField(_size * sizeof(T)), _size(_size) {
}

template <typename T>
volatile T* SSTFieldVector<T>::operator[](const size_t& row_index) const {
    return (T*)(base + row_index * row_length);
}

template <typename T>
size_t SSTFieldVector<T>::size() const {
    return _size;
}

template <typename DerivedSST>
void SST<DerivedSST>::compute_row_length(){};

template <typename DerivedSST>
template <typename Field, typename... Fields>
void SST<DerivedSST>::compute_row_length(Field& f, Fields&... rest) {
    row_length += padded_length(f.field_length);
    compute_row_length(rest...);
}

template <typename DerivedSST>
void SST<DerivedSST>::set_field_params(volatile char*&){};

template <typename DerivedSST>
template <typename Field, typename... Fields>
void SST<DerivedSST>::set_field_params(volatile char*& base, Field& f, Fields&... rest) {
    base += f.set_base(base);
    f.set_row_length(row_length);
    f.set_num_rows(members.num_nodes);
    set_field_params(base, rest...);
}

template <typename DerivedSST>
template <typename... Fields>
void SST<DerivedSST>::initialize_fields(Fields&... fields) {
    compute_row_length(fields...);
    rows = std::make_unique<volatile char[]>(row_length * members.num_nodes);
    volatile char* base = rows.get();
    set_field_params(base, fields...);
}

/**
 * This function is called by the static global predicate thread.
 * It continuously evaluates predicates one by one, and runs the
 * trigger functions for each predicate that fires.
 */
template <typename DerivedSST>
void SST<DerivedSST>::evaluate() {
    if(!start_eval || !initialization_done) {
        return;
    }
    // Take the predicate lock before reading the predicate lists
    std::lock_guard<std::recursive_mutex> predicates_lock(predicates.predicate_mutex);

    // one time predicates need to be evaluated only until they become true
    for(auto pred_iter = predicates.predicates.begin(); pred_iter != predicates.predicates.end(); ++pred_iter) {
        auto& pred = *pred_iter;
        if((pred.pred(*derived_sst_pointer) == true)) {
            (pred.trig)(*derived_sst_pointer);
            if(pred.type == PredicateType::ONE_TIME) {
                predicates.to_remove.push_back(pred_iter);
            }
        }
    }

    for(auto iter: predicates.to_remove) {
        predicates.predicates.erase(iter);
    }
    predicates.to_remove.clear();
}

template <typename DerivedSST>
SST<DerivedSST>::SST(DerivedSST* derived_sst_pointer, const node::NodeCollection& members, bool start_eval)
        : derived_sst_pointer(derived_sst_pointer),
          row_length(0),
          members(members),
          memory_regions(members.num_nodes),
	  start_eval(start_eval) {
    SSTRegistry::register_sst(this);
}

template <typename DerivedSST>
SST<DerivedSST>::~SST() {
    SSTRegistry::deregister_sst(this);
}

template <typename DerivedSST>
template <typename... Fields>
void SST<DerivedSST>::initialize(Fields&... fields) {
    if(initialization_done) {
        return;
    }
    //Initialize rows and set the "base" field of each SSTField
    initialize_fields(fields...);

    for(uint32_t other_index : members.other_ranks) {
        char* local_row_addr = const_cast<char*>(rows.get()) + row_length * members.my_rank;
        char* remote_row_addr = const_cast<char*>(rows.get()) + row_length * other_index;
        memory_regions[other_index] = std::make_unique<rdma::MemoryRegion>(
                members[other_index], local_row_addr,
                remote_row_addr, row_length);
    }
    initialization_done = true;
}

template <typename DerivedSST>
const char* SST<DerivedSST>::get_base_address(uint32_t row_index) {
    return const_cast<char*>(rows.get()) + row_index * row_length;
}

/**
 * An all member synchronization barrier
 */
template <typename DerivedSST>
void SST<DerivedSST>::sync_with_members() const {
    for(auto& memory_region : members.filter_self(memory_regions)) {
        memory_region->sync();
    }
}

template <typename DerivedSST>
uint32_t SST<DerivedSST>::get_num_members() const {
    return members.num_nodes;
}

template <typename DerivedSST>
uint32_t SST<DerivedSST>::get_my_index() const {
    return members.my_rank;
}

template <typename DerivedSST>
void SST<DerivedSST>::update_remote_rows(size_t offset, size_t size, bool completion) {
    if(size == 0) {
        size = row_length - offset;
    }
    assert(offset + size <= row_length);
    for(auto& memory_region : members.filter_self(memory_regions)) {
        memory_region->write_remote(offset, size, completion);
    }
}

template <typename DerivedSST>
void SST<DerivedSST>::start_predicate_evaluation() {
    start_eval = true;
}

}  // namespace sst
