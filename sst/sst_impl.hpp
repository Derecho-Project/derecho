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

#include "sst_registry.hpp"

namespace sst {
size_t _SSTField::set_base(volatile char* const base) {
    this->base = base;
    return padded_length(field_length);
}

_SSTField::_SSTField(const size_t field_length) : base(nullptr),
                                                  row_length(0),
                                                  field_length(field_length) {
}

char* _SSTField::get_base_address() {
    return const_cast<char*>(base);
}

void _SSTField::set_row_length(const size_t row_length) {
    this->row_length = row_length;
}

void _SSTField::set_num_nodes(const uint32_t num_nodes) {
    this->num_nodes = num_nodes;
}

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
volatile T* SSTFieldVector<T>::operator[](const size_t& index) const {
    return (T*)(base + index * row_length);
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
    f.set_num_nodes(members.num_nodes);
    set_field_params(base, rest...);
}

template <typename DerivedSST>
template <typename... Fields>
void SST<DerivedSST>::initialize_fields(Fields&... fields) {
    compute_row_length(fields...);
    rows = new char[row_length * members.num_nodes];
    volatile char* base = rows;
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
    std::unique_lock<std::mutex> predicates_lock(predicates.predicate_mutex);

    // one time predicates need to be evaluated only until they become true
    for(auto& pred : predicates.one_time_predicates) {
        if(pred != nullptr && (pred->first(*derived_sst_pointer) == true)) {
            // Copy the trigger pointer locally, so it can continue running without
            // segfaulting even if this predicate gets deleted when we unlock predicates_lock
            std::shared_ptr<typename Predicates<DerivedSST>::trig> trigger(pred->second);
            predicates_lock.unlock();
            (*trigger)(*derived_sst_pointer);
            predicates_lock.lock();
            // erase the predicate as it was just found to be true
            pred.reset();
        }
    }

    // recurrent predicates are evaluated each time they are found to be true
    for(auto& pred : predicates.recurrent_predicates) {
        if(pred != nullptr && (pred->first(*derived_sst_pointer) == true)) {
            std::shared_ptr<typename Predicates<DerivedSST>::trig> trigger(pred->second);
            predicates_lock.unlock();
            (*trigger)(*derived_sst_pointer);
            predicates_lock.lock();
        }
    }

    auto one_time_iter = predicates.one_time_predicates.begin();
    while(one_time_iter != predicates.one_time_predicates.end()) {
        if(!*one_time_iter) {
            one_time_iter = predicates.one_time_predicates.erase(one_time_iter);
        } else {
            ++one_time_iter;
        }
    }
    auto recurrent_iter = predicates.recurrent_predicates.begin();
    while(recurrent_iter != predicates.one_time_predicates.end()) {
        if(!*recurrent_iter) {
            recurrent_iter = predicates.one_time_predicates.erase(recurrent_iter);
        } else {
            ++recurrent_iter;
        }
    }
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

/**
 * Destructor for the SST object; sets thread_shutdown to true and waits for
 * background threads to exit cleanly.
 */
template <typename DerivedSST>
SST<DerivedSST>::~SST() {
    SSTRegistry::deregister_sst(this);

    delete[](const_cast<char*>(rows));
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
        char* local_row_addr = const_cast<char*>(rows) + row_length * members.my_rank;
        char* remote_row_addr = const_cast<char*>(rows) + row_length * other_index;
        memory_regions[other_index] = std::make_unique<rdma::MemoryRegion>(
                members.nodes[other_index], local_row_addr,
                remote_row_addr, row_length);
    }
    initialization_done = true;
}

template <typename DerivedSST>
const char* SST<DerivedSST>::get_base_address() {
    return const_cast<char*>(rows);
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
