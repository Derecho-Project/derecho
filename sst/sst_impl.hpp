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

#include "sst.hpp"

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

template <typename T>
SSTField<T>::SSTField() : _SSTField(sizeof(T)) {
}

template <typename T>
volatile T& SSTField<T>::operator[](const uint32_t row_index) const {
    return ((T&)base[row_index * row_length]);
}

template <typename T>
SSTFieldVector<T>::SSTFieldVector(size_t size) : _SSTField(num_elements * sizeof(T)), size(size) {
}

template <typename T>
volatile T* SSTFieldVector<T>::operator[](const uint32_t& index) const {
    return (T*)(base + index * row_length);
}

template <typename T>
size_t SSTFieldVector<T>::size() const {
    return size;
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
void SST<DerivedSST>::set_bases_and_row_lengths(volatile char*&){};

template <typename DerivedSST>
template <typename Field, typename... Fields>
void SST<DerivedSST>::set_bases_and_row_lengths(volatile char*& base, Field& f, Fields&... rest) {
    base += f.set_base(base);
    f.set_row_length(row_length);
    set_bases_and_row_lengths(base, rest...);
}

template <typename DerivedSST>
template <typename... Fields>
void SST<DerivedSST>::initialize_fields(Fields&... fields) {
    compute_row_length(fields...);
    rows = new char[row_length * num_members];
    volatile char* base = rows;
    set_bases_and_row_length(base, fields...);
}

template <typename DerivedSST>
SST<DerivedSST>::SST(DerivedSST* derived_class_pointer, std::vector<node_id_t>& members, node_id_t my_id)
        : derived_sst_pointer(derived_sst_pointer),
          row_length(0),
          members(members) {
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_id) {
            my_index = i;
            break;
        }
    }
    assert(my_index != (uint32_t)-1);
}

/**
 * Destructor for the SST object; sets thread_shutdown to true and waits for
 * background threads to exit cleanly.
 */
template <typename DerivedSST>
SST<DerivedSST>::~SST() {
    // unregister predicates

    delete[](const_cast<char*>(rows));
}

template <typename DerivedSST>
template <typename... Fields>
void SST<DerivedSST>::initialize(Fields&... fields) {
    //Initialize rows and set the "base" field of each SSTField
    initialize_fields(fields...);

    for(uint32_t index = 0; index < members.size(); ++index) {
        if(index == my_index) {
            continue;
        }
        char* local_row_addr = const_cast<char*>(rows) + row_length * my_index;
        char* remote_row_addr = const_cast<char*>(rows) + row_length * sst_index;
	// change it later
        memory_regions.push_back(MemoryRegion(members[index], local_row_addr,
					      remote_row_addr, row_length));
    }
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
  for(const auto& memory_region: memory_regions) {
      memory_region.sync();
  }
}

template <typename DerivedSST>
uint32_t SST<DerivedSST>::get_num_members() const {
    return num_members;
}

template <typename DerivedSST>
uint32_t SST<DerivedSST>::get_local_index() const {
    return my_index;
}

template <typename DerivedSST>
void SST<DerivedSST>::update_remote_rows(size_t offset, size_t size, bool completion) {
    assert(offset + size <= row_length);
    for (auto& memory_region: memory_regions) {
      memory_region.write_remote(offset, size, completion);
    }
}
}  // namespace sst
