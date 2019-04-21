#pragma once

#include <atomic>
#include <bitset>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string.h>
#include <string>
#include <thread>
#include <vector>

#include "node/node.hpp"
#include "rdma/memory_region.hpp"

namespace sst {
using node::node_id_t;

constexpr size_t padded_length(const size_t& length) {
    static const uint32_t align_to = 8;
    return (length < align_to) ? alignTo : (length + align_to) | (align_to - 1);
}

class _SSTField {
    template typename<T> friend class SST;

private:
    size_t set_base(volatile char* const base);

public:
    volatile char* base;
    size_t row_length;
    size_t field_length;

    _SSTField(const size_t field_length);

    char* get_base_address();

    void set_row_length(const size_t row_length);
};

/**
 * Clients should use instances of this class with the appropriate template
 * parameter to declare fields in their SST; for example, SSTField<int> is the
 * type of an integer-valued SST field.
 */
template <typename T>
class SSTField : public _SSTField {
public:
    using _SSTField::base;
    using _SSTField::field_length;
    using _SSTField::row_length;

    SSTField();

    // Tracks down the appropriate row
    volatile T& operator[](const uint32_t row_index) const;
};

/**
 * Clients should use instances of this class to declare vector-like fields in
 * their SST; the template parameter is the type of the vector's elements, just
 * like with std::vector. Unlike std::vector, these are fixed-size arrays and
 * cannot grow or shrink after construction.
 */
template <typename T>
class SSTFieldVector : public _SSTField {
private:
    const size_t size;

public:
    using _SSTField::base;
    using _SSTField::field_length;
    using _SSTField::row_length;

    SSTFieldVector(size_t size);

    // Tracks down the appropriate row
    volatile T* operator[](const uint32_t& index) const;

    /** Just like std::vector::size(), returns the number of elements in this vector. */
    size_t size() const;
};

template <class DerivedSST>
class SST {
private:
    DerivedSST* derived_sst_pointer;

    /** Pointer to memory where the SST rows are stored. */
    volatile char* rows;
    /** Length of each row in this SST, in bytes. */
    size_t row_length;
    
    /** List of nodes in the SST; indexes are row numbers, values are node IDs. */
    const std::vector<node_id_t> members;
    /** Index (row number) of this node in the SST. */
    uint32_t  my_index;

    /** one for every other member.
     * note that this does not have an entry for the local node
     * so be careful when indexing!! */
    std::vector<rdma::MemoryRegion> memory_regions;

    // computes the length of the row required for the SST table
    void compute_row_length();
    template <typename Field, typename... Fields>
    void SST<DerivedSST>::compute_row_length(Field& f, Fields&... rest);

    // sets the bases and row length for the SSTFIelds and SSTFieldVectors
    void set_bases_and_row_lengths(volatile char*&);
    template <typename Field, typename... Fields>
    void set_bases_and_row_lengths(volatile char*& base, Field& f, Fields&... rest);
  
    template <typename... Fields>
    void initialize_fields(Fields&... fields);

public:
    SST(DerivedSST* derived_sst_pointer, std::vector<node_id_t>& members, node_id_t my_id);

    ~SST();

    template <typename... Fields>
    void SST<DerivedSST>::initialize(Fields&... fields);

    // returns the start of the rows
    const char* get_base_address();

    /** Does a RDMA sync with every other member of the SST. */
    void sync_with_members() const;

    /** Returns the total number of rows in the table. */
    uint32_t get_num_members() const;
    /** Gets the index of the local row in the table. */
    uint32_t get_local_index() const;
  
    /** Update the remote copies of the local row. */
    void update_remote_rows(size_t offset = 0, size_t size = 0, bool completion = false);
};
} /* namespace sst */

#include "sst_impl.h"
