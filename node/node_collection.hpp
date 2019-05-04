#pragma once

#include <cassert>
#include <map>
#include <vector>

#include "node.hpp"

namespace node {
// caches important node id and rank logistics
// stores lots of redundant data
class NodeCollection {
    template <typename elementType>
    class Splice {
        elementType* ptr;
        uint32_t my_rank;
        uint32_t num_nodes;

        class SplicedIterator {
            elementType* ptr;
            uint32_t my_rank;

        public:
            uint32_t rank;
            SplicedIterator(elementType* ptr, uint32_t my_rank,
                            uint32_t rank) : ptr(ptr),
                                             my_rank(my_rank),
                                             rank(rank) {
            }
            bool operator!=(const SplicedIterator& other) const {
                return ptr != other.ptr;
            }
            SplicedIterator operator++() {
                ++ptr;
                ++rank;
                if(rank == my_rank) {
                    ++ptr;
                    ++rank;
                }
                return *this;
            }
            elementType& operator*() const {
                return *ptr;
            }
        };

    public:
        SplicedIterator begin() const {
            if(my_rank == 0) {
                return SplicedIterator(ptr + 1, my_rank, 1);
            }
            return SplicedIterator(ptr, my_rank, 0);
        }
        SplicedIterator end() const {
            return SplicedIterator(ptr + num_nodes, my_rank, 0);
        }
        Splice(elementType* ptr, uint32_t my_rank, uint32_t num_nodes) : ptr(ptr),
                                                                         my_rank(my_rank),
                                                                         num_nodes(num_nodes) {}
    };

    std::map<node_id_t, uint32_t> node_id_to_rank;

public:
    NodeCollection(const std::vector<node_id_t>& nodes, const node_id_t my_id);
    NodeCollection(const NodeCollection&) = default;

    const std::vector<node_id_t> nodes;
    const uint32_t num_nodes;
    const node_id_t my_id;
    /* can avoid storing it, since it is just node_id_to_rank[my_id]
     * but easier to access it from outside
     * compared to calling a function such as get_my_rank to get this */
    const uint32_t my_rank;
    // convenient vector storing rank of everyone else in the collection
    // used by the SST for example, when it updates all remote rows
    const std::vector<uint32_t> other_ranks;

    const node_id_t& operator[](const size_t rank) const;
    const node_id_t& at(const size_t rank) const;
    uint32_t get_rank_of(node_id_t node_id) const;

    template <typename elementType>
    Splice<elementType> filter_self(const std::vector<elementType>& vec) const {
        return Splice<elementType>((elementType*)&vec[0], my_rank, num_nodes);
    };
};
}  // namespace node
