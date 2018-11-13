/**
 * @file type_index_serialization.cpp
 *
 * @date May 28, 2018
 * @author edward
 */

#include "type_index_serialization.h"

namespace mutils {

std::size_t bytes_size(const std::type_index& type_index) {
    return sizeof(type_index);
}

/* Same implementation as rpc_utils.h's populate_header function */
std::size_t to_bytes(const std::type_index& type_index, char* buffer) {
    ((std::type_index*)(buffer))[0] = type_index;
    return sizeof(type_index);
}

void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::type_index& type_index) {
    f(reinterpret_cast<char const * const>(&type_index), sizeof(type_index));
}

}


