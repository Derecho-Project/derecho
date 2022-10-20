/**
 * @file container_template_functions.h
 *
 * Contains implementations of some useful functions on STL containers, most of
 * which add standard-but-missing functionality.
 */

#pragma once
#include <algorithm>
#include <list>
#include <map>
#include <set>
#include <vector>

namespace derecho {

/**
 * Inserts set b into set a and returns the modified a. Hack to get around the
 * fact that set::insert doesn't return *this.
 * @param a The set to modify and return
 * @param b The set that should be inserted into a.
 * @return Set a.
 */
template <typename T>
std::set<T> functional_insert(std::set<T>& a, const std::set<T>& b) {
    a.insert(b.begin(), b.end());
    return a;
}

/**
 * Base case for functional_append, with one argument.
 */
template <typename T>
std::vector<T> functional_append(const std::vector<T>& original, const T& item) {
    std::vector<T> appended_vec(original);
    appended_vec.emplace_back(item);
    return appended_vec;
}

/**
 * Returns a new std::vector value that is equal to the parameter std::vector
 * with the rest of the arguments appended. Adds some missing functionality to
 * std::vector: the ability to append to a const vector without taking a
 * several-line detour to call the void emplace_back() method.
 * @param original The vector that should be the prefix of the new vector
 * @param first_item The first element to append to the original vector
 * @param rest_items The rest of the elements to append to the original vector
 * @return A new vector (by value), containing a copy of original plus all the
 * elements given as arguments.
 */
template <typename T, typename... RestArgs>
std::vector<T> functional_append(const std::vector<T>& original, const T& first_item, RestArgs... rest_items) {
    std::vector<T> appended_vec = functional_append(original, first_item);
    return functional_append(appended_vec, rest_items...);
}

/**
 * Returns the size of a std::map of std::maps, by counting up the sizes of all
 * the inner maps. Sort of a "deep size" for the common case of a map-of-maps.
 * @param multimap A map of maps
 * @return The total number of elements in the std::map, counting all the
 * elements in all the "inner" std::maps.
 */
template <typename K1, typename K2, typename V>
std::size_t multimap_size(const std::map<K1, std::map<K2, V>>& multimap) {
    std::size_t count = 0;
    for(const auto& map_pair : multimap) {
        count += map_pair.second.size();
    }
    return count;
}

/**
 * Constructs a std::list of the keys in a std::map, in the same order as they
 * appear in the std::map.
 * @param map A std::map
 * @return A std::list containing a copy of each key in the map, in the same order
 */
template <typename K, typename V>
std::list<K> keys_as_list(const std::map<K, V>& map) {
    std::list<K> keys;
    for(const auto& pair : map) {
        keys.emplace_back(pair.first);
    }
    return keys;
}

/**
 * Finds a value in a STL container, and returns the index of that value in the
 * container. This simply combines std::find (which returns an opaque pointer to
 * the found value) with std::distance (which converts the pointer to a numeric
 * index).
 * @param container An STL container, which must provide a member type value_type
 * @param elem An element of type value_type to search for in the container
 * @return The index of the element within the container, or 1 greater than the
 * size of the container if the element was not found.
 */
template <typename Container>
std::size_t index_of(const Container& container, const typename Container::value_type& elem) {
    return std::distance(std::begin(container),
                         std::find(std::begin(container),
                                   std::end(container), elem));
}

}  // namespace derecho
