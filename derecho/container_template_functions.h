/**
 * @file container_template_functions.h
 *
 * @date Jun 12, 2018
 * @author edward
 */

#pragma once
#include <map>
#include <mutils-containers/KindMap.hpp>
#include <set>
#include <vector>

#include "derecho_internal.h"

namespace derecho {
template <typename MapType>
void kind_map_builder(MapType&){};

/**
 * Actual implementation of make_kind_map; needs to be a separate function
 * because the only way to build a KindMap is with a void mutator function.
 * @param map A mutable reference to the KindMap being constructed
 * @param curr_factory The first factory in the parameter pack of factories
 * @param rest_factories The rest of the parameter pack
 */
template <typename MapType, typename FirstType, typename... RestTypes>
void kind_map_builder(MapType& map, Factory<FirstType> curr_factory,
                      Factory<RestTypes>... rest_factories) {
    map.template get<FirstType>() = std::move(curr_factory);
    kind_map_builder<MapType, RestTypes...>(map, rest_factories...);
}

/**
 * Constructs a KindMap<Factory, Types...> from a list of factories of those
 * types. Could probably be made even more generic, to construct a KindMap of
 * any template given a list of objects that match that template, but that would
 * involve writing a template template parameter, which is too much black magic
 * for me to understand.
 * @param factories One instance of Factory<T> for each T in the type list
 * @return A KindMap of factories, mapping each type to a Factory for that type.
 */
template <typename... Types>
mutils::KindMap<Factory, Types...> make_kind_map(Factory<Types>... factories) {
    mutils::KindMap<Factory, Types...> factories_map;
    kind_map_builder<decltype(factories_map), Types...>(factories_map, factories...);
    return factories_map;
}

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
}  // namespace derecho
