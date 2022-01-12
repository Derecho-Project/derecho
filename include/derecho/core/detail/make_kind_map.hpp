/**
 * @file make_kind_map.h
 *
 * A useful template function that helps construct mutils::KindMap instances.
 * Separated out into its own file to reduce clutter in group.h and reduce
 * dependencies in container_template_functions.h.
 */

#pragma once

#include "derecho_internal.hpp"
#include <mutils-containers/KindMap.hpp>

namespace derecho {

//Base case for make_kind_map_impl, which does nothing
template <typename MapType, template<typename> typename MapTemplate>
void make_kind_map_impl(MapType& map) {}

/**
 * Implementation of make_kind_map, which recursively calls itself while
 * unpacking the list of items. It modifies its first argument rather than
 * returning a result because KindMap can only be modified with void functions.
 *
 * @tparam MapType The complete type of the KindMap, which should be of the
 * form KindMap<MapTemplate, FirstType, RestTypes...>
 * @tparam MapTemplate The template type that is the first template parameter
 * of KindMap
 * @param map A mutable reference to the KindMap being constructed
 * @param curr_item The first item in the parameter pack of items to put in the map
 * @param rest_items The rest of the parameter pack
 */
template <typename MapType, template <typename> typename MapTemplate, typename FirstType, typename... RestTypes>
void make_kind_map_impl(MapType& map,
                        MapTemplate<FirstType> curr_item,
                        MapTemplate<RestTypes>... rest_items) {
    map.template get<FirstType>() = std::move(curr_item);
    make_kind_map_impl<MapType, MapTemplate, RestTypes...>(map, rest_items...);
}

/**
 * Constructs a KindMap<MapTemplate, Types...> from a list of items of the
 * required types.
 *
 * @tparam MapTemplate The template type that is the first template parameter
 * of KindMap. This is the only template argument you need to provide when
 * calling this function.
 * @tparam Types... A list of types that form the remaining template parameters
 * of KindMap. These can be inferred from the function arguments.
 * @param items One instance of MapTemplate<T> for each T in the Types list
 * @return A KindMap<MapTemplate, Types...> initialized with each type mapped
 * to the item of that type in the parameter list.
 */
template <template <typename> typename MapTemplate, typename... Types>
mutils::KindMap<MapTemplate, Types...> make_kind_map(MapTemplate<Types>... items) {
    mutils::KindMap<MapTemplate, Types...> kind_map;
    make_kind_map_impl<decltype(kind_map), MapTemplate, Types...>(kind_map, items...);
    return kind_map;
}

}  // namespace derecho
