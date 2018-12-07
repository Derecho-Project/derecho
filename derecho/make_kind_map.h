/**
 * @file make_kind_map.h
 *
 * A useful template function that helps construct mutils::KindMap instances.
 * Separated out into its own file to reduce clutter in group.h and reduce
 * dependencies in container_template_functions.h.
 */

#pragma once

#include "derecho/derecho_internal.h"
#include <mutils-containers/KindMap.hpp>

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

}  // namespace derecho
