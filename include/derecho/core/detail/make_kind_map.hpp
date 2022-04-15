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

/*
 * GCC 8, which claims to implement C++17, can't correctly deduce the template template
 * parameters for make_kind_map<Factory>() because it thinks Factory<TestObject> is not
 * the same type as std::function<std::unique_ptr<TestObject>(persistent::PersistentRegistry*, subgroup_id_t)>.
 * Thus, for older versions of GCC, we must explicitly specialize make_kind_map for each
 * Factory template. Annoyingly, this also changes the way make_kind_map is invoked, so
 * the same macro must be used at the call site.
 */
#if __GNUC__ < 9

template <typename MapType>
void kind_map_builder(MapType&){};

template <typename MapType, typename FirstType, typename... RestTypes>
void kind_map_builder(MapType& map, Factory<FirstType> curr_factory,
                      Factory<RestTypes>... rest_factories) {
    map.template get<FirstType>() = std::move(curr_factory);
    kind_map_builder<MapType, RestTypes...>(map, rest_factories...);
}

template <typename MapType, typename FirstType, typename... RestTypes>
void kind_map_builder(MapType& map, NoArgFactory<FirstType> curr_factory,
                      NoArgFactory<RestTypes>... rest_factories) {
    map.template get<FirstType>() = std::move(curr_factory);
    kind_map_builder<MapType, RestTypes...>(map, rest_factories...);
}

template <typename... Types>
mutils::KindMap<Factory, Types...> make_kind_map(Factory<Types>... factories) {
    mutils::KindMap<Factory, Types...> factories_map;
    kind_map_builder<decltype(factories_map), Types...>(factories_map, factories...);
    return factories_map;
}

template <typename... Types>
mutils::KindMap<NoArgFactory, Types...> make_kind_map(NoArgFactory<Types>... factories) {
    mutils::KindMap<NoArgFactory, Types...> factories_map;
    kind_map_builder<decltype(factories_map), Types...>(factories_map, factories...);
    return factories_map;
}

#endif

}  // namespace derecho
