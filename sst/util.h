/*
 * util.h
 *
 *  Created on: Mar 16, 2016
 *      Author: edward
 */

#pragma once

#include "args-finder.hpp"
#include <memory>
#include <type_traits>

namespace sst {

/**
 * Assorted utility classes and functions that support SST internals.
 */
namespace util {

/**
 * Base case for the recursive template_and function.
 * @return True
 */
constexpr bool template_and() { return true; }

/**
 * Computes the logical AND of a list of parameters, at compile-time so it can
 * be used in template parameters.
 * @param first The first Boolean value
 * @param rest The rest of the Boolean values
 * @tparam Rest Type pack parameter for the rest of the arguments; all the types
 * must be bool, but there's no way to specify that.
 * @return True if every argument evaluates to True, False otherwise.
 */
template <typename... Rest>
constexpr bool template_and(bool first, Rest... rest) {
    return first && template_and(rest...);
}

/** An empty enum class, used to provide a default "none" argument for template
 * parameters. */
enum class NullEnum {};

/**
 * Base case for for_each_hlpr
 */
template <int index, typename F, typename Tuple1, typename Tuple2>
std::enable_if_t<index == std::tuple_size<Tuple2>::value> for_each_hlpr(
        const F &f, const Tuple1 &t1, Tuple2 &t2) {}

/**
 * Helper function that recursively applies a function to the tuples given to
 * for_each(const F,Tuple1&,const Tuple2&). This function should not be called
 * directly by clients; it's only visible in the header because templates must
 * be fully implemented in headers.
 */
template <int index, typename F, typename Tuple1, typename Tuple2,
          restrict(index < std::tuple_size<Tuple2>::value)>
void for_each_hlpr(const F &f, const Tuple1 &t1, Tuple2 &t2) {
    f(std::get<index>(t1), std::get<index>(t2));
    for_each_hlpr<index + 1>(f, t1, t2);
}

/**
 * Base case for for_each_hlpr
 */
template <int index, typename F, typename Tuple1>
std::enable_if_t<index == std::tuple_size<Tuple1>::value> for_each_hlpr(
        const F &f, const Tuple1 &t1) {}

/**
 * Helper function that recursively applies a function to the tuple given to
 * for_each(const F,const Tuple1&). This function should not be called
 * directly by clients; it's only visible in the header because templates must
 * be fully implemented in headers.
 */
template <int index, typename F, typename Tuple1,
          restrict(index < std::tuple_size<Tuple1>::value)>
void for_each_hlpr(const F &f, const Tuple1 &t1) {
    f(std::get<index>(t1));
    for_each_hlpr<index + 1>(f, t1);
}

/**
 * For-each lambda construct that can iterate over two tuples in parallel,
 * applying a two-argument function to each pair of elements.
 *
 * @param f The function to apply
 * @param t1 The first tuple, whose elements will be supplied to the first
 * argument of the function.
 * @param t2 The second tuple, whose elements will be supplied to the second
 * argument of the function.
 * @tparam F The type of the function
 * @tparam Tuple1 The type of the first tuple. This must be a specialization of
 * std::tuple, but there's no way to specify that.
 * @tparam Tuple2 The type of the second tuple. This must be a specialization of
 * std::tuple, but there's no way to specify that.
 */
template <typename F, typename Tuple1, typename Tuple2>
void for_each(const F &f, Tuple1 &t1, const Tuple2 &t2) {
    static_assert(
            std::tuple_size<Tuple1>::value >= std::tuple_size<Tuple2>::value,
            "Error, dual-foreach needs smaller one second!");
    for_each_hlpr<0>(f, t1, t2);
}

/**
 * For-each lambda construct that can iterate over one tuple,
 * applying a one-argument function to each element.
 *
 * @param f The function to apply
 * @param t1 The first tuple, whose elements will be supplied to the first
 * argument of the function.
 * @tparam F The type of the function
 * @tparam TupleMembers The types inside the tuple
 */
template <typename F, typename... TupleMembers>
void for_each(const F &f, const std::tuple<TupleMembers...> &t1) {
    for_each_hlpr<0>(f, t1);
}

template <typename T>
std::unique_ptr<T> heap_copy(const T &t) {
    return std::make_unique<T>(t);
}
}
}
