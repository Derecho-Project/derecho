#ifndef NAMED_FUNCTION_H_
#define NAMED_FUNCTION_H_

#include "args-finder.hpp"
#include "combinators.h"
#include <functional>
#include <type_traits>

namespace sst {

/**
 * Represents a named function over an SST; the function should take a
 * reference to SST as its argument, and have a name chosen from a user-defined
 * enum.
 *
 * @tparam NameEnum The enum type that will be used to name this function
 * @tparam Name The name of this function (which is an enum member)
 * @tparam Param The type of this function's parameter, which will be passed
 * by reference. In practice, this should be {@code const SST}
 * @tparam Ret The return type of this function, which must be POD because it
 * will be stored in an SST row.
 */
template <typename NameEnum, NameEnum Name, typename Param, typename Ret>
struct NamedFunction {
    static_assert(std::is_pod<Ret>::value, "Error: only POD return types");
    Ret (*fun)(const Param &);
    using ret_t = Ret;
    static constexpr NameEnum name = Name;
};

/**
 * Helper for the "make_named_function" macro, which constructs a named
 * function.
 * This helps hide the template parameters.
 * @param fun A pointer to the function to name.
 * @return A NamedFunction wrapping the given function.
 */
template <typename NameEnum, NameEnum Name, typename Param, typename Ret>
auto build_named_function(Ret (*fun)(const Param &)) {
    return NamedFunction<NameEnum, Name, Param, Ret>{fun};
}
template <typename NameEnum, NameEnum Name, typename F>
auto buid_named_function(F f) {
    return build_named_function(util::convert_fp(f));
}

/**
 * Constructs a NamedFunction, using the first argument as the name and the
 * second argument as the function.
 *
 * @param name The name of the function; must be an enum member
 * @param fun The function to name
 * @return A NamedFunction wrapping the given function and associating it with
 * the given name
 */
#define make_named_function(name, fun...) \
    build_named_function<decltype(name), name>(fun)
}

#endif /* NAMED_FUNCTION_H_ */
