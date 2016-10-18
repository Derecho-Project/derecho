#pragma once
#include "combinators.h"

namespace sst {

constexpr bool all_predicate_builders() { return true; }

template <typename Row, typename ExtensionList, typename... Rst>
constexpr bool all_predicate_builders(
    PredicateBuilder<Row, ExtensionList> const *const pb,
    Rst const *const... rst) {
    static_assert(std::is_pod<Row>::value,
                  "Error: Predicate Builders need POD rows!");
    return all_predicate_builders(rst...);
}

template <typename...>
struct NamedRowPredicates;

template <>
struct NamedRowPredicates<> {
    // tail guard
    using is_tail = std::true_type;

    // list memebers
    using predicate_types = std::tuple<>;
    using row_types = std::tuple<>;

    // list length
    using size = typename std::integral_constant<std::size_t, 0>::type;

    // convenience for referencing list members
    template <typename>
    using Getters = std::tuple<>;

    using NamedRowPredicatesTypePack = NamedRowPredicates;
};

template <typename PredicateBuilder, typename... PredBuilders>
struct NamedRowPredicates<PredicateBuilder, PredBuilders...> {
    static_assert(all_predicate_builders(util::mke_p<PredicateBuilder>(),
                                         util::mke_p<PredBuilders>()...),
                  "Error: this parameter pack must be of predicate builders!");

    /** tail guard; stop iterating over the list when this is true */
    using is_tail = std::false_type;

    /**list members; rather than using hd and rst, this contains the remaining
     * contents of the list as a tuple.  Occassionally convenient.*/
    using predicate_types = std::tuple<PredicateBuilder, PredBuilders...>;
    using row_types = std::tuple<typename PredicateBuilder::Row_Extension,
                                 typename PredBuilders::Row_Extension...>;

    /** current PredicateBuidler; the "head" of this list */
    using hd = PredicateBuilder;

    /** the "tail" of this list. */
    using rst = NamedRowPredicates<PredBuilders...>;

    /** including this hd, how many values are left? */
    using size =
        typename std::integral_constant<std::size_t,
                                        sizeof...(PredBuilders) + 1>::type;

    /** each different PredicateBuilder in this list can supply some number of
     * updater functions.
     *  this is the number of updater functions supplied by all
     * PredicateBuilders
     * combined. */
    using num_updater_functions = typename std::integral_constant<
        std::size_t,
        util::sum(PredicateBuilder::num_updater_functions::value,
                  PredBuilders::num_updater_functions::value...)>::type;

    using NamedRowPredicatesTypePack = NamedRowPredicates;

    /** this just maps the type-function PredicateBuilder::Getters with argument
     * T
     * over this list */
    template <typename T>
    using Getters = std::decay_t<decltype(std::tuple_cat(
        std::declval<typename PredicateBuilder::template Getters<T>>(),
        std::declval<typename PredBuilders::template Getters<T>>()...))>;
};

} /* namespace SST */
