#pragma once
#include "args-finder.hpp"
#include "combinator_utils.h"
#include <array>
#include <cassert>
#include <functional>
#include <memory>
#include <tuple>
#include <type_traits>

namespace sst {

/**
 * @file combinators.h
 * Combinators for SST predicates.  These combinators can be used to define
 * new predicates using a simple logic language consisting of conjuction,
 * disjunction, integral-type comparison, and knowledge operators.
 */

using util::TypeList;

/**
 * This poorly-named class contains:
 * 1) a Predicate
 * 2) functions required to update variables in the SST used by the predicate
 * 3) Row_Extension types required to support this predicate
 *
 * The Row is just the user's Row (as in SST).  "Metadata" is a struct which
 *contains handy type definitions (see its definition below)
 * TypeList is a list of types; it works similarly to NamedRowPredicates, but
 *with fewer handy accessors.
 *
 * Please note that you're looking at the base case.
 */
template <typename Row, typename Metadata>
struct PredicateBuilder<Row, TypeList<Metadata>> {
    /**
     * What the predicate returns
     */
    using row_entry = typename Metadata::ExtensionType;

    /**
     * similar to the function Cons in Racket.
     */
    template <typename T>
    using append = typename TypeList<Metadata>::template append<T>;

    /**
     * in the base case, there's no need to use the whole SST; this also means
     * we don't need an intermediate value
     */
    static_assert(std::is_pod<Row>::value, "Error: POD rows required!");
    struct Row_Extension {};

    /**
     * This is the raw lambda representing the current predicate.
     * Please don't use it unless you know you need to.
     */
    const typename Metadata::raw_getter curr_pred_raw;

    /**
     * This is the current predicate the user provided (or that we've built up
     * to
     * now).
     * Please note that the parameter supplied to this function's two arguments
     * ("Row" and "Row_Extension") will in practice *both* be the SST's internal
     * row.
     * You can't write <T extends Row & Row_Extension> in C++ yet.
     */
    const std::function<row_entry(volatile const Row &,
                                  volatile const Row_Extension &)>
            curr_pred;

    /**
     * The raw function passed in by the user
     */
    const std::function<row_entry(volatile const Row &)> base;

    /**
     * 0 (there aren't any)
     */
    using num_updater_functions =
            typename std::integral_constant<std::size_t, 0>::type;

    /**
     * If the user has named this Predicate, then this will be a tuple
     * consisting
     * of a
     * single function which calculates the predicate's result.  It's basically
     * the
     * user-facing interface to the predicate. Gets the result of the function
     * of
     * this name.
     * ("this name" is the index into the tuple)
     */
    template <typename T>
    using Getters = std::conditional_t<
            /* if */ Metadata::has_name::value,
            /* then */ std::tuple<std::function<row_entry(T)>>,
            /* else */ std::tuple<>>;

    /**
     * Remember where we wanted to have a <T extends Row & Row_Extension>?
     * This returns a function of T -> row_entry, which will internally call
     * the predicate represented by this class.
     *
     * In practice, there may be several getters if the user names the predicate
     *at several layers.
     * e.g. E(E(pred)) is named "foo" while pred itself is named "bar"
     * @return
     */
    template <typename T /*T extends Row & Row_Extension*/>
    std::enable_if_t<Metadata::has_name::value, Getters<const volatile T &>>
    wrap_getters() const {
        assert(&curr_pred == &this->curr_pred);
        // WARNING: This currently makes an extra copy of curr_pred to capture
        // it
        // TODO: Before running any tests, replace with the new C++14 feature:
        //{[captured_pred = this->curr_pred](volatile const T& t) { ...
        auto captured_pred = this->curr_pred;
        std::function<row_entry(volatile const T &)> f{[captured_pred](
                volatile const T &t) { return captured_pred(t, t); }};
        return std::make_tuple(f);
    }

    /**
     * base case
     * @return
     */
    template <typename T>
    std::enable_if_t<!Metadata::has_name::value, Getters<const volatile T &>>
    wrap_getters() const {
        return std::tuple<>{};
    }
};

/**
 * forward-declarations.
 */
template <typename, typename...>
struct NamedFunctionTuples;
template <typename...>
struct NamedRowPredicates;

/**
* "Normal" case for PredicateBuilder.
* See comments on the base case.
*/
template <typename Row, typename ExtensionList>
struct PredicateBuilder {
    template <typename T>
    using append = typename ExtensionList::template append<T>;

    using hd = typename ExtensionList::hd;

    /**
     * "Uniqueness Tag" is just the name of this predicate.  If this predicate
     * doesn't have a name yet,
     * it's just the name of the dependency-predicate; for example, if this is
     * E(pred), then it would be pred's name.
     * if no predicate has a name yet, this is -1.
     */
    using is_named = std::integral_constant<bool, (predicate_builder::choose_uniqueness_tag<hd>::value >= 0)>;

    static_assert(is_named::value
                          ? predicate_builder::choose_uniqueness_tag<hd>::value >= 0
                          : true,
                  "Internal Error: renaming failed!");

    using row_entry = typename hd::ExtensionType;
    using tl = typename ExtensionList::tl;

    /**
     * the intermediate value stored at this PredicateBuilder
     */
    struct Row_Extension : public PredicateBuilder<Row, tl>::Row_Extension {
        using super = typename PredicateBuilder<Row, tl>::Row_Extension;
        row_entry stored;
    };

    /**
     * This takes the SST::InternalRow, SST::operator[], and SST::size().
     */
    typedef std::function<void(
            volatile Row_Extension &,
            std::function<util::ref_pair<volatile Row, volatile Row_Extension>(
                    int)>,
            const int num_rows)>
            updater_function_t;

    using num_updater_functions = std::integral_constant<std::size_t,
                                                         PredicateBuilder<Row, tl>::num_updater_functions::value + 1>;

    template <typename InternalRow>
    using Getters = std::decay_t<decltype(std::tuple_cat(
            std::declval<
                    /*If this function is named, then register its getter */
                    std::conditional_t</*if*/ hd::has_name::value, /*then*/ std::tuple<std::function<row_entry(InternalRow)>>,
                                       /*else*/ std::tuple<>>>(),
            /*and append that to the getters you've already built */
            std::declval<typename PredicateBuilder<Row, tl>::template Getters<InternalRow>>()))>;

    using num_getters =
            typename std::tuple_size<Getters<int /*dummy param*/>>::type;

    /**
     * Takes a NameEnum, and returns a type-level boolean indicating whether
     * the Name of this NamedRowFunction was taken from that Enum.
     */
    template <typename NameEnum>
    using name_enum_matches = predicate_builder::NameEnumMatches<NameEnum, ExtensionList>;

    /**
     * Just don't touch this; it's the raw lambda for the updater function
     */
    const typename hd::raw_updater updater_function_raw;

    /**
     * This is the updater function whose purpose is defined above,
     * where the typename is defined.
     */
    const updater_function_t updater_function;

    /**
     * All the predicates upon which we depend.
     * If we're E(pred), then this contains pred.
     */
    const PredicateBuilder<Row, tl> prev_preds;

    /**
     * we discussed this before; just an interface to the NamedFunction
     */
    using pred_t = std::function<row_entry(volatile const Row &,
                                           volatile const Row_Extension &)>;

    /**
     * the raw lambda used to define the predicate.
     */
    const typename hd::raw_getter curr_pred_raw;

    /**
     * the actual predicate.
     */
    const pred_t curr_pred;

    /**
     * see the discussion of this function in the base case of NamedPredicate,
     * many lines up.
     * @return
     */
    template <typename T>
    std::enable_if_t<hd::has_name::value, Getters<const volatile T &>>
    wrap_getters() const {
        assert(&curr_pred == &this->curr_pred);
        // WARNING: This currently makes an extra copy of curr_pred to capture
        // it
        // TODO: Before running any tests, replace with the new C++14 feature:
        //{[captured_pred = this->curr_pred](volatile const T& t) { ...
        auto captured_pred = curr_pred;
        std::function<row_entry(volatile const T &)> f{[captured_pred](
                volatile const T &t) { return captured_pred(t, t); }};
        return std::tuple_cat(std::make_tuple(f),
                              prev_preds.template wrap_getters<T>());
    }

    template <typename T>
    std::enable_if_t<!hd::has_name::value, Getters<const volatile T &>>
    wrap_getters() const {
        return prev_preds.template wrap_getters<T>();
    }

    PredicateBuilder(const PredicateBuilder<Row, tl> &prev,
                     const decltype(updater_function_raw) &f,
                     const decltype(curr_pred_raw) curr_pred)
            : updater_function_raw(f),
              updater_function(f),
              prev_preds(prev),
              curr_pred_raw(curr_pred),
              curr_pred(curr_pred) {}

    // for convenience when parameterizing SST
    using NamedRowPredicatesTypePack = NamedRowPredicates<PredicateBuilder>;
    using NamedFunctionTypePack = NamedFunctionTuples<void>;
};

namespace predicate_builder {

/**
 * Converts the argument f to a row_predicate.
 * f *must* be convertible to a function-pointer.
 */
// F is a function of Row -> T, for any T.  This will deduce that.
template <typename F>
auto as_row_pred(const F &f) {
    using namespace util;
    using F2 = std::decay_t<decltype(convert(f))>;
    using undecayed_row = typename function_traits<F2>::template arg<0>::type;
    using Row = std::decay_t<undecayed_row>;
    using Entry = std::result_of_t<F(undecayed_row)>;
    auto converted_f = convert_fp(f);
    auto pred_f = [converted_f](const volatile Row &r, const volatile auto &) {
        return converted_f(r);
    };
    using pred_builder = PredicateBuilder<Row,
                                          TypeList<NamelessPredicateMetadata<Entry, -1, void, decltype(pred_f)>>>;
    return pred_builder{pred_f, pred_f};
}

// we don't have a name yet,
// but we're the tail.
template <typename NameEnum, NameEnum Name, typename Row, typename fun_return,
          typename Up, typename Get>
auto name_predicate(const PredicateBuilder<Row, TypeList<NamelessPredicateMetadata<fun_return, -1, Up, Get>>> &pb) {
    using This_list = TypeList<PredicateMetadata<NameEnum, Name, fun_return, Up, Get>>;
    using next_builder = PredicateBuilder<Row, This_list>;
    static auto ret = next_builder{pb.curr_pred_raw, pb.curr_pred_raw};
    return ret;
}

// nobody in this tree has a name yet;
// we need to propogate a uniqueness-tag change
// down the entire tree!
template <typename NameEnum, NameEnum Name, typename Row, typename Ext,
          typename Up, typename Get, typename hd, typename... tl>
const auto &name_predicate(const PredicateBuilder<Row, TypeList<NamelessPredicateMetadata<Ext, -1, Up, Get>, hd, tl...>> &
                                   pb) {
    constexpr int unique = static_cast<int>(Name);
    auto new_prev = change_uniqueness<unique>(pb.prev_preds);
    using This_list = typename decltype(new_prev)::template append<PredicateMetadata<NameEnum, Name, Ext, Up, Get>>;
    using next_builder = PredicateBuilder<Row, This_list>;
    static auto ret = next_builder{new_prev, pb.updater_function_raw, pb.curr_pred_raw};
    return ret;
}

// something else in here has been named, so we can just name this PB and need
// not touch previous ones
template <typename NameEnum, NameEnum Name, typename Row, typename Ext,
          typename Up, typename Get, int unique, typename... tl>
const auto &name_predicate(const PredicateBuilder<Row, TypeList<NamelessPredicateMetadata<Ext, unique, Up, Get>, tl...>> &
                                   pb) {
    static_assert(unique >= 0, "Internal error: overload resolution fails");
    using next_builder = PredicateBuilder<Row, TypeList<PredicateMetadata<NameEnum, Name, Ext, Up, Get>, tl...>>;
    static auto ret = next_builder{pb.prev_preds, pb.updater_function_raw, pb.curr_pred_raw};
    return ret;
}

template <typename T>
using rowpred_template_arg_t = std::decay_t<T>;
#define rowpred_template_arg(x...) rowpred_template_arg_t<decltype(x)>

template <typename Row, typename hd, typename... tl>
auto E(const PredicateBuilder<Row, TypeList<hd, tl...>> &pb) {
    auto curr_pred_raw = pb.curr_pred_raw;
    // using the raw type here is necessary for renaming to be supported.
    // however, we can no longer rely on subtyping to enforce that the
    // correct Row_Extension type is passed in recursively.
    // this is why there is a wrapping in std::function within this function
    // body.
    auto updater_f = [curr_pred_raw](volatile auto &my_row, auto lookup_row,
                                     const int num_rows) {
        using Prev_Row_Extension =
                typename std::decay_t<decltype(my_row)>::super;
        const std::function<bool(const volatile Row &,
                                 const volatile Prev_Row_Extension &)>
                curr_pred = curr_pred_raw;
        bool result = true;
        for(int i = 0; i < num_rows; ++i) {
            auto rowpair = lookup_row(i);
            const volatile Prev_Row_Extension &your_row = rowpair.r;
            if(!curr_pred(rowpair.l, your_row)) result = false;
        }
        my_row.stored = result;
    };

    auto getter_f =
            [](volatile const Row &, volatile const auto &r) { return r.stored; };

    using next_builder = PredicateBuilder<Row,
                                          TypeList<NamelessPredicateMetadata<bool, predicate_builder::choose_uniqueness_tag<hd>::value,
                                                                             decltype(updater_f), decltype(getter_f)>,
                                                   hd, tl...>>;

    return next_builder{pb, updater_f, getter_f};
}

template <typename Row, typename hd, typename... tl>
auto Min(const PredicateBuilder<Row, TypeList<hd, tl...>> &pb) {
    using min_t = typename hd::ExtensionType;

    auto curr_pred_raw = pb.curr_pred_raw;

    auto updater_f = [curr_pred_raw](
            volatile /*Row_Extension that is mine*/ auto &my_row,
            auto /*wrapped SST::operator[] (returns Row,Row_Extension, because no access to InternalRow in PredicateBuilder)*/ lookup_row,
            const int num_rows) {
        using Prev_Row_Extension =
                typename std::decay_t<decltype(my_row)>::super;
        const std::function<min_t(const volatile Row &,
                                  const volatile Prev_Row_Extension &)>
                curr_pred = curr_pred_raw;
        auto initial_val = lookup_row(0);
        min_t min = curr_pred(initial_val.l, initial_val.r);
        for(int i = 1; i < num_rows; ++i) {
            auto rowpair = lookup_row(i);
            auto candidate = curr_pred(rowpair.l, rowpair.r);
            if(candidate < min) min = candidate;
        }
        my_row.stored = min;
    };

    auto getter_f =
            [](volatile const Row &, volatile const auto &r) { return r.stored; };

    using outermore_builder = PredicateBuilder<Row,
                                               TypeList<NamelessPredicateMetadata<min_t, predicate_builder::choose_uniqueness_tag<hd>::value,
                                                                                  decltype(updater_f), decltype(getter_f)>,
                                                        hd, tl...>>;

    return outermore_builder{pb, updater_f, getter_f};
}
}
}

/*

So the idea is to have an SST builder which takes expressions in a constructor.

Something like build_sst(arguments, associate_name(foo, E (E ([](){row.k >
row.val}))) )

E ( E (...) ) would have to produce a structure with these components:
 - extra spaces in row they rely on
 - function(s) to populate those extra spaces
 - function to associate with foo
 - we *don't* give foo a space. Foo is just a name.

 */
