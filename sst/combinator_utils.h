#pragma once
#include <array>
#include <functional>
#include <memory>
#include <type_traits>
#include <cassert>
#include <tuple>
#include "args-finder.hpp"

namespace sst {
namespace util {

template <typename, typename...>
struct TypeList;
/*
 template<> struct TypeList<>{
 using is_tail = std::true_type;
 using size = std::integral_constant<std::size_t,0>;
 };//*/

template <typename Single>
struct TypeList<Single> {
    using hd = Single;
    using is_tail = std::true_type;
    using size = std::integral_constant<std::size_t, 1>;
    template <typename T>
    using append = TypeList<T, Single>;
};

template <typename Fst, typename Snd, typename... Tl>
struct TypeList<Fst, Snd, Tl...> {
    using hd = Fst;
    using tl = TypeList<Snd, Tl...>;
    using is_tail = std::false_type;
    using size = std::integral_constant<std::size_t, 1 + tl::size::value>;
    template <typename T>
    using append = TypeList<T, Fst, Snd, Tl...>;
};

template <typename L, typename R>
struct ref_pair {
    using l_t = L;
    using r_t = R;
    const L &l;
    const R &r;
};

template <typename T>
constexpr T *mke_p() {
    return nullptr;
}

template <typename T, typename... pack>
struct is_in_pack;

template <typename T>
struct is_in_pack<T> : std::false_type {};

template <typename T, typename hd, typename... rst>
struct is_in_pack<T, hd, rst...>
    : std::integral_constant<bool, std::is_same<T, hd>::value ||
                                       is_in_pack<T, rst...>::value>::type {};

template <typename...>
struct dedup_params_str;

template <>
struct dedup_params_str<> {
    using types = std::tuple<>;
};

template <typename T, typename... rst>
struct dedup_params_str<T, rst...> {
    using types = std::conditional_t<
        is_in_pack<T, rst...>::value, typename dedup_params_str<rst...>::types,
        std::decay_t<decltype(std::tuple_cat(
            std::declval<std::tuple<T>>(),
            std::declval<typename dedup_params_str<rst...>::types>()))>>;
};

template <typename... T>
using dedup_params = typename dedup_params_str<T...>::types;

/**
 * A utility meant to assist with extend_tuple_members;
 * This creates an internal struct which extends all the tuple_contents,
 * then returns a *nulled* pointer of the created struct.
 */
template <typename... tuple_contents>
auto *extend_tuple_members_f(const std::tuple<tuple_contents...> &) {
    struct extend_this : public tuple_contents... {};
    return util::mke_p<extend_this>();
}

/**
 * When "tpl" is a tuple of types T..., this creates an anonymous struct which
 * extends all T...
 * By extending this anonymous struct, a user can transitively extend all the
 * T... in tpl.
 * Undefined when "tpl" is not a tuple (you'll get an awful compile error)
 */
template <typename tpl>
using extend_tuple_members =
    std::decay_t<decltype(*extend_tuple_members_f(std::declval<tpl>()))>;

/**
 * Takes a list of types T...; returns a struct which extends all of the T.
 */
template <typename... T>
using extend_all = extend_tuple_members<dedup_params<T...>>;

template <template <typename> class Pred, typename typelist>
constexpr std::enable_if_t<typelist::is_tail::value, bool> forall_type_list() {
    using hd = typename typelist::hd;
    return Pred<hd>::value;
}

template <template <typename> class Pred, typename typelist>
constexpr std::enable_if_t<!typelist::is_tail::value, bool> forall_type_list() {
    using hd = typename typelist::hd;
    using rst = typename typelist::rst;
    return Pred<hd>::value && forall_type_list<Pred, rst>();
}

template <typename T1>
constexpr auto sum(const T1 &t1) {
    return t1;
}

template <typename T1, typename T2, typename... T>
constexpr auto sum(const T1 &t1, const T2 &t2, const T &... t) {
    return t1 + sum(t2, t...);
}

template <std::size_t n, typename T>
struct n_copies_str;

template <typename T>
struct n_copies_str<0, T> {
    using type = std::tuple<>;
};

template <std::size_t n, typename T>
struct n_copies_str {
    using type = std::decay_t<decltype(
        std::tuple_cat(std::make_tuple(std::declval<T>()),
                       std::declval<typename n_copies_str<n - 1, T>::type>()))>;
};

template <std::size_t n, typename T>
using n_copies = typename n_copies_str<n, T>::type;

}  // namespace util

template <typename Row, typename ExtensionList>
struct PredicateBuilder;

namespace predicate_builder {

using util::TypeList;

template <typename NameEnum, NameEnum Name, typename Ext_t, typename RawUpdater,
          typename RawGetter>
struct PredicateMetadata {
    using has_name = std::true_type;
    using Name_Enum = NameEnum;
    using name = std::integral_constant<NameEnum, Name>;
    using ExtensionType = Ext_t;
    using raw_updater = RawUpdater;
    using raw_getter = RawGetter;
};

template <typename Ext_t, int unique_tag, typename RawUpdater,
          typename RawGetter>
struct NamelessPredicateMetadata {
    using has_name = std::false_type;
    using ExtensionType = Ext_t;
    using raw_updater = RawUpdater;
    using raw_getter = RawGetter;
};

template <typename, typename>
struct NameEnumMatches_str;

template <typename NameEnum, typename NameEnum2, NameEnum2 Name,
          typename... Ext_t>
struct NameEnumMatches_str<
    NameEnum, TypeList<PredicateMetadata<NameEnum2, Name, Ext_t...>>>
    : std::integral_constant<bool, std::is_same<NameEnum, NameEnum2>::value> {};

template <typename NameEnum, int unique, typename Ext_t, typename... rst>
struct NameEnumMatches_str<
    NameEnum, TypeList<NamelessPredicateMetadata<Ext_t, unique, rst...>>>
    : std::true_type {
    static_assert(unique >= 0,
                  "Error: this query should be performed only on "
                  "named RowPredicates. This does not have a name "
                  "yet.");
};

template <typename NameEnum, typename NameEnum2, NameEnum2 Name, typename Ext_t,
          typename Up, typename Get, typename hd, typename... tl>
struct NameEnumMatches_str<
    NameEnum,
    TypeList<PredicateMetadata<NameEnum2, Name, Ext_t, Up, Get>, hd, tl...>>
    : std::integral_constant<
          bool, NameEnumMatches_str<NameEnum, TypeList<hd, tl...>>::value &&
                    std::is_same<NameEnum, NameEnum2>::value> {};

template <typename NameEnum, int unique, typename Ext_t, typename Up,
          typename Get, typename hd, typename... tl>
struct NameEnumMatches_str<
    NameEnum,
    TypeList<NamelessPredicateMetadata<Ext_t, unique, Up, Get>, hd, tl...>>
    : std::integral_constant<
          bool, NameEnumMatches_str<NameEnum, TypeList<hd, tl...>>::value> {};

template <typename NameEnum, typename List>
using NameEnumMatches = typename NameEnumMatches_str<NameEnum, List>::type;

template <typename>
struct choose_uniqueness_tag_str {
    using type = std::integral_constant<int, -1>;
};

template <typename NameEnum, NameEnum Name, typename... Ext_t>
struct choose_uniqueness_tag_str<PredicateMetadata<NameEnum, Name, Ext_t...>> {
    using type = std::integral_constant<int, static_cast<int>(Name)>;
};

template <int uid, typename Ext_t, typename... rst>
struct choose_uniqueness_tag_str<
    NamelessPredicateMetadata<Ext_t, uid, rst...>> {
    using type = std::integral_constant<int, uid>;
};

template <typename T>
using choose_uniqueness_tag = typename choose_uniqueness_tag_str<T>::type;

template <int name_index, typename Row, typename NameEnum, NameEnum Name,
          typename... Ext_t>
auto extract_predicate_getters(const PredicateBuilder<
    Row, TypeList<PredicateMetadata<NameEnum, Name, Ext_t...>>> &pb) {
    static_assert(
        static_cast<int>(Name) == name_index,
        "Error: names must be consecutive integer-valued enum members");
    return std::make_tuple(pb.curr_pred);
}

template <int name_index, typename Row, int uniqueness_tag, typename Ext_t,
          typename... rst>
auto extract_predicate_getters(const PredicateBuilder<
    Row, TypeList<NamelessPredicateMetadata<Ext_t, uniqueness_tag, rst...>>> &
                                   pb) {
    static_assert(
        uniqueness_tag >= 0,
        "Error: Please name this predicate before attempting to use it");
    return std::make_tuple(pb.curr_pred);
}

template <int name_index, typename Row, typename NameEnum, NameEnum Name,
          typename Ext_t, typename Up, typename Get, typename... tl>
auto extract_predicate_getters(const PredicateBuilder<
    Row, TypeList<PredicateMetadata<NameEnum, Name, Ext_t, Up, Get>, tl...>> &
                                   pb) {
    static_assert(
        static_cast<int>(Name) == name_index,
        "Error: names must be consecutive integer-valued enum members");
    return std::tuple_cat(
        std::make_tuple(pb.curr_pred),
        extract_predicate_getters<name_index + 1>(pb.prev_preds));
}

template <int name_index, typename Row, int uniqueness_tag, typename Ext_t,
          typename Up, typename Get, typename... tl>
auto extract_predicate_getters(const PredicateBuilder<
    Row, TypeList<NamelessPredicateMetadata<Ext_t, uniqueness_tag, Up, Get>,
                  tl...>> &pb) {
    static_assert(
        uniqueness_tag >= 0,
        "Error: Please name this predicate before attempting to use it");
    return std::tuple_cat(std::make_tuple(pb.curr_pred),
                          extract_predicate_getters<name_index>(pb.prev_preds));
}

template <typename Result, typename F, typename Row, typename NameEnum,
          NameEnum Name, typename... Ext_t>
auto map_updaters(
    std::vector<Result> &accum, const F &f,
    const PredicateBuilder<
        Row, TypeList<PredicateMetadata<NameEnum, Name, Ext_t...>>> &pb) {
    // This should be the tail. Nothing here.
}

template <typename Result, typename F, typename Row, int uniqueness_tag,
          typename Ext_t, typename... rst>
auto map_updaters(
    std::vector<Result> &accum, const F &f,
    const PredicateBuilder<Row, TypeList<NamelessPredicateMetadata<
                                    Ext_t, uniqueness_tag, rst...>>> &pb) {
    static_assert(
        uniqueness_tag >= 0,
        "Error: Please name this predicate before attempting to use it");
    // This should be the tail. Nothing here.
}

template <typename Result, typename F, typename Row, int uniqueness_tag,
          typename Ext_t, typename Up, typename Get, typename hd,
          typename... tl>
auto map_updaters(
    std::vector<Result> &accum, const F &f,
    const PredicateBuilder<
        Row, TypeList<NamelessPredicateMetadata<Ext_t, uniqueness_tag, Up, Get>,
                      hd, tl...>> &pb) {
    static_assert(
        uniqueness_tag >= 0,
        "Error: Please name this predicate before attempting to use it");
    using Row_Extension = typename std::decay_t<decltype(pb)>::Row_Extension;
    Row_Extension *nptr{nullptr};
    accum.push_back(f(pb.updater_function, nptr));
    map_updaters(accum, f, pb.prev_preds);
}

template <typename Result, typename F, typename Row, typename NameEnum,
          NameEnum Name, typename Ext_t, typename Up, typename Get, typename hd,
          typename... tl>
auto map_updaters(
    std::vector<Result> &accum, const F &f,
    const PredicateBuilder<
        Row, TypeList<PredicateMetadata<NameEnum, Name, Ext_t, Up, Get>, hd,
                      tl...>> &pb) {
    using Row_Extension = typename std::decay_t<decltype(pb)>::Row_Extension;
    Row_Extension *nptr{nullptr};
    accum.push_back(f(pb.updater_function, nptr));
    map_updaters(accum, f, pb.prev_preds);
}

template <int unique, typename Row, typename Ext, typename Get>
auto change_uniqueness(const PredicateBuilder<
    Row, TypeList<NamelessPredicateMetadata<Ext, -1, void, Get>>> &pb) {
    using next_builder = PredicateBuilder<
        Row, TypeList<NamelessPredicateMetadata<Ext, unique, void, Get>>>;
    return next_builder{pb.curr_pred_raw};
}

template <int unique, typename Row, typename Ext, typename Up, typename Get,
          typename hd, typename... tl>
auto change_uniqueness(const PredicateBuilder<
    Row, TypeList<NamelessPredicateMetadata<Ext, -1, Up, Get>, hd, tl...>> &
                           pb) {
    auto new_prev = change_uniqueness<unique>(pb.prev_preds);
    using This_list = typename decltype(new_prev)::template append<
        NamelessPredicateMetadata<Ext, unique, Up, Get>>;
    using next_builder = PredicateBuilder<Row, This_list>;
    return next_builder{new_prev, pb.updater_function_raw, pb.curr_pred_raw};
}

}  // namespace predicate_builder

}  // namespace sst
