#pragma once
#include "allocator-internal.hpp"

namespace derecho::derecho_allocator::internal {

template <typename alloc_outer, typename... T>
struct _build_allocator;

template <typename a>
struct _build_allocator<a> {
    using type = typename a::template alloc_inner<>;
};

template <typename _alloc_outer, typename Fst, typename... Rst>
struct _build_allocator<_alloc_outer, Fst, Rst...> {
    template <typename... outer_params>
    static decltype(auto) type_builder(alloc_outer<outer_params...>*) {
        if constexpr(std::is_trivially_copyable_v<Fst>) {
            using ret = typename _build_allocator<alloc_outer<outer_params..., Fst>,
                                                  Rst...>::type;
            return (ret*)nullptr;
        } else {
            using ret =
                    typename alloc_outer<outer_params...>::template alloc_inner<Fst,
                                                                                Rst...>;
            return (ret*)nullptr;
        }
    }

    using type = std::decay_t<decltype(*type_builder((_alloc_outer*)nullptr))>;
};

template <typename... T>
using build_allocator = typename _build_allocator<alloc_outer<>, T...>::type;
}  // namespace derecho::derecho_allocator::internal