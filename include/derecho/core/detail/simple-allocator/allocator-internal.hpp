#pragma once

#include "arg-ptr.hpp"
#include "indexed_varargs.hpp"
#include "mutils/mutils.hpp"
#include <derecho/mutils-serialization/SerializationSupport.hpp>

namespace derecho::derecho_allocator {

namespace internal {
template <typename... StaticArgs>
struct alloc_outer {
    static_assert((std::is_trivially_copyable_v<StaticArgs> && ...),
                  "Internal error: alloc_outer args must be trivially copyable");
    template <typename... DynamicArgs>
    struct alloc_inner {
        static_assert(
                ((!std::is_trivially_copyable_v<DynamicArgs>)&&...),
                "Error: trivially-copyable arguments appear after "
                "dynamically-allocted arguments. This method cannot be "
                "invoked via prepare");  // or whatever "prepare" is ultimately called

        template <std::size_t indx>
        static decltype(auto) get_arg_nullptr() {
            static_assert(indx < sizeof...(DynamicArgs) + sizeof...(StaticArgs),
                          "Error: attempt to access argument outside of range");
            if constexpr(indx < sizeof...(StaticArgs)) {
                return (type_at_index<indx, StaticArgs...>*)nullptr;
            } else if constexpr(indx >= sizeof...(StaticArgs)) {
                constexpr const auto new_indx = indx - sizeof...(StaticArgs);
                return (type_at_index<new_indx, DynamicArgs...>*)nullptr;
            }
            // intentionally reaching the end of non-void function here, should
            // hopefully only be an error when the static_assert would also fail
        }

        template <std::size_t s>
        using get_arg = std::decay_t<decltype(*get_arg_nullptr<s>())>;

        using char_p = unsigned char*;
        const char_p serial_region;
        const std::size_t serial_size;
        static const constexpr auto static_arg_size = (sizeof(StaticArgs) + ... + 0);

        std::tuple<StaticArgs*...> static_args;
        std::tuple<std::unique_ptr<DynamicArgs>...> allocated_dynamic_args;
        static const constexpr auto dynamic_arg_count = sizeof...(DynamicArgs);

        alloc_inner(char_p serial_region, std::size_t serial_size)
                : serial_region(serial_region), serial_size(serial_size) {
            assert(serial_size >= static_arg_size);
            if constexpr(sizeof...(StaticArgs) > 0) {
                char_p offset = serial_region;
                std::apply(
                        [&offset](auto&... static_args) {
                            std::size_t indx = 0;
                            const constexpr std::size_t sizes[] = {sizeof(StaticArgs)...};
                            for(char_p* ptr : {((unsigned char**)&static_args)...}) {
                                *ptr = offset;
                                offset += sizes[indx];
                                ++indx;
                            }
                        },
                        static_args);
            }
        }

        template <std::size_t s>
        static constexpr bool is_static() {
            return s < sizeof...(StaticArgs);
        }

        template <std::size_t arg, typename... CArgs>
        decltype(auto) build_arg(CArgs&&... cargs) {
            constexpr bool arg_in_bounds = (arg < (sizeof...(StaticArgs) + sizeof...(DynamicArgs)));
            static_assert(arg_in_bounds, "Error: index out of bounds");
            if constexpr(arg_in_bounds) {
                using Arg = get_arg<arg>;
                if constexpr(is_static<arg>()) {
                    auto* sarg = std::get<arg>(static_args);
                    new(sarg) Arg{std::forward<CArgs>(cargs)...};
                    return arg_ptr<Arg>{sarg};
                } else {
                    auto& uptr = std::get<arg - sizeof...(StaticArgs)>(allocated_dynamic_args);
                    uptr.reset(new Arg(std::forward<CArgs>(cargs)...));
                    return arg_ptr<Arg>{uptr.get()};
                }
            } else {
                struct error_arg_out_of_bounds {};
                return error_arg_out_of_bounds{};
            }
        }

        char* serialize() const {
            std::size_t offset{static_arg_size};
            unsigned char* step1 = serial_region;
            char* region_start = (char*)step1;
            mutils::foreach(allocated_dynamic_args, [&](const auto& uptr) {
                offset += mutils::to_bytes(*uptr, offset + region_start);
            });
            return region_start;
        }
    };
};

}  // namespace internal
}  // namespace derecho::derecho_allocator
