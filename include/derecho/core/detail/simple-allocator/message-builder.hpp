#pragma once
#include "build-allocator.hpp"

namespace derecho {
namespace derecho_allocator {

template <typename... Args>
class message_builder {
    using allocator = internal::build_allocator<Args...>;
    allocator a;

public:
    using static_size = std::integral_constant<std::size_t, allocator::static_arg_size>;
    using estimated_size = std::integral_constant<std::size_t,
                                                  static_size::value + 32 * allocator::dynamic_arg_count>;
    message_builder(unsigned char* serial_region, std::size_t size)
            : a(serial_region, size) {}
    template <std::size_t s, typename... CArgs>
    decltype(auto) build_arg(CArgs&&... cargs) {
        return a.template build_arg<s, CArgs...>(std::forward<CArgs>(cargs)...);
    }

    char* serialize(const arg_ptr<Args>&...) { return a.serialize(); }
};

}  // namespace derecho_allocator
}  // namespace derecho
