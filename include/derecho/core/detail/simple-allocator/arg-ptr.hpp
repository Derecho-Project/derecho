#pragma once
#include <memory>

namespace derecho::derecho_allocator {

namespace internal {
template <typename T>
struct deleter {
    constexpr deleter() noexcept = default;
    template <class U>
    deleter(const deleter<U>&) {}
    template <class U>
    deleter(deleter<U>&&) {}
    constexpr void operator()(T*) const {}
};
}  // namespace internal

template <typename T>
using arg_ptr = std::unique_ptr<T, internal::deleter<T>>;
}  // namespace derecho::derecho_allocator