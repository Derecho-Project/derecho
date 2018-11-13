/**
 * @file type_index_serialization.h
 *
 * @date May 28, 2018
 * @author edward
 */

#pragma once

#include <functional>
#include <memory>
#include <typeindex>
#include <type_traits>
#include <mutils-serialization/context_ptr.hpp>

namespace mutils {

//forward declaration
struct DeserializationManager;

/*
 * These functions implement serialization for std::type_index in the same way
 * it's "serialized" for RPC's Opcodes: by copying the raw bytes of a type_index
 * into the byte buffer via casting. This is unsafe and only works as long as
 * every Derecho instance uses the same compiler, since type_index's internal
 * representation is compiler-dependent.
 */

std::size_t bytes_size(const std::type_index& type_index);
std::size_t to_bytes(const std::type_index& type_index, char* buffer);
void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::type_index& type_index);

template<typename MustBeTypeIndex>
std::enable_if_t<std::is_same<MustBeTypeIndex, std::type_index>::value,
std::unique_ptr<MustBeTypeIndex>> from_bytes(DeserializationManager*, const char* buffer) {
    std::type_index reinterpreted_type_index = ((std::type_index const*)(buffer))[0];
    return std::make_unique<MustBeTypeIndex>(reinterpreted_type_index);
}

/* Specialize ContextDeleter for std::type_index to specify that
 * context_ptr<std::type_index> should have no deleter. */
template<>
struct ContextDeleter<std::type_index> : public ContextDeleter<void> {};

//Is this the right declaration for from_bytes_noalloc? Do I need another const on buffer? Does it need to be a template?
template<typename MustBeTypeIndex>
std::enable_if_t<std::is_same<MustBeTypeIndex, std::type_index>::value,
context_ptr<MustBeTypeIndex>> from_bytes_noalloc(DeserializationManager* ctx, char const * buffer,
                                                                 context_ptr<MustBeTypeIndex> = context_ptr<MustBeTypeIndex>{}) {
    //wrap a context_ptr around the pointer directly into the buffer, rather than copying out the type_index
    return context_ptr<MustBeTypeIndex>((std::type_index*)(buffer));;
}
}
