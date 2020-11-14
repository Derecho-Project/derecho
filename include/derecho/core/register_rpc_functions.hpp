#pragma once
#include "detail/rpc_utils.hpp"
#include <derecho/utils/map_macro.hpp>


#define make_p2p_tagger_expr(x) derecho::rpc::tag_p2p<derecho::rpc::hash_cstr(#x)>(&classname::x)
#define make_ordered_tagger_expr(x) derecho::rpc::tag_ordered<derecho::rpc::hash_cstr(#x)>(&classname::x)
#define applyp2p_(x) make_p2p_tagger_expr(x),
#define applyp2p(...) EVAL(MAP(applyp2p_, __VA_ARGS__))

#define applyordered_(x) make_ordered_tagger_expr(x),
#define applyordered(...) EVAL(MAP(applyordered_, __VA_ARGS__))


/**
 * This macro automatically generates a register_functions() method for a Derecho
 * Replicated Object. Example usage for a class Thing with methods foo() and bar():
 *
 * REGISTER_RPC_FUNCTIONS(Thing, ORDERED_TARGETS(foo), P2P_TARGETS(bar))
 *
 * The macro invocation belongs in your class definition, in the "public:" section.
 *
 * @param name The name of the class that is being declared as a Replicated Object
 * @param arg1 Either an invocation of the ORDERED_TARGETS macro containing the
 * names of each class method that should be callable by an ordered send, or an
 * invocation of the P2P_TARGETS macro containing the names of each class method
 * that should be callable by a P2P send.
 * @param arg2 Either ORDERED_TARGETS or P2P_TARGETS, just like arg1
 */
#define REGISTER_RPC_FUNCTIONS(name, arg1, arg2...)     static auto register_functions() { \
	constexpr char first_arg[] =  #arg1 ;				\
	constexpr char second_arg[] =  #arg2 ;				\
	constexpr bool firstarg_well_formed = derecho::rpc::well_formed_macro(first_arg); \
	constexpr bool secondarg_well_formed = derecho::rpc::well_formed_macro(second_arg); \
	if constexpr (firstarg_well_formed && secondarg_well_formed) {	\
	    using classname = name;					\
	    return std::tuple_cat(std::make_tuple(arg1), std::make_tuple(arg2)); \
	}								\
	else {								\
	    static_assert(firstarg_well_formed && secondarg_well_formed, "Error: bad invocation of REGISTER_RPC_FUNCTIONS, args were " #arg1 #arg2); \
	    return std::tuple<>{};					\
	}								\
    }

/**
 * This macro is one of the possible arguments to REGISTER_RPC_FUNCTIONS; its
 * parameters should be a list of method names that should be tagged as
 * P2P-callable RPC functions.
 */
#define P2P_TARGETS(arg1,args...) applyp2p(args) make_p2p_tagger_expr(arg1)

/**
 * This macro is one of the possible arugments to REGISTER_RPC_FUNCTIONS; its
 * parameters should be a list of method names that should be tagged as RPC
 * functions that can only be invoked via an ordered_send.
 */
#define ORDERED_TARGETS(arg1,args...) applyordered(args) make_ordered_tagger_expr(arg1)

/**
 * This macro generates the same "name" for an RPC function that REGISTER_RPC_FUNCTIONS
 * would have used when it generated a register_functions() method. You can use
 * it to supply the template parameter of ordered_send and p2p_send, given the
 * name of a Replicated Object method. For example, if you registered the method
 * Thing::foo() as an RPC function, you can call ordered_send on it with:
 *
 * thing_handle.ordered_send<RPC_NAME(foo)>(foo_args);
 */
#define RPC_NAME(x) derecho::rpc::hash_cstr(#x)
