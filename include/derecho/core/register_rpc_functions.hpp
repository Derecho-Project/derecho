#pragma once
#include "derecho/utils/map_macro.hpp"
#include "detail/rpc_utils.hpp"

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
 *
#define REGISTER_RPC_FUNCTIONS(name, arg1, arg2)                                                                                                     \
    static auto register_functions() {                                                                                                               \
        constexpr char first_arg[] = #arg1;                                                                                                          \
        constexpr char second_arg[] = #arg2;                                                                                                         \
        constexpr bool firstarg_well_formed = derecho::rpc::well_formed_macro(first_arg);                                                            \
        constexpr bool secondarg_well_formed = derecho::rpc::well_formed_macro(second_arg);                                                          \
        if constexpr(firstarg_well_formed && secondarg_well_formed) {                                                                                \
            using classname = name;                                                                                                                  \
            return std::tuple_cat(std::make_tuple(arg1), std::make_tuple(arg2));                                                                     \
        } else {                                                                                                                                     \
            static_assert(firstarg_well_formed && secondarg_well_formed, "Error: bad invocation of REGISTER_RPC_FUNCTIONS, args were " #arg1 #arg2); \
            return std::tuple<>{};                                                                                                                   \
        }                                                                                                                                            \
    }
*/

#define REGISTER_RPC_FUNCTIONS_NARGS_(_1, _2, _3, _4, _5, _6, _7, _8, _9, N, ...) N
#define REGISTER_RPC_FUNCTIONS_NARGS(...) REGISTER_RPC_FUNCTIONS_NARGS_(__VA_ARGS__, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define REGISTER_RPC_FUNCTIONS_CONCAT(arg1,arg2)    REGISTER_RPC_FUNCTIONS_CONCAT1(arg1,arg2)
#define REGISTER_RPC_FUNCTIONS_CONCAT1(arg1,arg2)   REGISTER_RPC_FUNCTIONS_CONCAT2(arg1,arg2)
#define REGISTER_RPC_FUNCTIONS_CONCAT2(arg1,arg2)   arg1##arg2
/**
 * Definitions and well form verification
 */
/*
#define REGISTER_RPC_FUNCTIONS_DEF_0(...)
#define REGISTER_RPC_FUNCTIONS_DEF_1(first_arg,rest_args...)                                                          \
    constexpr char arg_1[] = #first_arg;                                                                              \
    constexpr bool arg_1_well_formed = derecho::rpc::well_formed_macro(arg_1);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_0(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_2(first_arg,rest_args...)                                                          \
    constexpr char arg_2[] = #first_arg;                                                                              \
    constexpr bool arg_2_well_formed = derecho::rpc::well_formed_macro(arg_2);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_1(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_3(first_arg,rest_args...)                                                          \
    constexpr char arg_3[] = #first_arg;                                                                              \
    constexpr bool arg_3_well_formed = derecho::rpc::well_formed_macro(arg_3);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_2(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_4(first_arg,rest_args...)                                                          \
    constexpr char arg_4[] = #first_arg;                                                                              \
    constexpr bool arg_4_well_formed = derecho::rpc::well_formed_macro(arg_4);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_3(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_5(first_arg,rest_args...)                                                          \
    constexpr char arg_5[] = #first_arg;                                                                              \
    constexpr bool arg_5_well_formed = derecho::rpc::well_formed_macro(arg_5);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_4(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_6(first_arg,rest_args...)                                                          \
    constexpr char arg_6[] = #first_arg;                                                                              \
    constexpr bool arg_6_well_formed = derecho::rpc::well_formed_macro(arg_6);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_5(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_7(first_arg,rest_args...)                                                          \
    constexpr char arg_7[] = #first_arg;                                                                              \
    constexpr bool arg_7_well_formed = derecho::rpc::well_formed_macro(arg_7);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_6(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_8(first_arg,rest_args...)                                                          \
    constexpr char arg_8[] = #first_arg;                                                                              \
    constexpr bool arg_8_well_formed = derecho::rpc::well_formed_macro(arg_8);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_7(rest_args)
#define REGISTER_RPC_FUNCTIONS_DEF_9(first_arg,rest_args...)                                                          \
    constexpr char arg_9[] = #first_arg;                                                                              \
    constexpr bool arg_9_well_formed = derecho::rpc::well_formed_macro(arg_9);                                        \
    REGISTER_RPC_FUNCTIONS_DEF_8(rest_args)

#define REGISTER_RPC_FUNCTIONS_DEF_(nargs,...)                                                                        \
    REGISTER_RPC_FUNCTIONS_CONCAT(REGISTER_RPC_FUNCTIONS_DEF_, nargs) (__VA_ARGS__)

#define REGISTER_RPC_FUNCTIONS_DEF(...)                                                                               \
    REGISTER_RPC_FUNCTIONS_DEF_(REGISTER_RPC_FUNCTIONS_NARGS(__VA_ARGS__),__VA_ARGS__)
*/
/**
 * Condition test
 */
/*
#define REGISTER_RPC_FUNCTIONS_COND_1 arg_1_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_2 REGISTER_RPC_FUNCTIONS_COND_1 && arg_2_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_3 REGISTER_RPC_FUNCTIONS_COND_2 && arg_3_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_4 REGISTER_RPC_FUNCTIONS_COND_3 && arg_4_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_5 REGISTER_RPC_FUNCTIONS_COND_4 && arg_5_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_6 REGISTER_RPC_FUNCTIONS_COND_5 && arg_6_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_7 REGISTER_RPC_FUNCTIONS_COND_6 && arg_7_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_8 REGISTER_RPC_FUNCTIONS_COND_7 && arg_8_well_formed
#define REGISTER_RPC_FUNCTIONS_COND_9 REGISTER_RPC_FUNCTIONS_COND_8 && arg_9_well_formed

#define REGISTER_RPC_FUNCTIONS_COND_(nargs) REGISTER_RPC_FUNCTIONS_CONCAT(REGISTER_RPC_FUNCTIONS_COND_,nargs)

#define REGISTER_RPC_FUNCTIONS_COND(...) REGISTER_RPC_FUNCTIONS_COND_(REGISTER_RPC_FUNCTIONS_NARGS(__VA_ARGS__))
*/
/**
 * tuple concat
 */
#define REGISTER_RPC_FUNCTIONS_TUPLE_1(first_args,rest_args...) std::make_tuple(first_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_2(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_1(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_3(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_2(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_4(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_3(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_5(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_4(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_6(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_5(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_7(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_6(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_8(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_7(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_9(first_args,rest_args...)                                                       \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_8(rest_args)

#define REGISTER_RPC_FUNCTIONS_TUPLE_(nargs,...)                                                                      \
    REGISTER_RPC_FUNCTIONS_CONCAT(REGISTER_RPC_FUNCTIONS_TUPLE_,nargs) (__VA_ARGS__)

#define REGISTER_RPC_FUNCTIONS_TUPLE(...)                                                                             \
    REGISTER_RPC_FUNCTIONS_TUPLE_(REGISTER_RPC_FUNCTIONS_NARGS(__VA_ARGS__),__VA_ARGS__)

#define REGISTER_RPC_FUNCTIONS(name, args...)                                                                         \
    static auto register_functions() {                                                                                \
        if constexpr(derecho::rpc::well_formed_macro(#args)) {                                                        \
            using classname = name;                                                                                   \
            return std::tuple_cat(REGISTER_RPC_FUNCTIONS_TUPLE(args));                                                \
        } else {                                                                                                      \
            static_assert(derecho::rpc::well_formed_macro(#args),                                                     \
                "Error: bad invocation of REGISTER_RPC_FUNCTIONS, args were " #args);                                 \
            return std::tuple<>{};                                                                                    \
        }                                                                                                             \
    }

/**
 * This macro is one of the possible arguments to REGISTER_RPC_FUNCTIONS; its
 * parameters should be a list of method names that should be tagged as
 * P2P-callable RPC functions.
 */
#define P2P_TARGETS(arg1, args...)            \
    IF_ELSE(HAS_ARGS(args))                   \
    (                                         \
            applyp2p(args))(/* Do nothing */) \
            make_p2p_tagger_expr(arg1)

/**
 * This macro is one of the possible arugments to REGISTER_RPC_FUNCTIONS; its
 * parameters should be a list of method names that should be tagged as RPC
 * functions that can only be invoked via an ordered_send.
 */
#define ORDERED_TARGETS(arg1, args...)           \
    IF_ELSE(HAS_ARGS(args))                      \
    (                                            \
            applyordered(args))(/*Do nothing */) \
            make_ordered_tagger_expr(arg1)

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
