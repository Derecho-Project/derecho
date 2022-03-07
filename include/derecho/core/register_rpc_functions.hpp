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

#define REGISTER_RPC_FUNCTIONS_NARGS_(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_22,_23,_24,_25,_26,_27,_28,_29,_30,_31,_32,_33,_34,_35,_36,_37,_38,_39,_40,_41,_42,_43,_44,_45,_46,_47,_48,_49,_50,_51,_52,_53,_54,_55,_56,_57,_58,_59,_60,_61,_62,_63,_64,_65,_66,_67,_68,_69,_70,_71,_72,_73,_74,_75,_76,_77,_78,_79,_80,_81,_82,_83,_84,_85,_86,_87,_88,_89,_90,_91,_92,_93,_94,_95,_96,_97,_98,_99, N, ...) N
#define REGISTER_RPC_FUNCTIONS_NARGS(...) REGISTER_RPC_FUNCTIONS_NARGS_(__VA_ARGS__, 99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
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
