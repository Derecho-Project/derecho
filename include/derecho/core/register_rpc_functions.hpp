#pragma once
#include "derecho/utils/map_macro.hpp"
#include "detail/rpc_utils.hpp"

#define make_p2p_tagger_expr(x) derecho::rpc::tag_p2p<derecho::rpc::hash_cstr(#x)>(&classname::x)
#define make_ordered_tagger_expr(x) derecho::rpc::tag_ordered<derecho::rpc::hash_cstr(#x)>(&classname::x)
#define applyp2p_(x) make_p2p_tagger_expr(x),
#define applyp2p(...) EVAL(MAP(applyp2p_, __VA_ARGS__))

#define applyordered_(x) make_ordered_tagger_expr(x),
#define applyordered(...) EVAL(MAP(applyordered_, __VA_ARGS__))

#define REGISTER_RPC_FUNCTIONS_NARGS_(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_22,_23,_24,_25,_26,_27,_28,_29,_30,_31,_32,_33,_34,_35,_36,_37,_38,_39,_40,_41,_42,_43,_44,_45,_46,_47,_48,_49,_50,_51,_52,_53,_54,_55,_56,_57,_58,_59,_60,_61,_62,_63,_64,_65,_66,_67,_68,_69,_70,_71,_72,_73,_74,_75,_76,_77,_78,_79,_80,_81,_82,_83,_84,_85,_86,_87,_88,_89,_90,_91,_92,_93,_94,_95,_96,_97,_98,_99, N, ...) N
#define REGISTER_RPC_FUNCTIONS_NARGS(...) REGISTER_RPC_FUNCTIONS_NARGS_(__VA_ARGS__, 99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
#define REGISTER_RPC_FUNCTIONS_CONCAT(arg1,arg2)    REGISTER_RPC_FUNCTIONS_CONCAT1(arg1,arg2)
#define REGISTER_RPC_FUNCTIONS_CONCAT1(arg1,arg2)   REGISTER_RPC_FUNCTIONS_CONCAT2(arg1,arg2)
#define REGISTER_RPC_FUNCTIONS_CONCAT2(arg1,arg2)   arg1##arg2
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
#define REGISTER_RPC_FUNCTIONS_TUPLE_10(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_9(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_11(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_10(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_12(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_11(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_13(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_12(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_14(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_13(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_15(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_14(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_16(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_15(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_17(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_16(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_18(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_17(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_19(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_18(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_20(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_19(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_21(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_20(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_22(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_21(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_23(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_22(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_24(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_23(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_25(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_24(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_26(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_25(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_27(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_26(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_28(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_27(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_29(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_28(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_30(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_29(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_31(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_30(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_32(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_31(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_33(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_32(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_34(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_33(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_35(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_34(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_36(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_35(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_37(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_36(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_38(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_37(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_39(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_38(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_40(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_39(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_41(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_40(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_42(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_41(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_43(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_42(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_44(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_43(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_45(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_44(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_46(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_45(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_47(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_46(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_48(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_47(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_49(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_48(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_50(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_49(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_51(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_50(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_52(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_51(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_53(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_52(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_54(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_53(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_55(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_54(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_56(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_55(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_57(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_56(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_58(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_57(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_59(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_58(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_60(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_59(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_61(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_60(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_62(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_61(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_63(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_62(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_64(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_63(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_65(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_64(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_66(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_65(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_67(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_66(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_68(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_67(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_69(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_68(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_70(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_69(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_71(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_70(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_72(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_71(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_73(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_72(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_74(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_73(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_75(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_74(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_76(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_75(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_77(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_76(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_78(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_77(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_79(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_78(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_80(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_79(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_81(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_80(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_82(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_81(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_83(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_82(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_84(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_83(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_85(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_84(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_86(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_85(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_87(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_86(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_88(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_87(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_89(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_88(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_90(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_89(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_91(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_90(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_92(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_91(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_93(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_92(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_94(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_93(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_95(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_94(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_96(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_95(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_97(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_96(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_98(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_97(rest_args)
#define REGISTER_RPC_FUNCTIONS_TUPLE_99(first_args,rest_args...)                                                      \
    std::make_tuple(first_args),REGISTER_RPC_FUNCTIONS_TUPLE_98(rest_args)

#define REGISTER_RPC_FUNCTIONS_TUPLE_(nargs,...)                                                                      \
    REGISTER_RPC_FUNCTIONS_CONCAT(REGISTER_RPC_FUNCTIONS_TUPLE_,nargs) (__VA_ARGS__)

#define REGISTER_RPC_FUNCTIONS_TUPLE(...)                                                                             \
    REGISTER_RPC_FUNCTIONS_TUPLE_(REGISTER_RPC_FUNCTIONS_NARGS(__VA_ARGS__),__VA_ARGS__)

/**
 * This macro automatically generates a register_functions() method for a Derecho
 * Replicated Object. Example usage for a class Thing with methods foo() and bar():
 *
 * REGISTER_RPC_FUNCTIONS(Thing, ORDERED_TARGETS(foo1), P2P_TARGETS(bar1,bar2), ORDERED_TARGETS(foo2))
 *
 * The macro invocation belongs in your class definition, in the "public:" section.
 *
 * @param name  The name of the class that is being declared as a Replicated Object
 * @param args  Each element of the args is either an invocation of the ORDERED_TARGETS macro containing the names of
 *              each class method that should be callable by an ordered send, or an invocation of the P2P_TARGETS macro
 *              containing the names of each class method that should be callable by a P2P send. Please note that the
 *              number of arguments should not exceed 99, which, however can be extended using the above
 *              REGISTER_RPC_FUNCTIONS_TUPLE_n macros.
 *
 */
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
 * This macro is similar to REGISTER_RPC_FUNCTIONS with a hidden P2P call called 'notify', which enables the server
 * side notification to an external client. Please make sure the user provided subgroup type definition should derive
 * derecho::NotificationSupport to use REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION.
 */
#define REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(name, args...)                                                       \
    virtual void notify(const derecho::NotificationMessage& msg) const override {                                     \
        derecho::NotificationSupport::notify(msg);                                                                    \
    }                                                                                                                 \
    REGISTER_RPC_FUNCTIONS(name, P2P_TARGETS(notify), args)

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
