/**
 * @file remote_invocable.h
 *
 * @date Feb 13, 2017
 */

#pragma once

#include <functional>
#include <numeric>
#include <type_traits>

#include "rpc_utils.hpp"
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/utils/logger.hpp>
#include <mutils/FunctionalMap.hpp>
#include <mutils/tuple_extras.hpp>
#include <spdlog/spdlog.h>

namespace derecho {

namespace rpc {

template <typename Ret>
using PendingResults_ptr = PendingResults<Ret>*;

/** defined in rpc_manager.h */
bool in_rpc_handler();

//Technically, RemoteInvocable "specializes" this template for the case where
//the second parameter is a std::function<Ret(Args...)>. However, there is no
//implementation for any other specialization, so this template is meaningless.
template <FunctionTag, typename>
struct RemoteInvocable;

template <FunctionTag, typename>
struct RemoteInvoker;

/**
 * Provides functions to implement RPC sends for function calls to a single
 * function, identified by its compile-time "tag" or ID.
 * Many versions of this class will be extended by a single RemoteInvocableClass;
 * each specific instance of this class provides a mechanism for communicating
 * with remote sites that handles one particular function.
 * @tparam tag The compile-time ID value associated with this function
 * @tparam Ret The function's return type
 * @tparam Args The function's argument types
 */
template <FunctionTag Tag, typename Ret, typename... Args>
struct RemoteInvoker<Tag, std::function<Ret(Args...)>> {
    using remote_function_type = std::function<Ret(Args...)>;
    const Opcode invoke_opcode;
    const Opcode reply_opcode;

    Ret* returnRet() {
        return nullptr;
    }

    /* Maps invocation-instance IDs to results sets
       Weijia: Although the maximum pending RPC is controlled by the window
       size option in the configuration, the user application may not retrieve
       the results on time. Therefore, the results may piled up and we need more
       slots for the results than window size. We hardwired this to 4K here.
       TODO: Obviously, this is not space efficient. Let's find a better RPC
       return value mechanism.
     */

#define MAX_CONCURRENT_RPCS_PER_INVOKER (4096)
    PendingResults<Ret> results_vector[MAX_CONCURRENT_RPCS_PER_INVOKER];
    std::atomic<unsigned short> invocation_id_sequencer;
    // std::mutex map_lock; - we don't need a lock on the result map anymore.
    using lock_t = std::unique_lock<std::mutex>;

    /* use this from within a derived class to retrieve precisely this RemoteInvoker
     * (this way, all the inherited RemoteInvoker methods in the subclass do not need
     * to worry about type collisions)*/
    inline RemoteInvoker& get_invoker(
            std::integral_constant<FunctionTag, Tag> const* const,
            const Args&..., std::enable_if_t<(sizeof...(Args) > 0)>* = nullptr) {
        return *this;
    }
    inline RemoteInvoker& get_invoker(RemoteInvoker const* const) { return *this; }

    //only works if no overloads are registered!
    inline RemoteInvoker& get_invoker(std::integral_constant<FunctionTag, Tag> const* const) {
        return *this;
    }

    using barray = char*;
    using cbarray = const char*;

    inline auto serialize_one(barray) { return 0; }

    template <typename A, typename... Rest>
    inline auto serialize_one(barray v, const A& a, const Rest&... rest) {
        auto size = mutils::to_bytes(a, v);
        return size + serialize_one(v + size, rest...);
    }

    inline auto serialize_all(barray v, const Args&... args) {
        return serialize_one(v, args...);
    }

    /**
     * Return type for the send function. Contains the RPC-invoking message
     * (in a buffer of size "size"), a set of futures for the results, and
     * a set of promises for the results.
     */
    struct send_return {
        std::size_t size;
        char* buf;
        QueryResults<Ret> results;
        PendingResults<Ret>& pending;
    };

    /**
     * Called to construct an RPC message to send that will invoke the remote-
     * invocable function targeted by this RemoteInvoker.
     * @param out_alloc A function that can allocate buffers, which will be
     * used to store the constructed message
     * @param a The arguments to be used when calling the remote-invocable function
     */
    send_return send(const std::function<char*(int)>& out_alloc,
                     const std::decay_t<Args>&... remote_args) {
        // auto invocation_id = mutils::long_rand();
        std::size_t invocation_id = invocation_id_sequencer++;
        invocation_id %= MAX_CONCURRENT_RPCS_PER_INVOKER;
        std::size_t size = mutils::bytes_size(invocation_id);
        size += (mutils::bytes_size(remote_args) + ... + 0);

        char* serialized_args = out_alloc(size);
        {
            auto v = serialized_args + mutils::to_bytes(invocation_id, serialized_args);
            auto check_size = mutils::bytes_size(invocation_id) + serialize_all(v, remote_args...);
            assert_always(check_size == size);
        }

        // lock_t l{map_lock};
        results_vector[invocation_id].reset();
        PendingResults<Ret>& pending_results = results_vector[invocation_id];

        dbg_default_trace("Ready to send an RPC call message with invocation ID {}", invocation_id);
        return send_return{size, serialized_args, pending_results.get_future(),
                           pending_results};
    }

    auto prep_send(char* buffer, std::size_t size, PendingResults_ptr<Ret>& pending_ptr, std::function<void()> send) {
        std::size_t invocation_id = invocation_id_sequencer++;
        assert(size >= derecho_allocator::message_builder<Args...>::static_size::value + sizeof(invocation_id));
        invocation_id %= MAX_CONCURRENT_RPCS_PER_INVOKER;
        //TODO: DEBUG. The invocation_id might have been factored into the size already. Must check to be sure.
        mutils::to_bytes(invocation_id, buffer);
        results_vector[invocation_id].reset();
        pending_ptr = &results_vector[invocation_id];
        dbg_default_trace("Prep Send done for RPC call message with invocation ID {}", invocation_id);
        return message_builder<Ret, Args...>{
                pending_ptr->get_future(),
                send,
                (unsigned char*)(buffer + sizeof(mutils::bytes_size(invocation_id))),
                size - sizeof(mutils::bytes_size(invocation_id))};
    }

    /**
     * Specialization of receive_response for non-void functions. Stores the
     * response in the results map, or stores the exception if there was an
     * exception.
     * @return An empty recv_ret, since there is no response to a response
     */
    template <typename definitely_char>
    inline recv_ret receive_response(
            std::false_type*,
            mutils::DeserializationManager* dsm,
            const node_id_t& nid, const char* response,
            const std::function<definitely_char*(int)>&) {
        //note: find where this exception is set on the sending side!
        bool is_exception = response[0];
        long int invocation_id = ((long int*)(response + 1))[0];
        // lock_t l{map_lock};
        // we unlock the map here to avoid the deadlock:
        // The p2p handler thread, on receiving an RPC REPLY may get this lock
        // before sst_detect thread finish handling the corresponding ordered
        // send locally. However, sst_detect thread may be so slow that it
        // hasn't finish deliverying of a previous ordered send, therefore waits
        // for the map_lock in the following call stack:
        // - RPCManager::rpc_message_handler() ->
        // - RPCManager::parse_and_receive() ->
        // - RPCManager::receive_message()->
        // - receivers->at(indx)->
        // - RemoteInvoker::receive_response().
        // therefore, the promise for the next ordered_send, on which the
        // p2p handler thread is waiting for, cannot be fulfilled.
        // dead lock!!!
        // We use this workaround by just release the results_map lock as early
        // because we have 64K slots and we assume after 64K messages, this
        // corresponding ordered_send has been finished already.
        // TODO: make a better plan along with garbage collection.
        // l.unlock();
        // TODO: garbage collection for the responses.
        // More on May-7th (Weijia):
        // We don't need to lock the results container (now results_vector)
        // anymore. The vector is initialized at the beginning. The p2p rpc
        // reuse the slot by reseting it. No deleting, insertion, or
        // replacement operations at all. But we still need a better garbage
        // collection mechanism.
        if(is_exception) {
            results_vector[invocation_id].set_exception(nid, std::make_exception_ptr(remote_exception_occurred{nid}));
        } else {
            dbg_default_trace("Received an RPC response for invocation ID {} from node {}", invocation_id, nid);
            results_vector[invocation_id].set_value(nid, *mutils::from_bytes<Ret>(dsm, response + 1 + sizeof(invocation_id)));
        }
        return recv_ret{Opcode(), 0, nullptr, nullptr};
    }

    /**
     * Specialization of receive_response for void functions (which don't
     * expect any response).
     */
    inline recv_ret receive_response(std::true_type*,
                                     mutils::DeserializationManager*,
                                     const node_id_t& nid, const char* response,
                                     const std::function<char*(int)>&) {
        if(response[0]) throw remote_exception_occurred{nid};
        assert_always(false && "was not expecting a response!");
    }

    /**
     * Entry point for responses; called when a message is received that
     * contains a response to this RemoteInvocable function's RPC call.
     * @param dsm
     * @param nid The ID of the node that sent the response
     * @param response The byte buffer containing the response message
     * @param f
     * @return A recv_ret containing nothing of value.
     */
    inline recv_ret receive_response(  //mutils::DeserializationManager* dsm,
            mutils::RemoteDeserialization_v* rdv,
            const node_id_t& nid, const char* response,
            const std::function<char*(int)>& f) {
        constexpr std::is_same<void, Ret>* choice{nullptr};
        //        return receive_response(choice, dsm, nid, response, f);
        mutils::DeserializationManager dsm{*rdv};
        return receive_response(choice, &dsm, nid, response, f);
    }

    /**
     * Populates the pending-results map of a particular invocation of the remote-invocable
     * function, given the list of nodes that will be contacted to call the function.
     * @param invocation_id The ID referring to a particular invocation of the function
     * @param who The list of nodes that will service this RPC call
     */
    inline void fulfill_pending_results_vector(long int invocation_id, const node_list_t& who) {
        // I think this function is never called
        assert_always(false);
        results_vector[invocation_id].fulfill_map(who);
    }

    /**
     * Constructs a RemoteInvoker that provides RPC call marshalling and
     * response-handling for a specific function tag and function type (the one
     * specified in the class's template parameters). Registers a function
     * to handle responses for this RPC call in the given "receivers" map.
     * (The actual function implementation is not needed, since only the
     * remote side needs to know how to implement the RPC function.)
     *
     * @param receivers A map from RPC message opcodes to handler functions,
     * which this RemoteInvoker should add its functions to.
     */
    RemoteInvoker(uint32_t class_id, uint32_t instance_id,
                  std::map<Opcode, receive_fun_t>& receivers)
            : invoke_opcode{class_id, instance_id, Tag, false},
              reply_opcode{class_id, instance_id, Tag, true},
              invocation_id_sequencer(0) {
        receivers.emplace(reply_opcode, [this](auto... a) {
            return this->receive_response(a...);
        });
    }
};

/**
 * Provides functions to implement handling RPC calls to a single function,
 * identified by its compile-time "tag" or opcode. Many versions of this class
 * will be extended by a single RemoteInvocableClass; each specific instance of
 * this class provides a mechanism for handling RPC requests for one particular
 * function.
 * @tparam tag The compile-time value associated with this function
 * @tparam Ret The function's return type
 * @tparam Args The function's argument types
 */
template <FunctionTag Tag, typename Ret, typename... Args>
struct RemoteInvocable<Tag, std::function<Ret(Args...)>> {
    using remote_function_type = std::function<Ret(Args...)>;
    const remote_function_type remote_invocable_function;
    const Opcode invoke_opcode;
    const Opcode reply_opcode;

    /* use this from within a derived class to retrieve precisely this RemoteInvocable
     * (this way, all the inherited RemoteInvocable methods in the subclass do not need
     * to worry about type collisions)*/
    inline RemoteInvocable& get_handler(
            std::integral_constant<FunctionTag, Tag> const* const,
            const Args&...) {
        return *this;
    }

    /**
     * Specialization of receive_call for non-void functions. After calling the
     * function locally, it constructs a message containing the return value to
     * send as a response, and puts it in a buffer allocated by out_alloc. If
     * the function throws an exception, it catches the exception and puts that
     * in the response message instead.
     */
    inline recv_ret receive_call(std::false_type const* const,
                                 mutils::DeserializationManager* dsm,
                                 const node_id_t& caller, const char* _recv_buf,
                                 const std::function<char*(int)>& out_alloc) {
        long int invocation_id = ((long int*)_recv_buf)[0];
        auto recv_buf = _recv_buf + sizeof(long int);
        try {
            const auto result = mutils::deserialize_and_run(dsm, recv_buf, remote_invocable_function);
            const auto result_size = mutils::bytes_size(result) + sizeof(long int) + 1;
            auto out = out_alloc(result_size);
            out[0] = false;
            ((long int*)(out + 1))[0] = invocation_id;
            mutils::to_bytes(result, out + sizeof(invocation_id) + 1);
            dbg_default_trace("Ready to send an RPC reply for invocation ID {} to node {}", invocation_id, caller);
            return recv_ret{reply_opcode, result_size, out, nullptr};
        } catch(...) {
            char* out = out_alloc(sizeof(long int) + 1);
            out[0] = true;
            ((long int*)(out + 1))[0] = invocation_id;
            return recv_ret{reply_opcode, sizeof(long int) + 1, out,
                            std::current_exception()};
        }
    }

    /**
     * Specialization of receive_call for void functions, which do not need to
     * send a response. Simply calls the function and returns a trivial result.
     */
    inline recv_ret receive_call(std::true_type const* const,
                                 mutils::DeserializationManager* dsm,
                                 const node_id_t&, const char* _recv_buf,
                                 const std::function<char*(int)>&) {
        //TODO: Need to catch exceptions here, and possibly send them back, since void functions can still throw exceptions!
        auto recv_buf = _recv_buf + sizeof(long int);
        mutils::deserialize_and_run(dsm, recv_buf, remote_invocable_function);
        return recv_ret{reply_opcode, 0, nullptr};
    }

    /**
     * Entry point for handling an RPC function call to this RemoteInvocable
     * function. Called when a message is received that contains a request to
     * call this function.
     * @param dsm
     * @param who The node that sent the message
     * @param recv_buf The buffer containing the received message
     * @param out_alloc A function that can allocate a buffer for the response message
     * @return
     */
    inline recv_ret receive_call(  //mutils::DeserializationManager* dsm,
            mutils::RemoteDeserialization_v* rdv,
            const node_id_t& who, const char* recv_buf,
            const std::function<char*(int)>& out_alloc) {
        constexpr std::is_same<Ret, void>* choice{nullptr};
        //      return this->receive_call(choice, dsm, who, recv_buf, out_alloc);
        mutils::DeserializationManager dsm{*rdv};
        return this->receive_call(choice, &dsm, who, recv_buf, out_alloc);
    }

    /**
     * Constructs a RemoteInvocable that provides RPC call handling for a
     * specific function, and registers the RPC-handling functions in the
     * given "receivers" map.
     * @param receivers A map from RPC message opcodes to handler functions,
     * which this RemoteInvocable should add its functions to.
     * @param f The actual function that should be called when an RPC call
     * arrives.
     */
    RemoteInvocable(uint32_t class_id, uint32_t instance_id,
                    std::map<Opcode, receive_fun_t>& receivers,
                    std::function<Ret(Args...)> f)
            : remote_invocable_function(f),
              invoke_opcode{class_id, instance_id, Tag, false},
              reply_opcode{class_id, instance_id, Tag, true} {
        receivers.emplace(invoke_opcode, [this](auto... a) {
            return this->receive_call(a...);
        });
    }
};

/** This matches uses of wrapped<> where the second argument is not a function,
 * and does nothing. */
template <FunctionTag Tag, typename NotAFunction>
struct wrapped;

/**
 * Template that pairs a FunctionTag with a function pointer, thus
 * "naming" the function with a tag (ID) that is a compile-time value.
 * In practice, it has two template parameters, where the second one matches
 * std::function<Ret(Args...)>, but the two types in the std::function must
 * each be written as a separate template parameter.
 */
template <FunctionTag Tag, typename Ret, typename... Arguments>
struct wrapped<Tag, std::function<Ret(Arguments...)>> {
    using fun_t = std::function<Ret(Arguments...)>;
    fun_t fun;
};

/**
 * Template that pairs a FunctionTag with a pointer-to-member-function. This is
 * an intermediate stage in constructing a wrapped<>, since wrapped<> uses a
 * standalone std::function and doesn't need to know the class the function
 * came from.
 */
template <FunctionTag Tag, typename Ret, typename Class, typename... Arguments>
struct partial_wrapped {
    using fun_t = Ret (Class::*)(Arguments...);
    fun_t fun;
};

/**
 * Exactly the same as the partial_wrapped template, but
 * for pointer-to-member-functions that are const.
 */
template <FunctionTag Tag, typename Ret, typename Class, typename... Arguments>
struct const_partial_wrapped {
    using fun_t = Ret (Class::*)(Arguments...) const;
    fun_t fun;
};

/**
 * Converts a partial_wrapped<> containing a pointer-to-member-function to a
 * wrapped<> containing the same function as a std::function. It does this by
 * constructing a lambda that captures an instance of the function's class and
 * invokes the member function using that instance.
 * @param _this A pointer-to-pointer to an instance of NewClass, the class that
 * contains the member function
 * @param partial The partial_wrapped to convert
 * @return A wrapped<Tag, std::function> that calls partial's function using
 * instance _this
 */
template <typename NewClass, FunctionTag Tag, typename Ret, typename... Args>
wrapped<Tag, std::function<Ret(Args...)>> bind_to_instance(std::unique_ptr<NewClass>* _this,
                                                           const partial_wrapped<Tag, Ret, NewClass, Args...>& partial) {
    assert(_this);
    return wrapped<Tag, std::function<Ret(Args...)>>{
            [_this, fun = partial.fun](Args... arguments) {
                assert(_this);
                assert(_this->get());
                return (_this->get()->*fun)(arguments...);
            }};
}

/**
 * Exactly the same as the bind_to_instance function above, but for
 * pointer-to-member-functions that are const.
 */
template <typename NewClass, FunctionTag Tag, typename Ret, typename... Args>
wrapped<Tag, std::function<Ret(Args...)>> bind_to_instance(std::unique_ptr<NewClass>* _this,
                                                           const const_partial_wrapped<Tag, Ret, NewClass, Args...>& partial) {
    assert(_this);
    return wrapped<Tag, std::function<Ret(Args...)>>{
            [_this, fun = partial.fun](Args... arguments) {
                return (_this->get()->*fun)(arguments...);
            }};
}

/**
 * User-facing entry point for the series of functions that binds a FunctionTag
 * to a class's member function. A user's "replicated object" class should use
 * this function in its register_functions() method in order to supply the
 * parameters to setup_rpc_class.
 * @param fun A pointer-to-member-function from the class in the template parameters
 * @return A partial_wrapped struct, which must be further constructed with a
 * call to wrap(std::unique_ptr<NewClass>*, const partial_wrapped<...>&)
 * @tparam Tag The compile-time constant that should be used to "name" this
 * member function
 * @tparam NewClass The class that the function is a member of
 * @tparam Ret The return type of the function
 * @tparam Args The argument type of the function
 */
template <FunctionTag Tag, typename NewClass, typename Ret, typename... Args>
partial_wrapped<Tag, Ret, NewClass, Args...> tag(Ret (NewClass::*fun)(Args...)) {
    static_assert(!std::is_reference<Ret>::value && !std::is_pointer<Ret>::value, "RPC-registered functions cannot return references or pointers!");
    //Fold-expression asserts the boolean expression for all Args in the parameter pack
    static_assert(((std::is_reference<Args>::value || sizeof(Args) < 2 * sizeof(void*)) && ...), "RPC-registered functions must take non-pointer-size arguments by reference to avoid extra copying.");
    return partial_wrapped<Tag, Ret, NewClass, Args...>{fun};
}

/** Exactly the same as the above tag() function, but for const member functions. */
template <FunctionTag Tag, typename NewClass, typename Ret, typename... Args>
const_partial_wrapped<Tag, Ret, NewClass, Args...> tag(Ret (NewClass::*fun)(Args...) const) {
    static_assert(!std::is_reference<Ret>::value && !std::is_pointer<Ret>::value, "RPC-registered functions cannot return references or pointers!");
    static_assert(((std::is_reference<Args>::value || sizeof(Args) < 2 * sizeof(void*)) && ...), "RPC-registered functions must take non-pointer-size arguments by reference to avoid extra copying.");
    return const_partial_wrapped<Tag, Ret, NewClass, Args...>{fun};
}

/* Technically, RemoteInvocablePairs specializes this template for the cases
 * where the parameter pack is a list of types of the form wrapped<id, FunType>
 * However, there is only one specialization, so using RemoteInvocablePairs for
 * anything other than wrapped<id, FunType> is meaningless.
 */
template <typename...>
struct RemoteInvocablePairs;

/**
 * Base case for RemoteInvocablePairs, where the template parameter pack
 * contains only a single wrapped<id, FunType>
 */
template <FunctionTag id, typename FunType>
struct RemoteInvocablePairs<wrapped<id, FunType>>
        : public RemoteInvoker<id, FunType>, public RemoteInvocable<id, FunType> {
    RemoteInvocablePairs(uint32_t class_id,
                         uint32_t instance_id,
                         std::map<Opcode, receive_fun_t>& receivers, FunType function_ptr)
            : RemoteInvoker<id, FunType>(class_id, instance_id, receivers),
              RemoteInvocable<id, FunType>(class_id, instance_id, receivers, function_ptr) {}

    using RemoteInvoker<id, FunType>::get_invoker;
    using RemoteInvocable<id, FunType>::get_handler;
};

/**
 * This struct exists purely to inherit from a series of RemoteInvoker and
 * RemoteInvocables; it inherits one pair of RemoteInvoker/RemoteInvocable for
 * each wrapped<id, FunType> type in its template parameter pack.
 *
 * @tparam id The compile-time constant that names a function, which is associated
 * with a function via the wrapped<> struct and set by the user with the tag()
 * function.
 * @tparam FunType The type of the std::function (raw, not "wrapped") that
 * corresponds to the ID
 * @tparam rest The rest of the wrapped<> types
 */
template <FunctionTag id, typename FunType, typename... rest>
struct RemoteInvocablePairs<wrapped<id, FunType>, rest...>
        : public RemoteInvoker<id, FunType>, public RemoteInvocable<id, FunType>, public RemoteInvocablePairs<rest...> {
    template <typename... RestFunTypes>
    RemoteInvocablePairs(uint32_t class_id,
                         uint32_t instance_id,
                         std::map<Opcode, receive_fun_t>& receivers,
                         FunType function_ptr,
                         RestFunTypes&&... function_ptrs)
            : RemoteInvoker<id, FunType>(class_id, instance_id, receivers),
              RemoteInvocable<id, FunType>(class_id, instance_id, receivers, function_ptr),
              RemoteInvocablePairs<rest...>(class_id, instance_id, receivers, std::forward<RestFunTypes>(function_ptrs)...) {}

    //Ensure the inherited functions from RemoteInvoker and RemoteInvokable are visible
    using RemoteInvoker<id, FunType>::get_invoker;
    using RemoteInvocable<id, FunType>::get_handler;
    using RemoteInvocablePairs<rest...>::get_invoker;
    using RemoteInvocablePairs<rest...>::get_handler;
};

/**
 * Technically, RemoteInvokers is a specialization of this template, but it's
 * the only specialization, so this template does nothing.
 */
template <typename...>
struct RemoteInvokers;

/**
 * Base case for RemoteInvokers, where the template parameter pack
 * contains only a single wrapped<id, FunType>
 */
template <FunctionTag Tag, typename FunType>
struct RemoteInvokers<wrapped<Tag, FunType>> : public RemoteInvoker<Tag, FunType> {
    RemoteInvokers(uint32_t class_id,
                   uint32_t instance_id,
                   std::map<Opcode, receive_fun_t>& receivers)
            : RemoteInvoker<Tag, FunType>(class_id, instance_id, receivers) {}

    using RemoteInvoker<Tag, FunType>::get_invoker;
};

/**
 * This struct exists purely to inherit from a list of RemoteInvokers, one for
 * each wrapped<> type in its template parameter pack.
 *
 * @tparam Tag The compile-time constant integer that names a function, and is
 * associated with the function via the wrapped<> struct
 * @tparam FunType The type of the std::function (raw, not "wrapped") that
 * corersponds to the tag
 * @tparam RestTypes The rest of the wrapped<> types
 */
template <FunctionTag Tag, typename FunType, typename... RestWrapped>
struct RemoteInvokers<wrapped<Tag, FunType>, RestWrapped...>
        : public RemoteInvoker<Tag, FunType>, public RemoteInvokers<RestWrapped...> {
    RemoteInvokers(uint32_t class_id,
                   uint32_t instance_id,
                   std::map<Opcode, receive_fun_t>& receivers)
            : RemoteInvoker<Tag, FunType>(class_id, instance_id, receivers),
              RemoteInvokers<RestWrapped...>(class_id, instance_id, receivers) {}

    using RemoteInvoker<Tag, FunType>::get_invoker;
    using RemoteInvokers<RestWrapped...>::get_invoker;
};

/**
 * Transforms a class into a "replicated object" with methods that can be
 * invoked by RPC, given a place to store RPC message handlers (which should be
 * the "receivers map" of RPCManager). Each RPC-invokable method must be
 * supplied as a template parameter, in order to associate it with a compile-time
 * constant name (the tag).
 * @tparam IdentifyingClass The class to make into an RPC-invokable class
 * @tparam Fs A list of "wrapped" function pointers to members of that class,
 * each associated with a tag.
 */
template <class IdentifyingClass, typename... WrappedFuns>
class RemoteInvocableClass : private RemoteInvocablePairs<WrappedFuns...> {
public:
    const node_id_t nid;

    RemoteInvocableClass(node_id_t nid, uint32_t type_id, uint32_t instance_id,
                         std::map<Opcode, receive_fun_t>& rvrs, const WrappedFuns&... fs)
            : RemoteInvocablePairs<WrappedFuns...>(type_id, instance_id, rvrs, fs.fun...),
              nid(nid) {}

    template <FunctionTag Tag, typename... Args>
    std::size_t get_size_for_ordered_send(Args&&... a) {
        //only used for size calculation
        long int invocation_id = 0;
        std::size_t function_call_size = mutils::bytes_size(invocation_id);
        {
            function_call_size += (0 + ... + mutils::bytes_size(a));
        }
        //Add the header_size that send() adds to the invoker's out_alloc
        return function_call_size + remote_invocation_utilities::header_space();
    }

    template <FunctionTag>
    std::size_t get_header_size_for_ordered_send() {
        long int invocation_id = 0;
        return mutils::bytes_size(invocation_id) + remote_invocation_utilities::header_space();
    }

    template <FunctionTag Tag, typename... Args>
    auto* getReturnType(Args&&... args) {
        constexpr std::integral_constant<FunctionTag, Tag>* choice{nullptr};
        return this->get_invoker(choice, args...).returnRet();
    }

    template <FunctionTag Tag, typename Ret>
    auto prepare_send(char* buffer, std::size_t size, PendingResults_ptr<Ret>& pending_ptr, std::function<void()> send) {
        using namespace remote_invocation_utilities;
        constexpr std::integral_constant<FunctionTag, Tag>* choice{nullptr};
        using invoker_t = std::decay_t<decltype(this->get_invoker(choice))>;
        constexpr invoker_t const* const specific_choice{nullptr};
        auto& invoker = this->get_invoker(specific_choice);
        //TODO: is "size - header_space()" the right argument here?
        populate_header(buffer, size - header_space(), invoker.invoke_opcode, nid, 0);
        return invoker.prep_send(buffer + header_space(), size - header_space(), pending_ptr, send);
    }

    /**
     * Constructs a message that will remotely invoke a method of this class,
     * supplying the specified arguments, using RPC.
     * @param out_alloc A function that can allocate a buffer for the message
     * @param args The arguments that should be given to the method when
     * invoking it
     * @return A struct containing a set of futures for the remote-method
     * results ("results"), and a set of corresponding promises for those
     * results ("pending").
     */
    template <FunctionTag Tag, typename... Args>
    auto send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        using namespace remote_invocation_utilities;

        constexpr std::integral_constant<FunctionTag, Tag>* choice{nullptr};
        auto& invoker = this->get_invoker(choice, args...);
        constexpr const auto header_size = header_space();
        auto sent_return = invoker.send(
                [&out_alloc, &header_size](std::size_t size) {
                    return out_alloc(size + header_size) + header_size;
                },
                std::forward<Args>(args)...);

        std::size_t payload_size = sent_return.size;
        char* buf = sent_return.buf - header_size;
        uint32_t flags = 0;
        /*
         set the cascading flag if necessary.
         This is not important because, unlike p2p_send, ordered_send/query
         does not distinguish cascading and non cascading sends. However,
         to keep the format consistency, we keep the flags field in the RPC
         message header reserved for future use.

        if (in_rpc_handler()) {
            RPC_HEADER_FLAG_SET(flags,CASCADE);
            dbg_default_info("send cascading message.");
        }
        */
        populate_header(buf, payload_size, invoker.invoke_opcode, nid, flags);

        using Ret = typename decltype(sent_return.results)::type;
        /*
          much like previous definition, except with
          two fewer fields
        */
        struct send_return {
            QueryResults<Ret> results;
            PendingResults<Ret>& pending;
            //LifeTracker
            /*
class LifeTracker {
std::function<void () > deleter;
~LifeTraker(){
deleter();
}
};

LifeTracker{[invocation_id, &map](){map.erase(invocation_id);}};
	   */
        };
        return send_return{std::move(sent_return.results),
                           sent_return.pending};
    }

    using specialized_to = IdentifyingClass;
    RemoteInvocableClass& for_class(IdentifyingClass*) {
        return *this;
    }
};

/**
 * Specialization of RemoteInvocableClass for classes that have no RPC methods
 */
template <class IdentifyingClass>
class RemoteInvocableClass<IdentifyingClass> {
public:
    const node_id_t nid;

    RemoteInvocableClass(node_id_t nid, uint32_t type_id, uint32_t instance_id,
                         std::map<Opcode, receive_fun_t>& rvrs)
            : nid(nid) {}

    template <FunctionTag Tag, typename... Args>
    std::size_t get_size(Args&&... a) {
        auto invocation_id = mutils::long_rand();
        std::size_t size = mutils::bytes_size(invocation_id);
        {
            auto t = {std::size_t{0}, std::size_t{0}, mutils::bytes_size(a)...};
            size += std::accumulate(t.begin(), t.end(), 0);
        }
        return size;
    }

    template <FunctionTag Tag, typename... Args>
    auto* getReturnType(Args&&... args) {
        void* null_and_void(nullptr);
        return null_and_void;
    }

    using specialized_to = IdentifyingClass;
    RemoteInvocableClass& for_class(IdentifyingClass*) {
        return *this;
    }
};

/**
 * Constructs a RemoteInvocableClass instance that proxies for an instance of
 * the class in the template parameter (IdentifyingClass).
 * @param nid The Node ID of the node running this code
 * @param type_id A number uniquely identifying the type of IdentifyingClass;
 * in practice, the index of IdentifyingClass within the template parameters
 * of the Group that contains this replicated object.
 * @param instance_id A number uniquely identifying this instance of
 * IdentifyingClass; in practice, the ID of the subgroup that will be
 * replicating this object.
 * @param rvrs A map from RPC opcodes to RPC message handler functions, into
 * which new handlers will be added for this RemoteInvocableClass
 * @param fs A list of "wrapped" function pointers to members of the wrapped
 * class, each associated with a name, which should become RPC functions
 * @return A unique_ptr to a RemoteInvocableClass of type IdentifyingClass.
 */
template <class IdentifyingClass, typename... WrappedFuns>
auto build_remote_invocable_class(const node_id_t nid, const uint32_t type_id, const uint32_t instance_id,
                                  std::map<Opcode, receive_fun_t>& rvrs,
                                  const WrappedFuns&... fs) {
    return std::make_unique<RemoteInvocableClass<IdentifyingClass, WrappedFuns...>>(nid, type_id, instance_id, rvrs, fs...);
}

/**
 * Transforms a class into an RPC client for the methods of that class, given a
 * place to store RPC message handlers (which should be the "receivers map" of
 * RPCManager). Each RPC-invokable method must be supplied as a template
 * parameter, in order to associate it with a compile-time constant name (the
 * tag). This must be the same tag that the "RPC server" being contacted used
 * for the RemoteInvocableClass version of this class.
 * @tparam IdentifyingClass The class to make into an RPC-invoking client
 * @tparam Fs A list of "wrapped" function pointers to members of that class,
 * each associated with a tag.
 */
template <class IdentifyingClass, typename... WrappedFuns>
class RemoteInvokerForClass : private RemoteInvokers<WrappedFuns...> {
public:
    const node_id_t nid;

    RemoteInvokerForClass(node_id_t nid, uint32_t type_id, uint32_t instance_id,
                          std::map<Opcode, receive_fun_t>& rvrs)
            : RemoteInvokers<WrappedFuns...>(type_id, instance_id, rvrs),
              nid(nid) {}

    template <FunctionTag Tag, typename... Args>
    auto send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        using namespace remote_invocation_utilities;

        constexpr std::integral_constant<FunctionTag, Tag>* choice{nullptr};
        auto& invoker = this->get_invoker(choice, args...);
        const auto header_size = header_space();
        auto sent_return = invoker.send(
                [&out_alloc, &header_size](std::size_t size) {
                    return out_alloc(size + header_size) + header_size;
                },
                std::forward<Args>(args)...);

        std::size_t payload_size = sent_return.size;
        char* buf = sent_return.buf - header_size;
        uint32_t flags = 0;
        if(in_rpc_handler()) {
            RPC_HEADER_FLAG_SET(flags, CASCADE);
            dbg_default_info("sending cascading RPC.");
        }
        populate_header(buf, payload_size, invoker.invoke_opcode, nid, flags);

        using Ret = typename decltype(sent_return.results)::type;
        /*
          much like previous definition, except with
          two fewer fields
        */
        struct send_return {
            QueryResults<Ret> results;
            PendingResults<Ret>& pending;
        };
        return send_return{std::move(sent_return.results),
                           sent_return.pending};
    }
};

/**
 * Specialization of RemoteInvokerForClass for classes that have no RPC methods
 */
template <class IdentifyingClass>
class RemoteInvokerForClass<IdentifyingClass> {
public:
    const node_id_t nid;

    RemoteInvokerForClass(node_id_t nid, uint32_t type_id, uint32_t instance_id,
                          std::map<Opcode, receive_fun_t>& rvrs)
            : nid(nid) {}
};

/**
 * Constructs a RemoteInvokerForClass that can act as a client for the class in
 * the template parameter (IdentifyingClass).
 * @param nid The Node ID of the node running this code
 * @param type_id A number uniquely identifying the type of IdentifyingClass;
 * in practice, the index of IdentifyingClass within the template parameters
 * of the Group that contains that replicated object.
 * @param instance_id A number identifying the servers that should be contacted
 * when calling RPC methods on this class (since more than one instance of the
 * template-parameter class could be running as a RemoteInvocableClass); in
 * practice this is the subgroup ID of the subgroup to contact.
 * @param rvrs A map from RPC opcodes to RPC message handler functions, into
 * which new handlers will be added for this RemoteInvokerForClass
 * @return A unique_ptr to a RemoteInvokerForClass of type IdentifyingClass
 */
template <class IdentifyingClass, typename... WrappedFuns>
auto build_remote_invoker_for_class(const node_id_t nid, const uint32_t type_id, const uint32_t instance_id,
                                    std::map<Opcode, receive_fun_t>& rvrs) {
    return std::make_unique<RemoteInvokerForClass<IdentifyingClass, WrappedFuns...>>(nid, type_id, instance_id, rvrs);
}
}  // namespace rpc
}  // namespace derecho
