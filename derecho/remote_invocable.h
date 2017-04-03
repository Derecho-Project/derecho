/**
 * @file remote_invocable.h
 *
 * @date Feb 13, 2017
 * @author edward
 */

#pragma once

#include <functional>
#include <numeric>

#include "mutils-serialization/SerializationSupport.hpp"
#include "mutils/FunctionalMap.hpp"
#include "mutils/tuple_extras.hpp"

#include "rpc_utils.h"

namespace derecho {

namespace rpc {

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

    //Maps invocation-instance IDs to results sets
    std::map<std::size_t, PendingResults<Ret>> results_map;
    std::mutex map_lock;
    using lock_t = std::unique_lock<std::mutex>;

    /* use this from within a derived class to retrieve precisely this RemoteInvoker
     * (this way, all the inherited RemoteInvoker methods in the subclass do not need
     * to worry about type collisions)*/
    inline RemoteInvoker& get_invoker(
            std::integral_constant<FunctionTag, Tag> const* const,
            const Args&...) {
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
     * invocable function managed by this RemoteInvocable.
     * @param out_alloc A function that can allocate buffers, which will be
     * used to store the constructed message
     * @param a The arguments to be used when calling the remote-invocable function
     */
    send_return send(const std::function<char*(int)>& out_alloc,
                     const std::decay_t<Args>&... a) {
        auto invocation_id = mutils::long_rand();
        std::size_t size = mutils::bytes_size(invocation_id);
        {
            auto t = {std::size_t{0}, std::size_t{0}, mutils::bytes_size(a)...};
            size += std::accumulate(t.begin(), t.end(), 0);
        }
        char* serialized_args = out_alloc(size);
        {
            auto v = serialized_args + mutils::to_bytes(invocation_id, serialized_args);
            auto check_size = mutils::bytes_size(invocation_id) + serialize_all(v, a...);
            assert(check_size == size);
        }

        lock_t l{map_lock};
        // default-initialize the maps
        PendingResults<Ret>& pending_results = results_map[invocation_id];

        return send_return{size, serialized_args, pending_results.get_future(),
                           pending_results};
    }

    /**
     * Specialization of receive_response for non-void functions. Stores the
     * response in the results map, or stores the exception if there was an
     * exception.
     * @return An empty recv_ret, since there is no response to a response
     */
    template <typename definitely_char>
    inline recv_ret receive_response(
            std::false_type*, mutils::DeserializationManager* dsm,
            const node_id_t& nid, const char* response,
            const std::function<definitely_char*(int)>&) {
        bool is_exception = response[0];
        long int invocation_id = ((long int*)(response + 1))[0];
        assert(results_map.count(invocation_id));
        lock_t l{map_lock};
        // TODO: garbage collection for the responses.
        if(is_exception) {
            results_map.at(invocation_id).set_exception(nid, std::make_exception_ptr(remote_exception_occurred{nid}));
        } else {
            results_map.at(invocation_id).set_value(nid, *mutils::from_bytes<Ret>(dsm, response + 1 + sizeof(invocation_id)));
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
        assert(false && "was not expecting a response!");
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
    inline recv_ret receive_response(mutils::DeserializationManager* dsm,
                                     const node_id_t& nid, const char* response,
                                     const std::function<char*(int)>& f) {
        constexpr std::is_same<void, Ret>* choice{nullptr};
        return receive_response(choice, dsm, nid, response, f);
    }

    /**
     * Populates the pending-results map of a particular invocation of the remote-invocable
     * function, given the list of nodes that will be contacted to call the function.
     * @param invocation_id The ID referring to a particular invocation of the function
     * @param who The list of nodes that will service this RPC call
     */
    inline void fulfill_pending_results_map(long int invocation_id, const node_list_t& who) {
        results_map.at(invocation_id).fulfill_map(who);
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
    RemoteInvoker(const std::type_index& class_id, uint32_t instance_id,
                  std::map<Opcode, receive_fun_t>& receivers)
            : invoke_opcode{class_id, instance_id, Tag, false},
              reply_opcode{class_id, instance_id, Tag, true} {
        receivers[reply_opcode] = [this](auto... a) {
            return this->receive_response(a...);
        };
    }
};
// Everything above is used only by the send()/receive_response() side
// -------------------------------------------------------------------
// Everything below is used only by the receive_call() side

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

    std::tuple<> _deserialize(mutils::DeserializationManager*,
                              char const* const) {
        return std::tuple<>{};
    }

    template <typename fst, typename... rst>
    std::tuple<std::unique_ptr<fst>, std::unique_ptr<rst>...> _deserialize(
            mutils::DeserializationManager* dsm, char const* const buf, fst*,
            rst*... rest) {
        using Type = std::decay_t<fst>;
        auto ds = mutils::from_bytes<Type>(dsm, buf);
        const auto size = mutils::bytes_size(*ds);
        return std::tuple_cat(std::make_tuple(std::move(ds)),
                              _deserialize(dsm, buf + size, rest...));
    }

    /**
     * Deserializes a buffer containing a list of arguments into a tuple
     * containing the arguments, deserialized.
     * @param dsm
     * @param buf The buffer containing serialized objects
     * @return A tuple of deserialized objects
     */
    std::tuple<std::unique_ptr<std::decay_t<Args>>...> deserialize(
            mutils::DeserializationManager* dsm, char const* const buf) {
        return _deserialize(dsm, buf, ((std::decay_t<Args>*)(nullptr))...);
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
                                 const node_id_t&, const char* _recv_buf,
                                 const std::function<char*(int)>& out_alloc) {
        long int invocation_id = ((long int*)_recv_buf)[0];
        auto recv_buf = _recv_buf + sizeof(long int);
        try {
            const auto result = mutils::callFunc([&](const auto&... args) { return remote_invocable_function(*args...); },
                                                 deserialize(dsm, recv_buf));
            // const auto result = remote_invocable_function(*deserialize<Args>(dsm, recv_buf)...);
            const auto result_size = mutils::bytes_size(result) + sizeof(long int) + 1;
            auto out = out_alloc(result_size);
            out[0] = false;
            ((long int*)(out + 1))[0] = invocation_id;
            mutils::to_bytes(result, out + sizeof(invocation_id) + 1);
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
        mutils::callFunc([&](const auto&... args) { remote_invocable_function(*args...); },
                         deserialize(dsm, recv_buf));
        // remote_invocable_function(*deserialize<Args>(dsm, recv_buf)...);
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
    inline recv_ret receive_call(mutils::DeserializationManager* dsm,
                                 const node_id_t& who, const char* recv_buf,
                                 const std::function<char*(int)>& out_alloc) {
        constexpr std::is_same<Ret, void>* choice{nullptr};
        return this->receive_call(choice, dsm, who, recv_buf, out_alloc);
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
    RemoteInvocable(const std::type_index& class_id, uint32_t instance_id,
                    std::map<Opcode, receive_fun_t>& receivers,
                    std::function<Ret(Args...)> f)
            : remote_invocable_function(f),
              invoke_opcode{class_id, instance_id, Tag, false},
              reply_opcode{class_id, instance_id, Tag, true} {
        receivers[invoke_opcode] = [this](auto... a) {
            return this->receive_call(a...);
        };
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
                    [_this, fun = partial.fun](Args... a){return ((_this->get())->*fun)(a...);
}
};
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
    return partial_wrapped<Tag, Ret, NewClass, Args...>{fun};
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
    RemoteInvocablePairs(const std::type_index& class_id,
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
    RemoteInvocablePairs(const std::type_index& class_id,
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
    RemoteInvokers(const std::type_index& class_id,
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
    RemoteInvokers(const std::type_index& class_id,
                   uint32_t instance_id,
                   std::map<Opcode, receive_fun_t>& receivers)
            : RemoteInvoker<Tag, FunType>(class_id, instance_id, receivers),
              RemoteInvokers<RestWrapped...>(class_id, instance_id, receivers) {}

    using RemoteInvoker<Tag, FunType>::get_invoker;
    using RemoteInvokers<RestWrapped...>::get_invoker;
};

/**
 * Wraps a class to make it a "replicated object" with methods that
 * can be invoked by RPC. Each RPC-invokable method must be supplied as a
 * template parameter, in order to associate it with a compile-time constant
 * name (the tag).
 * @tparam IdentifyingClass The class to make into an RPC-invokable class
 * @tparam Fs A list of "wrapped" function pointers to members of that class, each associated with a name.
 */
template <class IdentifyingClass, typename... WrappedFuns>
class RemoteInvocableClass : private RemoteInvocablePairs<WrappedFuns...> {
public:
    const node_id_t nid;

    RemoteInvocableClass(node_id_t nid, uint32_t instance_id,
                         std::map<Opcode, receive_fun_t>& rvrs, const WrappedFuns&... fs)
            : RemoteInvocablePairs<WrappedFuns...>(std::type_index(typeid(IdentifyingClass)), instance_id, rvrs, fs.fun...),
              nid(nid) {}

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
        const auto header_size = header_space();
        auto sent_return = invoker.send(
                [&out_alloc, &header_size](std::size_t size) {
                    return out_alloc(size + header_size) + header_size;
                },
                std::forward<Args>(args)...);

        std::size_t payload_size = sent_return.size;
        char* buf = sent_return.buf - header_size;
        populate_header(buf, payload_size, invoker.invoke_opcode, nid);

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

    using specialized_to = IdentifyingClass;
    RemoteInvocableClass& for_class(IdentifyingClass*) {
        return *this;
    }
};

/**
 * Constructs a RemoteInvocableClass instance that proxies for an instance of
 * the class in the template parameter.
 * @param nid The Node ID of the node running this code
 * @param instance_id A number uniquely identifying this instance of the
 * template-parameter class; in practice, the ID of the subgroup that will be
 * replicating this object.
 * @param rvrs A map from RPC opcodes to RPC message handler functions
 * @param fs A list of "wrapped" function pointers to members of the wrapped
 * class, each associated with a name, which should become RPC functions
 * @return A unique_ptr to a RemoteInvocableClass of type IdentifyingClass.
 */
template <class IdentifyingClass, typename... WrappedFuns>
auto build_remoteinvocableclass(const node_id_t nid, const uint32_t instance_id,
                                std::map<Opcode, receive_fun_t>& rvrs,
                                const WrappedFuns&... fs) {
    return std::make_unique<RemoteInvocableClass<IdentifyingClass, WrappedFuns...>>(nid, instance_id, rvrs, fs...);
}

template <class IdentifyingClass, typename... WrappedFuns>
class RemoteInvokerForClass : private RemoteInvokers<WrappedFuns...> {
public:
    const node_id_t nid;

    RemoteInvokerForClass(node_id_t nid, uint32_t instance_id,
                          std::map<Opcode, receive_fun_t>& rvrs)
            : RemoteInvokers<WrappedFuns...>(std::type_index(typeid(IdentifyingClass)), instance_id, rvrs),
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
        populate_header(buf, payload_size, invoker.invoke_opcode, nid);

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

template <class IdentifyingClass, typename... WrappedFuns>
auto build_remote_invoker_for_class(const node_id_t nid, const uint32_t instance_id,
                                    std::map<Opcode, receive_fun_t>& rvrs) {
    return std::make_unique<RemoteInvokerForClass<IdentifyingClass, WrappedFuns...>>(nid, instance_id, rvrs);
}
}
}
