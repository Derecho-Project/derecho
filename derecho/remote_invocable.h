/**
 * @file remote_invocable.h
 *
 * @date Feb 13, 2017
 * @author edward
 */

#pragma once

#include <functional>
#include <numeric>

#include "mutils/FunctionalMap.hpp"
#include "mutils/tuple_extras.hpp"
#include "mutils-serialization/SerializationSupport.hpp"

#include "rpc_utils.h"

namespace derecho {

namespace rpc {

//Technically, RemoteInvocable "specializes" this template for the case where
//the second parameter is a std::function<Ret(Args...)>. However, there is no
//implementation for any other specialization, so this template is meaningless.
template <FunctionTag, typename>
struct RemoteInvocable;

/**
 * Provides functions to implement RPC sends and receives for function
 * calls and responses to a single function, identified by its compile-time
 * "tag" or opcode.
 * Many versions of this class will be extended by a single Handlers context.
 * each specific instance of this class provides a mechanism for communicating
 * with remote sites that handles one particular function.
 * @tparam tag The compile-time value associated with this function
 * @tparam Ret The function's return type
 * @tparam Args The function's argument types
 */
template <FunctionTag tag, typename Ret, typename... Args>
struct RemoteInvocable<tag, std::function<Ret(Args...)>> {
    using remote_function_type = std::function<Ret(Args...)>;
    const remote_function_type remote_invocable_function;
    static const Opcode invoke_id;
    static const Opcode reply_id;

    //Maps invocation-instance IDs to results sets
    std::map<std::size_t, PendingResults<Ret>> results_map;
    std::mutex map_lock;
    using lock_t = std::unique_lock<std::mutex>;

    /* use this from within a derived class to receive precisely this RemoteInvocable
     * (this way, all RemoteInvocable methods do not need to worry about type collisions) */
    inline RemoteInvocable& handler(
        std::integral_constant<FunctionTag, tag> const* const,
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
            auto v = serialized_args +
                     mutils::to_bytes(invocation_id, serialized_args);
            auto check_size =
                mutils::bytes_size(invocation_id) + serialize_all(v, a...);
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
        return recv_ret{0, 0, nullptr, nullptr};
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
     * @return
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

    // Everything above is used only by the send()/receive_response() side
    // -------------------------------------------------------------------
    // Everything below is used only by the receive_call() side

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
            const auto result = mutils::callFunc([&](const auto&... a) { return remote_invocable_function(*a...); },
                                                 deserialize(dsm, recv_buf));
            // const auto result = remote_invocable_function(*deserialize<Args>(dsm, recv_buf)...);
            const auto result_size = mutils::bytes_size(result) + sizeof(long int) + 1;
            auto out = out_alloc(result_size);
            out[0] = false;
            ((long int*)(out + 1))[0] = invocation_id;
            mutils::to_bytes(result, out + sizeof(invocation_id) + 1);
            return recv_ret{reply_id, result_size, out, nullptr};
        } catch(...) {
            char* out = out_alloc(sizeof(long int) + 1);
            out[0] = true;
            ((long int*)(out + 1))[0] = invocation_id;
            return recv_ret{reply_id, sizeof(long int) + 1, out,
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
        mutils::callFunc([&](const auto&... a) { remote_invocable_function(*a...); },
                         deserialize(dsm, recv_buf));
        // remote_invocable_function(*deserialize<Args>(dsm, recv_buf)...);
        return recv_ret{reply_id, 0, nullptr};
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

    RemoteInvocable(std::map<Opcode, receive_fun_t>& receivers,
                    std::function<Ret(Args...)> f)
            : remote_invocable_function(f) {
        receivers[invoke_id] = [this](auto... a) {
            return this->receive_call(a...);
        };
        receivers[reply_id] = [this](auto... a) {
            return this->receive_response(a...);
        };
    }
};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, std::function<Ret(Args...)>>::invoke_id{mutils::gensym()};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, std::function<Ret(Args...)>>::reply_id{mutils::gensym()};

/** This matches uses of wrapped<> where the second argument is not a function,
 * and does nothing. */
template <FunctionTag Opcode, typename Fun>
struct wrapped;

/**
 * Template that pairs a FunctionTag with a function pointer, thus
 * "naming" the function with a tag (opcode) that is a compile-time value.
 */
template <FunctionTag Opcode, typename Ret, typename... Arguments>
struct wrapped<Opcode, std::function<Ret(Arguments...)>> {
    using fun_t = std::function<Ret(Arguments...)>;
    fun_t fun;
};

template <FunctionTag Opcode, typename Ret, typename Class, typename... Arguments>
struct partial_wrapped {
    using fun_t = Ret (Class::*)(Arguments...);
    fun_t fun;
};

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(std::unique_ptr<NewClass>*, const wrapped<opcode, std::function<Ret(Args...)>>& passthrough) {
    return passthrough;
}

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(const partial_wrapped<opcode, Ret, NewClass, Args...>& partial) {
    return partial;
}

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(std::unique_ptr<NewClass>* _this, const partial_wrapped<opcode, Ret, NewClass, Args...>& partial) {
    assert(_this);
    assert(_this->get());
    return wrapped<opcode, std::function<Ret(Args...)>>{
        [ _this, fun = partial.fun ](Args... a){return ((_this->get())->*fun)(a...);
}
};
}

template <typename NewClass, typename Ret, typename... Args>
auto wrap(Ret (NewClass::*fun)(Args...)) {
    return partial_wrapped<0, Ret, NewClass, Args...>{fun};
}

template <FunctionTag Opcode, typename NewClass, typename Ret, typename... Args>
auto wrap(Ret (NewClass::*fun)(Args...)) {
    return partial_wrapped<Opcode, Ret, NewClass, Args...>{fun};
}  //*/

template <typename...>
struct RemoteInvocablePairs;

template <FunctionTag id, typename Q>
struct RemoteInvocablePairs<wrapped<id, Q>>
    : public RemoteInvocable<id, Q> {
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t>& receivers, Q q)
            : RemoteInvocable<id, Q>(receivers, q) {}

    using RemoteInvocable<id, Q>::handler;
};

// id better be an integral constant of Opcode
template <FunctionTag id, typename Q, typename... rest>
struct RemoteInvocablePairs<wrapped<id, Q>, rest...>
    : public RemoteInvocable<id, Q>, public RemoteInvocablePairs<rest...> {
    //^ could change this to extend RemoteCaller<id,Q> and RemoteResponder<id,Q> to split up RemoteInvocable
public:
    template <typename... T>
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t>& receivers, Q q,
                         T&&... t)
            : RemoteInvocable<id, Q>(receivers, q),
              RemoteInvocablePairs<rest...>(receivers, std::forward<T>(t)...) {}

    using RemoteInvocable<id, Q>::handler;
    using RemoteInvocablePairs<rest...>::handler;
};

/**
 * Wraps a class to make it a "replicated object" with methods that
 * can be invoked by RPC. Each RPC-invokable method must be supplied as a
 * template parameter, in order to associate it with a compile-time constant
 * name (the tag).
 * @tparam IdentifyingClass The class to make into an RPC-invokable class
 * @tparam Fs A list of "wrapped" function pointers to members of that class, each associated with a name.
 */
template <class IdentifyingClass, typename... Fs>
struct RemoteInvocableClass : private RemoteInvocablePairs<Fs...> {
    const node_id_t nid;

    // these are the functions (no names) from Fs
    // delegation so receivers exists during superclass construction
    RemoteInvocableClass(node_id_t nid, std::map<Opcode, receive_fun_t>& rvrs, const Fs&... fs)
            : RemoteInvocablePairs<Fs...>(rvrs, fs.fun...), nid(nid) {}

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
    template <FunctionTag tag, typename... Args>
    auto send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        using namespace remote_invocation_utilities;
        using namespace std::placeholders;

        constexpr std::integral_constant<FunctionTag, tag>* choice{nullptr};
        auto& hndl = this->handler(choice, args...);
        const auto header_size = header_space();
        auto sent_return = hndl.send(
            [&out_alloc, &header_size](std::size_t size) {
                return out_alloc(size + header_size) + header_size;
            },
            std::forward<Args>(args)...);

        std::size_t payload_size = sent_return.size;
        char* buf = sent_return.buf - header_size;
        populate_header(buf, payload_size, hndl.invoke_id, nid);

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
 * Constructs a RemoteInvocableClass that "wraps" the class in the template
 * parameter.
 * @param nid The Node ID of the node running this code
 * @param rvrs A map from RPC opcodes to RPC message handler functions
 * @param fs Pointers to the member functions of the wrapped class that should
 * be RPC functions.
 * @return A unique_ptr to a RemoteInvocableClass of type IdentifyingClass.
 */
template <class IdentifyingClass, typename... Fs>
auto build_remoteinvocableclass(const node_id_t nid, std::map<Opcode, receive_fun_t>& rvrs, const Fs&... fs) {
    return std::make_unique<RemoteInvocableClass<IdentifyingClass, Fs...>>(nid, rvrs, fs...);
}

class RPCManager;

template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<T>().register_functions(std::declval<RPCManager&>(),
                                                                                      std::declval<std::unique_ptr<T>*>()))>;
}
}
