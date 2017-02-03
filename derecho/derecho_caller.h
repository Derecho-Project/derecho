#pragma once

#include "mutils-serialization/SerializationSupport.hpp"
#include "mutils/FunctionalMap.hpp"
#include "mutils/tuple_extras.hpp"
#include "tcp/tcp.h"

#include <chrono>
#include <future>
#include <numeric>
#include <queue>
#include <set>

namespace rpc {

template <typename t>
auto& operator<<(std::ostream& out, const std::vector<t>& v) {
    out << "{";
    for(const auto& e : v) {
        out << e << ", ";
    }
    out << "}";
    return out;
}

struct Opcode {
    using t = unsigned long long;
    t id;
    Opcode(const decltype(id)& id) : id(id) {}
    Opcode() = default;
    bool operator==(const Opcode& n) const { return id == n.id; }
    bool operator<(const Opcode& n) const { return id < n.id; }
};
auto& operator<<(std::ostream& out, const Opcode& op) { return out << op.id; }
using FunctionTag = unsigned long long;
struct Node_id {
    unsigned long long id;
    Node_id(const decltype(id)& id) : id(id) {}
    Node_id() = default;
    bool operator==(const Node_id& n) const { return id == n.id; }
    bool operator<(const Node_id& n) const { return id < n.id; }
};
auto& operator<<(std::ostream& out, const Node_id& nid) {
    return out << nid.id;
}

using node_list_t = std::vector<Node_id>;

template <FunctionTag, typename>
struct RemoteInvocable;

struct remote_exception_occurred : public std::exception {
    Node_id who;
    remote_exception_occurred(Node_id who) : who(who) {}
    virtual const char* what() const noexcept override {
        std::ostringstream o_stream;
        o_stream << "An exception occured at node with id " << who.id;
        std::string str = o_stream.str();
        return str.c_str();
    }
};

struct node_removed_from_group_exception : public std::exception {
    Node_id who;
    node_removed_from_group_exception(Node_id who) : who(who) {}
    virtual const char* what() const noexcept override {
        std::ostringstream o_stream;
        o_stream << "Node with id " << who.id
                 << " has been removed from the group";
        std::string str = o_stream.str();
        return str.c_str();
    }
};

struct recv_ret {
    Opcode opcode;
    std::size_t size;
    char* payload;
    std::exception_ptr possible_exception;
};

using receive_fun_t = std::function<recv_ret(
    mutils::DeserializationManager* dsm, const Node_id&, const char* recv_buf,
    const std::function<char*(int)>& out_alloc)>;

template <typename T>
using reply_map = std::map<Node_id, std::future<T>>;

template <typename T>
struct QueryResults {
    using map_fut = std::future<std::unique_ptr<reply_map<T>>>;
    using map = reply_map<T>;
    using type = T;

    map_fut pending_rmap;
    QueryResults(map_fut pm) : pending_rmap(std::move(pm)) {}
    struct ReplyMap {
    private:
        QueryResults& parent;

    public:
        map rmap;

        ReplyMap(QueryResults& qr) : parent(qr){};
        ReplyMap(const ReplyMap&) = delete;
        ReplyMap(ReplyMap&& rm) : parent(rm.parent), rmap(std::move(rm.rmap)) {}

        bool valid(const Node_id& nid) {
            assert(rmap.size() == 0 || rmap.count(nid));
            return (rmap.size() > 0) && rmap.at(nid).valid();
        }

        /*
          returns true if we sent to this node,
          regardless of whether this node has replied.
        */
        bool contains(const Node_id& nid) { return rmap.count(nid); }

        auto begin() { return std::begin(rmap); }

        auto end() { return std::end(rmap); }

        auto get(const Node_id& nid) {
            if(rmap.size() == 0) {
                assert(parent.pending_rmap.valid());
                rmap = std::move(*parent.pending_rmap.get());
            }
            assert(rmap.size() > 0);
            assert(rmap.count(nid));
            assert(rmap.at(nid).valid());
            return rmap.at(nid).get();
        }
    };

private:
    ReplyMap replies{*this};

public:
    QueryResults(QueryResults&& o)
            : pending_rmap{std::move(o.pending_rmap)},
              replies{std::move(o.replies)} {}
    QueryResults(const QueryResults&) = delete;

    /*
      Wait the specified duration; if a ReplyMap is available
      after that duration, return it. Otherwise return nullptr.
    */
    template <typename Time>
    ReplyMap* wait(Time t) {
        if(replies.rmap.size() == 0) {
            if(pending_rmap.wait_for(t) == std::future_status::ready) {
                replies.rmap = std::move(*pending_rmap.get());
                return &replies;
            } else
                return nullptr;
        } else
            return &replies;
    }

    /*
      block until the map is fulfilled; then return the map.
    */
    ReplyMap& get() {
        using namespace std::chrono;
        while(true) {
            if(auto rmap = wait(5min)) {
                return *rmap;
            }
        }
    }
};

template <>
struct QueryResults<void> {
    using type = void;
    /* This currently has no functionality; Ken suggested a "flush," which
       we might want to have in both this and the non-void variant.
    */
};

/**
 * Data structure for storing results of a single RPC function call; holds one
 * response (either a value or an exception) for each node that was called.
 */
template <typename T>
struct PendingResults {
    std::promise<std::unique_ptr<reply_map<T>>> pending_map;
    std::map<Node_id, std::promise<T>> populated_promises;

    bool map_fulfilled = false;
    std::set<Node_id> dest_nodes, responded_nodes;

    /**
     * Fill the result map with an entry for each node that will be contacted
     * in this RPC call
     * @param who A list of nodes that will be contacted
     */
    void fulfill_map(const node_list_t& who) {
        map_fulfilled = true;
        std::unique_ptr<reply_map<T>> to_add = std::make_unique<reply_map<T>>();
        for(const auto& e : who) {
            to_add->emplace(e, populated_promises[e].get_future());
        }
        dest_nodes.insert(who.begin(), who.end());
        pending_map.set_value(std::move(to_add));
    }

    void set_exception_for_removed_node(const Node_id& removed_nid) {
        assert(map_fulfilled);
        if(dest_nodes.find(removed_nid) != dest_nodes.end() &&
           responded_nodes.find(removed_nid) == responded_nodes.end()) {
            set_exception(removed_nid,
                          std::make_exception_ptr(
                              node_removed_from_group_exception{removed_nid}));
        }
    }

    void set_value(const Node_id& nid, const T& v) {
        responded_nodes.insert(nid);
        return populated_promises[nid].set_value(v);
    }

    void set_exception(const Node_id& nid, const std::exception_ptr e) {
        responded_nodes.insert(nid);
        return populated_promises[nid].set_exception(e);
    }

    QueryResults<T> get_future() {
        return QueryResults<T>{pending_map.get_future()};
    }
};

template <>
struct PendingResults<void> {
    /* This currently has no functionality; Ken suggested a "flush," which
       we might want to have in both this and the non-void variant.
    */

    void fulfill_map(const node_list_t&) {}
    QueryResults<void> get_future() { return QueryResults<void>{}; }
};

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
     * Return type for the Send function. Contains the RPC-invoking message
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
    send_return Send(const std::function<char*(int)>& out_alloc,
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
        const Node_id& nid, const char* response,
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
                                     const Node_id& nid, const char* response,
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
                                     const Node_id& nid, const char* response,
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
                                 const Node_id&, const char* _recv_buf,
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
                                 const Node_id&, const char* _recv_buf,
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
                                 const Node_id& who, const char* recv_buf,
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
 * Utility functions for manipulating the headers of RPC messages
 */
namespace remote_invocation_utilities {

inline auto header_space() {
    return sizeof(std::size_t) + sizeof(Opcode) + sizeof(Node_id);
    //          size           operation           from
}

inline char* extra_alloc(int i) {
    const auto hs = header_space();
    return (char*)calloc(i + hs, sizeof(char)) + hs;
}

inline auto populate_header(char* reply_buf,
                            const std::size_t& payload_size,
                            const Opcode& op, const Node_id& from) {
    ((std::size_t*)reply_buf)[0] = payload_size;           // size
    ((Opcode*)(sizeof(std::size_t) + reply_buf))[0] = op;  // what
    ((Node_id*)(sizeof(std::size_t) + sizeof(Opcode) + reply_buf))[0] =
        from;  // from
}

inline auto retrieve_header(mutils::DeserializationManager* dsm,
                            char const* const reply_buf,
                            std::size_t& payload_size, Opcode& op,
                            Node_id& from) {
    payload_size = ((std::size_t const* const)reply_buf)[0];
    op = ((Opcode const* const)(sizeof(std::size_t) + reply_buf))[0];
    from = ((Node_id const* const)(sizeof(std::size_t) + sizeof(Opcode) +
                                   reply_buf))[0];
}
}

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
    const Node_id nid;

    // these are the functions (no names) from Fs
    // delegation so receivers exists during superclass construction
    RemoteInvocableClass(Node_id nid, std::map<Opcode, receive_fun_t>& rvrs, const Fs&... fs)
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
    auto Send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        using namespace remote_invocation_utilities;
        using namespace std::placeholders;

        constexpr std::integral_constant<FunctionTag, tag>* choice{nullptr};
        auto& hndl = this->handler(choice, args...);
        const auto header_size = header_space();
        auto sent_return = hndl.Send(
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
 * @param nid
 * @param rvrs
 * @param fs
 * @return
 */
template <class IdentifyingClass, typename... Fs>
auto build_remoteinvocableclass(const Node_id nid, std::map<Opcode, receive_fun_t>& rvrs, const Fs&... fs) {
    return std::make_unique<RemoteInvocableClass<IdentifyingClass, Fs...>>(nid, rvrs, fs...);
}

#include "contains_remote_invocable.hpp"

struct DefaultInvocationTarget {};

template <typename... T>
struct Dispatcher;

template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<T>().register_functions(std::declval<Dispatcher<>&>(),
                                                                                      std::declval<std::unique_ptr<T>*>()))>;

/**
 * Trivial specialization of an "empty" Dispatcher
 */
template <>
struct Dispatcher<> {
private:
    const Node_id nid;
    std::unique_ptr<std::map<Opcode, receive_fun_t>> receivers;

public:
    Dispatcher(Node_id nid)
            : nid(nid),
              receivers(new std::decay_t<decltype(*receivers)>()) {}

    std::exception_ptr handle_receive(...) {
        return nullptr;
    }
    void receive_objects(tcp::socket&) {}
    void send_objects(tcp::socket&) {}
    template <class NewClass, typename... NewFuns>
    auto register_functions(std::unique_ptr<NewClass>* cls, NewFuns... f) {
        return build_remoteinvocableclass<NewClass>(nid, *receivers, wrap(cls, wrap(f))...);
    }
};

template <typename... T>
struct Dispatcher {
    using impl_t = ContainsRemoteInvocableClass<RemoteInvocableOf<T>...>;

private:
    /** The ID of the node this Dispatcher is running on. */
    const Node_id nid;
    // listen here
    // constructed *before* initialization
    /** A map from opcodes to RPC functions, either the "server" stubs that receive
     * remote calls to invoke functions, or the "client" stubs that receive responses
     * from the targets of an earlier remote call. */
    std::unique_ptr<std::map<Opcode, receive_fun_t>> receivers;
    /** One or more pointers to replicated objects that are being
     * managed by this Dispatcher. Each object corresponds to a type in the
     * type parameter pack for Dispatcher<T...>. */
    std::tuple<std::unique_ptr<std::unique_ptr<T>>...> objects;
    /** An emtpy DeserializationManager, in case we need it later. */
    mutils::DeserializationManager dsm{{}};
    /** A pointer to a bag of RemoteInvocableClass objects being managed by this
     * Dispatcher, which can be selected with for_class().*/
    std::unique_ptr<impl_t> impl;

public:
    /**
     * Handles an RPC message for any of the RemoteInvocableClass objects managed
     * by this Dispatcher, by forwarding it to the correct object and method. The
     * opcode also determines whether it goes to the receive_call or recieve_response
     * half of the RemoteInvocable function.
     * @param indx The function opcode for this RPC message, which should
     * correspond to either a "call" or "response" function of some RemoteInvocable
     * @param received_from The ID of the node that sent the message
     * @param buf The buffer containing the message
     * @param payload_size The size of the message in bytes
     * @param out_alloc A function that can allocate a buffer for the response
     * to this message.
     * @return A pointer to the exception caused by invoking this RPC function,
     * if the message was an RPC function call and the function threw an exception.
     */
    std::exception_ptr handle_receive(
        const Opcode& indx, const Node_id& received_from, char const* const buf,
        std::size_t payload_size, const std::function<char*(int)>& out_alloc) {
        using namespace std::placeholders;
        using namespace remote_invocation_utilities;
        assert(payload_size);
        auto reply_header_size = header_space();
        recv_ret reply_return = receivers->at(indx)(
            &dsm, received_from, buf,
            [&out_alloc, &reply_header_size](std::size_t size) {
                return out_alloc(size + reply_header_size) + reply_header_size;
            });
        auto* reply_buf = reply_return.payload;
        if(reply_buf) {
            reply_buf -= reply_header_size;
            const auto id = reply_return.opcode;
            const auto size = reply_return.size;
            populate_header(reply_buf, size, id, nid);
        }
        return reply_return.possible_exception;
    }

    /**
     * Entry point for handling RPC messages received for RemoteInvocableClass
     * objects managed by this Dispatcher. Parses the header of the message to
     * retrieve the opcode and message size before forwarding the call to
     * handle_receive().
     * @param buf The buffer containing the message
     * @param size The size of the buffer
     * @param out_alloc A function that can allocate a buffer for the response
     * to this message
     * @return A pointer to the exception caused by invoking this RPC function,
     * if the message was an RPC function call and the function threw an exception.
     */
    std::exception_ptr handle_receive(
        char* buf, std::size_t size,
        const std::function<char*(int)>& out_alloc) {
        using namespace remote_invocation_utilities;
        std::size_t payload_size = size;
        Opcode indx;
        Node_id received_from;
        retrieve_header(&dsm, buf, payload_size, indx, received_from);
        return handle_receive(indx, received_from, buf + header_space(),
                              payload_size, out_alloc);
    }

    /* you *do not* need to delete the pointer in the pair this returns. */
    /**
     * Sends an RPC message to invoke the method with tag "tag" from class
     * ImplClass, calling with arguments "args."
     * @param out_alloc A function that can allocate buffers for the message
     * @param args The arguments to use when calling the function
     * @return A send_return object
     * @tparam ImplClass the class in which the method resides (should be
     * one of the classes used as template parameters to Dispatcher)
     * @tparam tag The FunctionTag for the method to invoke
     * @tparam Args A type pack for the arguments to the method
     */
    template <class ImplClass, FunctionTag tag, typename... Args>
    auto Send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        return impl->for_class((ImplClass*)nullptr).template Send<tag, Args...>(out_alloc, std::forward<Args>(args)...);
    }

    /** Wrapper for Send() that does not specify a function tag, and always
     * calls the first RPC function registered in class ImplClass. Used if
     * ImplClass only has one RPC function, I guess. */
    template <class ImplClass, typename... Args>
    auto Send(const std::function<char*(int)>& out_alloc, Args&&... args) {
        return Send<ImplClass, 0>(out_alloc, std::forward<Args>(args)...);
    }

private:
    /**
     * Calls the register_functions method (which must exist in the client class
     * for it to be supplied to Dispatcher) on all of the objects passed as arguments,
     * which has the effect of creating a RemoteInvocableClass instance for each
     * object.
     * @param cc Any number of pointers to objects
     * @return A pointer to a ContainsRemoteInvocableClass, which is a container
     * for all the RemoteInvocableObjects created by the calls to register_functions.
     */
    template <typename... ClientClasses>
    auto register_all(std::unique_ptr<ClientClasses>&... cc) {
        return std::make_unique<impl_t>(cc->register_functions(*this, &cc)...);
    }

    /** Base case for construct_objects with an empty template type pack. */
    template <typename>
    auto construct_objects(...) {
        return std::tuple<>{};
    }

    /**
     * Constructs a series of objects, one of each type in the template type
     * pack (after the TL type), using the corresponding tuple of arguments in
     * tuple_list as the constructor arguments. Even though the type of tuple_list
     * is a template "TL," it should always be mutils::TupleList.
     * @param tuple_list A lisp-style list of std::tuples, each of which contains
     * the constructor arguments for a single object. This list must be the same
     * length as the size of the template pack minus one (the first template type is
     * just TL, the TupleList type).
     * @return A tuple of objects, one for each type after TL in the template pack.
     */
    template <typename TL, typename FirstType, typename... EverythingElse>
    auto construct_objects(const TL& tuple_list) {
        auto internal_unique_ptr = mutils::make_unique_tupleargs<FirstType>(tuple_list.first);
        auto second_unique_ptr = std::make_unique<decltype(internal_unique_ptr)>(std::move(internal_unique_ptr));
        return std::tuple_cat(std::make_tuple(std::move(second_unique_ptr)),
                              construct_objects<typename TL::Rest, EverythingElse...>(tuple_list.rest));
    }

public:
    /**
     * Serializes and sends the state of each object managed by this Dispatcher
     * over the given socket.
     * @param receiver_socket
     */
    void send_objects(tcp::socket& receiver_socket) {
        auto total_size = mutils::fold(objects, [](const auto& obj, const auto& accumulated_state) {
            return accumulated_state + mutils::bytes_size(**obj);
        }, 0);
        auto bind_socket_write = [&receiver_socket](const char* bytes, std::size_t size) {receiver_socket.write(bytes, size); };
        mutils::post_object(bind_socket_write, total_size);
        mutils::fold(objects, [&](auto& obj, const auto& acc) {
            mutils::post_object(bind_socket_write,**obj);
            return acc;
        }, nullptr);
    }

    /**
     * Updates the state of all the objects managed by this Dispatcher by
     * deserializing a list of objects received over the given socket.
     * Blocks until the socket can read all the serialized objects sent by
     * another node's send_objects.
     * @param sender_socket
     */
    void receive_objects(tcp::socket& sender_socket) {
        size_t total_size;
        bool success = sender_socket.read((char*)&total_size, sizeof(size_t));
        assert(success);
        char* buf = new char[total_size];
        success = sender_socket.read(buf, total_size);
        assert(success);
        size_t offset = 0;
        mutils::fold(objects, [&](auto& obj, const size_t& offset) {
            using O = std::decay_t<decltype(**obj)>;
            *obj = mutils::from_bytes<O>(&dsm, buf + offset);
            std::cout << "Received obj" << std::endl;
            std::cout << "obj's state is: " << (*obj)->state << std::endl;
            return offset + mutils::bytes_size(**obj);
        }, offset);
    }
    //Dispatcher<Foo,Bar> b{id,std::make_tuple(/*Foo's ctr arguments*/),std::make_tuple(/*Bar's ctr arguments*/)}
    /**
     * Constructs a Dispatcher that will create and manage RemoteInvocableClass
     * versions of each of the classes supplied as template parameters to Dispatcher.
     * Used like this:
     * Dispatcher<Foo,Bar> b{id, std::make_tuple([Foo's constructor arguments]),
     *                       std::make_tuple([Bar's constructor arguments])}
     * @param nid The ID of the node on which this Dispatcher resides
     * @param a A variable-length list of tuples, one for each typename in Dispatcher's
     * template list. Each tuple should be the constructor arguments for the corresponding
     * class in Dispatcher's template parameter pack.
     * @tparam CtrTuples the types of the tuples
     */
    template <typename... CtrTuples>
    Dispatcher(Node_id nid, CtrTuples... a)
            : nid(nid),
              receivers(new std::decay_t<decltype(*receivers)>()),
              objects(construct_objects<mutils::TupleList<CtrTuples...>, T...>(mutils::TupleList<CtrTuples...>{a...})),
              impl(mutils::callFunc([&](auto&... obj) {return this->register_all(*obj...); },
                                    objects)) {}

    /** Move constructor for Dispatcher. */
    Dispatcher(Dispatcher&& other)
            : nid(other.nid),
              receivers(std::move(other.receivers)),
              objects(std::move(other.objects)),
              impl(std::move(other.impl)) {}

    /**
     * Given a pointer to an object and a list of its methods, constructs a
     * RemoteInvocableClass for that object that can be registered with this
     * Dispatcher.
     * @param cls A raw pointer(??) to a pointer to the object being set up as
     * a RemoteInvocableClass
     * @param f A variable-length list of pointer-to-member-functions, one for
     * each method of NewClass that should be an RPC function
     * @return The RemoteInvocableClass that wraps NewClass, by pointer
     * @tparam NewClass The type of the object being wrapped with a
     * RemoteInvocableClass
     * @tparam NewFuns The types of the member function pointers
     */
    template <class NewClass, typename... NewFuns>
    auto setup_rpc_class(std::unique_ptr<NewClass>* cls, NewFuns... f) {
        //NewFuns must be of type Ret (NewClass::*) (Args...)
        //or of type wrapped<opcode,Ret,Args...>
        return build_remoteinvocableclass<NewClass>(nid, *receivers, wrap(cls, wrap(f))...);
    }
};
}

using namespace rpc;
