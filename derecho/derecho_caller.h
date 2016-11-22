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
auto &operator<<(std::ostream &out, const std::vector<t> &v) {
    out << "{";
    for(const auto &e : v) {
        out << e << ", ";
    }
    out << "}";
    return out;
}

struct Opcode {
    using t = unsigned long long;
    t id;
    Opcode(const decltype(id) &id) : id(id) {}
    Opcode() = default;
    bool operator==(const Opcode &n) const { return id == n.id; }
    bool operator<(const Opcode &n) const { return id < n.id; }
};
auto &operator<<(std::ostream &out, const Opcode &op) { return out << op.id; }
using FunctionTag = unsigned long long;
struct Node_id {
    unsigned long long id;
    Node_id(const decltype(id) &id) : id(id) {}
    Node_id() = default;
    bool operator==(const Node_id &n) const { return id == n.id; }
    bool operator<(const Node_id &n) const { return id < n.id; }
};
auto &operator<<(std::ostream &out, const Node_id &nid) {
    return out << nid.id;
}

using who_t = std::vector<Node_id>;

template <FunctionTag, typename>
struct RemoteInvocable;

struct remote_exception_occurred : public std::exception {
    Node_id who;
    remote_exception_occurred(Node_id who) : who(who) {}
    virtual const char *what() const noexcept override {
        std::ostringstream o_stream;
        o_stream << "An exception occured at node with id " << who.id;
        std::string str = o_stream.str();
        return str.c_str();
    }
};

struct node_removed_from_group_exception : public std::exception {
    Node_id who;
    node_removed_from_group_exception(Node_id who) : who(who) {}
    virtual const char *what() const noexcept override {
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
    char *payload;
    std::exception_ptr possible_exception;
};

using receive_fun_t = std::function<recv_ret(
    mutils::DeserializationManager *dsm, const Node_id &, const char *recv_buf,
    const std::function<char *(int)> &out_alloc)>;

template <typename T>
using reply_map = std::map<Node_id, std::future<T> >;

template <typename T>
struct QueryResults {
    using map_fut = std::future<std::unique_ptr<reply_map<T> > >;
    using map = reply_map<T>;
    using type = T;

    map_fut pending_rmap;
    QueryResults(map_fut pm) : pending_rmap(std::move(pm)) {}
    struct ReplyMap {
    private:
        QueryResults &parent;

    public:
        map rmap;

        ReplyMap(QueryResults &qr) : parent(qr){};
        ReplyMap(const ReplyMap &) = delete;
        ReplyMap(ReplyMap &&rm) : parent(rm.parent), rmap(std::move(rm.rmap)) {}

        bool valid(const Node_id &nid) {
            assert(rmap.size() == 0 || rmap.count(nid));
            return (rmap.size() > 0) && rmap.at(nid).valid();
        }

        /*
          returns true if we sent to this node,
          regardless of whether this node has replied.
        */
        bool contains(const Node_id &nid) { return rmap.count(nid); }

        auto begin() { return std::begin(rmap); }

        auto end() { return std::end(rmap); }

        auto get(const Node_id &nid) {
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
    QueryResults(QueryResults &&o)
            : pending_rmap{std::move(o.pending_rmap)},
              replies{std::move(o.replies)} {}
    QueryResults(const QueryResults &) = delete;

    /*
      Wait the specified duration; if a ReplyMap is available
      after that duration, return it. Otherwise return nullptr.
    */
    template <typename Time>
    ReplyMap *wait(Time t) {
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
    ReplyMap &get() {
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

template <typename T>
struct PendingResults {
    std::promise<std::unique_ptr<reply_map<T> > > pending_map;
    std::map<Node_id, std::promise<T> > populated_promises;

    bool map_fulfilled = false;
    std::set<Node_id> dest_nodes, set_nodes;

    void fulfill_map(const who_t &who) {
        map_fulfilled = true;
        std::unique_ptr<reply_map<T> > to_add{new reply_map<T>{}};
        for(const auto &e : who) {
            to_add->emplace(e, populated_promises[e].get_future());
        }
        dest_nodes.insert(who.begin(), who.end());
        pending_map.set_value(std::move(to_add));
    }

    void set_exception_for_removed_node(const Node_id &removed_nid) {
        assert(map_fulfilled);
        if(dest_nodes.find(removed_nid) != dest_nodes.end() &&
           set_nodes.find(removed_nid) == set_nodes.end()) {
            set_exception(removed_nid,
                          std::make_exception_ptr(
                              node_removed_from_group_exception{removed_nid}));
        }
    }

    void set_value(const Node_id &nid, const T &v) {
        set_nodes.insert(nid);
        return populated_promises[nid].set_value(v);
    }

    void set_exception(const Node_id &nid, const std::exception_ptr e) {
        set_nodes.insert(nid);
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

    void fulfill_map(const who_t &) {}
    QueryResults<void> get_future() { return QueryResults<void>{}; }
};

// many versions of this class will be extended by a single Handlers context.
// each specific instance of this class provides a mechanism for communicating
// with
// remote sites.

template <FunctionTag tag, typename Ret, typename... Args>
struct RemoteInvocable<tag, std::function<Ret(Args...)> > {
    using f_t = std::function<Ret(Args...)>;
    const f_t f;
    static const Opcode invoke_id;
    static const Opcode reply_id;

    std::map<std::size_t, PendingResults<Ret> > ret;
    std::mutex ret_lock;
    using lock_t = std::unique_lock<std::mutex>;

    // use this from within a derived class to receive precisely this
    // RemoteInvocable
    //(this way, all RemoteInvocable methods do not need to worry about type
    // collisions)
    inline RemoteInvocable &handler(
        std::integral_constant<FunctionTag, tag> const *const,
        const Args &...) {
        return *this;
    }

    using barray = char *;
    using cbarray = const char *;

    inline auto serialize_one(barray) { return 0; }

    template <typename A, typename... Rest>
    inline auto serialize_one(barray v, const A &a, const Rest &... rest) {
        auto size = mutils::to_bytes(a, v);
        return size + serialize_one(v + size, rest...);
    }

    inline auto serialize_all(barray v, const Args &... args) {
        return serialize_one(v, args...);
    }

    struct send_return {
        std::size_t size;
        char *buf;
        QueryResults<Ret> results;
        PendingResults<Ret> &pending;
    };

    send_return Send(const std::function<char *(int)> &out_alloc,
                     const std::decay_t<Args> &... a) {
        auto invocation_id = mutils::long_rand();
        std::size_t size = mutils::bytes_size(invocation_id);
        {
            auto t = {std::size_t{0}, std::size_t{0}, mutils::bytes_size(a)...};
            size += std::accumulate(t.begin(), t.end(), 0);
        }
        char *serialized_args = out_alloc(size);
        {
            auto v = serialized_args +
                     mutils::to_bytes(invocation_id, serialized_args);
            auto check_size =
                mutils::bytes_size(invocation_id) + serialize_all(v, a...);
            assert(check_size == size);
        }

        lock_t l{ret_lock};
        // default-initialize the maps
        PendingResults<Ret> &pending_results = ret[invocation_id];

        return send_return{size, serialized_args, pending_results.get_future(),
                           pending_results};
    }

    template <typename definitely_char>
    inline recv_ret receive_response(
        std::false_type *, mutils::DeserializationManager *dsm,
        const Node_id &nid, const char *response,
        const std::function<definitely_char *(int)> &) {
        bool is_exception = response[0];
        long int invocation_id = ((long int *)(response + 1))[0];
        assert(ret.count(invocation_id));
        lock_t l{ret_lock};
        // TODO: garbage collection for the responses.
        if(is_exception) {
            ret.at(invocation_id)
                .set_exception(nid, std::make_exception_ptr(
                                        remote_exception_occurred{nid}));
        } else {
            ret.at(invocation_id)
                .set_value(nid, *mutils::from_bytes<Ret>(
                                    dsm, response + 1 + sizeof(invocation_id)));
        }
        return recv_ret{0, 0, nullptr, nullptr};
    }

    inline recv_ret receive_response(std::true_type *,
                                     mutils::DeserializationManager *,
                                     const Node_id &nid, const char *response,
                                     const std::function<char *(int)> &) {
        if(response[0]) throw remote_exception_occurred{nid};
        assert(false && "was not expecting a response!");
    }

    inline recv_ret receive_response(mutils::DeserializationManager *dsm,
                                     const Node_id &nid, const char *response,
                                     const std::function<char *(int)> &f) {
        constexpr std::is_same<void, Ret> *choice{nullptr};
        return receive_response(choice, dsm, nid, response, f);
    }

    inline void fulfill_pending_results_map(long int invocation_id, const who_t &who) {
        ret.at(invocation_id).fulfill_map(who);
    }

    std::tuple<> _deserialize(mutils::DeserializationManager *,
                              char const *const) {
        return std::tuple<>{};
    }

    template <typename fst, typename... rst>
    std::tuple<std::unique_ptr<fst>, std::unique_ptr<rst>...> _deserialize(
        mutils::DeserializationManager *dsm, char const *const buf, fst *,
        rst *... rest) {
        using Type = std::decay_t<fst>;
        auto ds = mutils::from_bytes<Type>(dsm, buf);
        const auto size = mutils::bytes_size(*ds);
        return std::tuple_cat(std::make_tuple(std::move(ds)),
                              _deserialize(dsm, buf + size, rest...));
    }

    std::tuple<std::unique_ptr<std::decay_t<Args> >...> deserialize(
        mutils::DeserializationManager *dsm, char const *const buf) {
        return _deserialize(dsm, buf, ((std::decay_t<Args> *)(nullptr))...);
    }

    inline recv_ret receive_call(std::false_type const *const,
                                 mutils::DeserializationManager *dsm,
                                 const Node_id &, const char *_recv_buf,
                                 const std::function<char *(int)> &out_alloc) {
        long int invocation_id = ((long int *)_recv_buf)[0];
        auto recv_buf = _recv_buf + sizeof(long int);
        try {
            const auto result =
                mutils::callFunc([&](const auto &... a) { return f(*a...); },
                                 deserialize(dsm, recv_buf));
            // const auto result = f(*deserialize<Args>(dsm, recv_buf)...);
            const auto result_size =
                mutils::bytes_size(result) + sizeof(long int) + 1;
            auto out = out_alloc(result_size);
            out[0] = false;
            ((long int *)(out + 1))[0] = invocation_id;
            mutils::to_bytes(result, out + sizeof(invocation_id) + 1);
            return recv_ret{reply_id, result_size, out, nullptr};
        } catch(...) {
            char *out = out_alloc(sizeof(long int) + 1);
            out[0] = true;
            ((long int *)(out + 1))[0] = invocation_id;
            return recv_ret{reply_id, sizeof(long int) + 1, out,
                            std::current_exception()};
        }
    }

    inline recv_ret receive_call(std::true_type const *const,
                                 mutils::DeserializationManager *dsm,
                                 const Node_id &, const char *_recv_buf,
                                 const std::function<char *(int)> &) {
        auto recv_buf = _recv_buf + sizeof(long int);
        mutils::callFunc([&](const auto &... a) { f(*a...); },
                         deserialize(dsm, recv_buf));
        // f(*deserialize<Args>(dsm, recv_buf)...);
        return recv_ret{reply_id, 0, nullptr};
    }

    inline recv_ret receive_call(mutils::DeserializationManager *dsm,
                                 const Node_id &who, const char *recv_buf,
                                 const std::function<char *(int)> &out_alloc) {
        constexpr std::is_same<Ret, void> *choice{nullptr};
        return this->receive_call(choice, dsm, who, recv_buf, out_alloc);
    }

    RemoteInvocable(std::map<Opcode, receive_fun_t> &receivers,
                    std::function<Ret(Args...)> f)
            : f(f) {
        receivers[invoke_id] = [this](auto... a) {
            return this->receive_call(a...);
        };
        receivers[reply_id] = [this](auto... a) {
            return this->receive_response(a...);
        };
    }
};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, std::function<Ret(Args...)> >::invoke_id{mutils::gensym()};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, std::function<Ret(Args...)> >::reply_id{mutils::gensym()};

template <FunctionTag Opcode, typename Fun>
struct wrapped;

template <FunctionTag Opcode, typename Ret, typename... Arguments>
struct wrapped<Opcode, std::function<Ret(Arguments...)> > {
    using fun_t = std::function<Ret(Arguments...)>;
    fun_t fun;
};

template <FunctionTag Opcode, typename Ret, typename Class, typename... Arguments>
struct partial_wrapped {
    using fun_t = Ret (Class::*)(Arguments...);
    fun_t fun;
};

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(std::unique_ptr<NewClass> *, const wrapped<opcode, std::function<Ret(Args...)> > &passthrough) {
    return passthrough;
}

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(const partial_wrapped<opcode, Ret, NewClass, Args...> &partial) {
    return partial;
}

template <typename NewClass, FunctionTag opcode, typename Ret, typename... Args>
auto wrap(std::unique_ptr<NewClass> *_this, const partial_wrapped<opcode, Ret, NewClass, Args...> &partial) {
    assert(_this);
    assert(_this->get());
    return wrapped<opcode, std::function<Ret(Args...)> >{
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
struct RemoteInvocablePairs<wrapped<id, Q> >
    : public RemoteInvocable<id, Q> {
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t> &receivers, Q q)
            : RemoteInvocable<id, Q>(receivers, q) {}

    using RemoteInvocable<id, Q>::handler;
};

// id better be an integral constant of Opcode
template <FunctionTag id, typename Q, typename... rest>
struct RemoteInvocablePairs<wrapped<id, Q>, rest...>
    : public RemoteInvocable<id, Q>, public RemoteInvocablePairs<rest...> {
public:
    template <typename... T>
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t> &receivers, Q q,
                         T &&... t)
            : RemoteInvocable<id, Q>(receivers, q),
              RemoteInvocablePairs<rest...>(receivers, std::forward<T>(t)...) {}

    using RemoteInvocable<id, Q>::handler;
    using RemoteInvocablePairs<rest...>::handler;
};

namespace remote_invocation_utilities {

inline auto header_space() {
    return sizeof(std::size_t) + sizeof(Opcode) + sizeof(Node_id);
    //          size           operation           from
}

inline char *extra_alloc(int i) {
    const auto hs = header_space();
    return (char *)calloc(i + hs, sizeof(char)) + hs;
}

inline auto populate_header(char *reply_buf,
                            const std::size_t &payload_size,
                            const Opcode &op, const Node_id &from) {
    ((std::size_t *)reply_buf)[0] = payload_size;           // size
    ((Opcode *)(sizeof(std::size_t) + reply_buf))[0] = op;  // what
    ((Node_id *)(sizeof(std::size_t) + sizeof(Opcode) + reply_buf))[0] =
        from;  // from
}

inline auto retrieve_header(mutils::DeserializationManager *dsm,
                            char const *const reply_buf,
                            std::size_t &payload_size, Opcode &op,
                            Node_id &from) {
    payload_size = ((std::size_t const *const)reply_buf)[0];
    op = ((Opcode const *const)(sizeof(std::size_t) + reply_buf))[0];
    from = ((Node_id const *const)(sizeof(std::size_t) + sizeof(Opcode) +
                                   reply_buf))[0];
}
}

template <class IdentifyingClass, typename... Fs>
struct RemoteInvocableClass : private RemoteInvocablePairs<Fs...> {
    const Node_id nid;

    // these are the functions (no names) from Fs
    // delegation so receivers exists during superclass construction
    RemoteInvocableClass(Node_id nid, std::map<Opcode, receive_fun_t> &rvrs, const Fs &... fs)
            : RemoteInvocablePairs<Fs...>(rvrs, fs.fun...), nid(nid) {}

    /* you *do not* need to delete the pointer in the pair this returns. */
    template <FunctionTag tag, typename... Args>
    auto Send(const std::function<char *(int)> &out_alloc, Args &&... args) {
        using namespace remote_invocation_utilities;
        using namespace std::placeholders;
        constexpr std::integral_constant<FunctionTag, tag> *choice{nullptr};
        auto &hndl = this->handler(choice, args...);
        const auto header_size = header_space();
        auto sent_return = hndl.Send(
            [&out_alloc, &header_size](std::size_t size) {
                return out_alloc(size + header_size) + header_size;
            },
            std::forward<Args>(args)...);
        std::size_t payload_size = sent_return.size;
        char *buf = sent_return.buf - header_size;
        populate_header(buf, payload_size, hndl.invoke_id, nid);
        using Ret = typename decltype(sent_return.results)::type;

        /*
		  much like previous definition, except with
		  two fewer fields
		*/
        struct send_return {
            QueryResults<Ret> results;
            PendingResults<Ret> &pending;
        };
        return send_return{std::move(sent_return.results),
                           sent_return.pending};
    }

    using specialized_to = IdentifyingClass;
    RemoteInvocableClass &for_class(IdentifyingClass *) {
        return *this;
    }
};

template <class IdentifyingClass, typename... Fs>
auto build_remoteinvocableclass(const Node_id nid, std::map<Opcode, receive_fun_t> &rvrs, const Fs &... fs) {
    return std::make_unique<RemoteInvocableClass<IdentifyingClass, Fs...> >(nid, rvrs, fs...);
}

#include "contains_remote_invocable.hpp"

struct DefaultInvocationTarget {};

template <typename... T>
struct Dispatcher;

template <typename T>
using RemoteInvocableOf = std::decay_t<decltype(*std::declval<T>().register_functions(std::declval<Dispatcher<> &>(),
        std::declval<std::unique_ptr<T> *>()))>;

template<>
struct Dispatcher<>{
private:
    const Node_id nid;
    // listen here
    // constructed *before* initialization
    std::unique_ptr<std::map<Opcode, receive_fun_t> > receivers;
public:
    Dispatcher(Node_id nid)
            : nid(nid),
              receivers(new std::decay_t<decltype(*receivers)>()) {}

    std::exception_ptr handle_receive(...){
        return nullptr;
    }
    void receive_objects(tcp::socket &){}
    void send_objects(tcp::socket &){}
    template <class NewClass, typename... NewFuns>
    auto register_functions(std::unique_ptr<NewClass> *cls, NewFuns... f) {
        return build_remoteinvocableclass<NewClass>(nid, *receivers, wrap(cls, wrap(f))...);
    }
};

template <typename... T>
struct Dispatcher {
    using impl_t = ContainsRemoteInvocableClass<RemoteInvocableOf<T>...>;

private:
    const Node_id nid;
    // listen here
    // constructed *before* initialization
    std::unique_ptr<std::map<Opcode, receive_fun_t> > receivers;
    // constructed *after* initialization
    std::unique_ptr<std::thread> receiver;
    std::tuple<std::unique_ptr<std::unique_ptr<T> >...> objects;
    mutils::DeserializationManager dsm{{}};
    std::unique_ptr<impl_t> impl;

public:
    std::exception_ptr handle_receive(
        const Opcode &indx, const Node_id &received_from, char const *const buf,
        std::size_t payload_size, const std::function<char *(int)> &out_alloc) {
        using namespace std::placeholders;
        using namespace remote_invocation_utilities;
        assert(payload_size);
        auto reply_header_size = header_space();
        auto reply_return = receivers->at(indx)(
            &dsm, received_from, buf,
            [&out_alloc, &reply_header_size](std::size_t size) {
                return out_alloc(size + reply_header_size) + reply_header_size;
            });
        auto *reply_buf = reply_return.payload;
        if(reply_buf) {
            reply_buf -= reply_header_size;
            const auto id = reply_return.opcode;
            const auto size = reply_return.size;
            populate_header(reply_buf, size, id, nid);
        }
        return reply_return.possible_exception;
    }

    std::exception_ptr handle_receive(
        char *buf, std::size_t size,
        const std::function<char *(int)> &out_alloc) {
        using namespace remote_invocation_utilities;
        std::size_t payload_size = size;
        Opcode indx;
        Node_id received_from;
        retrieve_header(&dsm, buf, payload_size, indx, received_from);
        return handle_receive(indx, received_from, buf + header_space(),
                              payload_size, out_alloc);
    }

    /* you *do not* need to delete the pointer in the pair this returns. */
    template <class ImplClass, FunctionTag tag, typename... Args>
    auto Send(const std::function<char *(int)> &out_alloc, Args &&... args) {
        return impl->for_class((ImplClass *)nullptr).template Send<tag, Args...>(out_alloc, std::forward<Args>(args)...);
    }

    template <class ImplClass, typename... Args>
    auto Send(const std::function<char *(int)> &out_alloc, Args &&... args) {
        return Send<ImplClass, 0>(out_alloc, std::forward<Args>(args)...);
    }

private:
    template <typename... ClientClasses>
    auto register_all(std::unique_ptr<ClientClasses> &... cc) {
        return std::make_unique<impl_t>(cc->register_functions(*this, &cc)...);
    }

    template <typename>
    auto construct_objects(...) {
        return std::tuple<>{};
    }

    template <typename TL, typename FirstType, typename... EverythingElse>
    auto construct_objects(const TL &tl) {
        auto internal_unique_ptr = mutils::make_unique_tupleargs<FirstType>(tl.first);
        auto second_unique_ptr = std::make_unique<decltype(internal_unique_ptr)>(std::move(internal_unique_ptr));
        return std::tuple_cat(std::make_tuple(std::move(second_unique_ptr)),
                              construct_objects<typename TL::Rest, EverythingElse...>(tl.rest));
    }

public:
    void send_objects(tcp::socket &receiver_socket) {
        auto total_size = mutils::fold(objects, [](const auto &obj, const auto &accumulated_state) {
	  return accumulated_state + mutils::bytes_size(**obj);
        }, 0);
        auto bind_socket_write = [&receiver_socket](const char *bytes, std::size_t size) {receiver_socket.write(bytes, size); };
        mutils::post_object(bind_socket_write, total_size);
        mutils::fold(objects, [&](auto &obj, const auto &acc) {
	    mutils::post_object(bind_socket_write,**obj);
	    return acc;
        }, nullptr);
    }

    void receive_objects(tcp::socket &sender_socket) {
        size_t total_size;
        bool success = sender_socket.read((char *)&total_size, sizeof(size_t));
        assert(success);
        char *buf = new char[total_size];
        success = sender_socket.read(buf, total_size);
        assert(success);
        size_t offset = 0;
        mutils::fold(objects, [&](auto &obj, const size_t &offset) {
	  using O = std::decay_t<decltype(**obj)>;
	  *obj = mutils::from_bytes<O>(&dsm, buf + offset);
	  std::cout << "Received obj" << std::endl;
	  std::cout << "obj's state is: " << (*obj)->state << std::endl;
	  return offset + mutils::bytes_size(**obj);
        }, offset);
    }
//Dispatcher<Foo,Bar> b{id,std::make_tuple(/*Foo's ctr arguments*/),std::make_tuple(/*Bar's ctr arguments*/)}
    template <typename... CtrTuples>
    Dispatcher(Node_id nid, CtrTuples... a)
            : nid(nid),
              receivers(new std::decay_t<decltype(*receivers)>()),
              objects(construct_objects<mutils::TupleList<CtrTuples...>, T...>(mutils::TupleList<CtrTuples...>{a...})),
              impl(mutils::callFunc([&](auto &... obj) {return this->register_all(*obj...); },
                                    objects)) {}

    Dispatcher(Dispatcher &&other)
            : nid(other.nid),
              receivers(std::move(other.receivers)),
              objects(std::move(other.objects)),
              impl(std::move(other.impl)) {}

    template <class NewClass, typename... NewFuns>
    auto register_functions(std::unique_ptr<NewClass> *cls, NewFuns... f) {
        //NewFuns must be of type Ret (NewClass::*) (Args...)
        //or of type wrapped<opcode,Ret,Args...>
        return build_remoteinvocableclass<NewClass>(nid, *receivers, wrap(cls, wrap(f))...);
    }
};
}

using namespace rpc;
