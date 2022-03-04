/**
 * @file rpc_utils.h
 *
 * @date Feb 3, 2017
 */

#pragma once

#include "../derecho_exception.hpp"
#include "../derecho_type_definitions.hpp"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "derecho/utils/logger.hpp"
#include "derecho_internal.hpp"

#include <mutils/macro_utils.hpp>

#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>
#include <cstdarg>

namespace derecho {

namespace rpc {

/**
 * Computes a simple hash of a C-string (raw array of char) in a constexpr
 * function that can be run at compile-time. This allows us to generate
 * FunctionTags at compile time, inside template parameters, using the literal
 * names of functions (which can be converted to C-string constants using macro
 * stringization).
 */
template <typename Carr>
constexpr std::size_t hash_cstr(Carr&& c_str) {
    std::size_t hash_code = 0;
    for(char c : c_str) {
        if(c == 0) break;
        hash_code = hash_code * 31 + c;
    }
    return hash_code;
}

/**
 * A helper function that examines a C-string to determine whether it matches
 * one of the two method-registering macros in register_rpc_functions, P2P_TARGETS
 * and ORDERED_TARGETS. This is used by the REGISTER_RPC_FUNCTIONS macro to
 * ensure that it is only called with the correct arguments.
 */
template <typename Carr>
constexpr bool well_formed_macro(Carr&& c_str) {
    constexpr char option1[] = "P2P_TARGETS";
    constexpr std::size_t length1 = 11;
    constexpr char option2[] = "ORDERED_TARGETS";
    constexpr std::size_t length2 = 15;
    bool option1_active = false;
    bool option2_active = false;
    std::size_t index = 0;
    for(char c : c_str) {
        if(c == 0 || (index >= length1 && option1_active) || (index >= length2 && option2_active))
            return true;
        else if(!option1_active && !option2_active && index == 0) {
            //first iter
            if(c == option1[0])
                option1_active = true;
            else if(c == option2[0])
                option2_active = true;
            else
                return false;
            ++index;
        } else if(option1_active) {
            if(c != option1[index]) return false;
            ++index;
        } else if(option2_active) {
            if(c != option2[index]) return false;
            ++index;
        } else
            return false;
    }
    return true;
}

template <typename Carr,typename...RestArgs>
constexpr bool well_formed_macro(Carr&& first, Carr&& second, RestArgs... args) {
    if (!well_formed_macro(first)) {
        return false;
    }
    
    return well_formed_macro(second,args...);
}

using FunctionTag = unsigned long long;

/**
 * Converts a user-supplied FunctionTag (from the result of hash_cstr) to an
 * "internal" FunctionTag, which uses its least-significant bit to indicate
 * whether it is an ordered function or a P2P function.
 * @tparam IsP2P true if this tag is for a P2P-callable function, false if it
 * is for an ordered function
 * @param user_tag The user-supplied FunctionTag based on the function's name
 * @return A FunctionTag that is even if the RPC function is ordered, and odd
 * if the RPC function is P2P-callable
 */
template <bool IsP2P>
constexpr FunctionTag to_internal_tag(FunctionTag user_tag) {
    if constexpr(IsP2P) {
        return 2 * user_tag + 1;
    } else {
        return 2 * user_tag;
    }
}

/**
 * An RPC function call can be uniquely identified by the tuple
 * (class, subgroup ID, function ID, is-reply), which is what this struct
 * encapsulates. Its comparsion operators simply inherit the ones from
 * std::tuple.
 */
struct Opcode {
    subgroup_type_id_t class_id;
    subgroup_id_t subgroup_id;
    FunctionTag function_id;
    bool is_reply;
};
inline bool operator<(const Opcode& lhs, const Opcode& rhs) {
    return std::tie(lhs.class_id, lhs.subgroup_id, lhs.function_id, lhs.is_reply)
           < std::tie(rhs.class_id, rhs.subgroup_id, rhs.function_id, rhs.is_reply);
}
inline bool operator==(const Opcode& lhs, const Opcode& rhs) {
    return lhs.class_id == rhs.class_id && lhs.subgroup_id == rhs.subgroup_id
           && lhs.function_id == rhs.function_id && lhs.is_reply == rhs.is_reply;
}

using node_list_t = std::vector<node_id_t>;

/**
 * A simple serializable struct that contains two strings, one for the typename
 * of an exception and one for its what() value. This is used to send exception
 * information back to a caller if an RPC function throws an exception.
 */
struct remote_exception_info : public mutils::ByteRepresentable {
    std::string exception_name;
    std::string exception_what;
    remote_exception_info(const std::string& exception_name, const std::string& exception_what)
            : exception_name(exception_name), exception_what(exception_what) {}
    DEFAULT_SERIALIZATION_SUPPORT(remote_exception_info, exception_name, exception_what);
};

/**
 * Indicates that an RPC call failed because executing the RPC function on the
 * remote node resulted in an exception.
 */
struct remote_exception_occurred : public derecho_exception {
    node_id_t who;
    std::string exception_name;
    std::string exception_what;
    remote_exception_occurred(node_id_t who, const std::string& name, const std::string& what)
            : derecho_exception(std::string("Node ID ") + std::to_string(who) + std::string(" encountered an exception of type ")
                                + name + std::string(". what(): ") + what),
              who(who),
              exception_name(name),
              exception_what(what) {}
};

/**
 * Indicates that an RPC call to a node failed because the node was removed
 * from the Replicated Object's subgroup (and possibly from the enclosing Group
 * entirely) after the RPC message was sent but before a reply was received.
 */
struct node_removed_from_group_exception : public derecho_exception {
    node_id_t who;
    node_removed_from_group_exception(node_id_t who)
            : derecho_exception(std::string("Node with ID ")
                                + std::to_string(who)
                                + std::string(" has been removed from the group.")),
              who(who) {}
};

/**
 * Indicates that an RPC call from this node was aborted because this node was
 * removed from its subgroup/shard (and reassigned to another one) during the
 * view change.
 */
struct sender_removed_from_group_exception : public derecho_exception {
    sender_removed_from_group_exception()
            : derecho_exception("This node was removed from its subgroup or shard "
                                "and can no longer send the RPC message.") {}
};

/**
 * Return type of all the RemoteInvocable::receive_* methods. If the method is
 * receive_call, this struct contains the message to send in reply, along with
 * its size in bytes, and a pointer to the exception generated by the function
 * call if one was thrown.
 */
struct recv_ret {
    Opcode opcode;
    std::size_t size;
    uint8_t* payload;
    std::exception_ptr possible_exception;
};

/**
 * Type signature for all the RemoteInvocable::receive_* methods. This alias is
 * helpful for declaring a map of "RPC receive handlers" that are called when
 * some RPC message is received.
 */
using receive_fun_t = std::function<recv_ret(
        mutils::RemoteDeserialization_v* rdv, const node_id_t&, const uint8_t* recv_buf,
        const std::function<uint8_t*(int)>& out_alloc)>;

//Forward declaration of PendingResults, to be used by QueryResults
template <typename Ret>
class PendingResults;

/**
 * The type of map contained in a QueryResults::ReplyMap. The template parameter
 * should be the return type of the query.
 */
template <typename T>
using futures_map = std::map<node_id_t, std::future<T>>;

/**
 * Data structure that (indirectly) holds a set of futures for a single RPC
 * function call; there is one future for each node contacted to make the
 * call, and it will eventually contain that node's reply. The futures are
 * actually stored inside an internal struct of type ReplyMap, which can be
 * retrieved with the get() method. The ReplyMap will not be returned until
 * it is "fulfilled" by the sender, which should happen when the RPC call
 * is delivered in the current View (and thus, the current View is the set
 * of nodes who should reply to the RPC).
 * @tparam Ret The return type of the RPC function that this query invoked
 */
template <typename Ret>
class QueryResults {
public:
    /** A future for a futures_map held by unique_ptr (so it can be more easily moved) */
    using map_fut = std::future<std::unique_ptr<futures_map<Ret>>>;
    using type = Ret;

    /**
     * A wrapper around a std::map from node IDs to std::futures. Implements
     * the iterator interface by passing calls through to the underlying std::map,
     * so ReplyMap can be iterated over in a for-each loop as if it is actually
     * a map rather than a wrapper around a map.
     */
    class ReplyMap {
    private:
        QueryResults& parent;

    public:
        futures_map<Ret> rmap;

        ReplyMap(QueryResults& qr) : parent(qr){};
        ReplyMap(const ReplyMap&) = delete;
        ReplyMap(ReplyMap&& rm) : parent(rm.parent), rmap(std::move(rm.rmap)) {}

        bool valid(const node_id_t& nid) {
            assert(rmap.size() == 0 || rmap.count(nid) != 0);
            return (rmap.size() > 0) && rmap.at(nid).valid();
        }

        /*
          returns true if we sent to this node,
          regardless of whether this node has replied.
        */
        bool contains(const node_id_t& nid) { return rmap.count(nid); }

        auto begin() { return std::begin(rmap); }

        auto end() { return std::end(rmap); }

        Ret get(const node_id_t& nid) {
            if(rmap.size() == 0) {
                //This should never happen. Since the ReplyMap member is private, the only way to
                //invoke get(nid) on a ReplyMap is to retrieve a reference to it with the parent
                //QueryResults's wait() or get(), which must have executed the assignment
                //replies.rmap = std::move(*pending_rmap.get()).
                assert(parent.pending_rmap.valid());
                rmap = std::move(*parent.pending_rmap.get());
            }
            assert(rmap.size() > 0);
            assert(rmap.count(nid));
            assert(rmap.at(nid).valid());
            return rmap.at(nid).get();
        }
    };

    map_fut pending_rmap;

private:
    ReplyMap replies{*this};
    /** This will be fulfilled with this RPC function call's version-timestamp pair, once it has been assigned */
    std::future<std::pair<persistent::version_t, uint64_t>> persistent_version;
    /** This signals that local persistence has completed for the version assigned to this RPC function call */
    std::future<void> local_persistence_done;
    /** This signals that global persistence has completed for the version assigned to this RPC function call */
    std::future<void> global_persistence_done;
    /** This signals that the signature has been verified at all replicas on the version assigned to this RPC function call */
    std::future<void> signature_done;
    /**
     * An owning pointer to the PendingResults that is paired with this QueryResults
     * (i.e. the one that constructed this QueryResults). It ensures that the
     * PendingResults has the same lifetime as the QueryResults.
     */
    std::shared_ptr<PendingResults<Ret>> paired_pending_results;

public:
    /**
     * Constructs a QueryResults from a future for a reply-map (which is itself
     * a map of futures), and the futures for the persistent events it will also
     * track. The promise ends of these futures should reside in a corresponding
     * PendingResults that constructed this QueryResults, and the first parameter
     * should be a shared_ptr to that PendingResults.
     */
    QueryResults(std::shared_ptr<PendingResults<Ret>> paired_pending_results,
                 map_fut reply_map_future, std::future<std::pair<persistent::version_t, uint64_t>> persistent_version,
                 std::future<void> local_persistence_done, std::future<void> global_persistence_done,
                 std::future<void> signature_done)
            : pending_rmap(std::move(reply_map_future)),
              persistent_version(std::move(persistent_version)),
              local_persistence_done(std::move(local_persistence_done)),
              global_persistence_done(std::move(global_persistence_done)),
              signature_done(std::move(signature_done)),
              paired_pending_results(paired_pending_results) {}
    /** Move constructor for QueryResults. */
    QueryResults(QueryResults&& o)
            : pending_rmap{std::move(o.pending_rmap)},
              replies{std::move(o.replies)},
              persistent_version{std::move(o.persistent_version)},
              local_persistence_done{std::move(o.local_persistence_done)},
              global_persistence_done{std::move(o.global_persistence_done)},
              signature_done{std::move(o.signature_done)},
              paired_pending_results{std::move(o.paired_pending_results)} {}
    /** QueryResults, like std::future, is not copyable. */
    QueryResults(const QueryResults&) = delete;

    /**
     * Wait the specified duration; if a ReplyMap is available
     * after that duration, return it. Otherwise return nullptr.
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

    /**
     * Block until the ReplyMap is fulfilled, then return the map by reference.
     * The ReplyMap is only valid as long as this QueryResults remains in
     * scope, and cannot be copied.
     */
    ReplyMap& get() {
        using namespace std::chrono;
        while(true) {
            if(auto rmap = wait(5min)) {
                return *rmap;
            }
        }
    }

    /**
     * Test if all the future entries are ready.
     * A true return value indicates that get() will not block.
     *
     * @return true/false
     */
    bool is_ready() {
        if(replies.rmap.size() != 0) {
            for(auto& reply : replies) {
                using namespace std::chrono;
                if(reply.second.wait_for(std::chrono::seconds(0s)) != std::future_status::ready) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Retrieves the persistent version number and timestamp that has been assigned
     * to this RPC function call. The persistent version number is only available
     * once the ReplyMap has been fulfilled (they are set at the same time), so this
     * will block unless get() has been previously called on the QueryResults. This
     * also will only work for an ordered_send, not a p2p_send, since the sender of
     * a P2P RPC call is not a member of the group that receives the message and
     * thus will not learn which version number was assigned to it. (A P2P function
     * should be const anyway, so it should not generate a new version).
     */
    std::pair<persistent::version_t, uint64_t> get_persistent_version() {
        return persistent_version.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has finished
     * persisting locally (i.e. the version number assigned to it has reached
     * the "locally persisted" state). Note that this is meaningless if the
     * Replicated Object has no Persistent<T> fields, and it will only work on
     * QueryResults that are generated by ordered_send calls. It will block
     * forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * persistence events related to it.
     */
    void await_local_persistence() {
        local_persistence_done.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has finished
     * persisting on all replicas (i.e. the version number assigned to it has
     * reached the "globally persisted" state). Note that this is meaningless
     * if the Replicated Object has no Persistent<T> fields, and it will only
     * work on QueryResults that are generated by ordered_send calls. It will
     * block forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * persistence events related to it.
     */
    void await_global_persistence() {
        global_persistence_done.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has been signed
     * on all replicas and the signatures have been verified. Note that this is
     * meaningless if signed updates are not enabled, and it will only work on
     * QueryResults that are generated by ordered_send calls. It will block
     * forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * signature events related to it.
     */
    void await_signature_verification() {
        signature_done.get();
    }

    /**
     * Checks if a call to await_local_persistence() would succeed without
     * blocking; returns true if so.
     */
    bool local_persistence_is_ready() const {
        return local_persistence_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    /**
     * Checks if a call to await_global_persistence() would succeed without
     * blocking; returns true if so.
     */
    bool global_persistence_is_ready() const {
        return global_persistence_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    /**
     * Checks if a call to await_signature_verification() would succeed without
     * blocking; returns true if so.
     */
    bool global_verification_is_ready() const {
        return signature_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }
};

/**
 * Specialization of QueryResults for void functions, which do not generate
 * replies. Here the "reply map" is actually a set, and simply records the set
 * of nodes to which the RPC was sent. The internal ReplyMap is fulfilled when
 * the set of nodes that received the RPC is known, which is when the RPC
 * message was delivered in the current View.
 */
template <>
class QueryResults<void> {
public:
    using map_fut = std::future<std::unique_ptr<std::set<node_id_t>>>;
    using type = void;

    class ReplyMap {
    private:
        QueryResults& parent;

    public:
        std::set<node_id_t> rmap;

        ReplyMap(QueryResults& qr) : parent(qr){};
        ReplyMap(const ReplyMap&) = delete;
        ReplyMap(ReplyMap&& rm) : parent(rm.parent), rmap(std::move(rm.rmap)) {}

        bool valid(const node_id_t& nid) {
            assert(rmap.size() == 0 || rmap.count(nid) != 0);
            return (rmap.size() > 0) && rmap.count(nid) > 0;
        }

        /*
          returns true if we sent to this node,
          regardless of whether this node has replied.
        */
        bool contains(const node_id_t& nid) { return rmap.count(nid); }

        auto begin() { return std::begin(rmap); }

        auto end() { return std::end(rmap); }
    };

    map_fut pending_rmap;

private:
    ReplyMap replies{*this};
    /** This will be fulfilled with this RPC function call's version-timestamp pair, once it has been assigned */
    std::future<std::pair<persistent::version_t, uint64_t>> persistent_version;
    /** This signals that local persistence has completed for the version assigned to this RPC function call */
    std::future<void> local_persistence_done;
    /** This signals that global persistence has completed for the version assigned to this RPC function call */
    std::future<void> global_persistence_done;
    /** This signals that the signature has been verified at all replicas on the version assigned to this RPC function call */
    std::future<void> signature_done;
    /**
     * An owning pointer to the PendingResults that is paired with this QueryResults
     * (i.e. the one that constructed this QueryResults). It ensures that the
     * PendingResults has the same lifetime as the QueryResults.
     */
    std::shared_ptr<PendingResults<void>> paired_pending_results;

public:
    QueryResults(std::shared_ptr<PendingResults<void>> paired_pending_results,
                 map_fut reply_map_future, std::future<std::pair<persistent::version_t, uint64_t>> persistent_version,
                 std::future<void> local_persistence_done, std::future<void> global_persistence_done,
                 std::future<void> signature_done)
            : pending_rmap(std::move(reply_map_future)),
              persistent_version(std::move(persistent_version)),
              local_persistence_done(std::move(local_persistence_done)),
              global_persistence_done(std::move(global_persistence_done)),
              signature_done(std::move(signature_done)),
              paired_pending_results(paired_pending_results) {}
    QueryResults(QueryResults&& o)
            : pending_rmap{std::move(o.pending_rmap)},
              replies{std::move(o.replies)},
              persistent_version{std::move(o.persistent_version)},
              local_persistence_done{std::move(o.local_persistence_done)},
              global_persistence_done{std::move(o.global_persistence_done)},
              signature_done{std::move(o.signature_done)},
              paired_pending_results{std::move(o.paired_pending_results)} {}
    QueryResults(const QueryResults&) = delete;

    /**
     * Wait the specified duration; if a ReplyMap is available
     * after that duration, return it. Otherwise return nullptr.
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

    /**
     * Block until the ReplyMap is fulfilled, then return the map by reference.
     * The ReplyMap is only valid as long as this QueryResults remains in
     * scope, and cannot be copied.
     */
    ReplyMap& get() {
        using namespace std::chrono;
        while(true) {
            if(auto rmap = wait(5min)) {
                return *rmap;
            }
        }
    }

    /**
     * Test if all the future entries are ready.
     * A true return value indicates that get() will not block.
     *
     * @return true/false
     */
    bool is_ready() {
        return (replies.rmap.size() != 0);
    }

    /**
     * Retrieves the persistent version number and timestamp that has been assigned
     * to this RPC function call. The persistent version number is only available
     * once the ReplyMap has been fulfilled (they are set at the same time), so this
     * will block unless get() has been previously called on the QueryResults.
     */
    std::pair<persistent::version_t, uint64_t> get_persistent_version() {
        return persistent_version.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has finished
     * persisting locally (i.e. the version number assigned to it has reached
     * the "locally persisted" state). Note that this is meaningless if the
     * Replicated Object has no Persistent<T> fields, and it will only work on
     * QueryResults that are generated by ordered_send calls. It will block
     * forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * persistence events related to it.
     */
    void await_local_persistence() {
        local_persistence_done.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has finished
     * persisting locally (i.e. the version number assigned to it has reached
     * the "locally persisted" state). Note that this is meaningless if the
     * Replicated Object has no Persistent<T> fields, and it will only work on
     * QueryResults that are generated by ordered_send calls. It will block
     * forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * persistence events related to it.
     */
    void await_global_persistence() {
        global_persistence_done.get();
    }

    /**
     * Blocks until the update caused by this RPC function call has been signed
     * on all replicas and the signatures have been verified. Note that this is
     * meaningless if signed updates are not enabled, and it will only work on
     * QueryResults that are generated by ordered_send calls. It will block
     * forever if called on the result of a p2p_send call, since only the
     * members of the subgroup that receives an RPC message will be notified of
     * signature events related to it.
     */
    void await_signature_verification() {
        signature_done.get();
    }

    /**
     * Checks if a call to await_local_persistence() would succeed without
     * blocking; returns true if so.
     */
    bool local_persistence_is_ready() const {
        return local_persistence_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    /**
     * Checks if a call to await_global_persistence() would succeed without
     * blocking; returns true if so.
     */
    bool global_persistence_is_ready() const {
        return global_persistence_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    /**
     * Checks if a call to await_signature_verification() would succeed without
     * blocking; returns true if so.
     */
    bool global_verification_is_ready() const {
        return signature_done.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }
};

/**
 * Abstract base type for PendingResults. This allows us to store a pointer to
 * any template specialization of PendingResults without knowing the template
 * parameter.
 */
class AbstractPendingResults {
public:
    virtual void fulfill_map(const node_list_t&) = 0;
    virtual void delete_self_ptr() = 0;
    virtual void set_persistent_version(persistent::version_t, uint64_t) = 0;
    virtual void set_local_persistence() = 0;
    virtual void set_global_persistence() = 0;
    virtual void set_signature_verified() = 0;
    virtual void set_exception_for_removed_node(const node_id_t&) = 0;
    virtual void set_exception_for_caller_removed() = 0;
    virtual bool all_responded() = 0;
    virtual ~AbstractPendingResults() {}
};

/**
 * Data structure that holds a set of promises for a single RPC function call;
 * the promises transmit one response (either a value or an exception) for
 * each node that was called. The future ends of these promises are stored in
 * a corresponding QueryResults object.
 * @tparam Ret The return type of the RPC function, which is the type of a
 * response's value.
 */
template <typename Ret>
class PendingResults : public AbstractPendingResults, public std::enable_shared_from_this<PendingResults<Ret>> {
private:
    /** A promise for a map containing one future for each reply to the RPC function
     * call. The future end of this promise lives in QueryResults, and is fulfilled
     * when the RPC function call is actually sent and the set of repliers is known. */
    std::promise<std::unique_ptr<futures_map<Ret>>> promise_for_pending_map;

    std::promise<std::map<node_id_t, std::promise<Ret>>> promise_for_reply_promises;
    /** A future for a map containing one promise for each reply to the RPC function
     * call. It will be fulfilled when fulfill_map is called, which means the RPC
     * function call was actually sent and the set of destination nodes is known. */
    std::future<std::map<node_id_t, std::promise<Ret>>> reply_promises_are_ready;
    std::mutex reply_promises_are_ready_mutex;
    /**
     * Contains one promise for each node that the RPC function call was sent to,
     * which will be fulfilled when that node replies. Indexed by the node's ID.
     */
    std::map<node_id_t, std::promise<Ret>> reply_promises;
    /**
     * True if the reply map has been fulfilled, i.e. fulfill_map() has been
     * called and promise_for_pending_map has had a value set. This is necessary
     * because it's impossible to ask a std::promise if set_value() has been
     * called on it.
    */
    bool map_fulfilled = false;
    /**
     * The set of nodes that the RPC function call was sent to; equal to the
     * key set of reply_promises.
     */
    std::set<node_id_t> dest_nodes;
    /**
     * The set of nodes that have responded to the RPC function call, either by
     * delivering a reply or by failing. If a node ID is in this set, its future
     * in reply_promises has been fulfilled with either set_value or set_exception.
     */
    std::set<node_id_t> responded_nodes;
    /**
     * Controls access to responded_nodes because std::set isn't thread-safe.
     * Both the predicates thread and the P2P worker thread may access
     * responded_nodes at the same time (especially during view changes).
     */
    std::mutex responded_nodes_mutex;

    /**
     * A promise for a persistent version (which is actually a pair of a version
     * number and a timestamp) assigned to the update represented by this RPC
     * function call. The future end of this promise lives in the corresponding
     * QueryResults. It is fulfilled when the RPC function call is actually sent,
     * which means it has been ordered and a version is assigned to it.
     */
    std::promise<std::pair<persistent::version_t, uint64_t>> version_promise;
    /**
     * A promise representing the "local persistence" event for the update
     * caused by this RPC call; the future end lives in QueryResults. This is
     * fulfilled to signal that the update has finished persisting locally.
     */
    std::promise<void> local_persistence_promise;
    /**
     * A promise representing the "global persistence" event for the update
     * caused by this RPC call; the future end lives in QueryResults. This is
     * fulfilled to signal that the update has finished persisting on all
     * replicas.
     */
    std::promise<void> global_persistence_promise;
    /**
     * A promise representing the "signature verified" event for the update
     * caused by this RPC call; the future end lives in QueryResults.
     */
    std::promise<void> signature_verified_promise;

    /**
     * A flag set to true the first time delete_self_ptr() is called, to
     * prevent it from attempting to delete the pointer again if it is
     * mistakenly called twice on the same object.
     */
    bool heap_pointer_deleted;

    /**
     * A raw pointer to a heap-allocated shared_ptr to this PendingResults object,
     * which is used by RemoteInvoker to locate this object when a reply arrives.
     * Stored here so that the shared_ptr can still be deleted by RPCManager if
     * a node fails before sending its reply; RemoteInvoker will lose the
     * shared_ptr if it doesn't get all the replies to a given invocation.
     */
    std::shared_ptr<PendingResults<Ret>>* self_heap_ptr;

    /**
     * A mutex that can be used to control access to this object "as a whole,"
     * in order to make a sequence of method invocations atomic. This is
     * necessary to make receive_response thread-safe, since the sequence
     * "set_value()/set_exception() then all_responded()" needs to be atomic.
     */
    std::mutex this_object_mutex;

public:
    PendingResults()
            : reply_promises_are_ready(promise_for_reply_promises.get_future()),
              heap_pointer_deleted(false),
              self_heap_ptr(nullptr) {}
    virtual ~PendingResults() {}

    /**
     * Constructs and returns a QueryResults representing the "future" end of
     * the response promises in this PendingResults.
     * @return A new QueryResults holding a set of futures for this RPC function call
     */
    std::unique_ptr<QueryResults<Ret>> get_future() {
        return std::make_unique<QueryResults<Ret>>(this->shared_from_this(),
                                                   promise_for_pending_map.get_future(),
                                                   version_promise.get_future(),
                                                   local_persistence_promise.get_future(),
                                                   global_persistence_promise.get_future(),
                                                   signature_verified_promise.get_future());
    }

    /**
     * Fill pending_map and reply_promises with one promise/future pair for
     * each node that was contacted in this RPC call
     * @param who A list of nodes from which to expect responses.
     */
    void fulfill_map(const node_list_t& who) {
        dbg_default_trace("Got a call to fulfill_map for PendingResults<{}>", typeid(Ret).name());
        std::unique_ptr<futures_map<Ret>> futures = std::make_unique<futures_map<Ret>>();
        std::map<node_id_t, std::promise<Ret>> promises;
        for(const auto& e : who) {
            futures->emplace(e, promises[e].get_future());
        }
        dest_nodes.insert(who.begin(), who.end());
        dbg_default_trace("Setting a value for reply_promises_are_ready");
        promise_for_reply_promises.set_value(std::move(promises));
        promise_for_pending_map.set_value(std::move(futures));
        map_fulfilled = true;
    }

    /**
     * Stores the address of a heap-allocated shared_ptr to this PendingResults
     * object, which was allocated by RemoteInvoker and used as an invocation ID.
     * @param heap_ptr A pointer to a shared_ptr to this PendingResults object.
     */
    void set_self_ptr(std::shared_ptr<PendingResults<Ret>>* heap_ptr) {
        self_heap_ptr = heap_ptr;
    }

    /**
     * Deletes RemoteInvoker's heap-allocated shared_ptr to this PendingResults
     * object, assuming it was previously set with set_self_ptr(). This should
     * only be called by RemoteInvoker or RPCManager. It is safe to call this
     * method more than once, and it will have no effect after the first call.
     */
    void delete_self_ptr() {
        if(!heap_pointer_deleted) {
            dbg_default_trace("delete_self_ptr() deleting the shared_ptr at {}", fmt::ptr(self_heap_ptr));
            heap_pointer_deleted = true;
            //This must be the last statement in the method, since it might result in this object getting deleted
            delete self_heap_ptr;
        }
    }

    /**
     * Sets exceptions to indicate to the sender of this RPC call that it has been
     * removed from its subgroup/shard, and can no longer expect responses.
     */
    void set_exception_for_caller_removed() {
        if(!map_fulfilled) {
            promise_for_pending_map.set_exception(
                    std::make_exception_ptr(sender_removed_from_group_exception{}));
        } else {
            if(reply_promises.size() == 0) {
                reply_promises = std::move(reply_promises_are_ready.get());
            }
            //Set exceptions for any nodes that have not yet responded
            for(auto& node_and_promise : reply_promises) {
                std::lock_guard<std::mutex> lock(responded_nodes_mutex);
                if(responded_nodes.find(node_and_promise.first)
                   == responded_nodes.end()) {
                    node_and_promise.second.set_exception(
                            std::make_exception_ptr(sender_removed_from_group_exception{}));
                }
            }
        }
    }

    /**
     * Fulfills a promise for a single node's reply to indicate that the node
     * will never reply, by putting a node_removed_from_group_exception in the
     * promise. This happens if the node is removed in a View change while the
     * RPC is still awaiting its reply.
     */
    void set_exception_for_removed_node(const node_id_t& removed_nid) {
        assert(map_fulfilled);
        if(reply_promises.size() == 0) {
            reply_promises = std::move(reply_promises_are_ready.get());
        }
        std::lock_guard<std::mutex> lock(responded_nodes_mutex);
        if(dest_nodes.find(removed_nid) != dest_nodes.end()
           && responded_nodes.find(removed_nid) == responded_nodes.end()) {
            //Mark the node as "responded" for the purposes of the other methods
            responded_nodes.insert(removed_nid);
            reply_promises.at(removed_nid).set_exception(std::make_exception_ptr(node_removed_from_group_exception{removed_nid}));
        }
    }

    /**
     * Fulfills a promise for a single node's reply by setting the value that
     * the node returned for the RPC call
     * @param nid The node that responded to the RPC call
     * @param v The value that it returned as the result of the RPC function
     */
    void set_value(const node_id_t& nid, const Ret& v) {
        std::lock_guard<std::mutex> lock(reply_promises_are_ready_mutex);
        {
            std::lock_guard<std::mutex> lock(responded_nodes_mutex);
            responded_nodes.insert(nid);
        }
        if(reply_promises.size() == 0) {
            dbg_default_trace("PendingResults<{}>::set_value about to wait on reply_promises_are_ready", typeid(Ret).name());
            reply_promises = std::move(reply_promises_are_ready.get());
        }
        reply_promises.at(nid).set_value(v);
    }

    /**
     * Fulfills a promise for a single node's reply by setting an exception that
     * was thrown by the RPC function call.
     * @param nid The node that responded to the RPC call with an exception
     * @param e The exception_ptr that the RPC function call returned
     */
    void set_exception(const node_id_t& nid, const std::exception_ptr e) {
        {
            std::lock_guard<std::mutex> lock(responded_nodes_mutex);
            responded_nodes.insert(nid);
        }
        if(reply_promises.size() == 0) {
            reply_promises = std::move(reply_promises_are_ready.get());
        }
        reply_promises.at(nid).set_exception(e);
    }

    /**
     * @return True if all destination nodes for this RPC function call have
     * responded, either by sending a reply or by being removed from the group
     */
    bool all_responded() {
        std::lock_guard<std::mutex> lock(responded_nodes_mutex);
        return map_fulfilled && (responded_nodes == dest_nodes);
    }

    /**
     * Fulfills the promise for the persistent version number.
     * @param assigned_version The persistent version number that was assigned
     * to the update generated by this RPC function call
     */
    void set_persistent_version(persistent::version_t assigned_version, uint64_t assigned_timestamp) {
        version_promise.set_value({assigned_version, assigned_timestamp});
    }

    /**
     * Fulfills the local persistence promise, unblocking the future end. This
     * should be called to signal client code that the update has finished
     * persisting locally on this node.
     */
    void set_local_persistence() {
        local_persistence_promise.set_value();
    }

    /**
     * Fulfills the global persistence promise, unblocking the future end. This
     * should be called to signal client code that the update has finished
     * persisting on all replicas of this subgroup.
     */
    void set_global_persistence() {
        global_persistence_promise.set_value();
    }

    /**
     * Fulfills the signature verification promise, unblocking the future end.
     * This should be called to signal client code that the update has been
     * correctly signed on all replicas of this subgroup.
     */
    void set_signature_verified() {
        signature_verified_promise.set_value();
    }

    /**
     * @return A reference to the "object mutex" stored in this PendingResults.
     * Callers should lock this mutex before performing a sequence of multiple
     * method calls to prevent threads from interleaving between them.
     */
    std::mutex& object_mutex() {
        return this_object_mutex;
    }
};

/**
 * Specialization of PendingResults for void functions, which do not generate
 * replies. It still fulfills the "reply map" in its corresponding QueryResults<void>,
 * which is just a set of nodes to which the RPC message was delivered. It also
 * fulfills the local and global persistence promises, since void functions can still
 * cause new persistent versions to be generated.
 */
template <>
class PendingResults<void> : public AbstractPendingResults, public std::enable_shared_from_this<PendingResults<void>> {
private:
    std::promise<std::unique_ptr<std::set<node_id_t>>> promise_for_pending_map;
    bool map_fulfilled = false;
    std::promise<std::pair<persistent::version_t, uint64_t>> version_promise;
    std::promise<void> local_persistence_promise;
    std::promise<void> global_persistence_promise;
    std::promise<void> signature_verified_promise;

public:
    std::unique_ptr<QueryResults<void>> get_future() {
        return std::make_unique<QueryResults<void>>(shared_from_this(),
                                                    promise_for_pending_map.get_future(),
                                                    version_promise.get_future(),
                                                    local_persistence_promise.get_future(),
                                                    global_persistence_promise.get_future(),
                                                    signature_verified_promise.get_future());
    }

    void set_self_ptr(std::shared_ptr<PendingResults<void>>* heap_ptr) {
        //Does nothing because RemoteInvoker doesn't create a heap-allocated pointer for PendingResults<void>
    }

    void delete_self_ptr() {
        //Also does nothing for the above reason
    }

    void fulfill_map(const node_list_t& sent_nodes) {
        auto nodes_sent_set = std::make_unique<std::set<node_id_t>>();
        for(const node_id_t& node : sent_nodes) {
            nodes_sent_set->emplace(node);
        }
        promise_for_pending_map.set_value(std::move(nodes_sent_set));
        map_fulfilled = true;
    }

    void set_exception_for_removed_node(const node_id_t&) {}

    void set_exception_for_caller_removed() {
        if(!map_fulfilled) {
            promise_for_pending_map.set_exception(
                    std::make_exception_ptr(sender_removed_from_group_exception()));
        }
    }

    bool all_responded() {
        return map_fulfilled;
    }

    /**
     * Fulfills the promise for the persistent version number.
     * @param assigned_version The persistent version number that was assigned
     * to the update generated by this RPC function call
     */
    void set_persistent_version(persistent::version_t assigned_version, uint64_t assigned_timestamp) {
        version_promise.set_value({assigned_version, assigned_timestamp});
    }

    /**
     * Fulfills the local persistence promise, unblocking the future end. This
     * should be called to signal client code that the update has finished
     * persisting locally on this node.
     */
    void set_local_persistence() {
        local_persistence_promise.set_value();
    }

    /**
     * Fulfills the global persistence promise, unblocking the future end. This
     * should be called to signal client code that the update has finished
     * persisting on all replicas of this subgroup.
     */
    void set_global_persistence() {
        global_persistence_promise.set_value();
    }

    /**
     * Fulfills the signature verification promise, unblocking the future end.
     * This should be called to signal client code that the update has been
     * correctly signed on all replicas of this subgroup.
     */
    void set_signature_verified() {
        signature_verified_promise.set_value();
    }
};

/**
 * Utility functions for manipulating the headers of RPC messages
 */
namespace remote_invocation_utilities {
#define RPC_HEADER_FLAG_TST(f, name) \
    ((f) & (((uint32_t)1L) << (_RPC_HEADER_FLAG_##name)))
#define RPC_HEADER_FLAG_SET(f, name) \
    ((f) |= (((uint32_t)1L) << (_RPC_HEADER_FLAG_##name)))
#define RPC_HEADER_FLAG_CLR(f, name) \
    ((f) &= ~(((uint32_t)1L) << (_RPC_HEADER_FLAG_##name)))

// add new rpc header flags here.
#define _RPC_HEADER_FLAG_CASCADE (0)
#define _RPC_HEADER_FLAG_RESERVED (1)

inline std::size_t header_space() {
    return sizeof(std::size_t) + sizeof(Opcode) + sizeof(node_id_t) + sizeof(uint32_t);
    //            size                  operation        from                flags
}

inline uint8_t* extra_alloc(int i) {
    const auto hs = header_space();
    return (uint8_t*)calloc(i + hs, sizeof(char)) + hs;
}

inline void populate_header(uint8_t* reply_buf,
                            const std::size_t& payload_size,
                            const Opcode& op, const node_id_t& from,
                            const uint32_t& flags) {
    std::size_t offset = 0;
    static_assert(sizeof(op) == sizeof(Opcode), "Opcode& is not the same size as Opcode!");
    reinterpret_cast<std::size_t*>(reply_buf + offset)[0] = payload_size;  // size
    offset += sizeof(payload_size);
    reinterpret_cast<Opcode*>(reply_buf + offset)[0] = op;  // what
    offset += sizeof(op);
    reinterpret_cast<node_id_t*>(reply_buf + offset)[0] = from;  // from
    offset += sizeof(from);
    reinterpret_cast<uint32_t*>(reply_buf + offset)[0] = flags;  // flags
}

//inline void retrieve_header(mutils::DeserializationManager* dsm,
inline void retrieve_header(mutils::RemoteDeserialization_v* rdv,
                            const uint8_t* reply_buf,
                            std::size_t& payload_size, Opcode& op,
                            node_id_t& from, uint32_t& flags) {
    std::size_t offset = 0;
    payload_size = reinterpret_cast<const std::size_t*>(reply_buf + offset)[0];
    offset += sizeof(payload_size);
    op = reinterpret_cast<const Opcode*>(reply_buf + offset)[0];
    offset += sizeof(op);
    from = reinterpret_cast<const node_id_t*>(reply_buf + offset)[0];
    offset += sizeof(from);
    flags = reinterpret_cast<const uint32_t*>(reply_buf + offset)[0];
}
}  // namespace remote_invocation_utilities

}  // namespace rpc
}  // namespace derecho

#define CT_STRING(...) derecho::rpc::String<MACRO_GET_STR(#__VA_ARGS__)>
