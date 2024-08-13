#pragma once
#include <derecho/config.h>
#include "SerializationMacros.hpp"
#include "context_ptr.hpp"
#include <cstring>
#include <mutils/macro_utils.hpp>
#include <mutils/mutils.hpp>
#include <mutils/tuple_extras.hpp>
#include <mutils/type_utils.hpp>
#include <tuple>
#include <vector>
#include <set>
#include <unordered_set>
#include <map>
#include <unordered_map>

// BEGIN DECLARATIONS AND USAGE INFORMATION

namespace mutils {

// forward declaration
struct DeserializationManager;

/**
 * A non-POD type which wishes to mark itself byte representable should extend
 * this class. Intended to use to convert classes to and from contiguous
 * sequences of bytes.
 */
struct ByteRepresentable {
    /**
     * Write this class's marshalled representation into the array found at dest.
     * Assumes dest has at least bytes_size() of free memory available; behavior
     * is undefined otherwise.
     *
     * Returns number of bytes written, which should be the same as bytes_size().
     *
     * NOTE: it is recommended that users not call this directly, and prefer
     * to use mutils::to_bytes(T, dest) instead.
     */
    virtual std::size_t to_bytes(uint8_t* dest) const = 0;

    /**
     * Pass a pointer to a buffer containing this class's marshalled
     * representation into the function f.  This pointer is not guaranteed to live
     * beyond the duration of the call to f, so make a copy if you need to keep it
     * around.
     *
     * NOTE: it is recommended that users not call this directly, and prefer
     * to use mutils::post_object(f,T) instead.
     */
    virtual void post_object(
            const std::function<void(uint8_t const* const, std::size_t)>&) const = 0;

    /**
     * the size of the marshalled representation of this object.
     * useful when allocating arrays in which to store this object.
     *
     * NOTE: it is recommended that users not call this directly, and prefer
     * to use mutils::bytes_size(T) instead.
     */
    virtual std::size_t bytes_size() const = 0;

#ifdef MUTILS_DEBUG
    /**
     * If this object requires context in order to correctly
     * deserialize, this method will associate that context
     * with the provided DeserializationManager.  Many objects
     * will not require context, and so can leave this function
     * empty.
     */
    virtual void ensure_registered(DeserializationManager&) = 0;
#endif
    virtual ~ByteRepresentable() {}

    /**
     * from_bytes takes the DeserializationManager which manages this object's
     * context (or nullptr, if this object does not require a context), a byte
     * array of size at least bytes_size(), and returns a new heap-allocated
     * instance of that object.
     *
     * NOTE: it is recommended that users not call this directly, and prefer
     * to use mutils::from_bytes<T>(DeserializationManager*, buf) instead.
     */
    // needs to exist, but can't declare virtual statics
    // virtual static std::unique_ptr<T> from_bytes(DeserializationManager *p, const uint8_t* buf) const  = 0;

    /**
     * from_bytes_noalloc takes the DeserializationManager which manages this
     * object's context (or nullptr, if this object does not require a context), a
     * byte array of size at least bytes_size(), and returns an instance of that
     * object.  This instance may share storage with the provided byte array, and
     * is not valid past the end of life of the byte array.
     *
     * NOTE: it is recommended that users not call this directly, and prefer
     * to use mutils::deserialize_and_run<T>(DeserializationManager*, buf, f)
     * instead.  If the cost of passing a function is too high, please still
     * prefer mutils::from_bytes_noalloc<T>(DeserializationManager*, buf).
     */
    // needs to exist, but can't declare virtual statics
    // virtual static context_ptr<T> from_bytes_noalloc(DeserializationManager *p, const uint8_t* buf) const  = 0;
};

/**
 * If a class which implements ByteRepresentable requires a context in order
 * to correctly deserialize, that context should be represented as a class that
 * extends RemoteDeserializationContext.  If no context is required, then this
 * class is not necessary.
 */
struct RemoteDeserializationContext {
    RemoteDeserializationContext(const RemoteDeserializationContext&) = delete;
    RemoteDeserializationContext(const RemoteDeserializationContext&&) = delete;
    virtual ~RemoteDeserializationContext() {}
    RemoteDeserializationContext() {}
};

// a pointer to a RemoteDeserializationContext. This exists
// so that I can switch it out with smart pointer types when
// debugging stuff.
using RemoteDeserializationContext_p = RemoteDeserializationContext*;

// a vector of RemoteDeserializationContext*.
// it's got a using declaration for the same reason
// as the above.
using RemoteDeserialization_v = std::vector<RemoteDeserializationContext_p>;

/**
 * The manager for any RemoteDeserializationContexts.
 * Don't subclass this; rather construct it with any context managers
 * you need as arguments to it.
 * /be sure to have a pointer to this on hand whenever you need to deserialize
 * something. If you're dead certain you never need a deserialization
 * context, then you can not use this at all and just pass null
 * to from_bytes* in place of this.
 */
struct DeserializationManager {
    /**
     * Various registered managers. Please note that this class
     * does *not* own these pointers; you need to keep them
     * managed somewhere else. Also ensure lifetime of this
     * class is shorter than or the same as those registered
     * contexts.
     */
    RemoteDeserialization_v registered_v;
    DeserializationManager(RemoteDeserialization_v rv) : registered_v(rv) {}

    DeserializationManager(const DeserializationManager&) = delete;

    DeserializationManager(DeserializationManager&& o)
            : registered_v(std::move(o.registered_v)) {}

    DeserializationManager& register_ctx(RemoteDeserializationContext_p ctx) {
        registered_v.emplace_back(ctx);
        return *this;
    }

    /**
     * Lookup the context registered at this DeserializationManager
     * whose type is T.  Note this means we assume that types uniquely
     * identify contexts.
     */
    template <typename T>
    T& mgr() {
        for(auto& candidate : registered_v) {
            if(auto* t = dynamic_cast<T*>(candidate))
                return *t;
        }
        assert(false && "Error: no registered manager exists");
        struct dead_code {};
        throw dead_code{};
    }

    /**
     * As the above, but const.
     */
    template <typename T>
    const T& mgr() const {
        for(auto& candidate : registered_v) {
            if(auto* t = dynamic_cast<T*>(candidate))
                return t;
        }
        assert(false && "Error: no registered manager exists");
        struct dead_code {};
        throw dead_code{};
    }

    /**
     * checks to see if a context of type T has been
     * registered with this DeserializationManager.
     */
    template <typename T>
    bool registered() const {
        for(auto& candidate : registered_v) {
            if(dynamic_cast<T const*>(candidate))
                return true;
        }
        return false;
    }
};

/**
 * Just calls sizeof(T)
 */
template <typename T, restrict2(std::is_pod<T>::value)>
auto bytes_size(const T&) {
    return sizeof(T);
}

/**
 * calls b.bytes_size() when b is a ByteRepresentable;
 * calls sizeof(decay_t<decltype(b)>) when b is a POD;
 * custom logic is implemented for some STL types.
 */
std::size_t bytes_size(const ByteRepresentable& b);

/**
 * effectively strlen().
 */
std::size_t bytes_size(const std::string& b);

template <typename... T>
std::size_t bytes_size(const std::tuple<T...>& t);

/**
 * sums the size of both pair elements
 */
template <typename T, typename V>
std::size_t bytes_size(const std::pair<T, V>& pair) {
    return bytes_size(pair.first) + bytes_size(pair.second);
}

/**
 * all of the elements of this vector, plus one int for the number of elements.
 */
std::size_t bytes_size(const std::vector<bool>& v);

template <typename T>
std::size_t bytes_size(const std::vector<T>& v) {
    whenmutilsdebug(
            static const auto typenonce_size = bytes_size(
                    type_name<std::vector<T>>());) if(std::is_pod<T>::value) return v
                            .size()
                    * bytes_size(v.back())
            + sizeof(int) whenmutilsdebug(+typenonce_size);
    else {
        int accum = 0;
        for(auto& e : v)
            accum += bytes_size(e);
        return accum + sizeof(int) whenmutilsdebug(+typenonce_size);
    }
}

/**
 * Sums the size of all elements of this list, plus one int for the number
 * of elements.
 */
template <typename T>
std::size_t bytes_size(const std::list<T>& list) {
    if(std::is_pod<T>::value)
        return list.size() * bytes_size(list.back()) + sizeof(int);
    else {
        int accum = 0;
        for(const auto& e : list)
            accum += bytes_size(e);
        return accum + sizeof(int);
    }
}

/**
 * All the elements of the set, plus one int for the number of elements.
 */
template <typename T>
std::size_t bytes_size(const std::set<T>& s) {
    int size = sizeof(int);
    for(auto& a : s) {
        size += bytes_size(a);
    }
    return size;
}

/**
 * All the elements of the hashset, plus one int for the number of elements.
 */
template <typename T>
std::size_t bytes_size(const std::unordered_set<T>& s) {
    int size = sizeof(int);
    for(auto& a : s) {
        size += bytes_size(a);
    }
    return size;
}

/**
 * All the elements of the hashset, plus one int for the number of elements.
 */
template <typename T>
std::size_t bytes_size(const std::multiset<T>& s) {
    int size = sizeof(int);
    for(auto& a : s) {
        size += bytes_size(a);
    }
    return size;
}

/**
 * All the elements of the hashset, plus one int for the number of elements.
 */
template <typename T>
std::size_t bytes_size(const std::unordered_multiset<T>& s) {
    int size = sizeof(int);
    for(auto& a : s) {
        size += bytes_size(a);
    }
    return size;
}

/**
 * Sums the size of each key and value in the map, plus one int for the
 * number of entries
 */
template <typename K, typename V>
std::size_t bytes_size(const std::map<K, V>& m) {
    int size = sizeof(int);
    for(const auto& p : m) {
        size += bytes_size(p.first);
        size += bytes_size(p.second);
    }
    return size;
}

/**
 * Sums the size of each key and value in the unordered_map, plus one int for the
 * number of entries
 */
template <typename K, typename V>
std::size_t bytes_size(const std::unordered_map<K, V>& m) {
    int size = sizeof(int);
    for(const auto& p : m) {
        size += bytes_size(p.first);
        size += bytes_size(p.second);
    }
    return size;
}

/**
 * Sums the size of each key and value in the multimap, plus one int for the
 * number of entries
 */
template <typename K, typename V>
std::size_t bytes_size(const std::multimap<K, V>& m) {
    int size = sizeof(int);
    for(const auto& p : m) {
        size += bytes_size(p.first);
        size += bytes_size(p.second);
    }
    return size;
}

/**
 * Sums the size of each key and value in the multimap, plus one int for the
 * number of entries
 */
template <typename K, typename V>
std::size_t bytes_size(const std::unordered_multimap<K, V>& m) {
    int size = sizeof(int);
    for(const auto& p : m) {
        size += bytes_size(p.first);
        size += bytes_size(p.second);
    }
    return size;
}

/**
 * Sums the size of each element of the tuple
 */
template <typename... T>
std::size_t bytes_size_helper(const T&... t) {
    return (bytes_size(t) + ... + 0);
}
template <typename... T>
std::size_t bytes_size(const std::tuple<T...>& t) {
    return std::apply(bytes_size_helper<T...>, t);
}

template <typename T>
std::size_t bytes_size(const std::unique_ptr<T>& ptr) {
    return 1 + (ptr ? bytes_size(*ptr) : 0);
}

/**
 * In-place serialization is also sometimes possible.
 * This will take a function that expects buffers to be posted,
 * and will post the object (potentially in multiple buffers)
 * via repeated calls to the function
 */
template <typename F, typename BR, typename... Args>
std::enable_if_t<std::is_pod<BR>::value> post_object(const F& consumer, const BR& br,
                                                     Args&&... args) {
    consumer(std::forward<Args>(args)..., (uint8_t*)&br, sizeof(BR));
}

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const ByteRepresentable& br);

#ifdef MUTILS_DEBUG
/**
 * Calls b.ensure_registered(dm) when b is a ByteRepresentable;
 * returns true when b is POD.
 */
void ensure_registered(ByteRepresentable& b, DeserializationManager& dm);

/**
 * Calls b.ensure_registered(dm) when b is a ByteRepresentable;
 * returns true when b is POD.
 */
template <typename T, restrict(std::is_pod<T>::value)>
void ensure_registered(const T&, DeserializationManager&) {}
#endif

/**
 * calls b.to_bytes(buf) when b is a ByteRepresentable;
 * calls std::memcpy() when b is POD.  Custom logic
 * is implemented for some STL types.  When ubuntu
 * gets GCC5.0 or better, this will also work if
 * b is trivially copyable.
 */
std::size_t to_bytes(const ByteRepresentable& b, uint8_t* buffer);

/**
 * extracts the C string (char*) equivalent to this
 * std::string and stores it in buf
 */
std::size_t to_bytes(const std::string& b, uint8_t* buffer);

/**
 * Calls T::from_bytes(ctx,buf) when T is a ByteRepresentable.
 * uses std::memcpy() when T is a POD.
 * custom logic is implemented for some STL types.
 */
template <typename T>
std::enable_if_t<std::is_base_of<ByteRepresentable CMA T>::value,
                 std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, uint8_t const* buffer) {
    static_assert(!std::is_same<std::decay_t<T>, ByteRepresentable>::value,
                  "Error: must deserialize as implementing type, not as ByteRepresentable");
    return T::from_bytes(ctx, buffer);
}

/**
 * Calls T::from_bytes(ctx,buf) when T is a ByteRepresentable.
 * uses std::memcpy() when T is a POD.
 * custom logic is implemented for some STL types.
 */
template <typename T>
std::enable_if_t<std::is_pod<T>::value, std::unique_ptr<std::decay_t<T>>>
from_bytes(DeserializationManager*, uint8_t const* buffer);

/**
 * Calls T::from_bytes_noalloc(ctx,buf) when T is a ByteRepresentable.
 * returns raw pointer when T is a POD
 * custom logic is implemented for some STL types.
 */
template <typename T>
std::enable_if_t<std::is_base_of<ByteRepresentable CMA std::decay_t<T>>::value,
                 context_ptr<T>>
from_bytes_noalloc(
        DeserializationManager* ctx, uint8_t* buffer,
        context_ptr<std::decay_t<T>> = context_ptr<std::decay_t<T>>{}) {
    return std::decay_t<T>::from_bytes_noalloc(ctx, buffer);
}
template <typename T>
std::enable_if_t<std::is_base_of<ByteRepresentable CMA std::decay_t<T>>::value,
                 context_ptr<T>>
from_bytes_noalloc(
        DeserializationManager* ctx, uint8_t const* const buffer,
        context_ptr<const std::decay_t<T>> = context_ptr<const std::decay_t<T>>{}) {
    if constexpr(std::is_const<T>::value) {
        return std::decay_t<T>::from_bytes_noalloc_const(ctx, buffer);
    } else
        return std::decay_t<T>::from_bytes_noalloc(ctx, buffer);
}

/**
 * Calls T::from_bytes_noalloc(ctx,v) when T is a ByteRepresentable.
 * returns raw pointer when T is a POD
 * custom logic is implemented for some STL types.
 */

template <typename T>
std::enable_if_t<std::is_pod<T>::value, context_ptr<std::decay_t<T>>>
from_bytes_noalloc(DeserializationManager*, uint8_t* buffer,
                   context_ptr<std::decay_t<T>> = context_ptr<std::decay_t<T>>{});

template <typename T>
std::enable_if_t<std::is_pod<T>::value, context_ptr<const std::decay_t<T>>>
from_bytes_noalloc(DeserializationManager*, uint8_t const* const buffer,
                   context_ptr<const std::decay_t<T>> = context_ptr<const std::decay_t<T>>{});

/**
 * Calls mutils::from_bytes_noalloc<T>(ctx,buf), dereferences the result, and
 * passes it to fun.  Returns whatever fun returns.  Memory safe, assuming fun
 * doesn't do something stupid.
 */
template <typename T, typename F>
auto deserialize_and_run(DeserializationManager* dsm, uint8_t* buf, const F& fun);

/**
 * The "marshalled" type is a wrapper for already-serialized types;
 */

struct marshalled : public ByteRepresentable {
    const std::size_t size;
    uint8_t const* const data;

    marshalled(decltype(size) size, decltype(data) data)
            : size(size), data(data) {}

    std::size_t to_bytes(uint8_t* v) const {
        assert(false && "revisit this");
        std::memcpy(v, data, size);
        return size;
    }
    std::size_t bytes_size() const { return size; }

#ifdef MUTILS_DEBUG

    void ensure_registered(DeserializationManager&) {}

#endif

    template <typename DSM>
    static std::unique_ptr<marshalled> from_bytes(DSM const* const,
                                                  uint8_t const* const) {
        static_assert(std::is_same<DSM, void>::value && !std::is_same<DSM, void>::value,
                      "Do not deserialize into a marshalled. please.");
        return nullptr;
    }

    static context_ptr<marshalled>
    from_bytes_noalloc(DeserializationManager const* const, uint8_t* v);
};

/**
 * Serialization is also implemented for the following STL types:
 * vector
 * pair
 * string
 * set
 */

// end forward-declaring; everything past this point is implementation,
// and not essential to understanding  the interface.

// Templates that become true_type when matched to the thing they identify,
// or become false_type if they fail to match, similar to std::is_pod

template <typename>
struct is_pair : std::false_type {};

template <typename T, typename U>
struct is_pair<std::pair<T, U>> : std::true_type {};

template <typename T, typename U>
struct is_pair<const std::pair<T, U>> : std::true_type {};

/* use the definition in mutils/tuple_extras.hpp +21
template <typename> struct is_tuple : std::false_type {};

template <typename...T>
struct is_tuple<std::tuple<T...>> : std::true_type {};
*/

template <typename>
struct is_list : std::false_type {};

template <typename T>
struct is_list<std::list<T>> : std::true_type {};

template <typename T>
struct is_list<const std::list<T>> : std::true_type {};

template <typename T>
struct is_set<std::unordered_set<T>> : std::true_type {};

template <typename T>
struct is_set<std::multiset<T>> : std::true_type {};

template <typename T>
struct is_set<std::unordered_multiset<T>> : std::true_type {};

template <typename>
struct is_map : std::false_type {};

template <typename K, typename V>
struct is_map<std::map<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_map<const std::map<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_map<std::multimap<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_map<const std::multimap<K, V>> : std::true_type {};

template <typename>
struct is_unordered_map : std::false_type {};

template <typename K, typename V>
struct is_unordered_map<std::unordered_map<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_unordered_map<const std::unordered_map<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_unordered_map<std::unordered_multimap<K, V>> : std::true_type {};

template <typename K, typename V>
struct is_unordered_map<const std::unordered_multimap<K, V>> : std::true_type {};

template <typename>
struct is_string : std::false_type {};

template <>
struct is_string<std::string> : std::true_type {};

template <>
struct is_string<const std::string> : std::true_type {};

/**
 * Constructs a buffer-consuming function that will copy its input to the
 * provided destination buffer at the specified index. The created function
 * can be used as an input to post_object to make post_object serialize the
 * object to a buffer.
 * @param index The offset within dest_buf at which the function should copy
 * inputs
 * @param dest_buf The buffer that should receive bytes read by the function
 * @return A function that consumes a byte buffer and writes it to dest_buf
 */
std::function<void(uint8_t const* const, std::size_t)>
post_to_buffer(std::size_t& index, uint8_t* dest_buf);

// Forward declarations of post_object functions for STL types

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::string& str);

template <typename T, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::pair<T, V>& pair);

template <typename... T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::tuple<T...>& t);

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::vector<bool>& vec);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::vector<T>& vec);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::list<T>& list);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::set<T>& s);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_set<T>& s);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::multiset<T>& s);

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_multiset<T>& s);

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::map<K, V>& map);

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::multimap<K, V>& map);

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_map<K, V>& map);

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_multimap<K, V>& map);

// Forward declarations of to_bytes functions for STL types

template <typename T, restrict(std::is_pod<T>::value)>
std::size_t to_bytes(const T& t, uint8_t* buffer);

std::size_t to_bytes(const std::vector<bool>& vec, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::vector<T>& vec, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::list<T>& list, uint8_t* buffer);

template <typename T, typename V>
std::size_t to_bytes(const std::pair<T, V>& pair, uint8_t* buffer);

template <typename... T>
std::size_t to_bytes(const std::tuple<T...>& tuple, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::set<T>& s, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::unordered_set<T>& s, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::multiset<T>& s, uint8_t* buffer);

template <typename T>
std::size_t to_bytes(const std::unordered_multiset<T>& s, uint8_t* buffer);

template <typename K, typename V>
std::size_t to_bytes(const std::map<K, V>& m, uint8_t* buffer);

template <typename K, typename V>
std::size_t to_bytes(const std::multimap<K, V>& m, uint8_t* buffer);

template <typename K, typename V>
std::size_t to_bytes(const std::unordered_map<K, V>& m, uint8_t* buffer);

template <typename K, typename V>
std::size_t to_bytes(const std::unordered_multimap<K, V>& m, uint8_t* buffer);

// Forward declarations of from_bytes functions for STL types

template <typename T>
std::unique_ptr<type_check<is_string, T>> from_bytes(DeserializationManager*,
                                                     uint8_t const* v);

template <typename T>
context_ptr<type_check<is_string, T>>
from_bytes_noalloc(DeserializationManager*, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_string<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager*, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

// Note that remove_cv_t is needed here because is_set won't match const set,
// whereas is_string does match const string
template <typename T>
std::enable_if_t<is_set<std::remove_cv_t<T>>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, const uint8_t* _buffer);

template <typename T>
context_ptr<type_check<is_set, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_set<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

template <typename T>
std::unique_ptr<type_check<is_pair, T>> from_bytes(DeserializationManager* ctx,
                                                   const uint8_t* buffer);
template <typename L>
std::unique_ptr<type_check<is_list, L>> from_bytes(DeserializationManager* ctx,
                                                   const uint8_t* buffer);

template <typename T>
context_ptr<type_check<is_pair, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_pair<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

template <typename T>
context_ptr<type_check<is_tuple, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_tuple<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

template <typename T>
std::unique_ptr<T> boolvec_from_bytes(DeserializationManager* ctx,
                                      uint8_t const* v);

template <typename T>
std::unique_ptr<type_check<is_tuple, T>> from_bytes(DeserializationManager* ctx, uint8_t const* buffer);

template <typename T>
std::enable_if_t<is_vector<std::remove_cv_t<T>>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, uint8_t const* buffer);

template <typename T>
context_ptr<type_check<is_vector, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_vector<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

template <typename T>
std::enable_if_t<is_map<T>::value || is_unordered_map<T>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, uint8_t const* buffer);

template <typename T>
context_ptr<type_check<is_map, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_map<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

template <typename T>
context_ptr<type_check<is_unordered_map, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T> = context_ptr<T>{});

template <typename T>
std::enable_if_t<is_unordered_map<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T> = context_ptr<const T>{});

// End forward declarations of STL support

// Implementations of post_object functions for STL types

template <typename T, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::pair<T, V>& pair) {
    post_object(consumer, pair.first);
    post_object(consumer, pair.second);
}

template <typename... T>
void post_object_helper(
        const std::function<void(uint8_t const* const, std::size_t)>& f,
        const T&... t) {
    (post_object(f, t), ...);
}

template <typename... T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::tuple<T...>& t) {
    // std::apply(std::bind(post_object_helper<T...>,f,/*variadic template?*/), t);
    std::apply([consumer](T... args) { post_object_helper(consumer, args...); }, t);
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::vector<T>& vec) {
    whenmutilsdebug(post_object(f, type_name<std::vector<T>>());) int size = vec.size();
    consumer((uint8_t*)&size, sizeof(size));
    if(std::is_pod<T>::value) {
        std::size_t size = vec.size() * bytes_size(vec.back());
        consumer((uint8_t*)vec.data(), size);
    } else {
        for(const auto& e : vec) {
            post_object(consumer, e);
        }
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::list<T>& list) {
    int size = list.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& e : list) {
        post_object(consumer, e);
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::set<T>& s) {
    int size = s.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& a : s) {
        post_object(consumer, a);
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_set<T>& s) {
    int size = s.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& a : s) {
        post_object(consumer, a);
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::multiset<T>& s) {
    int size = s.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& a : s) {
        post_object(consumer, a);
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_multiset<T>& s) {
    int size = s.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& a : s) {
        post_object(consumer, a);
    }
}

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::map<K, V>& map) {
    int size = map.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& pair : map) {
        post_object(consumer, pair.first);
        post_object(consumer, pair.second);
    }
}

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::multimap<K, V>& map) {
    int size = map.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& pair : map) {
        post_object(consumer, pair.first);
        post_object(consumer, pair.second);
    }
}

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_map<K, V>& map) {
    int size = map.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& pair : map) {
        post_object(consumer, pair.first);
        post_object(consumer, pair.second);
    }
}

template <typename K, typename V>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer,
                 const std::unordered_multimap<K, V>& map) {
    int size = map.size();
    consumer((uint8_t*)&size, sizeof(size));
    for(const auto& pair : map) {
        post_object(consumer, pair.first);
        post_object(consumer, pair.second);
    }
}

template <typename T>
void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f,
                 const std::unique_ptr<T>& ptr) {
    bool has_value(ptr);
    f((uint8_t*)&has_value, sizeof(has_value));
    if(has_value) {
        post_object(f, *ptr);
    }
}

// end post_object section

// Implementation of to_bytes functions for STL types
// To reduce code duplication, these are all implemented in terms of post_object

/**
 * Special to_bytes for POD types, which just uses memcpy
 */
template <typename T, typename>
std::size_t to_bytes(const T& t, uint8_t* buffer) {
    auto res = std::memcpy(buffer, &t, sizeof(T));
    assert(res);
    (void)res;
    return sizeof(T);
}

template <typename T>
std::size_t to_bytes(const std::vector<T>& vec, uint8_t* buffer) {
    int vector_size = vec.size();
    std::size_t bsize = to_bytes(vector_size, buffer);
    for (const auto& e: vec) {
        bsize += to_bytes(e,buffer+bsize);
    }
    return bsize;
}

template <typename T>
std::size_t to_bytes(const std::list<T>& list, uint8_t* buffer) {
    int list_size = list.size();
    std::size_t bsize = to_bytes(list_size, buffer);
    for (const auto& e: list) {
        bsize += to_bytes(e,buffer+bsize);
    }
    return bsize;
}

template <typename T, typename V>
std::size_t to_bytes(const std::pair<T, V>& pair, uint8_t* buffer) {
    std::size_t bsize = 0;
    bsize += to_bytes(pair.first,buffer+bsize);
    bsize += to_bytes(pair.second,buffer+bsize);
    return bsize;
}

template <typename T>
void to_bytes_helper1(uint8_t* buffer,std::size_t& offset,const T& t) {
    offset += to_bytes(t,buffer+offset);
}

template <typename... T>
std::size_t to_bytes_helper(uint8_t* buffer,const T&... t) {
    std::size_t bsize = 0;
    (to_bytes_helper1(buffer,bsize,t), ...);
    return bsize;
}

template <typename... T>
std::size_t to_bytes(const std::tuple<T...>& tuple, uint8_t* buffer) {
    return std::apply([buffer](T... args){return to_bytes_helper(buffer,args...);}, tuple);
}

template <typename T>
std::size_t to_bytes(const std::set<T>& s, uint8_t* buffer) {
    int set_size = s.size();
    std::size_t bsize = to_bytes(set_size,buffer);
    for (const auto& e: s) {
        bsize += to_bytes(e, buffer+bsize);
    }
    return bsize;
}

template <typename T>
std::size_t to_bytes(const std::unordered_set<T>& s, uint8_t* buffer) {
    int set_size = s.size();
    std::size_t bsize = to_bytes(set_size,buffer);
    for (const auto& e: s) {
        bsize += to_bytes(e, buffer+bsize);
    }
    return bsize;
}

template <typename T>
std::size_t to_bytes(const std::multiset<T>& s, uint8_t* buffer) {
    int set_size = s.size();
    std::size_t bsize = to_bytes(set_size,buffer);
    for (const auto& e: s) {
        bsize += to_bytes(e, buffer+bsize);
    }
    return bsize;
}

template <typename T>
std::size_t to_bytes(const std::unordered_multiset<T>& s, uint8_t* buffer) {
    int set_size = s.size();
    std::size_t bsize = to_bytes(set_size,buffer);
    for (const auto& e: s) {
        bsize += to_bytes(e, buffer+bsize);
    }
    return bsize;
}

template <typename K, typename V>
std::size_t to_bytes(const std::map<K, V>& m, uint8_t* buffer) {
    int map_size = m.size();
    std::size_t bsize = to_bytes(map_size,buffer);
    for (const auto& e: m) {
        bsize += to_bytes(e.first,buffer+bsize);
        bsize += to_bytes(e.second,buffer+bsize);
    }
    return bsize;
}

template <typename K, typename V>
std::size_t to_bytes(const std::multimap<K, V>& m, uint8_t* buffer) {
    int map_size = m.size();
    std::size_t bsize = to_bytes(map_size,buffer);
    for (const auto& e: m) {
        bsize += to_bytes(e.first,buffer+bsize);
        bsize += to_bytes(e.second,buffer+bsize);
    }
    return bsize;
}

template <typename K, typename V>
std::size_t to_bytes(const std::unordered_map<K, V>& m, uint8_t* buffer) {
    int map_size = m.size();
    std::size_t bsize = to_bytes(map_size,buffer);
    for (const auto& e: m) {
        bsize += to_bytes(e.first,buffer+bsize);
        bsize += to_bytes(e.second,buffer+bsize);
    }
    return bsize;
}

template <typename K, typename V>
std::size_t to_bytes(const std::unordered_multimap<K, V>& m, uint8_t* buffer) {
    int map_size = m.size();
    std::size_t bsize = to_bytes(map_size,buffer);
    for (const auto& e: m) {
        bsize += to_bytes(e.first,buffer+bsize);
        bsize += to_bytes(e.second,buffer+bsize);
    }
    return bsize;
}

template <typename T>
std::size_t to_bytes(const std::unique_ptr<T>& ptr, uint8_t* buffer) {
    std::size_t index = 0;
    post_object(post_to_buffer(index, buffer), ptr);
    return bytes_size(ptr);
}
// end to_bytes section

#ifdef MUTILS_DEBUG
// ensure_registered definitions -- these could go anywhere since they don't
// depend on any other functions
void ensure_registered(const std::vector<bool>& v, DeserializationManager& dm);
template <typename T>
void ensure_registered(const std::vector<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename L, typename R>
void ensure_registered(const std::pair<L, R>& v, DeserializationManager& dm) {
    ensure_registered(v.first, dm);
    ensure_registered(v.second, dm);
}

template <typename T>
void ensure_registered(const std::set<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename T>
void ensure_registered(const std::unordered_set<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename T>
void ensure_registered(const std::multiset<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename T>
void ensure_registered(const std::unordered_multiset<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename T>
void ensure_registered(const std::list<T>& v, DeserializationManager& dm) {
    for(auto& e : v)
        ensure_registered(e, dm);
}

template <typename T>
void ensure_registered(const std::unique_ptr<T>& ptr, DeserializationManager& dm) {
    if(ptr)
        ensure_registered(*ptr, dm);
}
// end ensure_registered section
#endif

// from_string definition

template <typename T>
std::unique_ptr<type_check<std::is_integral, T>>
from_string(DeserializationManager*, char const* buffer, std::size_t length) {
    return std::make_unique<T>(std::stoll(std::string{buffer, length}));
}

template <typename T>
std::unique_ptr<type_check<std::is_floating_point, T>>
from_string(DeserializationManager*, char const* buffer, std::size_t length) {
    return std::make_unique<T>(std::stold(std::string{buffer, length}));
}

template <typename T>
std::unique_ptr<type_check<is_string, T>>
from_string(DeserializationManager*, char const* buffer, std::size_t length) {
    return std::make_unique<T>(std::string{buffer, length});
}

// Implementation of from_bytes functions for STL types

template <typename T>
std::enable_if_t<std::is_pod<T>::value, std::unique_ptr<std::decay_t<T>>>
from_bytes(DeserializationManager*, uint8_t const* buffer) {
    using T2 = std::decay_t<T>;
    if(buffer) {
        auto pod = std::make_unique<T2>(*(T2*)buffer);
        // std::memcpy(t.get(),v,sizeof(T));
#if __GNUC_PREREQ(9, 0)
        return pod;  // RVO optimization is default for
#else
        return std::move(pod);
#endif
    } else
        return nullptr;
}

template <typename T>
std::enable_if_t<std::is_pod<T>::value, context_ptr<std::decay_t<T>>>
from_bytes_noalloc(DeserializationManager*, uint8_t* buffer,
                   context_ptr<std::decay_t<T>>) {
    using T2 = std::decay_t<T>;
    return context_ptr<T2>{(T2*)buffer};
}

template <typename T>
std::enable_if_t<std::is_pod<T>::value, context_ptr<const std::decay_t<T>>>
from_bytes_noalloc(DeserializationManager*, uint8_t const* const buffer,
                   context_ptr<const std::decay_t<T>>) {
    using T2 = std::decay_t<T>;
    return context_ptr<const T2>{(const T2*)buffer};
}

template <typename T>
std::unique_ptr<type_check<is_string, T>> from_bytes(DeserializationManager*,
                                                     uint8_t const* buffer) {
    assert(buffer);
    // This reinterpret_cast is safe because the buffer actually contains a null-terminated C-string
    return std::make_unique<T>(reinterpret_cast<char const*>(buffer));
}

template <typename T>
context_ptr<type_check<is_string, T>>
from_bytes_noalloc(DeserializationManager*, uint8_t* buffer,
                   context_ptr<T>) {
    assert(buffer);
    return context_ptr<T>(new std::string{reinterpret_cast<char*>(buffer)});
}

template <typename T>
std::enable_if_t<is_string<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager*, uint8_t const* const buffer,
                   context_ptr<const T>) {
    assert(buffer);
    return context_ptr<const T>(new std::string{reinterpret_cast<char const*>(buffer)});
}

template <typename T>
std::enable_if_t<is_set<std::remove_cv_t<T>>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, const uint8_t* _buffer) {
    int size = ((int*)_buffer)[0];
    const uint8_t* buffer = _buffer + sizeof(int);
    auto r = std::make_unique<std::remove_cv_t<T>>();
    for(int i = 0; i < size; ++i) {
        auto e = from_bytes<typename T::key_type>(ctx, buffer);
        buffer += bytes_size(*e);
        r->insert(*e);
    }
#if __GNUC_PREREQ(9, 0)
    return r;
#else
    return std::move(r);
#endif
}

template <typename T>
context_ptr<type_check<is_set, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T>) {
    return context_ptr<T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_set<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::unique_ptr<type_check<is_pair, T>> from_bytes(DeserializationManager* ctx,
                                                   const uint8_t* buffer) {
    using ft = typename T::first_type;
    using st = typename T::second_type;
    auto fst = from_bytes_noalloc<const ft>(ctx, buffer);
    return std::make_unique<std::pair<ft, st>>(
            *fst, *from_bytes_noalloc<const st>(ctx, buffer + bytes_size(*fst)));
}

template <typename L>
std::unique_ptr<type_check<is_list, L>> from_bytes(DeserializationManager* ctx,
                                                   const uint8_t* buffer) {
    using elem = typename L::value_type;
    int size = ((int*)buffer)[0];
    const uint8_t* buf_ptr = buffer + sizeof(int);
    std::unique_ptr<std::list<elem>> return_list{new L()};
    for(int i = 0; i < size; ++i) {
        context_ptr<const elem> item = from_bytes_noalloc<const elem>(ctx, buf_ptr, context_ptr<const elem>{});
        buf_ptr += bytes_size(*item);
        return_list->push_back(*item);
    }
#if __GNUC_PREREQ(9, 0)
    return return_list;
#else
    return std::move(return_list);
#endif
}

template <typename L>
context_ptr<type_check<is_list, L>>
from_bytes_noalloc(DeserializationManager* ctx, const uint8_t* buffer,
                   context_ptr<L> = context_ptr<L>{}) {
    return context_ptr<L>{from_bytes<std::decay_t<L>>(ctx, buffer).release()};
}

template <typename T>
context_ptr<type_check<is_pair, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T>) {
    return context_ptr<T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_pair<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename TupleType, std::size_t N>
auto from_bytes_helper(DeserializationManager* ctx, uint8_t const* buffer) {
    using ElementType = typename std::tuple_element<N, TupleType>::type;
    ElementType e(*from_bytes<ElementType>(ctx, buffer));
    auto t = std::make_tuple<ElementType>(std::move(e));
    if constexpr((N + 1) == std::tuple_size<TupleType>::value) {
        return t;
    } else {
        return std::tuple_cat(t, from_bytes_helper<TupleType, N + 1>(ctx, buffer + bytes_size(std::get<0>(t))));
    }
}

template <typename T>
std::unique_ptr<type_check<is_tuple, T>> from_bytes(DeserializationManager* ctx, uint8_t const* buffer) {
    return std::make_unique<T>(std::move(from_bytes_helper<T, 0>(ctx, buffer)));
}

template <typename T>
context_ptr<type_check<is_tuple, T>> from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                                                        context_ptr<T>) {
    return context_ptr<T>{new T(std::move(from_bytes_helper<T, 0>(ctx, buffer)))};
}

template <typename T>
std::enable_if_t<is_tuple<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{new T(std::move(from_bytes_helper<T, 0>(ctx, buffer)))};
}

// Note: T is the type of the vector, not the vector's type parameter T
template <typename T>
std::enable_if_t<is_vector<std::remove_cv_t<T>>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, uint8_t const* buffer) {
#ifdef MUTILS_DEBUG
    const static std::string typenonce = type_name<T>();
    const auto typenonce_size = bytes_size(typenonce);
    auto remote_string = *from_bytes<std::string>(ctx, buffer);
    if(typenonce != remote_string) {
        std::cout << typenonce << std::endl
                  << std::endl;
        std::cout << remote_string << std::endl;
    }
    assert(typenonce == buffer);
    assert(typenonce == remote_string);
    buffer += typenonce_size;
#endif
    using member = typename T::value_type;
    if constexpr(std::is_same<bool, member>::value) {
        return boolvec_from_bytes<T>(ctx, buffer);
    } else if constexpr(std::is_pod<member>::value && !std::is_same<bool, member>::value) {
        member const* const start = (member*)(buffer + sizeof(int));
        const int size = ((int*)buffer)[0];
        return std::unique_ptr<T>{new T{start, start + size}};
    } else {
        int size = ((int*)buffer)[0];
        auto* buffer2 = buffer + sizeof(int);
        std::size_t accumulated_offset = 0;
        std::unique_ptr<std::remove_cv_t<T>> accum{new std::remove_cv_t<T>()};
        for(int i = 0; i < size; ++i) {
            std::unique_ptr<member> item = from_bytes<member>(ctx, buffer2 + accumulated_offset);
            accumulated_offset += bytes_size(*item);
            accum->push_back(*item);
        }
        return accum;
    }
}

template <typename T>
context_ptr<type_check<is_vector, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T>) {
    return context_ptr<T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_vector<std::remove_cv_t<T>>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_map<T>::value || is_unordered_map<T>::value, std::unique_ptr<T>>
from_bytes(DeserializationManager* ctx, uint8_t const* buffer) {
    using key_t = typename T::key_type;
    using value_t = typename T::mapped_type;
    int size = ((int*)buffer)[0];
    const uint8_t* buf_ptr = buffer + sizeof(int);

    // T could be const std::map, but we don't want to create a const map here
    auto new_map = std::make_unique<std::remove_cv_t<T>>();
    for(int i = 0; i < size; ++i) {
        auto key = from_bytes_noalloc<const key_t>(ctx, buf_ptr);
        buf_ptr += bytes_size(*key);
        auto value = from_bytes_noalloc<const value_t>(ctx, buf_ptr);
        buf_ptr += bytes_size(*value);
        new_map->emplace(*key, *value);
    }
#if __GNUC_PREREQ(9, 0)
    return new_map;  // RVO
#else
    return std::move(new_map);
#endif
}

template <typename T>
context_ptr<type_check<is_map, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T>) {
    return context_ptr<T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_map<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
context_ptr<type_check<is_unordered_map, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t* buffer,
                   context_ptr<T>) {
    return context_ptr<T>{from_bytes<T>(ctx, buffer).release()};
}

template <typename T>
std::enable_if_t<is_unordered_map<T>::value, context_ptr<const T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* const buffer,
                   context_ptr<const T>) {
    return context_ptr<const T>{from_bytes<T>(ctx, buffer).release()};
}

/* Note that from_bytes<unique_ptr<X>> will return a unique_ptr<unique_ptr<X>>,
 * which is probably not what you want. This is necessary to maintain compatibility
 * with the std::set and std::map deserializers, which dereference each
 * from_bytes<value_t> before inserting it. */
template <typename T>
std::unique_ptr<type_check<is_unique_ptr, T>> from_bytes(DeserializationManager* ctx, uint8_t const* buffer) {
    using value_t = typename T::element_type;
    bool is_valid = ((bool*)buffer)[0];
    const uint8_t* buf_ptr = buffer + sizeof(bool);
    if(is_valid) {
        return std::make_unique<T>(from_bytes<value_t>(ctx, buf_ptr));
    } else {
        return std::make_unique<T>();
    }
}

template <typename T>
context_ptr<type_check<is_unique_ptr, T>>
from_bytes_noalloc(DeserializationManager* ctx, uint8_t const* buffer,
                   context_ptr<T> = context_ptr<T>{}) {
    using value_t = typename T::element_type;
    bool is_valid = ((bool*)buffer)[0];
    const uint8_t* buf_ptr = buffer + sizeof(bool);
    context_ptr<T> ret{new T()};
    if(is_valid) {
        *ret = from_bytes<value_t>(ctx, buf_ptr);
    } else {
        *ret = std::make_unique<value_t>();
    }
    return ret;
}

/**
   For Serializing and Deserializing many objects at once.
   The deserialization expects unique_ptr references; this
   is important!
   Also the buffer is at the beginning! This is important.
 */

std::size_t to_bytes_v(uint8_t*);

template <typename T, typename... Rest>
std::size_t to_bytes_v(uint8_t* buf, const T& first, const Rest&... rest) {
    auto size = to_bytes(first, buf);
    return size + to_bytes_v(buf + size, rest...);
}

std::size_t from_bytes_v(DeserializationManager*, uint8_t const* const);

template <typename T, typename... Rest>
std::size_t from_bytes_v(DeserializationManager* dsm, uint8_t const* const buf,
                         std::unique_ptr<T>& first, Rest&... rest) {
    first = from_bytes<T>(dsm, buf);
    auto size = bytes_size(*first);
    return size + from_bytes_v(dsm, buf + size, rest...);
}

std::size_t from_bytes_noalloc_v(DeserializationManager*, uint8_t const* const);

template <typename T, typename... Rest>
std::size_t from_bytes_noalloc_v_nc(DeserializationManager* dsm, uint8_t* buf,
                                    context_ptr<T>& first,
                                    context_ptr<Rest>&... rest) {
    first = from_bytes_noalloc<T>(dsm, buf, context_ptr<T>{});
    auto size = bytes_size(*first);
    return size + from_bytes_noalloc_v(dsm, buf + size, rest...);
}

template <typename T, typename... Rest>
std::size_t from_bytes_noalloc_v(DeserializationManager* dsm, uint8_t* buf,
                                 context_ptr<T>& first,
                                 context_ptr<Rest>&... rest) {
    return from_bytes_noalloc_v_nc(dsm, buf, first, rest...);
}

template <typename T, typename... Rest>
std::size_t from_bytes_noalloc_v(DeserializationManager* dsm,
                                 uint8_t const* const buf,
                                 context_ptr<const T>& first,
                                 context_ptr<const Rest>&... rest) {
    first = from_bytes_noalloc<const T>(dsm, buf, context_ptr<const T>{});
    auto size = bytes_size(*first);
    return size + from_bytes_noalloc_v(dsm, buf + size, rest...);
}

// sample of how this might work.  Nocopy, plus complete memory safety, but
// at the cost of callback land.

struct dsr_info {
    std::chrono::nanoseconds to_callfunc;
    std::chrono::nanoseconds to_exit;
    typename std::chrono::high_resolution_clock::time_point start_time;
};
#ifdef SERIALIZATION_STATS
inline auto& get_dsr_info() {
    static thread_local dsr_info ret;
    return ret;
}
#endif
template <typename F, typename R, typename... Args>
auto deserialize_and_run(DeserializationManager* dsm, uint8_t const* const buffer,
                         const F& fun, std::function<R(Args...)> const* const) {
#ifdef SERIALIZATION_STATS
    using namespace std;
    using namespace chrono;
    static thread_local auto deserialize_and_run_start = high_resolution_clock::now();
    static thread_local auto callfunc_time = deserialize_and_run_start;
    struct on_function_end {
        ~on_function_end() {
            auto& r = get_dsr_info();
            r.start_time = deserialize_and_run_start;
            r.to_callfunc = callfunc_time - deserialize_and_run_start;
            r.to_exit = high_resolution_clock::now() - deserialize_and_run_start;
        }
    };
    on_function_end ofe;
    deserialize_and_run_start = high_resolution_clock::now();
#endif
    using result_t = std::result_of_t<F(Args...)>;
    static_assert(std::is_same<result_t, R>::value,
                  "Error: function types mismatch.");
    using fun_t = std::function<result_t(Args...)>;
    // ensure implicit conversion can run
    static_assert(
            std::is_convertible<F, fun_t>::value,
            "Error: type mismatch on function and target deserialialized type");
    std::tuple<DeserializationManager*, const uint8_t*,
               context_ptr<const std::decay_t<Args>>...>
            args_tuple;
    std::get<0>(args_tuple) = dsm;
    std::get<1>(args_tuple) = buffer;
    // Make a pointer to from_bytes_noalloc_v with concrete argument types, to
    // help the compiler
    using from_bytes_type = std::size_t (*)(DeserializationManager*, const uint8_t*,
                                            context_ptr<const std::decay_t<Args>>&...);
    from_bytes_type from_bytes_noalloc_concrete =
            [](DeserializationManager* d, const uint8_t* c,
               context_ptr<const std::decay_t<Args>>&... args) {
                return from_bytes_noalloc_v(d, c, args...);
            };
    // Unmarshall fun's arguments into the context_ptrs
    /*auto size = */ callFunc(from_bytes_noalloc_concrete, args_tuple);
    // Call fun, but ignore the first two arguments in args_tuple
    return callFunc(
            [&fun](const auto&, const auto&, auto&... ctx_ptrs) {
#ifdef SERIALIZATION_STATS
                callfunc_time = high_resolution_clock::now();
#endif
                return fun(*ctx_ptrs...);
            },
            args_tuple);
}

template <typename F>
auto deserialize_and_run(DeserializationManager* dsm, uint8_t const* const buffer,
                         const F& fun) {
    using fun_t = std::decay_t<decltype(convert(fun))>;
    return deserialize_and_run<F>(dsm, buffer, fun, (fun_t*)nullptr);
}

}  // namespace mutils
