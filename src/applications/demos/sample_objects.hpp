/**
 * @file sample_objects.h
 *
 * Some ReplicatedObject definitions used by demonstration programs.
 */

#pragma once
#include <map>
#include <memory>
#include <string>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

/*
 * The Eclipse CDT parser crashes if it tries to expand the REGISTER_RPC_FUNCTIONS
 * macro, probably because there are too many layers of variadic argument expansion.
 * This definition makes the RPC macros no-ops when the CDT parser tries to expand
 * them, which allows it to continue syntax-highlighting the rest of the file.
 */
#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

/**
 * Example replicated object, containing some serializable state and providing
 * two RPC methods.
 */
struct Foo : public mutils::ByteRepresentable {

    int state;

    int read_state() const {
        return state;
    }
    bool change_state(const int& new_state) {
        if(new_state == state) {
            return false;
        }
        state = new_state;
        return true;
    }

    /**
     * Constructs a Foo with an initial value. Also needed by serialization.
     * @param initial_state
     */
    Foo(int initial_state = 0) : state(initial_state) {}
    Foo(const Foo&) = default;

    DEFAULT_SERIALIZATION_SUPPORT(Foo, state);
    REGISTER_RPC_FUNCTIONS(Foo, P2P_TARGETS(read_state), ORDERED_TARGETS(read_state, change_state))
};

/**
 * Another example replicated object, where the serializable state is not a POD.
 */
class Bar : public mutils::ByteRepresentable {
    std::string log;

public:
    void append(const std::string& words) {
        log += words;
    }
    void clear() {
        log.clear();
    }
    std::string print() const {
        return log;
    }

    /**
     * Constructs a Bar with an initial value for its string state.
     * Required by serialization support.
     * @param s The initial state string to store in this Bar object.
     */
    Bar(const std::string& s = "") : log(s) {}

    DEFAULT_SERIALIZATION_SUPPORT(Bar, log);
    REGISTER_RPC_FUNCTIONS(Bar, ORDERED_TARGETS(append, clear, print));
};

/**
 * An example replicated object formatted like a key-value cache, where both
 * keys and values are strings.
 */
class Cache : public mutils::ByteRepresentable {
    std::map<std::string, std::string> cache_map;

public:
    void put(const std::string& key, const std::string& value) {
        cache_map[key] = value;
    }
    std::string get(const std::string& key) const {
        return cache_map.at(key);
    }
    bool contains(const std::string& key) const {
        return cache_map.find(key) != cache_map.end();
    }
    bool invalidate(const std::string& key) {
        auto key_pos = cache_map.find(key);
        if(key_pos == cache_map.end()) {
            return false;
        }
        cache_map.erase(key_pos);
        return true;
    }


    Cache() : cache_map() {}
    /**
     * This constructor is required by default serialization support, in order
     * to reconstruct an object after deserialization.
     * @param cache_map The state of the cache.
     */
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    REGISTER_RPC_FUNCTIONS(Cache, ORDERED_TARGETS(put, get, invalidate, contains), P2P_TARGETS(get, contains));
    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};
