/**
 * @file test_objects.h
 *
 * Some ReplicatedObject definitions used by typed_subgroup_test and variants.
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
struct Foo {
    int state;

    int read_state() {
        return state;
    }
    bool change_state(int new_state) {
        if(new_state == state) {
            return false;
        }
        state = new_state;
        return true;
    }

    REGISTER_RPC_FUNCTIONS(Foo, read_state, change_state);
    /**
     * Constructs a Foo with an initial value.
     * @param initial_state
     */
    Foo(int initial_state = 0) : state(initial_state) {}
    Foo() = default;
    Foo(const Foo&) = default;
};

class Bar : public mutils::ByteRepresentable {
    std::string log;

public:
    void append(const std::string& words) {
        log += words;
    }
    void clear() {
        log.clear();
    }
    std::string print() {
        return log;
    }

    REGISTER_RPC_FUNCTIONS(Bar, append, clear, print);

    DEFAULT_SERIALIZATION_SUPPORT(Bar, log);
    Bar(const std::string& s = "") : log(s) {}
};

class Cache : public mutils::ByteRepresentable {
    std::map<std::string, std::string> cache_map;

public:
    void put(const std::string& key, const std::string& value) {
        cache_map[key] = value;
    }
    std::string get(const std::string& key) {
        return cache_map[key];
    }
    bool contains(const std::string& key) {
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

    REGISTER_RPC_FUNCTIONS(Cache, put, get, contains, invalidate);

    Cache() : cache_map() {}
    /**
     * This constructor is required by default serialization support, in order
     * to reconstruct an object after deserialization.
     * @param cache_map The state of the cache.
     */
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};
