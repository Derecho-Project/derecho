/**
 * @file container_ostreams.hpp
 *
 * Provides implementations of the ostream operator<< for some STL containers,
 * so they can be displayed in logging and console statements. Note that mutils
 * provides an operator<< for std::vector, which is why we don't write one here.
 */
#pragma once

#include <map>
#include <ostream>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace std {

/**
 * Writes a string representation of a std::set to an output stream.
 * Assumes the element type of the set has an operator<< implementation.
 */
template <typename T>
std::ostream& operator<<(std::ostream& out, const std::set<T>& s) {
    out << "{";
    auto s_iter = s.begin();
    auto last_item = s.empty() ? s.end() : std::prev(s.end());
    while(s_iter != last_item) {
        out << *s_iter << ", ";
        s_iter++;
    }
    out << *s_iter << "}";
    return out;
}

/**
 * Writes a string representation of a std::unordered_set to an output stream.
 * Assumes the element type of the set has an operator<< implementation.
 */
template <typename T>
std::ostream& operator<<(std::ostream& out, const std::unordered_set<T>& s) {
    out << "{";
    auto s_iter = s.begin();
    auto last_item = s.empty() ? s.end() : std::prev(s.end());
    while(s_iter != last_item) {
        out << *s_iter << ", ";
        s_iter++;
    }
    out << *s_iter << "}";
    return out;
}

/**
 * Writes a string representation of a std::map to an output stream.
 * Assumes each entry in the map has an operator<< implementation for
 * the key and value types.
 */
template <typename K, typename V>
std::ostream& operator<<(std::ostream& out, const std::map<K, V>& m) {
    out << "{";
    auto map_iter = m.begin();
    auto last_item = m.empty() ? m.end() : std::prev(m.end());
    while(map_iter != last_item) {
        out << "(" << map_iter->first << " => " << map_iter->second << "), ";
        map_iter++;
    }
    out << "(" << map_iter->first << " => " << map_iter->second << ")}";
    return out;
}

/**
 * Writes a string representation of a std::unordered_map to an output stream.
 * Assumes each entry in the map has an operator<< implementation for the key
 * and value types.
 */
template <typename K, typename V>
std::ostream& operator<<(std::ostream& out, const std::unordered_map<K, V>& m) {
    out << "{";
    auto map_iter = m.begin();
    auto last_item = m.empty() ? m.end() : std::prev(m.end());
    while(map_iter != last_item) {
        out << "(" << map_iter->first << " => " << map_iter->second << "), ";
        map_iter++;
    }
    out << "(" << map_iter->first << " => " << map_iter->second << ")}";
    return out;
}

}  // namespace std