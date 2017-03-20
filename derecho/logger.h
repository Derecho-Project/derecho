#pragma once

#include <chrono>
#include <vector>
#include <mutex>
#include <string>
#include <sstream>
#include <type_traits>
#include <utility>

namespace derecho {

/**
 * General-purpose operator<< overload for ostringstreams that forces them to
 * return their correct subtype instead of ostream&. This enables Logger to
 * cleanly accept stream-constructed messages.
 * @param out The ostringstream to operate on
 * @param t The item to input into the string
 * @return An rvalue reference to the ostringstream (basically, "return *this"
 * but correctly typed).
 */
template <typename S, typename T,
          class = typename std::enable_if<std::is_base_of<std::basic_ostream<typename S::char_type, typename S::traits_type>,
                                                          S>::value>::type>
S&& operator<<(S&& out, const T& t) {
    static_cast<std::basic_ostream<typename S::char_type, typename S::traits_type>&>(out) << t;
    return std::move(out);
}

/** The start time of the program, to be used for timestamps in Logger entries.
 * main() should set this after synchronizing clocks. */
extern std::chrono::high_resolution_clock::time_point program_start_time;

namespace util {

class Logger {
private:
    std::mutex log_mutex;

public:
    std::vector<std::string> events;
    std::vector<std::chrono::microseconds::rep> times;
    size_t curr_event;

    Logger() : events(10000000), times(10000000), curr_event(0){};

    void log_event(std::string event_text);
    void log_event(const std::stringstream& event_text);
};

/** Gets a single global Logger instance to use for debugging. */
Logger& debug_log();

} /* namespace util */
} /* namespace derecho */

//Lift the generic operator<< into the global namespace
//so it's always in scope when logger.h is included
using derecho::operator<<;
