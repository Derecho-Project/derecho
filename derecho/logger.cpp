#include "logger.h"

namespace derecho {

std::chrono::high_resolution_clock::time_point program_start_time;

namespace util {

Logger debug_log_instance;

Logger& debug_log() { return debug_log_instance; }

void Logger::log_event(std::string event_text) {
    std::lock_guard<std::mutex> lock(log_mutex);
    auto currtime = std::chrono::high_resolution_clock::now();
    times[curr_event] = std::chrono::duration_cast<std::chrono::microseconds>(
                                currtime - derecho::program_start_time).count();
    events[curr_event] = event_text;
    curr_event++;
}

void Logger::log_event(const std::stringstream& event_text) {
    log_event(event_text.str());
}

} /* namespace util */
} /* namespace derecho */
