#ifndef RDMC_UTIL_HPP
#define RDMC_UTIL_HPP

#include <derecho/config.h>
#include <derecho/utils/time.h>

#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <vector>

template <class T, class U>
size_t index_of(T container, U elem) {
    size_t n = 0;
    for(auto it = begin(container); it != end(container); ++it) {
        if(*it == elem) return n;

        n++;
    }
    return container.size();
}
bool file_exists(const std::string &name);
void create_directory(const std::string &name);
double compute_data_rate(size_t numBytes, uint64_t sTime, uint64_t eTime);
void put_flush(const char *str);
void reset_epoch();
void query_peer_addresses(std::map<uint32_t, std::string> &addresses,
                          uint32_t &node_rank);

double compute_mean(std::vector<double> v);
double compute_stddev(std::vector<double> v);

#define TRACE(x)      \
    do {              \
        put_flush(x); \
    } while(0)

struct event {
    const char *file;
    const char *event_name;
    uint64_t time;

    int line;
    uint32_t group_number;
    size_t message_number;
    size_t block_number;
};
extern std::vector<event> events;
extern std::mutex events_mutex;
inline void log_event(const char *file, int line, uint32_t group_number,
                      size_t message_number, size_t block_number,
                      const char *event_name) {
    std::unique_lock<std::mutex> lock(events_mutex);
    events.emplace_back(event{file, event_name, get_time(), line, group_number,
                              message_number, block_number});
}
void flush_events();
void start_flush_server();
#define DERECHO_LOG(sender, message_number, event_name)                        \
    do {                                                                       \
        log_event(__FILE__, __LINE__, sender, message_number, -1, event_name); \
    } while(0)

#define LOG_EVENT(group_number, message_number, block_number, event_name) \
    do {                                                                  \
    } while(0)

inline void CHECK(bool b) {
    if(!b) {
        puts("CHECK failed, aborting.");
        abort();
    }
}

#endif /* UTIL_HPP */
