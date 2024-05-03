#pragma once

#include "derecho/config.h"

#include <condition_variable>
#include <optional>
#include <list>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

namespace sst {
namespace util {
class PollingData {
    // map:node_id->[list of return values]
    std::vector<std::map<int32_t, std::list<int32_t>>> completion_entries;
    std::map<std::thread::id, uint32_t> tid_to_index;
    std::vector<bool> if_waiting;
    std::condition_variable poll_cv;
    std::mutex poll_mutex;
    bool check_waiting();
public:
    void insert_completion_entry(uint32_t index, std::pair<int32_t, int32_t> ce);

    // std::optional<std::pair<int32_t, int32_t>> get_completion_entry(const std::thread::id id);
    std::optional<int32_t> get_completion_entry(const std::thread::id tid, const int nid);

    uint32_t get_index(const std::thread::id id);

    void set_waiting(const std::thread::id id);

    void reset_waiting(const std::thread::id id);

    void wait_for_requests();
};

//There is one global instance of PollingData
extern PollingData polling_data;
}  // namespace util
}  // namespace sst
