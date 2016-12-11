#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <experimental/optional>
#include <thread>
#include <vector>

namespace sst {
namespace util {
class PollingData {
    static std::vector<std::list<std::pair<int32_t, int32_t>>> completion_entries;
    static std::map<std::thread::id, uint32_t> tid_to_index;
    static std::vector<bool> if_waiting;
    static std::condition_variable poll_cv;
    static std::mutex poll_mutex;

    static bool check_waiting();

public:
    void insert_completion_entry(uint32_t index, std::pair<int32_t, int32_t> ce);

    std::experimental::optional<std::pair<int32_t, int32_t>> get_completion_entry(const std::thread::id id);

    uint32_t get_index(const std::thread::id id);

    void set_waiting(const std::thread::id id);

    void reset_waiting(const std::thread::id id);

    void wait_for_requests();
};

static PollingData polling_data;
}
}
