
#include "util.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <numeric>
#include <sstream>
#include <sys/stat.h>
#include <thread>

#ifdef USE_SLURM
#include <slurm/slurm.h>
#endif

using namespace std;

// This holds the REALTIME timestamp corresponding to the value on the
// master node once all other nodes have connected. Until this takes
// place, it is initialized to zero.
static uint64_t epoch_start = 0;

template <class T, class U>
T lexical_cast(U u) {
    stringstream s{};
    s << u;
    T t{};
    s >> t;
    return t;
}

bool file_exists(const string &name) {
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

void create_directory(const string &name) {
    mkdir(name.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
}

// return bits / nanosecond = Gigabits/second
double compute_data_rate(size_t num_bytes, uint64_t sTime, uint64_t eTime) {
    return ((double)num_bytes) * 8.0 / (eTime - sTime);
}
void put_flush(const char *str) {
    //    printf("[%6.3f]%s\n", 1.0e-6 * (get_time() - epoch_start), str);
    puts(str);
    fflush(stdout);
}

// Attempts to init environment using slurm and returns whether it was
// successful.
bool slurm_query_addresses(map<uint32_t, string> &addresses,
                           uint32_t &node_rank) {
#ifdef USE_SLURM
    char *nodeid_ptr = getenv("SLURM_NODEID");
    char *nnodes_ptr = getenv("SLURM_NNODES");
    char *hostnames = getenv("SLURM_JOB_NODELIST");
    if(!nodeid_ptr || !nnodes_ptr || !hostnames) return false;

    hostlist_t hostlist = slurm_hostlist_create(hostnames);
    if(!hostlist) return false;

    char *host;
    uint32_t i = 0;
    while((host = slurm_hostlist_shift(hostlist))) {
        addresses.emplace(i++, host);
    }

    slurm_hostlist_destroy(hostlist);

    node_rank = lexical_cast<uint32_t>(string(nodeid_ptr));
    uint32_t num_nodes = lexical_cast<uint32_t>(string(nnodes_ptr));

    assert(addresses.size() == num_nodes);
    assert(node_rank < num_nodes);
    return true;
#else
    return false;
#endif
}

void query_peer_addresses(map<uint32_t, string> &addresses,
                          uint32_t &node_rank) {
    if(slurm_query_addresses(addresses, node_rank)) return;

    uint32_t num_nodes;
    cout << "Please enter '[node_id] [num_nodes]': ";
    cin >> node_rank >> num_nodes;

    string addr;
    for(uint32_t i = 0; i < num_nodes; ++i) {
        // input the connection information here
        cout << "Please enter IP Address for node " << i << ": ";
        cin >> addr;
        addresses.emplace(i, addr);
    }
}

void reset_epoch() {
    LOG_EVENT(-1, -1, -1, "begin_epoch_reset");
    epoch_start = get_time();
    LOG_EVENT(-1, -1, -1, "end_epoch_reset");
}

double compute_mean(std::vector<double> v) {
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    return sum / v.size();
}
double compute_stddev(std::vector<double> v) {
    double mean = compute_mean(v);
    double sq_sum = std::inner_product(v.begin(), v.end(), v.begin(), 0.0);
    return std::sqrt(sq_sum / v.size() - mean * mean);
}

vector<event> events;
std::mutex events_mutex;
void start_flush_server() {
    auto flush_server = []() {
        while(true) {
            flush_events();
            this_thread::sleep_for(chrono::seconds(10));
        }
    };
    thread t(flush_server);
    t.detach();
}
void flush_events() {
    std::unique_lock<std::mutex> lock(events_mutex);

    auto basename = [](const char *path) {
        const char *base = strrchr(path, '/');
        return base ? base + 1 : path;
    };

    static bool print_header = true;
    if(print_header) {
        printf(
                "time, file:line, event_name, group_number, message_number, "
                "block_number\n");
        print_header = false;
    }
    for(const auto &e : events) {
        if(e.group_number == (uint32_t)(-1)) {
            printf("%5.6f, %s:%d, %s\n", 1.0e-6 * (e.time - epoch_start),
                   basename(e.file), e.line, e.event_name);

        } else if(e.message_number == (size_t)(-1)) {
            printf("%5.6f, %s:%d, %s, %" PRIu32 "\n",
                   1.0e-6 * (e.time - epoch_start), basename(e.file), e.line,
                   e.event_name, e.group_number);

        } else if(e.block_number == (size_t)(-1)) {
            printf("%5.6f, %s:%d, %s, %" PRIu32 ", %zu\n",
                   1.0e-6 * (e.time - epoch_start), basename(e.file), e.line,
                   e.event_name, e.group_number, e.message_number);

        } else {
            printf("%5.6f, %s:%d, %s, %" PRIu32 ", %zu, %zu\n",
                   1.0e-6 * (e.time - epoch_start), basename(e.file), e.line,
                   e.event_name, e.group_number, e.message_number,
                   e.block_number);
        }
    }
    fflush(stdout);
    events.clear();
}
