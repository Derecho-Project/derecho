#include <chrono>

#include "sst/sst.h"

struct TimeRow {
    long long int time_in_nanoseconds;
};

long long int server(sst::SST<TimeRow> &sst, uint node_index, uint remote_index) {
    int num_measurements = 10000;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    struct timespec start_time, end_time;
    long long int sum_skew = 0;
    for(int i = 0; i < num_measurements; ++i) {
        clock_gettime(CLOCK_REALTIME, &start_time);
        sst[node_index].time_in_nanoseconds = (start_time.tv_sec * 1e9 + start_time.tv_nsec);
        sst.put();
        while(sst[remote_index].time_in_nanoseconds < 0) {
        }
        clock_gettime(CLOCK_REALTIME, &end_time);
        long long int end_time_in_nanoseconds = (end_time.tv_sec * 1e9 + end_time.tv_nsec);
        sum_skew += (end_time_in_nanoseconds + sst[node_index].time_in_nanoseconds) / 2 - sst[remote_index].time_in_nanoseconds;
        sst[remote_index].time_in_nanoseconds = -1;
    }
    return sum_skew / num_measurements;
}

void client(sst::SST<TimeRow> &sst, uint node_index, uint remote_index) {
    int num_measurements = 10000;
    for(int i = 0; i < num_measurements; ++i) {
        while(sst[remote_index].time_in_nanoseconds < 0) {
        }
        struct timespec start_time;
        clock_gettime(CLOCK_REALTIME, &start_time);
        sst[node_index].time_in_nanoseconds = (start_time.tv_sec * 1e9 + start_time.tv_nsec);
        sst[remote_index].time_in_nanoseconds = -1;
        sst.put();
    }
}
