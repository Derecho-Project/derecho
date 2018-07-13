
#ifndef TIME_TIME_H
#define TIME_TIME_H

#include <cstdint>
#include <sys/resource.h>
#include <time.h>

// Returns the number of nanoseconds since some fixed time in the past.

inline uint64_t get_time_timeh() {
        struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec * 1000000000L + now.tv_nsec;
    
}

inline uint64_t get_time() {
    return get_time_timeh();
}

// Returns the number of nanoseconds of CPU time that have been used by this
// process since some fixed time in the past.
inline uint64_t get_process_time() {
	rusage usage;
	getrusage(RUSAGE_SELF, &usage);
    return (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000000L +
           (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) * 1000L;
}

#endif /* TIME_TIME_H */
