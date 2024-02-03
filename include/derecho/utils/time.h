
#ifndef TIME_TIME_H
#define TIME_TIME_H

#include <cstdint>
#include <sys/resource.h>
#include <time.h>
#include <derecho/config.h>

#define INT64_1E3  (1000L)
#define INT64_1E6  (1000000L)
#define INT64_1E9  (1000000000L)

#ifdef __cplusplus
extern "C" {
#endif//__cplusplus
// Returns the number of nanoseconds since some fixed time in the past.
inline uint64_t get_time() {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec * INT64_1E9 + now.tv_nsec;
}

inline uint64_t get_walltime() {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    return now.tv_sec * INT64_1E9 + now.tv_nsec;
}

// Returns the number of nanoseconds of CPU time that have been used by this
// process since some fixed time in the past.
inline uint64_t get_process_time() {
    rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * INT64_1E9 + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) * INT64_1E3;
}

#ifdef __cplusplus
}
#endif//__cplusplus

#endif /* TIME_TIME_H */
