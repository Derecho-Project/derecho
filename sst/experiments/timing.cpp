#include "timing.h"

#include <ctime>

namespace sst {
namespace experiments {

/**
 * @details
 * A convenience wrapper for clock_gettime().
 *
 * @return The current time in nanoseconds.
 */
long long int get_realtime_clock() {
	struct timespec time;
	clock_gettime(CLOCK_REALTIME, &time);
	return time.tv_sec * SECONDS_TO_NS + time.tv_nsec;
}

/**
 * @details
 * This continuously checks the current time until at least `wait_nsec` has
 * elapsed. It is not guaranteed to wait exactly `wait_nsec`, and in practice
 * will usually overshoot by a few dozen nanoseconds.
 *
 * @param wait_nsec The number of nanoseconds to wait for.
 */
void busy_wait_for(int wait_nsec) {
	long long int wait_start_nsec = get_realtime_clock();

	long long int wait_end_nsec = get_realtime_clock();
	while(wait_end_nsec < wait_start_nsec + wait_nsec) {
		wait_end_nsec = get_realtime_clock();
	}

}

}
}
