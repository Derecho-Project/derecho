/*
 * timing.h
 *
 *  Created on: Jan 26, 2016
 */

#ifndef EXPERIMENTS_TIMING_H_
#define EXPERIMENTS_TIMING_H_

static const long long int SECONDS_TO_NS = 1000000000LL;
static const long long int MILLIS_TO_NS = 1000000LL;

namespace sst {

/**
 * Utility functions and objects related to running experiments with SST.
 */
namespace experiments {

/** Polls the system realtime clock for the current time. */
long long int get_realtime_clock();
/** Idles the current thread in a busy loop (to avoid descheduling) until a certain amount of time has elapsed. */
void busy_wait_for(int wait_nsec);

}
}


#endif /* EXPERIMENTS_TIMING_H_ */
