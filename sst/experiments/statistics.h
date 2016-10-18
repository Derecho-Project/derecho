#ifndef STATISTICS_H
#define STATISTICS_H

#include <tuple>
#include <vector>

namespace sst {

namespace experiments {

/** Computes the mean and standard deviation of the sequence of times. */
std::tuple<double, double> compute_statistics(const std::vector <long long int> &start_times,
		const std::vector <long long int> &end_times, const double divisor = 1);

/** Computes and prints some statistics to stdout, including mean and standard deviation. */
void print_statistics (const std::vector <long long int> &start_times,
		const std::vector <long long int> &end_times, double factor = 1);

/** Converts parallel vectors of timestamps to a vector of elapsed times. */
std::vector<double> timestamps_to_elapsed(const std::vector <long long int> &start_times, const std::vector <long long int> &end_times);

}

}

#endif
