#include <algorithm>
#include <cmath>
#include <iostream>
#include <numeric>
#include <tuple>
#include <vector>

namespace sst {

namespace experiments {

const double NSEC_TO_USEC = 1000.0;

using std::vector;
using std::cout;
using std::endl;
using std::accumulate;
using std::sqrt;
using std::tuple;
using std::make_tuple;
using std::tie;

class Sum_Sqr {
public:
  double operator()(double x, double y) {return x+y*y;}
} sum_sqr;

/**
 * @details
 * This takes two parallel vectors of timestamps, representing the start and
 * end times of a series of time measurements, and converts them to elapsed-time
 * sequences before finding the mean and standard deviation. It also converts
 * the times to microseconds, to make them easier to read.
 *
 * @param start_times A vector of timestamps representing the beginning of each
 * time sequence in nanoseconds.
 * @param end_times A vector of timestamps representing the end of each time
 * sequence in nanoseconds.
 * @param divisor Optional divisor to divide every time sequence by; defaults
 * to 1. This can be used if we know that every time represents a round trip,
 * for example, to divide every time by 2.
 * @return A tuple containing the mean and standard deviation, in that order,
 * in microseconds.
 */
tuple<double, double> compute_statistics(const vector <long long int> &start_times, const vector <long long int> &end_times, const double divisor = 1) {
	int num_times = start_times.size();
	vector<double> times;
	times.resize (num_times);
	// Convert to elapsed time
	for (int i = 0; i < num_times; ++i) {
		times[i] = (end_times[i] - start_times[i]) / (NSEC_TO_USEC * divisor);
	}

	double mean = accumulate (times.begin(), times.end(), 0.0)/num_times;
	double stdev = sqrt (accumulate (times.begin(), times.end(), 0.0, sum_sqr)/num_times - mean*mean);
	return make_tuple(mean, stdev);
}

/**
 * @param start_times A vector of timestamps representing the beginning of each
 * time sequence in nanoseconds.
 * @param end_times A vector of timestamps representing the end of each time
 * sequence in nanoseconds.
 * @return A vector of elapsed times, in microseconds, aligned with the input
 * vectors.
 */
vector<double> timestamps_to_elapsed(const vector <long long int> &start_times, const vector <long long int> &end_times) {
	vector<double> times(start_times.size());
	for (size_t i = 0; i < times.size(); ++i) {
		times[i] = (end_times[i] - start_times[i]) / (NSEC_TO_USEC);
	}
	return times;
}

/**
 * @param start_times A vector of timestamps representing the beginning of each
 * time sequence in nanoseconds.
 * @param end_times A vector of timestamps representing the end of each time
 * sequence in nanoseconds.
 * @param factor Optional divisor to divide every time sequence by; defaults
 * to 1. This can be used if we know that every time represents a round trip,
 * for example, to divide every time by 2.
 */
void print_statistics (const vector <long long int> &start_times, const vector <long long int> &end_times, double factor = 1) {
  int num_times = start_times.size();
  vector<double> times;
  times.resize (num_times);
  for (int i = 0; i < num_times; ++i) {
		times[i] = (end_times[i] - start_times[i]) / (NSEC_TO_USEC * factor);
  }
  sort (times.begin(), times.end());
  cout << "Minimum 20 values : " << endl;
  for (int i = 0; i < 20; ++i) {
    cout << times[i] << endl;
  }
  cout << endl;
  cout << "Maximum 20 values : " << endl;
  for (int j = num_times-1; j > num_times-1-20; --j) {
    cout << times[j] << endl;
  }
  cout << endl;
  double mean, stdev;
  tie(mean, stdev) = compute_statistics(start_times, end_times, factor);
  cout << "Mean:" << mean << endl;
  cout << "Standard Deviation: " << stdev << endl;
}

} //namespace experiments

} //namespace sst
