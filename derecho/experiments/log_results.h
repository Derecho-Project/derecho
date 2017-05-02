#ifndef LOG_RESULTS_H
#define LOG_RESULTS_H

#include <fstream>

template <class params>
void log_results(params t, std::string filename) {
    std::ofstream fout;
    fout.open(filename, std::ofstream::app);
    t.print(fout);
    fout.close();
}
#endif
