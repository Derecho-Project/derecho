#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>

int stick_this_thread_to_core(int core_id) {
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}


void print_affinity() {
    cpu_set_t mask;
    long nproc, i;

    if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1) {
        std::cerr << "sched_getaffinity" << std::endl;
        assert(false);
    }
    nproc = sysconf(_SC_NPROCESSORS_ONLN);
    std::cout << "sched_getaffinity = ";
    for (i = 0; i < nproc; i++) {
        std::cout << CPU_ISSET(i, &mask) << " ";
    }
    std::cout << std::endl;

}

int main(void) {
    print_affinity();    
    if (stick_this_thread_to_core(1) == EINVAL) {
        std::cerr << "sched_setaffinity" << std::endl;
        assert(false);
    }
    print_affinity();
    return EXIT_SUCCESS;
}