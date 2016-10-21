#include "block_size.h"
#include <cstdlib>

size_t get_block_size(long long int msg_size) {
    switch(msg_size) {
        case 10:
        case 100:
        case 1000:
            return msg_size;
        case 10000:
            return 5000;
        case 100000:
        case 1000000:
            return 100000;
        case 10000000:
        case 100000000:
        case 1000000000:
        case 10000000000:
            return 1000000;
        default:
            std::cout << "Not handled" << std::endl;
            exit(0);
    }
}
