#include <derecho/objectstore/ObjectStore.hpp>
#include <derecho/conf/conf.hpp>
#include <iostream>
#include <time.h>
#define NUM_APP_ARGS (1)

int main(int argc, char** argv) {
    if((argc < (NUM_APP_ARGS + 1)) || ((argc > (NUM_APP_ARGS + 1)) && strcmp("--", argv[argc - NUM_APP_ARGS - 1]))) {
        std::cerr << "Usage: " << argv[0] << " [ derecho-config-list -- ] <aio|bio>" << std::endl;
        return -1;
    }

    bool use_aio = false;
    if(strcmp("aio", argv[argc - NUM_APP_ARGS]) == 0) {
        use_aio = true;
    } else if(strcmp("bio", argv[argc - NUM_APP_ARGS]) != 0) {
        std::cerr << "unrecognized argument:" << argv[argc - NUM_APP_ARGS] << ". Using bio (blocking io) instead." << std::endl;
    }

    struct timespec t_start, t_end;
    derecho::Conf::initialize(argc, argv);
    std::cout << "Starting object store service..." << std::endl;
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                        [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                            //std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                        });
    // print some message
    std::cout << "Object store service started. Is replica:" << std::boolalpha << oss.isReplica()
              << std::noboolalpha << "." << std::endl;

    int runtime = 60 * 1000;  // approximate runtime
    int num_msg = 10000;      // num_msg sent for the trial run
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    int msg_size = max_msg_size - 128;
    if(msg_size > 2000000) {
        num_msg = 5000;
    }
    char odata[msg_size];
    srand(time(0));
    for(int i = 0; i < msg_size; i++) {
        odata[i] = '1' + (rand() % 74);
    }
    // create a pool of objects
    std::vector<objectstore::Object> objpool;
    for(int i = 0; i < num_msg; i++) {
        objpool.push_back(objectstore::Object(i, odata, msg_size + 1));
    }

    // trial run to get an approximate number of objects to reach runtime
    clock_gettime(CLOCK_REALTIME, &t_start);
    if(use_aio) {
        for(int i = 0; i < num_msg; i++) {
            oss.aio_put(objpool[i]);
        }
    } else {
        for(int i = 0; i < num_msg; i++) {
            oss.bio_put(objpool[i]);
        }
    }
    oss.bio_get(num_msg - 1);
    clock_gettime(CLOCK_REALTIME, &t_end);

    long long int nsec = (t_end.tv_sec - t_start.tv_sec) * 1000000000 + (t_end.tv_nsec - t_start.tv_nsec);
    double msec = (double)nsec / 1000000;

    int multiplier = 1;
    if(msec < runtime) {
        multiplier = ceil(runtime / msec);

        // real benchmarking starts
        clock_gettime(CLOCK_REALTIME, &t_start);
        if(use_aio) {
            for(int i = 0; i < num_msg * multiplier; i++) {
                oss.aio_put(objpool[i % num_msg]);
            }
        } else {
            for(int i = 0; i < num_msg * multiplier; i++) {
                oss.bio_put(objpool[i % num_msg]);
            }
        }
        oss.bio_get(num_msg - 1);
        clock_gettime(CLOCK_REALTIME, &t_end);

        nsec = (t_end.tv_sec - t_start.tv_sec) * 1000000000 + (t_end.tv_nsec - t_start.tv_nsec);
        msec = (double)nsec / 1000000;
    }
    double thp_mBps = ((double)max_msg_size * num_msg * multiplier * 1000) / nsec;
    double thp_ops = ((double)num_msg * multiplier * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_mBps << "MB/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "op/s." << std::endl;
    std::cout << std::flush;
    oss.leave();
}
