
#include "rdmc.h"
#include "schedule.h"
#include "util.h"
#include "verbs_helper.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std;
using namespace rdma;

template <class T>
struct stat {
    T mean;
    T stddev;
};

struct send_stats {
    stat<double> time;  // in ms
    stat<double> bandwidth;  // in Gb/s
	stat<double> cpu_usage; // core seconds/wall time seconds

    size_t size;
    size_t block_size;
    size_t group_size;
    size_t iterations;
};

uint32_t node_rank;
uint32_t num_nodes;

unique_ptr<rdmc::barrier_group> universal_barrier_group;
uint16_t next_group_number;

send_stats measure_partially_concurrent_multicast(
    size_t size, size_t block_size, uint32_t group_size, uint32_t num_senders,
    size_t iterations, rdmc::send_algorithm type = rdmc::BINOMIAL_SEND,
    bool use_cv = true) {
    if(node_rank >= group_size) {
        // Each iteration involves two barriers: one at the start and one at the
        // end.
        for(size_t i = 0; i < iterations * 2; i++) {
            universal_barrier_group->barrier_wait();
        }

        return send_stats();
    }

	std::mutex send_mutex;
	std::condition_variable send_done_cv;
	atomic<uint64_t> end_time;
	atomic<uint64_t> end_ptime;
	
    size_t num_blocks = (size - 1) / block_size + 1;
    size_t buffer_size = num_blocks * block_size;
	auto mr = make_shared<memory_region>(buffer_size * num_senders);
	char* buffer = mr->buffer;

    uint16_t base_group_number = next_group_number;
    atomic<uint32_t> sends_remaining;

    for(uint16_t i = 0u; i < num_senders; i++) {
        vector<uint32_t> members;
        for(uint32_t j = 0; j < group_size; j++) {
            members.push_back((j + i) % group_size);
        }
        CHECK(rdmc::create_group(
            base_group_number + i, members, block_size, type,
            [&mr, i, buffer_size](size_t length) -> rdmc::receive_destination {
                return {mr, buffer_size * i};
            },
            [&](char *data, size_t) {
                if(--sends_remaining == 0) {
					universal_barrier_group->barrier_wait();
					end_ptime = get_process_time();
					end_time = get_time();
					unique_lock<mutex> lk(send_mutex);
                    send_done_cv.notify_all();
                }
            },
            [group_number = base_group_number + i](optional<uint32_t>) {
                LOG_EVENT(group_number, -1, -1, "send_failed");
                CHECK(false);
            }));
    }

    vector<double> rates;
    vector<double> times;
	vector<double> cpu_usages;

    for(size_t i = 0; i < iterations; i++) {
        sends_remaining = num_senders;
		end_time = 0;
		end_ptime = 0;
		
        if(node_rank < num_senders) {
            for(size_t j = 0; j < size; j += 256)
                buffer[node_rank * buffer_size + j] = (rand() >> 5) % 256;
        }

        universal_barrier_group->barrier_wait();

		uint64_t start_ptime = get_process_time();
        uint64_t start_time = get_time();

        if(node_rank < num_senders) {
            CHECK(rdmc::send(base_group_number + node_rank, mr,
                             buffer_size * node_rank, size));
        }

		if(use_cv) {
			unique_lock<mutex> lk(send_mutex);
			send_done_cv.wait(lk, [&] { return end_time != 0; });
		} else {
            while(end_time == 0)
                /* do nothing*/;
        }

		uint64_t time_diff = end_time - start_time;
		uint64_t ptime_diff = end_ptime - start_ptime;
		rates.push_back(8.0 * size * num_senders / time_diff);
		times.push_back(1.0e-6 * time_diff);
		cpu_usages.push_back((double)ptime_diff / time_diff);
    }

    for(auto i = 0u; i < group_size; i++) {
        rdmc::destroy_group(base_group_number + i);
    }

    send_stats s;
    s.size = size;
    s.block_size = block_size;
    s.group_size = group_size;
    s.iterations = iterations;

    s.time.mean = compute_mean(times);
    s.time.stddev = compute_stddev(times);
    s.bandwidth.mean = compute_mean(rates);
    s.bandwidth.stddev = compute_stddev(rates);
	s.cpu_usage.mean = compute_mean(cpu_usages);
	s.cpu_usage.stddev = compute_stddev(cpu_usages);
    return s;
}

send_stats measure_multicast(size_t size, size_t block_size,
                             uint32_t group_size, size_t iterations,
                             rdmc::send_algorithm type = rdmc::BINOMIAL_SEND,
							 bool use_cv = true) {
	return measure_partially_concurrent_multicast(size, block_size, group_size,
												  1, iterations, type, use_cv);
}

void blocksize_v_bandwidth(uint16_t gsize) {
    const size_t min_block_size = 16ull << 10;
    const size_t max_block_size = 16ull << 20;

    puts("=========================================================");
    puts("=             Block Size vs. Bandwdith (Gb/s)           =");
    puts("=========================================================");
    printf("Group Size = %d\n", (int)gsize);
    printf("Send Size, ");
    for(auto block_size = min_block_size; block_size <= max_block_size;
        block_size *= 2) {
        if(block_size >= 1 << 20)
            printf("%d MB, ", (int)(block_size >> 20));
        else
            printf("%d KB, ", (int)(block_size >> 10));
    }
    for(auto block_size = min_block_size; block_size <= max_block_size;
        block_size *= 2) {
        if(block_size >= 1 << 20)
            printf("%d MB stddev, ", (int)(block_size >> 20));
        else
            printf("%d KB stddev, ", (int)(block_size >> 10));
    }
    puts("");
    fflush(stdout);
    for(auto size : {256ull << 20, 64ull << 20, 16ull << 20, 4ull << 20,
                     1ull << 20, 256ull << 10, 64ull << 10, 16ull << 10}) {
        if(size >= 1 << 20)
            printf("%d MB, ", (int)(size >> 20));
        else if(size >= 1 << 10)
            printf("%d KB, ", (int)(size >> 10));
        else
            printf("%d B, ", (int)(size));

        vector<double> stddevs;
        for(auto block_size = min_block_size; block_size <= max_block_size;
            block_size *= 2) {
            if(block_size > size) {
                printf(", ");
                continue;
            }
            auto s = measure_multicast(size, block_size, gsize, 8);
            printf("%f, ", s.bandwidth.mean);
            fflush(stdout);

            stddevs.push_back(s.bandwidth.stddev);
        }
        for(auto s : stddevs) {
            printf("%f, ", s);
        }
        puts("");
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void compare_send_types() {
    puts("=========================================================");
    puts("=         Compare Send Types - Bandwidth (Gb/s)         =");
    puts("=========================================================");
    puts(
        "Group Size,"
        "Binomial Pipeline (256 MB),Chain Send (256 MB),Sequential Send (256 "
        "MB),Tree Send (256 MB),"
        "Binomial Pipeline (64 MB),Chain Send (64 MB),Sequential Send (64 "
        "MB),Tree Send (64 MB),"
        "Binomial Pipeline (8 MB),Chain Send (8 MB),Sequential Send (8 "
        "MB),Tree Send (8 MB),");
    fflush(stdout);

    const size_t block_size = 1 << 20;
    const size_t iterations = 64;
    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp8 = measure_multicast(8 << 20, block_size, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp64 = measure_multicast(64 << 20, block_size, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp256 = measure_multicast(256 << 20, block_size, gsize, iterations,
                                       rdmc::BINOMIAL_SEND);
        auto cs8 = measure_multicast(8 << 20, block_size, gsize, iterations,
                                     rdmc::CHAIN_SEND);
        auto cs64 = measure_multicast(64 << 20, block_size, gsize, iterations,
                                      rdmc::CHAIN_SEND);
        auto cs256 = measure_multicast(256 << 20, block_size, gsize, iterations,
                                       rdmc::CHAIN_SEND);
        auto ss8 = measure_multicast(8 << 20, block_size, gsize, iterations,
                                     rdmc::SEQUENTIAL_SEND);
        auto ss64 = measure_multicast(64 << 20, block_size, gsize, iterations,
                                      rdmc::SEQUENTIAL_SEND);
        auto ss256 = measure_multicast(256 << 20, block_size, gsize, iterations,
                                       rdmc::SEQUENTIAL_SEND);
        auto ts8 = measure_multicast(8 << 20, block_size, gsize, iterations,
                                     rdmc::TREE_SEND);
        auto ts64 = measure_multicast(64 << 20, block_size, gsize, iterations,
                                      rdmc::TREE_SEND);
        auto ts256 = measure_multicast(256 << 20, block_size, gsize, iterations,
                                       rdmc::TREE_SEND);
        printf(
            "%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, "
            "%f, %f, %f, %f, %f, %f, %f, %f, %f\n",
            gsize, bp256.bandwidth.mean, cs256.bandwidth.mean,
            ss256.bandwidth.mean, ts256.bandwidth.mean, bp64.bandwidth.mean,
            cs64.bandwidth.mean, ss64.bandwidth.mean, ts64.bandwidth.mean,
            bp8.bandwidth.mean, cs8.bandwidth.mean, ss8.bandwidth.mean,
            ts8.bandwidth.mean, bp256.bandwidth.stddev, cs256.bandwidth.stddev,
            ss256.bandwidth.stddev, ts256.bandwidth.stddev,
            bp64.bandwidth.stddev, cs64.bandwidth.stddev, ss64.bandwidth.stddev,
            ts64.bandwidth.stddev, bp8.bandwidth.stddev, cs8.bandwidth.stddev,
            ss8.bandwidth.stddev, ts8.bandwidth.stddev);

        // ss256.bandwidth.mean, 0.0f /*ts256.bandwidth.mean*/,
        // bp64.bandwidth.mean, cs64.bandwidth.mean, ss64.bandwidth.mean,
        // 0.0f /*ts64.bandwidth.mean*/, bp256.bandwidth.stddev,
        // cs256.bandwidth.stddev, ss256.bandwidth.stddev,
        // 0.0f /*ts256.bandwidth.stddev*/, bp64.bandwidth.stddev,
        // cs64.bandwidth.stddev, ss64.bandwidth.stddev,
        // 0.0f /*ts64.bandwidth.stddev*/);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void bandwidth_group_size() {
    puts("=========================================================");
    puts("=              Bandwidth vs. Group Size                 =");
    puts("=========================================================");
    puts(
        "Group Size, 256 MB, 64 MB, 16 MB, 4 MB, "
        "256stddev, 64stddev, 16stddev, 4stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp256 = measure_multicast(256 << 20, 1 << 20, gsize, 64,
                                       rdmc::BINOMIAL_SEND);
        auto bp64 = measure_multicast(64 << 20, 1 << 20, gsize, 64,
                                      rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16 << 20, 1 << 20, gsize, 64,
                                      rdmc::BINOMIAL_SEND);
        auto bp4 =
            measure_multicast(4 << 20, 1 << 20, gsize, 64, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, bp64.bandwidth.mean, bp16.bandwidth.mean,
               bp4.bandwidth.mean, bp256.bandwidth.stddev,
               bp64.bandwidth.stddev, bp16.bandwidth.stddev,
               bp4.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void concurrent_bandwidth_group_size() {
    puts("=========================================================");
    puts("=         Concurrent Bandwidth vs. Group Size           =");
    puts("=========================================================");
    puts(
        "Group Size, 256 MB, 64 MB, 16 MB, 4 MB, "
        "256stddev, 64stddev, 16stddev, 4stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp256 = measure_partially_concurrent_multicast(256 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp64 = measure_partially_concurrent_multicast(64 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp16 = measure_partially_concurrent_multicast(16 << 20, 1 << 20,
			gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp4 = measure_partially_concurrent_multicast(4 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, bp64.bandwidth.mean, bp16.bandwidth.mean,
               bp4.bandwidth.mean, bp256.bandwidth.stddev,
               bp64.bandwidth.stddev, bp16.bandwidth.stddev,
               bp4.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void active_senders() {
    auto compute_block_size = [](size_t message_size) -> size_t {
        if(message_size < 4096 * 2) return message_size;
        if(message_size < 4096 * 10) return 4096;
        if(message_size < 10 * (1 << 20)) return message_size / 10;
        return 1 << 20;
    };

    auto compute_iterations = [](size_t message_size) -> size_t {
        if(message_size == 1) return 20000;
        if(message_size == 10000) return 10000;
        if(message_size == 1'000'000) return 1000;
        if(message_size == 100'000'000) return 100;
        return max<size_t>(100000000 / message_size, 4u);
    };

    printf(
        "Message Size, Group Size, 1-sender Bandwidth, half-sending Bandwidth, "
        "all-sending Bandwidth, 1-sender CPU, half-sending CPU, all-sending CPU\n");

    for(size_t message_size : {1, 10000, 1'000'000, 100'000'000}) {
        for(uint32_t group_size = 3; group_size <= num_nodes; group_size++) {
            printf("%d, %d, ", (int)message_size, (int)group_size);
            fflush(stdout);

			vector<double> cpu_usage;
            for(uint32_t num_senders : {1u, (group_size + 1) / 2, group_size}) {
                auto s = measure_partially_concurrent_multicast(
                    message_size, compute_block_size(message_size), group_size,
                    num_senders, compute_iterations(message_size),
                    rdmc::BINOMIAL_SEND);
                printf("%f, ", s.bandwidth.mean);
                fflush(stdout);
				cpu_usage.push_back(s.cpu_usage.mean * 100);
            }
			for(double usage : cpu_usage) {
                printf("%f, ", usage);
			}
            printf("\n");
            fflush(stdout);
        }
    }
}
void latency_group_size() {
    puts("=========================================================");
    puts("=               Latency vs. Group Size                  =");
    puts("=========================================================");
    puts(
        "Group Size,64 KB,16 KB,4 KB,1 KB,256 B,"
        "64stddev,16stddev,4stddev,1stddev,256stddev");
    fflush(stdout);

    size_t iterations = 10000;

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2) {
        auto bp64 = measure_multicast(64 << 10, 32 << 10, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16 << 10, 8 << 10, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp4 = measure_multicast(4 << 10, 4 << 10, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp1 = measure_multicast(1 << 10, 1 << 10, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp256 =
            measure_multicast(256, 256, gsize, iterations, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp64.time.mean, bp16.time.mean, bp4.time.mean, bp1.time.mean,
               bp256.time.mean, bp64.time.stddev, bp16.time.stddev,
               bp4.time.stddev, bp1.time.stddev, bp256.time.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
// void small_send_latency_group_size() {
//     puts("=========================================================");
//     puts("=               Latency vs. Group Size                  =");
//     puts("=========================================================");
//     puts(
//         "Group Size, 16 KB, 4 KB, 1 KB, 256 Bytes, "
//         "64stddev, 16stddev, 4stddev, 1stddev");
//     fflush(stdout);

//     for(int gsize = num_nodes; gsize >= 2; --gsize) {
//         auto bp16 = measure_small_multicast(16 << 10, gsize, 16, 512);
//         auto bp4 = measure_small_multicast(4 << 10, gsize, 16, 512);
//         auto bp1 = measure_small_multicast(1 << 10, gsize, 16, 512);
//         auto bp256 = measure_small_multicast(256, gsize, 16, 512);
//         printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize, bp16.time.mean,
//                bp4.time.mean, bp1.time.mean, bp256.time.mean,
//                bp16.time.stddev,
//                bp4.time.stddev, bp1.time.stddev, bp256.time.stddev);
//         fflush(stdout);
//     }
//     puts("");
//     fflush(stdout);
// }
void large_send() {
    LOG_EVENT(-1, -1, -1, "start_large_send");
    auto s = measure_multicast(16 << 20, 1 << 20, num_nodes, 16,
                               rdmc::BINOMIAL_SEND);
    //    flush_events();
    printf("Bandwidth = %f(%f) Gb/s\n", s.bandwidth.mean, s.bandwidth.stddev);
    printf("Latency = %f(%f) ms\n", s.time.mean, s.time.stddev);
    // uint64_t eTime = get_time();
    // double diff = 1.e-6 * (eTime - sTime);
    // printf("Percent time sending: %f%%", 100.0 * s.time.mean * 16 / diff);
    fflush(stdout);
}
void concurrent_send() {
    LOG_EVENT(-1, -1, -1, "start_concurrent_send");
    auto s = measure_partially_concurrent_multicast(128 << 20, 1 << 20,
													num_nodes, num_nodes, 16);
    //    flush_events();
    printf("Bandwidth = %f(%f) Gb/s\n", s.bandwidth.mean, s.bandwidth.stddev);
    printf("Latency = %f(%f) ms\n", s.time.mean, s.time.stddev);
    // uint64_t eTime = get_time();
    // double diff = 1.e-6 * (eTime - sTime);
    // printf("Percent time sending: %f%%", 100.0 * s.time.mean * 16 / diff);
    fflush(stdout);
}
// void small_send() {
//     auto s = measure_small_multicast(1024, num_nodes, 4, 128);
//     printf("Latency = %.2f(%.2f) us\n", s.time.mean * 1000.0,
//            s.time.stddev * 1000.0);
//     fflush(stdout);
// }

void test_cross_channel() {
	if(node_rank > 1) {
		return;
	}
	
    static volatile atomic<bool> done_flag;
    done_flag = false;

    auto nop_handler = [](auto, auto, auto) {};
    auto done_handler = [](uint64_t tag, uint32_t immediate, size_t length) {
        if(tag == 0x6000000) done_flag = true;
    };

    static message_type mtype_done("ccc.done", nop_handler, nop_handler,
                                   done_handler);

    const int steps = 128;
	const size_t chunk_size = 1024;
    const size_t buffer_size = (steps + 1) * chunk_size;

    // Setup memory region
    memory_region mr{buffer_size};
    memset(mr.buffer, 1 + node_rank, buffer_size);
    memset(mr.buffer, node_rank * 10 + 10, chunk_size);
    mr.buffer[buffer_size - 1] = 0;

	auto mqp = make_shared<manager_queue_pair>();
    managed_queue_pair qp(node_rank == 0 ? 1 : 0, [&](managed_queue_pair *qp) {
        qp->post_recv(mr, chunk_size, chunk_size, 125, mtype_done);
    });

	rdma::task t(mqp);
	for(int i = 1; i < steps; i++) {
		t.append_recv(qp, mr, (i+1) * chunk_size, chunk_size);
	}
	for(int i = 0; i < steps; i++) {
		t.append_send(qp, mr, i * chunk_size, chunk_size, 0);
	}
	for(int i = 0; i < steps; i++) {
        t.append_enable_send(qp, i + 1);
        t.append_wait(qp.rcq, i + 1, false, false, 0x321000 + i, mtype_done);
    }
    t.append_wait(qp.scq, 0, true, true, 0x6000000, mtype_done);
    CHECK(t.post());

    while(!done_flag) {
    }

    // std::this_thread::sleep_for(std::chrono::seconds(1));
	// for(int i = 0; i < steps && i < 16; i++) {
	// 	printf("%2d ", mr.buffer[i * chunk_size]);
	// }
	// printf("\n");
	
	puts("PASS");
}

void test_create_group_failure() {
    if(num_nodes <= 2) {
        puts("FAILURE: must run with at least 3 nodes");
    }
    if(node_rank == 0) {
        puts("Node 0 exiting...");
        exit(0);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    vector<uint32_t> members;
    for(uint32_t i = 0; i < num_nodes; i++) {
        members.push_back(i);
    }

    puts("Starting test...");
    uint64_t t = get_time();
    bool ret = rdmc::create_group(
        0, members, 1 << 20, rdmc::BINOMIAL_SEND,
        [&](size_t length) -> rdmc::receive_destination {
            puts("FAILURE: incoming message called");
            return {nullptr, 0};
        },
        [&](char *data, size_t) { puts("FAILURE: received message called"); },
        [group_number = next_group_number](optional<uint32_t>){});

    t = get_time() - t;
    if(ret) {
        puts("FAILURE: Managed to create group containing failed node");
    } else {
        printf("time taken: %f ms\n", t * 1e-6);
        puts("PASS");
    }
}

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_YELLOW "\x1b[33m"
#define ANSI_COLOR_BLUE "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN "\x1b[36m"
#define ANSI_COLOR_RESET "\x1b[0m"
void test_pattern() {
    size_t n = 0;
    auto t = get_time();
    for(size_t group_size = 2; group_size <= 64; group_size++) {
        for(size_t message_size = 1; message_size <= 32; message_size++) {
            size_t total_steps = message_size + ceil(log2(group_size)) - 1;
            for(unsigned int step = 0; step < total_steps; step++) {
                for(unsigned int node = 0; node < group_size; node++) {
                    // Compute the outgoing transfer for this node/step
                    auto transfer = binomial_schedule::get_outgoing_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) {
                        // See what the supposed sender is doing this step
                        auto reverse = binomial_schedule::get_incoming_transfer(
                            transfer->target, step, group_size,
                            floor(log2(group_size)), message_size, total_steps);
                        n++;

                        // Make sure the two nodes agree
                        if(!reverse) throw false;
                        if(transfer->block_number != reverse->block_number)
                            throw false;

                        // If we aren't the root sender, also check that the
                        // node got this block on a past step.
                        if(node != 0) {
                            for(int s = step - 1; s >= 0; s--) {
                                auto prev =
                                    binomial_schedule::get_incoming_transfer(
                                        node, s, group_size,
                                        floor(log2(group_size)), message_size,
                                        total_steps);
                                n++;
                                if(prev &&
                                   prev->block_number == transfer->block_number)
                                    break;

                                if(s == 0) {
                                    throw false;
                                }
                            }
                        }
                    }

                    // Compute the incoming transfer for this node/step
                    transfer = binomial_schedule::get_incoming_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) {
                        // Again make sure the supposed receiver agrees
                        auto reverse = binomial_schedule::get_outgoing_transfer(
                            transfer->target, step, group_size,
                            floor(log2(group_size)), message_size, total_steps);
                        n++;
                        if(!reverse) throw false;
                        if(transfer->block_number != reverse->block_number)
                            throw false;
                        if(reverse->target != node) throw false;

                        // Make sure we don't already have the block we're
                        // getting.
                        for(int s = step - 1; s >= 0; s--) {
                            auto prev = binomial_schedule::get_incoming_transfer(
                                node, s, group_size, floor(log2(group_size)),
                                message_size, total_steps);
                            n++;
                            if(prev &&
                               prev->block_number == transfer->block_number) {
                                throw false;
                            }
                        }
                    }
                }
            }

            // Make sure that all nodes get every block
            for(unsigned int node = 1; node < group_size; node++) {
                set<size_t> blocks;
                for(unsigned int step = 0; step < total_steps; step++) {
                    auto transfer = binomial_schedule::get_incoming_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) blocks.insert(transfer->block_number);
                }
                if(blocks.size() != message_size) throw false;
            }
        }
    }
    auto diff = get_time() - t;
    printf("average time = %f ns\n", (double)diff / n);
    puts("PASS");
}

int main(int argc, char *argv[]) {
    // rlimit rlim;
    // rlim.rlim_cur = RLIM_INFINITY;
    // rlim.rlim_max = RLIM_INFINITY;
    // setrlimit(RLIMIT_CORE, &rlim);

    if(argc >= 2 && strcmp(argv[1], "test_pattern") == 0) {
        test_pattern();
        exit(0);
    }

    LOG_EVENT(-1, -1, -1, "querying_addresses");
    map<uint32_t, string> addresses;
	rdmc::query_addresses(addresses, node_rank);
    num_nodes = addresses.size();

    LOG_EVENT(-1, -1, -1, "calling_init");
    assert(rdmc::initialize(addresses, node_rank));

    LOG_EVENT(-1, -1, -1, "creating_barrier_group");
    vector<uint32_t> members;
    for(uint32_t i = 0; i < num_nodes; i++) members.push_back(i);
    universal_barrier_group = make_unique<rdmc::barrier_group>(members);

    universal_barrier_group->barrier_wait();
    uint64_t t1 = get_time();
    universal_barrier_group->barrier_wait();
    uint64_t t2 = get_time();
    reset_epoch();
    universal_barrier_group->barrier_wait();
    uint64_t t3 = get_time();

    printf(
        "Synchronized clocks.\nTotal possible variation = %5.3f us\n"
        "Max possible variation from local = %5.3f us\n",
        (t3 - t1) * 1e-3f, max(t2 - t1, t3 - t2) * 1e-3f);
    fflush(stdout);

    TRACE("Finished initializing.");

    printf("Experiment Name: %s\n", argv[1]);
    if(argc <= 1 || strcmp(argv[1], "custom") == 0) {
        for(int i = 0; i < 3; i++) {
            large_send();
        }
    } else if(strcmp(argv[1], "blocksize4") == 0) {
        blocksize_v_bandwidth(4);
    } else if(strcmp(argv[1], "blocksize16") == 0) {
        blocksize_v_bandwidth(16);
    } else if(strcmp(argv[1], "sendtypes") == 0) {
        compare_send_types();
    } else if(strcmp(argv[1], "bandwidth") == 0) {
        bandwidth_group_size();
    } else if(strcmp(argv[1], "overhead") == 0) {
        latency_group_size();
    } else if(strcmp(argv[1], "smallsend") == 0) {
        // small_send_latency_group_size();
    } else if(strcmp(argv[1], "concurrent") == 0) {
        concurrent_bandwidth_group_size();
    } else if(strcmp(argv[1], "active_senders") == 0) {
		for(bool interrupts : {true, false}){
			puts("==================================================================");
			printf("interrupts: %s\n\n", interrupts ? "enabled" : "disabled");
			rdma::impl::set_interrupt_mode(interrupts);
			active_senders();
		}
    } else if(strcmp(argv[1], "test_create_group_failure") == 0) {
        test_create_group_failure();
        exit(0);
	} else if(strcmp(argv[1], "test_cross_channel") == 0) {
		test_cross_channel();
		exit(0);
    } else {
        puts("Unrecognized experiment name.");
        fflush(stdout);
    }

    TRACE("About to trigger shutdown");
    universal_barrier_group->barrier_wait();
    universal_barrier_group.reset();
    rdmc::shutdown();
}
