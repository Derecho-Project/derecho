#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <malloc.h>
#include <map>
#include <memory>
#include <time.h>
#include <unistd.h>
#include <vector>

#include "derecho/derecho.h"
#include "block_size.h"
#include "rdmc/util.h"
#include "aggregate_bandwidth.h"
#include "block_size.h"
#include "log_results.h"

#include "rdmc/rdmc.h"

using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::ifstream;

using derecho::RawObject;

void generate_buffer(char *buf, long long int buffer_size) {
    srand(time(NULL));
    // ifstream fin ("youtube_movie.mp4", ifstream::binary);
    // ifstream fin ("Facebook.html", ifstream::binary);
    // cout << "Facebook.html" << endl;
    // cout << "youtube_movie.mp4" << endl;
    cout << "All zeroes" << endl;
    // cout << "Random zeroes" << endl;
    // cout << "Data with patterns" << endl;
    // char *read_buf;
    // long long int read_buf_size;
    // if (fin) {
    //   fin.seekg(0,fin.end);
    //   long long int length = fin.tellg();
    //   fin.seekg(0,fin.beg);
    //   cout << "Length of the file is: " << length << endl;
    //   read_buf = new char [length];
    //   read_buf_size = length;
    //   fin.read(read_buf, length);
    //   if (fin) {
    //     cout << "All characters read successfully.";
    //   }
    //   else {
    //     cout << "error: only " << fin.gcount() << " could be read";
    //     exit(1);
    //   }
    //   fin.close();
    // }
    // else {
    //   cout << "Cannot open file. May be, it does not exist?" << endl;
    //   exit(1);
    // }
    for(int j = 0; j < buffer_size; ++j) {
        // buf[j] = 0;
        // buf[j] = 'a'+(rand()%26);
        buf[j] = 'a' + (j % 26);
    }
    // copy from read_buf to buf repeated so that buf is filled
    // long long int start = 0;
    // while (start + read_buf_size < buffer_size) {
    //   memcpy(buf+start, read_buf, read_buf_size);
    //   start += read_buf_size;
    // }
    // memcpy(buf+start, read_buf, buffer_size-start);
}

int main(int argc, char *argv[]) {
    // srand(time(NULL));

    long long unsigned buffer_size = (1 << 24);
    cout << buffer_size << endl;
    char *buf = (char *)memalign(buffer_size, buffer_size);
    generate_buffer(buf, buffer_size);
    int fd = open("messages", O_WRONLY | O_CREAT | O_DIRECT);
    if(fd < 0) {
        cout << "Failed to open the file" << endl;
        return 0;
    }

    uint32_t server_rank = 0;
    uint32_t node_rank;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

	rdmc::query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    // long long unsigned int max_msg_size = buffer_size;
    long long unsigned int block_size = 1000000ull;
    int num_messages = 100;

    bool done = false;
    auto stability_callback = [
        &num_messages,
        &done,
        &num_nodes,
        &fd,
        &buf,
        &buffer_size,
        num_last_received = 0u
    ](uint32_t subgroup, int sender_id, long long int index, char *msg_buf,
      long long int msg_size) mutable {
        // cout << "In stability callback; sender = " << sender_id << ", index =
        // " << index << endl;
        int ret = write(fd, buf, buffer_size);
        if(ret < 0) {
            cout << "Write failed" << endl;
            exit(1);
        }

        if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
            done = true;
        }
    };

    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams param_object{buffer_size, block_size};
    derecho::SubgroupInfo one_raw_group{ {{std::type_index(typeid(RawObject)), 1}},
        {{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}
    };
    std::unique_ptr<derecho::Group<>> managed_group;

    if(node_rank == server_rank) {
        managed_group = std::make_unique<derecho::Group<>>(
                node_addresses[node_rank], callbacks, one_raw_group, param_object);
    } else {
        managed_group = std::make_unique<derecho::Group<>>(
                node_rank, node_addresses[node_rank],
                node_addresses[server_rank],
                callbacks, one_raw_group);
    }

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group->get_members().size() < num_nodes) {
    }
    auto members_order = managed_group->get_members();
    cout << "The order of members is :" << endl;
    for(auto id : members_order) {
        cout << id << " ";
    }
    cout << endl;

    auto send_all = [&]() {
        derecho::RawSubgroup& group_as_subgroup = managed_group->get_subgroup<RawObject>();
        for(int i = 0; i < num_messages; ++i) {
            char *buf = group_as_subgroup.get_sendbuffer_ptr(buffer_size);
            while(!buf) {
                buf = group_as_subgroup.get_sendbuffer_ptr(buffer_size);
            }
            group_as_subgroup.send();
        }
    };
    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    send_all();
    while(!done) {
    }
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    long long int nanoseconds_elapsed =
        (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 +
        (end_time.tv_nsec - start_time.tv_nsec);
    double bw;
    bw = (buffer_size * num_messages * num_nodes + 0.0) / nanoseconds_elapsed;
    double avg_bw = aggregate_bandwidth(members, node_rank, bw);
    cout << avg_bw << endl;
    // log_results(num_nodes, buffer_size, avg_bw, "data_derecho_bw");

    managed_group->barrier_sync();
    std::string log_filename =
        (std::stringstream() << "events_node" << node_rank << ".csv").str();
    std::ofstream logfile(log_filename);
    managed_group->print_log(logfile);
    managed_group->leave();
    cout << "Finished destroying managed_group" << endl;
}
