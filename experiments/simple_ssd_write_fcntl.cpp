#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <time.h>
#include <string.h>
#include <malloc.h>
#include <memory>

using std::cout;
using std::cin;
using std::endl;
using std::ifstream;

int main() {
    srand(time(NULL));
    // Facebook.html
    ifstream fin("youtube_movie.mp4", ifstream::binary);
    char *read_buf;
    long long int read_buf_size;
    if(fin) {
        fin.seekg(0, fin.end);
        long long int length = fin.tellg();
        fin.seekg(0, fin.beg);
        cout << "Length of the file is: " << length << endl;
        read_buf = new char[length];
        read_buf_size = length;
        fin.read(read_buf, length);
        if(fin) {
            cout << "All characters read successfully.";
        } else {
            cout << "error: only " << fin.gcount() << " could be read";
            return -1;
        }
        fin.close();
    } else {
        cout << "Cannot open file. May be, it does not exist?" << endl;
        return -1;
    }
    long long unsigned buffer_size = (1 << 24);
    cout << buffer_size << endl;
    char *buf = (char *)memalign(buffer_size, buffer_size);
    for(int j = 0; j < buffer_size; ++j) {
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
    long long int num_messages = 400;
    // int fd =
    // open("messages",O_RDWR|O_CREAT|O_DIRECT|O_SYNC,S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    int fd =
        open("messages", O_WRONLY | O_CREAT | O_DIRECT);  // also tried O_SYNC:
                                                          // O_WRONLY | O_CREAT
                                                          // | O_DIRECT | O_SYNC
    if(fd < 0) {
        cout << "Failed to open the file" << endl;
        return 0;
    }
    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    for(int i = 0; i < num_messages; ++i) {
        int ret = write(fd, buf, buffer_size);
        if(ret < 0) {
            cout << "Write failed" << endl;
            return 0;
        }
    }
    // fsync(fd);
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    close(fd);
    long long int nanoseconds_elapsed =
        (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 +
        (end_time.tv_nsec - start_time.tv_nsec);
    double bw = (buffer_size * (long long int)num_messages * 1024 + 0.0) /
                nanoseconds_elapsed;
    cout << bw << endl;
}
