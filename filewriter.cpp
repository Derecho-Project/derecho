
#include "filewriter.h"
#include "mutils-serialization/SerializationSupport.hpp"

#include <cstring>
#include <thread>
#include <iostream>
#include <fstream>
#include <utility>
#include <functional>

using std::mutex;
using std::unique_lock;
using std::ofstream;

namespace derecho {

using namespace persistence;

const uint8_t MAGIC_NUMBER[8] = {'D', 'E', 'R', 'E', 'C', 'H', 'O', 29};

FileWriter::FileWriter(const std::function<void(message)>& _message_written_upcall,
                       const std::string& filename)
        : message_written_upcall(_message_written_upcall),
          exit(false),
          writer_thread(&FileWriter::perform_writes, this, filename),
          callback_thread(&FileWriter::issue_callbacks, this) {}

FileWriter::~FileWriter() {
    {
        // must hold both mutexes to change exit, since either thread could be about
        // to read it before calling wait()
        unique_lock<mutex> writes_lock(pending_writes_mutex);
        unique_lock<mutex> callbacks_lock(pending_callbacks_mutex);
        exit = true;
    }
    pending_callbacks_cv.notify_all();
    pending_writes_cv.notify_all();
    if(writer_thread.joinable()) writer_thread.join();
    if(callback_thread.joinable()) callback_thread.join();
}

void FileWriter::set_message_written_upcall(
    const std::function<void(message)>& _message_written_upcall) {
    message_written_upcall = _message_written_upcall;
}

void FileWriter::perform_writes(std::string filename) {
    ofstream data_file(filename, std::ios::app);
    ofstream metadata_file(filename + METADATA_EXTENSION, std::ios::app);

    unique_lock<mutex> writes_lock(pending_writes_mutex);

    uint64_t current_offset = 0;

    persistence::header h;
    memcpy(h.magic, MAGIC_NUMBER, sizeof(MAGIC_NUMBER));
    h.version = 0;
    metadata_file.write((char*)&h, sizeof(h));

    while(!exit) {
        pending_writes_cv.wait(writes_lock);

        while(!pending_writes.empty()) {
            using namespace std::placeholders;

            message m = pending_writes.front();
            pending_writes.pop();

            message_metadata metadata;
            metadata.view_id = m.view_id;
            metadata.sender = m.sender;
            metadata.index = m.index;
            metadata.offset = current_offset;
            metadata.length = m.length;
            metadata.is_cooked = m.cooked;

            data_file.write(m.data, m.length);
            mutils::post_object(std::bind(&std::ofstream::write, &metadata_file, _1, _2), metadata);

            data_file.flush();
            metadata_file.flush();

            current_offset += m.length;

            {
                unique_lock<mutex> callbacks_lock(pending_callbacks_mutex);
                pending_callbacks.push(std::bind(message_written_upcall, m));
            }
            pending_callbacks_cv.notify_all();
        }
    }
}

void FileWriter::issue_callbacks() {
    unique_lock<mutex> lock(pending_callbacks_mutex);

    while(!exit) {
        pending_callbacks_cv.wait(lock);

        while(!pending_callbacks.empty()) {
            auto callback = pending_callbacks.front();
            pending_callbacks.pop();
            lock.unlock();
            callback();
            lock.lock();
        }
    }
}

void FileWriter::write_message(message m) {
    {
        unique_lock<mutex> lock(pending_writes_mutex);
        pending_writes.push(m);
    }
    pending_writes_cv.notify_all();
}
}
