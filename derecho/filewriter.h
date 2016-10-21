#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "persistence.h"

namespace derecho {

class FileWriter {
public:
    //  const uint32_t MSG_LOCALLY_STABLE = 0x1;
    //  const uint32_t MSG_GLOBAL_ORDERED = 0x2;
    //  const uint32_t MSG_GLOBAL_STABLE = 0x4;
    //  const uint32_t MSG_LOCALLY_PERSISTENT = 0x8;

private:
    std::function<void(persistence::message)> message_written_upcall;

    std::mutex pending_writes_mutex;
    std::condition_variable pending_writes_cv;
    std::queue<persistence::message> pending_writes;

    std::mutex pending_callbacks_mutex;
    std::condition_variable pending_callbacks_cv;
    std::queue<std::function<void()>> pending_callbacks;

    bool exit;

    std::thread writer_thread;
    std::thread callback_thread;

    void perform_writes(std::string filename);
    void issue_callbacks();

public:
    FileWriter(const std::function<void(persistence::message)> &_message_written_upcall,
               const std::string &filename);
    ~FileWriter();

    FileWriter(FileWriter &) = delete;
    FileWriter(FileWriter &&) = default;

    FileWriter &operator=(FileWriter &) = delete;
    FileWriter &operator=(FileWriter &&) = default;

    void set_message_written_upcall(const std::function<void(persistence::message)> &
                                        _message_written_upcall);
    void write_message(persistence::message m);
};
}
