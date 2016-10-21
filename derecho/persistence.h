#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdio>
#include <string>

#include <mutils-serialization/SerializationSupport.hpp>

#include "derecho_row.h"

namespace derecho {

namespace persistence {

struct message {
    char *data;
    uint64_t length;
    uint32_t view_id;
    uint32_t sender;
    uint64_t index;
    bool cooked;
};

struct __attribute__((__packed__)) header {
    uint8_t magic[8];
    uint32_t version;
};

struct __attribute__((__packed__)) message_metadata {
    uint32_t view_id;
    uint8_t is_cooked;
    uint8_t padding0;
    uint16_t padding1;
    uint32_t sender;
    uint32_t padding2;
    uint64_t index;

    uint64_t offset;
    uint64_t length;
};

static const std::string METADATA_EXTENSION = ".metadata";
static const std::string PAXOS_STATE_EXTENSION = ".paxosstate";
static const std::string PARAMATERS_EXTENSION = ".params";
static const std::string SWAP_FILE_EXTENSION = ".swp";

/**
 * Persists an object to disk, using mutils-serialization functions. Uses the
 * "safe save" method (writing to a swap file first) to handle crashes during
 * the write to disk. The created file will first contain the size of the
 * serialized object, followed by the serialized object itself.
 * @param object Any object that is serializable using mutils::to_bytes
 * @param filename The name of the file to create when saving this object to disk.
 */
template<typename T>
void persist_object(const T& object, const std::string& filename) {
    std::ofstream swap_file(filename + SWAP_FILE_EXTENSION);
    auto swap_file_write_func = [&](char const* const c, std::size_t n) {
        swap_file.write(c,n);
    };
    mutils::post_object(swap_file_write_func, mutils::bytes_size(object));
    mutils::post_object(swap_file_write_func, object);
    swap_file.close();

     if(std::rename((filename + SWAP_FILE_EXTENSION).c_str(), filename.c_str()) < 0) {
        std::cerr << "Error updating saved-state file on disk! " << strerror(errno) << std::endl;
    }
}

/**
 * Inverse of persist_object; loads an object from disk using the
 * mutils-serialization deserialize functions. This function tries to load the
 * object from both the given filename and its corresponding swap file, and
 * returns the object from the swap file if (and only if) the object from the
 * expected file is empty or incomplete.
 * @param filename The name of the file to read for a serialized object
 * @return (by pointer) A new object of type T constructed with the data in this file
 */
template <typename T>
std::unique_ptr<T> load_object(const std::string& filename) {
    std::ifstream file(filename);
    std::ifstream swap_file(filename + persistence::SWAP_FILE_EXTENSION);
    std::unique_ptr<T> object;
    std::unique_ptr<T> swap_object;
    //The expected saved-parameters file might not exist, in which case we'll fall back to the swap file
    if(file.good()) {
        //Each file contains the size of the object (an int copied as bytes),
        //followed by a serialized object
        std::size_t size_of_object;
        file.read((char*)&size_of_object, sizeof(size_of_object));
        char buffer[size_of_object];
        file.read(buffer, size_of_object);
        //If the file doesn't contain a complete object (due to a crash
        //during writing), the read() call will set failbit. In that case,
        //leave object a null pointer.
        if(!file.fail()) {
            object = mutils::from_bytes<T>(nullptr, buffer);
        }
    }
    if(swap_file.good()) {
        std::size_t size_of_object;
        swap_file.read((char*)&size_of_object, sizeof(size_of_object));
        char buffer[size_of_object];
        swap_file.read(buffer, size_of_object);
        if(!swap_file.fail()) {
            swap_object = mutils::from_bytes<T>(nullptr, buffer);
        }
    }
    if(object == nullptr) {
        return swap_object;
    } else {
        return object;
    }
}

}  // namespace persistence

using persistence::persist_object;
using persistence::load_object;

}  // namespace derecho
