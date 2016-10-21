/**
 * @file log_tail_length.cpp
 * A small executable that calculates the length, in bytes, of the tail of a
 * Derecho log file, from the provided message number to the end. Since message
 * numbers are a tuple (view_id, sender, index), the message number is provided
 * as three (whitespace-separated) input arguments.
 */

#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <cstdint>

#include <mutils-serialization/SerializationSupport.hpp>
#include "persistence.h"

using namespace derecho::persistence;

int main(int argc, char* argv[]) {
    if(argc < 4) {
        std::cout << "Usage: log_tail_length [-m] <filename> <vid> <sender> <index>" << std::endl;
        return 1;
    }
    bool print_metadata = false;
    int trailing_args_start = 1;
    if(strcmp(argv[1], "-m") == 0) {
        print_metadata = true;
        trailing_args_start = 2;
    }
    std::string filename(argv[trailing_args_start]);
    uint32_t target_vid = std::atoi(argv[trailing_args_start + 1]);
    uint32_t target_sender = std::atoi(argv[trailing_args_start + 2]);
    uint64_t target_index = std::atol(argv[trailing_args_start + 3]);

    uint64_t target_offset = 0;
    uint64_t target_size = 0;
    std::ifstream metadata_file(filename + METADATA_EXTENSION, std::ios::binary);
    message_metadata dummy_for_size;
    std::size_t size_of_metadata = mutils::bytes_size(dummy_for_size);
    char buffer[size_of_metadata];
    //Read the header to get past it
    header file_header;
    metadata_file.read((char*)&file_header, sizeof(file_header));
    //Scan through the metadata blocks until we find the one with the right sequence number
    uint32_t vid = 0, sender = 0;
    uint64_t index = 0;
    while(!(vid == target_vid && sender == target_sender && index == target_index) && metadata_file) {
        metadata_file.read(buffer, size_of_metadata);
        auto metadata = mutils::from_bytes<message_metadata>(nullptr, buffer);
        vid = metadata->view_id;
        sender = metadata->sender;
        index = metadata->index;
        if(print_metadata) {
            target_offset = metadata_file.tellg();
        } else {
            target_offset = metadata->offset;
            target_size = metadata->length;
        }
    }
    //Get the size of the whole file, so we can subtract from it
    std::streamoff file_size = 0;
    if(print_metadata) {
        metadata_file.seekg(0, std::ios::end);
        file_size = metadata_file.tellg();
        target_size = size_of_metadata;
    } else {
        std::ifstream logfile(filename, std::ios::binary);
        logfile.seekg(0, std::ios::end);
        file_size = logfile.tellg();
    }

    auto endoftarget = target_offset + target_size;
    auto distance = file_size - endoftarget;
    std::cout << distance << std::endl;
    return 0;
}
