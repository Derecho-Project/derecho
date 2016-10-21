/**
 * @file latest_logged_message.cpp
 * Prints out the message number of the last message in a Derecho logfile.
 * Since "sequence numbers" depend on the number of members in a group, the
 * least confusing way to print the message number is the tuple (view_id,
 * sender, index). This is printed without the parentheses or comma for ease
 * of parsing by bash scripts.
 */

#include <iostream>
#include <fstream>
#include <string>
#include <strings.h>

#include "serialization/SerializationSupport.hpp"
#include "persistence.h"

using namespace derecho::persistence;

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "Usage: latest_logged_message <filename>" << std::endl;
        return 1;
    }

    std::string filename(argv[1]);
    std::ifstream metadata_file(filename + METADATA_EXTENSION);
    //Since metadatas are written in chronological order, the one at the end
    //of the file is the latest one.
    message_metadata dummy_for_size;
    std::size_t size_of_metadata = mutils::bytes_size(dummy_for_size);
    metadata_file.seekg(-1 * size_of_metadata, std::ios::end);
    char buffer[size_of_metadata];
    metadata_file.read(buffer, size_of_metadata);
    std::unique_ptr<message_metadata> metadata = mutils::from_bytes<message_metadata>(nullptr, buffer);
    std::cout << metadata->view_id << " " << metadata->sender << " " << metadata->index << std::endl;
    return 0;
}
