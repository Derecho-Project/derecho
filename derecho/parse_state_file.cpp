/**
 * @file parse_state_file.cpp
 * A small executable utility that reads a serialized View from a "state
 * file" (saved by ManagedGroup during execution) and prints out its fields to
 * stdout, each on its own line. This allows Bash utilities to easily read
 * saved state files.
 */

#include <iostream>
#include <memory>
#include <string>

#include "view.h"
#include "derecho_caller.h"

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "Usage: parse_state_file <filename>" << std::endl;
        return 1;
    }

    std::string view_file_name(argv[1]);
    std::unique_ptr<derecho::View<rpc::Dispatcher<>>> view = derecho::load_view<rpc::Dispatcher<>>(view_file_name);
    //Use View's overloaded operator<< to write it out as parseable text
    std::cout << *view;
    return 0;
}
