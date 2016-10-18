#include <iostream>
#include <memory>
#include <string>

#include "../filewriter.h"
#include "../derecho_group.h"

using std::cout;
using std::endl;
using namespace derecho;

int main (int argc, char *argv[])  {
    auto file_written_callback = [](persistence::message m) {
        cout << "Message " << m.index << " written to file!" << endl;
    };

    std::string filename = "data0.dat";
    auto file_writer =
        std::make_unique<FileWriter>(file_written_callback, filename);

    //    MessageBuffer buffer(1000);
    //    for(size_t i = 0; i < 1000; ++i) {
    //        buffer.buffer[i] = 'a';
    //    }
    char buf[1000];
    for(auto& byte : buf) {
        byte = 'a';
    }

    Message m;
    m.sender_rank = 1;
    m.index = 0;
    m.size = 1000;
    //    m.message_buffer = std::move(buffer);

    persistence::message message{&buf[0], m.size, 1, m.sender_rank, (uint64_t) m.index};

    file_writer->write_message(message);

    return 0;
}
