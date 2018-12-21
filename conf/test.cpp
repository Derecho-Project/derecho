#include "conf.hpp"
#include <iostream>
#include <spdlog/spdlog.h>

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);
    // spdlog::set_level(spdlog::level::trace);
    std::cout << "list of configurations:" << std::endl;
    // const derecho::Conf * pc = derecho::Conf::get();
    // const std::string k("RDMA/provider");
    // const std::string x = pc->getString(k);
    int ind = 0;
    while(1) {
        if(derecho::Conf::long_options[ind].name == nullptr) {
            break;
        }
        std::cout << derecho::Conf::long_options[ind].name << ":\t" << (derecho::Conf::get()->getString(derecho::Conf::long_options[ind].name)) << std::endl;
        ind++;
    }
    // print undocumented keys:
    if(derecho::hasCustomizedConfKey("UNDOCUMENTED/key")) {
        std::cout << "UNDOCUMENTED/key"
                  << ":\t" << derecho::getConfString("UNDOCUMENTED/key") << std::endl;
    } else {
        std::cout << "UNDOCUMENTED/key is not found." << std::endl;
    }
    return 0;
}
