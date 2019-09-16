#include <derecho/tcp/tcp.hpp>
#include <derecho/conf/conf.hpp>

using namespace derecho;
using namespace tcp;

int main(int argc, char* argv[]) {
    pthread_setname_np(pthread_self(), "failure_map");
    Conf::initialize(argc, argv);

    const std::vector<std::string>& config_failure_map = socket::get_failure_map();
    std::cout << "Failure map:" << std::endl;
    for (const std::string& ip_address_mask : config_failure_map)
        std::cout << ip_address_mask << std::endl;

    const std::string& ip1 = "192.168.1.1";
    const std::string& ip2 = "127.0.0.1";
    const std::string& ip3 = "192.168.3.4";
    const std::vector<std::string>& failure_map1 = {"192.168"};
    const std::vector<std::string>& failure_map2 = {};
    const std::vector<std::string>& failure_map3 = {"192.168.1"};
    const std::vector<std::string>& failure_map4 = {"192.168.1", "127.0.0"};

    std::cout << socket::fails_together(ip1, ip3, failure_map1) << std::endl;
    std::cout << socket::fails_together(ip1, ip3, failure_map2) << std::endl;
    std::cout << socket::fails_together(ip1, ip3, failure_map3) << std::endl;
    std::cout << socket::fails_together(ip1, ip2, failure_map4) << std::endl;
}