#pragma once

#include "rdmc/connection.h"

#include <map>
#include <mutex>
#include <cassert>

namespace tcp {
using ip_addr_t = std::string;
using node_id_t = uint32_t;
class tcp_connections {
    std::mutex sockets_mutex;

    node_id_t my_id;
    const uint32_t port;
    std::unique_ptr<connection_listener> conn_listener;
    std::map<node_id_t, socket> sockets;
    bool add_connection(const node_id_t other_id,
                        const ip_addr_t& other_ip);
    void establish_node_connections(const std::map<node_id_t, ip_addr_t>& ip_addrs);

public:
    tcp_connections(node_id_t _my_id,
                    const std::map<node_id_t, ip_addr_t>& ip_addrs,
                    uint32_t _port);
    void destroy();
    bool write(node_id_t node_id, char const* buffer, size_t size);
    bool write_all(char const* buffer, size_t size);
    bool read(node_id_t node_id, char* buffer, size_t size);
    bool add_node(node_id_t new_id, const ip_addr_t new_ip_addr);
    template <class T>
    bool exchange(node_id_t node_id, T local, T& remote) {
        std::lock_guard<std::mutex> lock(sockets_mutex);
        const auto it = sockets.find(node_id);
        assert(it != sockets.end());
        return it->second.exchange(local, remote);
    }
    int32_t probe_all();
};
}
