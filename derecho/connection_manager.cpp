#include "connection_manager.h"

#include <cassert>
#include <iostream>
#include <set>

namespace tcp {
bool tcp_connections::add_connection(const node_id_t other_id,
                                     const ip_addr_t& other_ip) {
    if(other_id < my_id) {
        try {
            sockets[other_id] = socket(other_ip, port);
        } catch(exception) {
            std::cerr << "WARNING: failed to node " << other_id << " at "
                      << other_ip << ":" << port << std::endl;
            return false;
        }

        uint32_t remote_id = 0;
        if(!sockets[other_id].exchange(my_id, remote_id)) {
            std::cerr << "WARNING: failed to exchange rank with node "
                      << other_id << " at " << other_ip << ":" << port
                      << std::endl;
            sockets.erase(other_id);
            return false;
        } else if(remote_id != other_id) {
            std::cerr << "WARNING: node at " << other_ip << ":" << port
                      << " replied with wrong id (expected " << other_id
                      << " but got " << remote_id << ")" << std::endl;

            sockets.erase(other_id);
            return false;
        }
        return true;
    } else if(other_id > my_id) {
        while(true) {
            try {
                socket s = conn_listener->accept();

                uint32_t remote_id = 0;
                if(!s.exchange(my_id, remote_id)) {
                    std::cerr << "WARNING: failed to exchange id with node"
                              << std::endl;
                    return false;
                } else {
                    sockets[remote_id] = std::move(s);
                    //If the connection we got wasn't the intended node, keep
                    //looping and try again; there must be multiple nodes connecting
                    //simultaneously
                    if(remote_id == other_id)
                        return true;
                }
            } catch(exception) {
                std::cerr << "Got error while attempting to listing on port"
                          << std::endl;
                return false;
            }
        }
    }

    return false;
}

void tcp_connections::establish_node_connections(const std::map<node_id_t, ip_addr_t>& ip_addrs) {
    conn_listener = std::make_unique<connection_listener>(port);

    for(auto it = ip_addrs.begin(); it != ip_addrs.end(); it++) {
        //Check that there isn't already be a connection to this ID,
        //since an earlier add_connection could have connected to it by "mistake"
        if(it->first != my_id && sockets.count(it->first) == 0) {
            if(!add_connection(it->first, it->second)) {
                std::cerr << "WARNING: failed to connect to node " << it->first
                          << " at " << it->second << std::endl;
            }
        }
    }
}

tcp_connections::tcp_connections(node_id_t _my_id,
                                 const std::map<node_id_t, ip_addr_t>& ip_addrs,
                                 uint32_t _port)
        : my_id(_my_id), port(_port) {
    establish_node_connections(ip_addrs);
}

void tcp_connections::destroy() {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    sockets.clear();
    conn_listener.reset();
}

bool tcp_connections::write(node_id_t node_id, char const* buffer,
                            size_t size) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    const auto it = sockets.find(node_id);
    assert(it != sockets.end());
    return it->second.write(buffer, size);
}

bool tcp_connections::write_all(char const* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    bool success = true;
    for(auto& p : sockets) {
        if(p.first == my_id) {
            continue;
        }
        success = success && p.second.write(buffer, size);
    }
    return success;
}

bool tcp_connections::read(node_id_t node_id, char* buffer,
                           size_t size) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    const auto it = sockets.find(node_id);
    assert(it != sockets.end());
    return it->second.read(buffer, size);
}

bool tcp_connections::add_node(node_id_t new_id, const ip_addr_t new_ip_addr) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    assert(new_id != my_id);
    //If there's already a connection to this ID, just return "success"
    if(sockets.count(new_id) > 0)
        return true;
    return add_connection(new_id, new_ip_addr);
}

bool tcp_connections::delete_node(node_id_t remove_id) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    return (sockets.erase(remove_id) > 0);
}

int32_t tcp_connections::probe_all() {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    for(auto& p : sockets) {
        bool new_data_available = p.second.probe();
        if(new_data_available == true) {
            return p.first;
        }
    }
    return -1;
}

derecho::LockedReference<std::unique_lock<std::mutex>, socket> tcp_connections::get_socket(node_id_t node_id) {
    return derecho::LockedReference<std::unique_lock<std::mutex>, socket>(sockets.at(node_id), sockets_mutex);
}
}
