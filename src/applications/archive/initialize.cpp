#include "initialize.h"
#include <iostream>

using namespace derecho;
using namespace tcp;

void write_string(socket& s, const std::string str) {
    // We are going to write the null character
    s.write(str.size() + 1);
    s.write(str.c_str(), str.size() + 1);
}

std::string read_string(socket& s) {
    // Read the size of the null-terminated string
    size_t size;
    s.read(size);
    // Read into a character array
    char* ch_arr = new char[size];
    s.read(ch_arr, size);
    // Return the string constructed from the character array read from the socket
    return std::string(ch_arr);
}

std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> initialize(const uint32_t num_nodes) {
    // Get conf file parameters
    // Global information
    const std::string leader_ip = getConfString(CONF_DERECHO_LEADER_IP);
    const uint16_t leader_gms_port = getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT);
    // Local information
    const uint32_t local_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const std::string local_ip = getConfString(CONF_DERECHO_LOCAL_IP);
    const uint16_t sst_port = getConfUInt16(CONF_DERECHO_SST_PORT);
    const uint16_t gms_port = getConfUInt16(CONF_DERECHO_GMS_PORT);

    bool is_leader = (leader_ip == local_ip && leader_gms_port == gms_port);

    if(is_leader) {
        // The map from id to IP and SST Port.
        std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports{{local_id, {local_ip, sst_port}}};

        std::map<uint32_t, socket> id_sockets_map;
        tcp::connection_listener c_l(leader_gms_port);

        for(uint32_t i = 0; i < num_nodes - 1; ++i) {
            socket s = c_l.accept();

            uint32_t id;
            s.read(id);
            std::string ip = read_string(s);
            uint16_t port;
            s.read(port);

            ip_addrs_and_ports[id] = {ip, port};
            id_sockets_map[id] = std::move(s);
        }

        /** Distribute the map
         */
        // ip_port_pair
        for(auto& id_socket_pair : id_sockets_map) {
            socket& s = id_socket_pair.second;
            for(auto [id, ip_port_pair] : ip_addrs_and_ports) {
                auto ip = ip_port_pair.first;
                auto port = ip_port_pair.second;
                s.write(id);
                write_string(s, ip);
                s.write(port);
            }
        }
        return ip_addrs_and_ports;
    } else {
        tcp::socket s(leader_ip, leader_gms_port);
        // Send local_id, local_ip, and sst_port to the leader.
        s.write(local_id);
        write_string(s, local_ip);
        s.write(sst_port);

        // The map from id to IP and SST Port to be received from the leader.
        std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports;
        for(uint32_t i = 0; i < num_nodes; ++i) {
            uint32_t id;
            s.read(id);
            std::string ip = read_string(s);
            uint16_t port;
            s.read(port);
            ip_addrs_and_ports[id] = {ip, port};
        }

        return ip_addrs_and_ports;
    }
}
