#include <map>
#include <memory>

#include "connection_manager.h"
#include "mutils-serialization/SerializationSupport.hpp"

using namespace std;

int main() {
    const int leader_port = 67599, port = 25739;
    uint32_t my_id;
    cin >> my_id;
    tcp::ip_addr_t ip_addr;
    cin >> ip_addr;
    map<tcp::node_id_t, tcp::ip_addr_t> ip_addrs;
    if(my_id == 0) {
        ip_addrs[0] = ip_addr;
        tcp::tcp_connections connections(0, ip_addrs, port);
        while(true) {
            auto conn_listener = make_unique<tcp::connection_listener>(leader_port);
            tcp::socket client_socket = conn_listener->accept();
            uint32_t remote_id = 0;
            if(!client_socket.exchange(my_id, remote_id)) {
                cerr << "WARNING: failed to exchange id with node"
                     << endl;
                return 0;
            } else {
                uint32_t ip_size;
                // get the ip addr of the new node
                char* remote_ip_c;
                client_socket.read((char*)&ip_size, sizeof(ip_size));
                client_socket.read(remote_ip_c, ip_size);
                string remote_ip = remote_ip_c;
                // update the ip addrs map
                ip_addrs[remote_id] = remote_ip;
                // send the connections over to the remote node
                auto bind_socket_write = [&client_socket](const char* bytes, size_t size) {client_socket.write(bytes, size); };
                size_t size_of_map = mutils::bytes_size(ip_addrs);
                client_socket.write((char*)&size_of_map, sizeof(size_of_map));
                mutils::post_object(bind_socket_write, ip_addrs);
                connections.write_all((char*)&remote_id, sizeof(remote_id));
                connections.write_all(ip_size, sizeof(ip_size));
                connections.write_all(remote_ip.c_str(), ip_size);
                connections.add(remote_id, remote_ip);
            }
        }
    } else {
        tcp::ip_addr_t leader_ip;
        cin >> leader_ip;
        tcp::socket leader_socket(leader_ip, leader_port);
        if(!leader_socket.exchange(my_id, remote_id)) {
            cerr << "WARNING: failed to exchange id with node"
                 << endl;
            return 0;
        } else {
            // send the ip addr to the leader
            uint32_t ip_size = ip_addr.size();
            leader_socket.write(ip_size, sizeof(ip_size));
            leader_socket.write(ip_addr.c_str(), ip_size);
            size_t size_of_map;
            leader_socket.read((char*)&size_of_map, sizeof(size_t));
            char buffer[size_of_map];
            leader_socket.read(buffer, size_of_map);
            ip_addrs = mutils::from_bytes(nullptr, buffer);
            tcp::tcp_connections connections(my_id, ip_addrs);
        }

        while(true) {
            uint32_t new_id;
            connections.read(0, (char*)&new_id, sizeof(new_id));
            size_t ip_size;
            connections.read(0, (char*)&ip_size, sizeof(ip_size));
            char* new_ip_c;
            connections.read(0, new_ip_c, ip_size);
            string new_ip = new_ip_c;
            connections.add(new_id, new_ip);
        }
    }
}
