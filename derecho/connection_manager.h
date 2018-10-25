#pragma once

#include <cassert>
#include <map>
#include <mutex>

#include "derecho/derecho_type_definitions.h"
#include "locked_reference.h"
#include "tcp/tcp.h"

namespace tcp {
class tcp_connections {  
  std::mutex sockets_mutex;

  node_id_t my_id;
  std::unique_ptr<connection_listener> conn_listener;
  std::map<node_id_t, socket> sockets;
  bool add_connection(const node_id_t other_id,
                      const std::pair<ip_addr_t, uint16_t> &other_ip_and_port);
  void establish_node_connections(
      const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>
          &ip_addrs_and_ports);

public:
  tcp_connections(node_id_t _my_id, const std::map < node_id_t,
                  std::pair<ip_addr_t, uint16_t>> &ip_addrs_and_ports);
  void destroy();
  bool write(node_id_t node_id, char const *buffer, size_t size);
  bool write_all(char const *buffer, size_t size);
  bool read(node_id_t node_id, char *buffer, size_t size);
  bool add_node(node_id_t new_id,
                const std::pair<ip_addr_t, uint16_t> new_ip_addr_and_port);
  bool delete_node(node_id_t remove_id);
  template <class T> bool exchange(node_id_t node_id, T local, T &remote) {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    const auto it = sockets.find(node_id);
    assert(it != sockets.end());
    return it->second.exchange(local, remote);
  }
  int32_t probe_all();
  derecho::LockedReference<std::unique_lock<std::mutex>, socket>
  get_socket(node_id_t node_id);
};
} // namespace tcp
