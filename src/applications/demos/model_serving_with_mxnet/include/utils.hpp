#pragma once

#include <derecho/core/derecho.hpp>
#include <vector>

/**
 * NDArray data type
 */
enum TypeFlag {
  kFloat32 = 0,
  kFloat64 = 1,
  kFloat16 = 2,
  kUint8 = 3,
  kInt32 = 4,
  kInt8  = 5,
  kInt64 = 6,
};


/**
 * helper functions making source code neat go here.
 */

/**
 * parse nodes list
 * @PARAM node_list_str
 *     a list of nodes in string representation like: 1,2,5-7,100
 * @RETURN
 *     a vector of node ids
*/
std::vector<node_id_t> parse_node_list(const std::string& node_list_str);

struct frontend_info_t {
    node_id_t id;
    std::string ip_and_port;
    // constructor
    frontend_info_t(node_id_t id,std::string ip_and_port):
        id(id),
        ip_and_port(ip_and_port) {}
};
/**
 * parse frontend list
 * @PARAM frontend_list_str
 *     a list of nodes in string representation in format: <id>:<ip>:<port>;<id>:<ip>:<port>;...
 * @RETURN
 *     a vector of frontend list
 */
std::vector<struct frontend_info_t> parse_frontend_list(const std::string& frontend_list_str);
