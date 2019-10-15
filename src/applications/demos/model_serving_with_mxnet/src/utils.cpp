#include <utils.hpp>
#include <sstream>

std::vector<node_id_t> parse_node_list(const std::string& node_list_str) {
    std::string::size_type s = 0, e;
    std::vector<node_id_t> nodes;
    while(s < node_list_str.size()) {
        e = node_list_str.find(',', s); 
        if(e == std::string::npos) {
            e = node_list_str.size();
        }   
        if(e > s) {
            std::string range = node_list_str.substr(s, e - s); 
            std::string::size_type hyphen_pos = range.find('-');
            if(hyphen_pos != std::string::npos) {
                // range is "a-b"
                node_id_t rsid = std::stol(range.substr(0, hyphen_pos));
                node_id_t reid = std::stol(range.substr(hyphen_pos + 1));
                while(rsid <= reid) {
                    nodes.push_back(rsid);
                    rsid++;
                }   
            } else {
                nodes.push_back((node_id_t)std::stol(range));
            }   
        }   
        s = e + 1;
    }   
    return std::move(nodes);
}

std::vector<struct frontend_info_t> parse_frontend_list(const std::string& frontend_list_str) {
    std::vector<struct frontend_info_t> nodes;
    std::istringstream is(frontend_list_str);
    std::string s;
    while (std::getline(is, s, ',')) {
        std::string::size_type colon_pos = s.find(':');
        if (colon_pos == std::string::npos) {
            continue;
        }
        std::string id = s.substr(0,colon_pos);
        std::string ip_and_port = s.substr(colon_pos + 1);
        nodes.emplace_back(static_cast<node_id_t>(std::atoi(id.c_str())),ip_and_port);
    }
    return std::move(nodes);
}
