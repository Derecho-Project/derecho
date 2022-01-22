/**
 * @file public_key_store.cpp
 */
#include "derecho/core/detail/public_key_store.hpp"
#include "derecho/persistent/detail/util.hpp"

#include <dirent.h>
#include <regex>

namespace derecho {

PublicKeyStore::PublicKeyStore(const std::string& key_file_directory) : key_directory(key_file_directory) {
    //Ensure the directory exists using this function from Persistent
    checkOrCreateDir(key_directory);
    //Scan the directory for existing key files and add them to the store
    DIR* dir_stream = opendir(key_directory.c_str());
    if(!dir_stream) {
        throw derecho_exception("Failed to open the public key directory after creating it!");
    }
    struct dirent* curr_entry = readdir(dir_stream);
    while(curr_entry != NULL) {
        if(curr_entry->d_type == DT_REG) {
            std::string key_file_name(curr_entry->d_name);
            try {
                ip_addr_t node_ip = ip_from_filename(key_file_name);
                add_public_key(node_ip, key_file_name);
            } catch(invalid_filename& ex) {
                //Continue and try the next file
            }
        }
        curr_entry = readdir(dir_stream);
    }
    closedir(dir_stream);
}

void PublicKeyStore::add_public_key(const ip_addr_t& node_ip, const std::string& key_file_name) {
    keys.emplace(node_ip, openssl::EnvelopeKey::from_pem_public(key_directory + "/" + key_file_name));
}

void PublicKeyStore::add_public_key(const ip_addr_t& node_ip, const uint8_t* key_bytes, std::size_t key_size) {
    keys.emplace(node_ip, openssl::EnvelopeKey::from_pem_public(key_bytes, key_size));
}

void PublicKeyStore::persist_key_for(const ip_addr_t& node_ip) {
    try {
        keys.at(node_ip).to_pem_public(key_directory + "/" + filename_for_ip(node_ip));
    } catch(std::out_of_range& err) {
        throw public_key_not_found(node_ip);
    }
}

openssl::EnvelopeKey PublicKeyStore::get_key_for(const ip_addr_t& ip_address) {
    try {
        return keys.at(ip_address);
    } catch(std::out_of_range& err) {
        //Rename this exception to something more meaningful
        throw public_key_not_found(ip_address);
    }
}

bool PublicKeyStore::contains_key_for(const ip_addr_t& ip_address) {
    return keys.find(ip_address) != keys.end();
}

std::string PublicKeyStore::filename_for_ip(const ip_addr_t& ip_address) {
    return ip_address + ".pem";
}

ip_addr_t PublicKeyStore::ip_from_filename(const std::string& key_file_name) {
    std::regex key_file_pattern("([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})\\.pem$");
    std::smatch regex_result;
    if(std::regex_match(key_file_name, regex_result, key_file_pattern)) {
        if(regex_result.size() == 2) {
            return regex_result[1].str();
        }
    }
    throw invalid_filename(key_file_name, "Public-key file did not match the expected naming pattern");
}

}  // namespace derecho
