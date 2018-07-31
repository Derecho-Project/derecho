#include <iostream>
#include <map>

#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>

template <class DataType>
class HashTable;

template <class DataType>
class HashTable : public mutils::ByteRepresentable, public derecho::GroupReference {
private:
    std::map<uint64_t, DataType> table;

public:
    using derecho::GroupReference::group;
    using derecho::GroupReference::subgroup_index;
    enum Functions {
        PUT,
        GET,
        FUN,
        PRINT
    };

    void put(uint64_t key, DataType value) {
        table[key] = value;
    }

    DataType get(uint64_t key) {
        // for now, create the entry if it doesn't exist
        return table[key];
    }

    void fun(DataType v0, DataType v1, DataType v2) {
        put(0, v0);
        put(1, v1);
        put(2, v2);
    }

    void print() {
        for(auto p : table) {
            std::cout << p.first << " " << p.second << std::endl;
        }
        derecho::Replicated<HashTable<DataType>>& subgroup_handle = group->template get_subgroup<HashTable<DataType>>();
        std::thread temp([&]() {
            subgroup_handle.template ordered_send<HashTable<DataType>::PRINT>();
        });
        temp.detach();
    }

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<PUT>(&HashTable<DataType>::put),
                               derecho::rpc::tag<GET>(&HashTable<DataType>::get),
                               derecho::rpc::tag<FUN>(&HashTable<DataType>::fun),
                               derecho::rpc::tag<PRINT>(&HashTable<DataType>::print));
    }

    HashTable() : table() {}

    HashTable(const std::map<uint64_t, DataType>& table) : table(table) {}

    DEFAULT_SERIALIZATION_SUPPORT(HashTable, table);
};

int main() {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 10000;
    long long unsigned int block_size = 10100;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};

    derecho::CallbackSet callback_set{{}, {}};

    std::mutex main_mutex;
    std::condition_variable main_cv;

    bool groups_provisioned = false;
    auto announce_groups_provisioned = [&groups_provisioned, &main_cv, &main_mutex](const derecho::View& view) {
        if(view.is_adequately_provisioned) {
            std::unique_lock<std::mutex> lock(main_mutex);
            groups_provisioned = true;
            main_cv.notify_all();
        }
    };

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(HashTable<std::string>)), [](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < 2) {
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  for(uint i = 0; i < (uint32_t)curr_view.num_members / 2; ++i) {
                      subgroup_vector[0].emplace_back(curr_view.make_subview({2 * i, 2 * i + 1}));
                  }
                  next_unassigned_rank = curr_view.num_members;
                  return subgroup_vector;
              }}}};

    derecho::Group<HashTable<std::string>>* group;
    auto HashTableGenerator = [&](PersistentRegistry*) { return std::make_unique<HashTable<std::string>>(); };

    if(my_ip == leader_ip) {
        group = new derecho::Group<HashTable<std::string>>(node_id, my_ip, callback_set, subgroup_info, derecho_params, std::vector<derecho::view_upcall_t>{announce_groups_provisioned}, derecho::derecho_gms_port, HashTableGenerator);
    } else {
        group = new derecho::Group<HashTable<std::string>>(node_id, my_ip, leader_ip, callback_set, subgroup_info, std::vector<derecho::view_upcall_t>{announce_groups_provisioned}, derecho::derecho_gms_port, HashTableGenerator);
    }

    std::unique_lock<std::mutex> main_lock(main_mutex);
    main_cv.wait(main_lock, [&groups_provisioned]() { return groups_provisioned; });
    std::cout << "Subgroups provisioned" << std::endl;
    auto& subgroup_handle = group->get_subgroup<HashTable<std::string>>();
    if(node_id == 0) {
        subgroup_handle.ordered_send<HashTable<std::string>::FUN>("hello", "hi", "bye");
    } else {
        subgroup_handle.ordered_send<HashTable<std::string>::PRINT>();
    }
    std::cout << "Entering infinite loop" << std::endl;
    while(true) {
    }
}
