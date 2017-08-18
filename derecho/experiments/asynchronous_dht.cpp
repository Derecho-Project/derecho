#include <iostream>
#include <map>

#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>

template <class T>
class HashTable : public mutils::ByteRepresentable {
private:
  std::map<uint64_t, T> table;

public:
  void put(uint64_t key, T value) {
        table[key] = value;
    }

  T get(uint64_t key) {
        // for now, create the entry if it doesn't exist
        return table[key];
    }

    enum Functions {
        PUT,
        GET
    };

    static auto register_functions() {
      return std::make_tuple(derecho::rpc::tag<PUT>(&HashTable<T>::put),
			     derecho::rpc::tag<GET>(&HashTable<T>::get));
    }

    HashTable() : table() {}

    HashTable(const std::map<uint64_t, T>& table) : table(table) {}

    DEFAULT_SERIALIZATION_SUPPORT(HashTable, table);
};

int main() {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 10000;
    long long unsigned int block_size = 10100;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

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
                  if(curr_view.num_members < 10) {
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  for(uint i = 0; i < (uint32_t)curr_view.num_members / 2; ++i) {
                      subgroup_vector[0].emplace_back(curr_view.make_subview({2 * i, 2 * i + 1}));
                  }
                  next_unassigned_rank = curr_view.num_members;
                  return subgroup_vector;
              }}}};

    auto HashTableGenerator = []() { return std::make_unique<HashTable<std::string>>(); };

    std::unique_ptr<derecho::Group<HashTable<std::string>>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<HashTable<std::string>>>(node_id, my_ip, callback_set, subgroup_info, derecho_params, std::vector<derecho::view_upcall_t>{announce_groups_provisioned}, derecho::derecho_gms_port, HashTableGenerator);
    } else {
        group = std::make_unique<derecho::Group<HashTable<std::string>>>(node_id, my_ip, leader_ip, callback_set, subgroup_info, std::vector<derecho::view_upcall_t>{announce_groups_provisioned}, derecho::derecho_gms_port, HashTableGenerator);
    }

    std::unique_lock<std::mutex> main_lock(main_mutex);
    main_cv.wait(main_lock, [&groups_provisioned]() { return groups_provisioned; });
    while(true) {
    }
}
