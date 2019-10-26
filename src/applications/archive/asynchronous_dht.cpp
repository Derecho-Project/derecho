#include <iostream>
#include <map>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/conf/conf.hpp>

using namespace persistent;

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

    void put(uint64_t key, const DataType& value) {
        table[key] = value;
    }

    DataType get(uint64_t key) {
        // for now, create the entry if it doesn't exist
        return table[key];
    }

    void fun(const DataType& v0, const DataType& v1, const DataType& v2) {
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

int main(int argc, char* argv[]) {
    derecho::Conf::initialize(argc, argv);

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

    derecho::SubgroupInfo subgroup_function{[](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < 2) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        for(uint i = 0; i < (uint32_t)curr_view.num_members / 2; ++i) {
            subgroup_vector[0].emplace_back(curr_view.make_subview({2 * i, 2 * i + 1}));
        }
        curr_view.next_unassigned_rank = curr_view.num_members;
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(HashTable<std::string>)),
                                    std::move(subgroup_vector));
        return subgroup_allocation;
    }};

    auto HashTableGenerator = [&](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<HashTable<std::string>>(); };
    derecho::Group<HashTable<std::string>> group({}, subgroup_function, nullptr,
                                                 std::vector<derecho::view_upcall_t>{announce_groups_provisioned},
                                                 HashTableGenerator);

    std::unique_lock<std::mutex> main_lock(main_mutex);
    main_cv.wait(main_lock, [&groups_provisioned]() { return groups_provisioned; });
    std::cout << "Subgroups provisioned" << std::endl;
    auto& subgroup_handle = group.get_subgroup<HashTable<std::string>>();
    uint32_t node_rank = group.get_my_rank();

    if(node_rank == 0) {
        subgroup_handle.ordered_send<HashTable<std::string>::FUN>("hello", "hi", "bye");
    } else {
        subgroup_handle.ordered_send<HashTable<std::string>::PRINT>();
    }
    std::cout << "Entering infinite loop" << std::endl;
    while(true) {
    }
}
