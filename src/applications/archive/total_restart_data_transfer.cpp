

#include <iostream>
#include <derecho/core/derecho.hpp>

#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif


using namespace persistent;
using derecho::Replicated;

/** A test object like PersistentThing in total_restart_test,
 * but whose state can be much larger than an int. */
class TestObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
    Persistent<std::string> state;

public:
    TestObject(Persistent<std::string>& init_state) : state(std::move(init_state)) {}
    TestObject(PersistentRegistry* registry) : state([](){return std::make_unique<std::string>();},
                                                     nullptr,
                                                     registry) {}
    void update(const int update_num, const int sender, const std::string& new_state) {
        // std::cout << "Received update " << update_num << " from " << sender << std::endl;
        *state = new_state;
    }
    DEFAULT_SERIALIZATION_SUPPORT(TestObject, state);
    REGISTER_RPC_FUNCTIONS(TestObject, update);

};

std::string make_random_string(uint length) {
    std::stringstream string;
    for(uint i = 0; i < length; ++i) {
        char next = 'a'+ (rand() % 26);
        string << next;
    }
    return string.str();
}

int main(int argc, char** argv) {
    //Get custom arguments from the end of the arguments list
    const uint num_shards = 3;
    const uint nodes_per_shard = 3;
    const uint updates_behind = std::stoi(argv[argc - 2]);
    const uint update_size = std::stoi(argv[argc - 1]);
    unsigned int pid = getpid();
    srand(pid);

    derecho::Conf::initialize(argc, argv);

    const uint minimum_members_per_shard = nodes_per_shard - 1;
    derecho::SubgroupInfo subgroup_config(derecho::DefaultSubgroupAllocator({
        {std::type_index(typeid(TestObject)), derecho::one_subgroup_policy(derecho::flexible_even_shards(
                num_shards, minimum_members_per_shard, nodes_per_shard))}
    }));


    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    derecho::Group<TestObject> group(derecho::CallbackSet{nullptr}, subgroup_config, nullptr,
                                     std::vector<derecho::view_upcall_t>{},
                                     [](PersistentRegistry* pr) { return std::make_unique<TestObject>(pr); });

//    std::cout << "Waiting for all " << (num_shards * nodes_per_shard) << " members to join before starting updates" << std::endl;
    while(group.get_members().size() < num_shards * nodes_per_shard) {
    }
    group.barrier_sync();

     // end timer
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> time_in_ms = end_time - start_time;

    uint32_t my_rank = group.get_my_rank();
    if (my_rank == 0) {
        std::ofstream fout;
        fout.open("restart_data_transfer_times", std::ofstream::app);
        fout << updates_behind << " " << update_size << " " << time_in_ms.count() << std::endl;
        fout.close();
        std::cout << "Done writing the time measurement" << std::endl;
    }

    
    std::vector<node_id_t> my_shard_members = group.get_subgroup_members<TestObject>(0).at(
            group.get_my_shard<TestObject>(0));
    uint my_shard_rank;
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    for(my_shard_rank = 0; my_shard_rank < my_shard_members.size(); ++my_shard_rank) {
        if(my_shard_members[my_shard_rank] == my_id)
            break;
    }
    Replicated<TestObject>& obj_handle = group.get_subgroup<TestObject>();
    uint num_updates = 1000 + updates_behind;
    std::string new_value = make_random_string(update_size);
    for(uint counter = 0; counter < num_updates - updates_behind; ++counter) {
        // std::cout << "counter = " << counter << std::endl;
        derecho::rpc::QueryResults<void> result = obj_handle.ordered_send<RPC_NAME(update)>(counter, my_id, new_value);
    }
    group.barrier_sync();
    //The second node in each shard will exit early
    if(my_shard_rank == 2) {
        std::cout << "Waiting to be killed" << std::endl;
        while(true) {
        }
    }
    else {
        std::cout << "Enter something" << std::endl;
        int n;
        std::cin >> n;
        std::string new_value = make_random_string(update_size);
        for(uint counter = num_updates - updates_behind; counter < num_updates; ++counter) {
            // std::cout << "counter = " << counter << std::endl;
            derecho::rpc::QueryResults<void> result = obj_handle.ordered_send<RPC_NAME(update)>(counter, my_id, new_value);
        }
    }
    std::cout << "All updates sent, spinning" << std::endl;
    while(true) {
    }
    return 0;
}
