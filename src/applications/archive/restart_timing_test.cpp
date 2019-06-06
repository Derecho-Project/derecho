/**
 * @file restart_timing_test.cpp
 * Used to record the timing breakdown data for total restart experiments
 * @date Jun 5, 2019
 * @author edward
 */

#include <iostream>
#include <derecho/core/derecho.hpp>

#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif


using namespace persistent;
using derecho::Replicated;
//like std::chrono::milliseconds, but floating-point instead of integer
using milliseconds_fp = std::chrono::duration<double, std::milli>;

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
    unsigned int pid = getpid();
    srand(pid);
    int num_args = 3;
    if(argc < (num_args + 1) || (argc > (num_args + 1) && strcmp("--", argv[argc - (num_args + 1)]))) {
        std::cout << "Invalid command line arguments." << std::endl;
        std::cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] num_shards updates_behind update_size" << std::endl;
        return -1;
    }

    const uint nodes_per_shard = 3;
    const uint num_shards = std::stoi(argv[argc - num_args]);
    const uint updates_behind = std::stoi(argv[argc - num_args + 1]);
    const uint update_size = std::stoi(argv[argc - num_args + 2]);

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

    auto constructor_done_time = std::chrono::high_resolution_clock::now();
//    std::cout << "Waiting for all " << (num_shards * nodes_per_shard) << " members to join before starting updates" << std::endl;
    while(group.get_members().size() < num_shards * nodes_per_shard) {
    }
    group.barrier_sync();

     // end timer
    auto sync_done_time = std::chrono::high_resolution_clock::now();
    milliseconds_fp total_time = sync_done_time - start_time;

    uint32_t my_rank = group.get_my_rank();
    if (my_rank == 0) {
        using namespace derecho;
        milliseconds_fp quorum_time = ViewManager::restart_timepoints[0] - start_time;
        milliseconds_fp truncation_time = ViewManager::restart_timepoints[1] -
                ViewManager::restart_timepoints[0];
        milliseconds_fp transfer_time = ViewManager::restart_timepoints[2] -
                ViewManager::restart_timepoints[1];
        milliseconds_fp commit_time = ViewManager::restart_timepoints[3] -
                ViewManager::restart_timepoints[2];
        milliseconds_fp finish_setup_time = constructor_done_time - ViewManager::restart_timepoints[3];
        milliseconds_fp barrier_time = sync_done_time - constructor_done_time;
        std::ofstream fout;
        fout.open("restart_data_transfer_times", std::ofstream::app);
        fout << updates_behind << " " << update_size << " "
                << quorum_time.count() << " " << truncation_time.count() << " "
                << transfer_time.count() << " " << commit_time.count() << " "
                << finish_setup_time.count() << " " << barrier_time.count() << " "
                << total_time.count() << std::endl;
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
