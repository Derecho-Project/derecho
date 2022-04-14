#include <iostream>
#include <sstream>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

using derecho::ExternalClientCaller;
using derecho::Replicated;
using std::cout;
using std::endl;


class TestObject: public derecho::NotificationSupport, public mutils::ByteRepresentable {
public:
    REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(TestObject);

    std::size_t to_bytes(uint8_t* v) const {
        return 0;

    }

    std::size_t bytes_size() const {
        return 0;
    }

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
        // f((char*)&size, sizeof(size));
        // f(bytes, size);
    }

    static std::unique_ptr<TestObject> from_bytes(mutils::DeserializationManager *, const uint8_t *const v) {
        return nullptr;
    }

    static mutils::context_ptr<TestObject> from_bytes_noalloc(mutils::DeserializationManager *, const uint8_t *const v) {
        return nullptr;
    }

    static mutils::context_ptr<TestObject> from_bytes_noalloc_const(mutils::DeserializationManager *, const uint8_t *const v) {
        return nullptr;
    }


};

int main(int argc, char** argv) {
    if(argc < 5 || (argc > 5 && strcmp("--", argv[argc - 5]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] is_external (0 - internal, 1 - external) is_sender num_nodes num_messages" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    derecho::Conf::initialize(argc, argv);
    const uint is_external = std::stoi(argv[argc-4]);
    const uint is_sender = std::stoi(argv[argc-3]);
    int num_of_nodes = std::stoi(argv[argc-2]);
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    const uint count = std::stoi(argv[argc-1]);
    node_id_t my_id = derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID);

    if (!is_external) {
        derecho::SubgroupInfo subgroup_info{[num_of_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_of_nodes) {
            std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        // derecho::subgroup_shard_layout_t subgroup_layout(1);

        // std::vector<uint32_t> members(num_of_nodes);
        // for(int i = 0; i < num_of_nodes; i++) {
        //     members[i] = i;
        // }

        // subgroup_layout[0].emplace_back(curr_view.make_subview(members));

        derecho::subgroup_shard_layout_t subgroup_layout(num_of_nodes);
        for (int i = 0; i < num_of_nodes; i++){
            std::vector<uint32_t> members(1);
            members[0] = i;
            subgroup_layout[i].emplace_back(curr_view.make_subview(members));
        }
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_of_nodes);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(TestObject)), std::move(subgroup_layout));
        return subgroup_allocation;
        }};
        auto ba_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<TestObject>(); };

        derecho::Group<TestObject> group({},subgroup_info,{},std::vector<derecho::view_upcall_t>{},ba_factory);
        std::cout << "Finished constructing/joining Group" << std::endl;

        if(is_sender) {
            for (uint i = 0; i < count; ++i){
                // group.notify(2, bytes);
                derecho::ExternalClientCallback<TestObject>& handle = group.get_client_callback<TestObject>(my_id);
                std::cout << "acquired notification support callback!" << std::endl;
                uint64_t msg_size = max_msg_size - 128;
                derecho::NotificationMessage message(1, msg_size);
                for (uint64_t j = 0; j < msg_size - 1; ++j){
                    message.body[j] = 'a' + j % 26;
                }
                // notification!
                handle.p2p_send<RPC_NAME(notify)>(2, message);
            }
        }
        while(true) {
        }

    } else {
        derecho::ExternalGroupClient<TestObject> group;

        cout << "Finished constructing ExternalGroupClient" << endl;

        std::vector<node_id_t> members = group.get_members();
        std::vector<node_id_t> shard_members = group.get_shard_members(0, 0);
        ExternalClientCaller<TestObject, decltype(group)>& handle1 = group.get_subgroup_caller<TestObject>(0);
        ExternalClientCaller<TestObject, decltype(group)>& handle2 = group.get_subgroup_caller<TestObject>(1);


        // register notification handler
        handle1.register_notification_handler([](const derecho::NotificationMessage& data){std::cout << "Notification Successful from subgroup 0! Data: " << data.body << " Size: " << data.size << std::endl;});
        handle2.register_notification_handler([](const derecho::NotificationMessage& data){std::cout << "Notification Successful from subgroup 1! Data: " << data.body << " Size: " << data.size << std::endl;});

        cout << "Reached end of scope, entering infinite loop so program doesn't exit" << std::endl;
        while(true) {
        }
    }

}
