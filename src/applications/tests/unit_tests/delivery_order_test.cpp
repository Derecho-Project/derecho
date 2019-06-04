#include <derecho/core/derecho.hpp>
#include <derecho/core/view.hpp>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace derecho;
using std::cout;
using std::endl;
using std::ifstream;
using std::string;
using std::vector;

int main(int argc, char* argv[]) {
    if(argc < 5) {
        // Print out error message
        cout << "Usage: " << argv[0] << " <input_file_path> <num_nodes> <num_msgs> <msg_size> [configuration options...]" << endl;
        exit(1);
    }

    std::map<node_id_t, ifstream*> input_file_map;
    const string my_input_file = argv[1];
    const uint32_t num_nodes = std::stoi(argv[2]);
    const uint32_t num_msgs = std::stoi(argv[3]);
    const uint32_t msg_size = std::stoi(argv[4]);

    Conf::initialize(argc, argv);

    uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

    SubgroupInfo subgroup_info([num_nodes](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.members.size() < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        return one_subgroup_entire_view(subgroup_type_order, prev_view, curr_view);
    });

    auto get_next_line = [&input_file_map](node_id_t sender_id) {
        string line;
        if(input_file_map.at(sender_id)->is_open()) {
            getline(*(input_file_map.at(sender_id)), line);
            return line;
        }
        cout << "Error: Input file not open!!!" << endl;
        exit(1);
    };

    std::unique_ptr<Group<RawObject>> group;
    uint32_t my_rank;
    uint64_t max_msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    volatile bool done = false;
    auto delivery_callback = [&, num_received_msgs_map = std::map<node_id_t, uint32_t>(),
                              received_msgs_index_map = std::map<node_id_t, uint32_t>(),
                              received_msgs = std::vector<node_id_t>(),
                              finished_nodes = std::set<node_id_t>()](subgroup_id_t subgroup_id,
                                                                      node_id_t sender_id,
                                                                      message_id_t index,
                                                                      std::optional<std::pair<char*, long long int>> data,
                                                                      persistent::version_t ver) mutable {
        char* buf;
        long long int size;
        std::tie(buf, size) = data.value();

        if(num_received_msgs_map[sender_id] == 0) {
            ifstream* input_file = new ifstream(buf);
            if(input_file->is_open()) {
                input_file_map[sender_id] = input_file;
            } else {
                cout << "Unable to open input file!" << endl;
                exit(1);
            }
        } else if(num_received_msgs_map[sender_id] <= num_msgs) {
            received_msgs.push_back(sender_id);
            string line = get_next_line(sender_id);
            string msg_received(buf, size);
            if(line != msg_received) {
                cout << "Error: Message contents mismatch or violation of local order!!!" << endl;
                exit(1);
            }
            if(received_msgs.size() == num_msgs * num_nodes) {
                cout << "Local ordering test successful!" << endl;
                if(my_rank != 0) {
                    std::thread temp([&]() {
                        Replicated<RawObject>& group_as_subgroup = group->get_subgroup<RawObject>();
                        for(uint64_t num_entries_sent = 0; num_entries_sent < received_msgs.size();) {
                            uint64_t num_entries = std::min(received_msgs.size() - num_entries_sent, max_msg_size / sizeof(node_id_t) - 50);
                            group_as_subgroup.send(num_entries * sizeof(node_id_t),
                                                   [&received_msgs, &num_entries_sent, &num_entries](char* buf) {
                                                       for(uint i = 0; i < num_entries; i++) {
                                                           (node_id_t&)buf[sizeof(node_id_t) * i] = received_msgs[num_entries_sent + i];
                                                       }
                                                   });
                            num_entries_sent += num_entries;
                        }
                    });
                    temp.detach();
                }
            }
        } else {
            if(my_rank == 0) {
                long long int curr_size = 0;
                while(curr_size < size) {
                    node_id_t val = *((node_id_t*)buf);
                    buf += sizeof(node_id_t);
                    curr_size += sizeof(node_id_t);
                    node_id_t node = received_msgs[received_msgs_index_map[sender_id]];
                    if(node != val) {
                        cout << "Error: Violation of global order!!!" << endl;
                        exit(1);
                    }
                    received_msgs_index_map[sender_id] += 1;
                }
                if(received_msgs_index_map[sender_id] == received_msgs.size()) {
                    finished_nodes.insert(sender_id);
                }
                if(finished_nodes.size() == num_nodes - 1) {
                    done = true;
                }
            }
        }
        num_received_msgs_map[sender_id] = num_received_msgs_map[sender_id] + 1;
    };

    group = std::make_unique<Group<RawObject>>(CallbackSet{delivery_callback}, subgroup_info,
                nullptr, std::vector<view_upcall_t>{}, &raw_object_factory);
    cout << "Finished constructing/joining Group" << endl;
    Replicated<RawObject>& group_as_subgroup = group->get_subgroup<RawObject>();
    // my_rank = group_as_subgroup.get_my_rank();
    my_rank = -1;
    auto members_order = group->get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
        if(members_order[i] == node_id) {
            my_rank = i;
        }
    }
    cout << endl;

    ifstream input_file(my_input_file);
    group_as_subgroup.send(my_input_file.size(), [my_input_file](char* buf) {
        my_input_file.copy(buf, my_input_file.size());
    });

    string line;
    uint32_t msg_counter = 0;
    while(msg_counter < num_msgs) {
        getline(input_file, line);
        group_as_subgroup.send(msg_size, [line](char* buf) {
            line.copy(buf, line.size());
        });
        msg_counter = msg_counter + 1;
    }

    input_file.close();
    if(my_rank != 0) {
        done = true;
    }
    while(!done) {
    }
    if(my_rank == 0) {
        cout << "Global ordering test successful!" << endl;
    }
    group->barrier_sync();
    group->leave();
    return 0;
}
