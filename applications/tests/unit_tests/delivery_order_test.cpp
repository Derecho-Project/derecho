#include <derecho/derecho.h>
#include <derecho/view.h>

#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>

using namespace derecho;
using std::ifstream;
using std::string;
using std::vector;

int main(int argc, char* argv[]) {
    if (argc < 5) {
      // Print out error message
      exit(1);
    }
    
    std::map<node_id_t, ifstream*> input_file_map;
    const string my_input_file = argv[1];
    const uint32_t num_nodes = std::stoi(argv[2]);
    const uint32_t num_msgs = std::stoi(argv[3]);
    const uint32_t msg_size = std::stoi(argv[4]);

    Conf::initialize(argc, argv);

    uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

    SubgroupInfo subgroup_info(
            {{std::type_index(typeid(RawObject)), [num_nodes](const View& curr_view, int& next_unassigned_rank) {
                  if(curr_view.members.size() < num_nodes) {
                      std::cout << "Waiting for all nodes to join. Throwing subgroup provisioning exception!" << std::endl;
                      throw subgroup_provisioning_exception();
                  }
                  return one_subgroup_entire_view(curr_view, next_unassigned_rank);
		}}});

    
    auto get_next_line = [&input_file_map](node_id_t sender_id) {
        string line;
        if(input_file_map.at(sender_id)->is_open()) {
            getline(*(input_file_map.at(sender_id)), line);
            return line;
        }
        std::cout << "Error: Input file not open!!!" << std::endl;
	exit(1);
    };

    Group<>* group;
    uint32_t my_rank;
    uint32_t max_msg_size = getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
    volatile bool done = false;
    auto delivery_callback = [&, num_received_msgs_map = std::map<node_id_t, uint32_t>(),
			      received_msgs_index_map = std::map<node_id_t, uint32_t>(),
                              received_msgs = std::vector<node_id_t>(),
                              finished_nodes = std::set<node_id_t>()](subgroup_id_t subgroup_id,
                                                                      node_id_t sender_id,
                                                                      message_id_t index,
								      std::optional<std::pair<char*, long long int>> data,
								      persistent::version_t ver) mutable {
        char *buf;
        long long int size;
	std::tie(buf, size) = data.value();
	
        if(num_received_msgs_map[sender_id] == 0) {
	  std::cout << "Received input file name for node " << sender_id << std::endl;
            ifstream* input_file = new ifstream(buf);
            if(input_file->is_open()) {
                input_file_map[sender_id] = input_file;
            } else {
                std::cout << "Unable to open input file!" << std::endl;
                exit(1);
            }
        } else if(num_received_msgs_map[sender_id] <= num_msgs) {
	    //std::cout << "Receiving random message from node " << sender_id << std::endl;
	    //std::cout << "Num received messages is: " << num_received_msgs_map[sender_id] << std::endl;
            received_msgs.push_back(sender_id);
	    string line = get_next_line(sender_id);
	    string msg_received(buf, size);
            if(line != msg_received) {
                std::cout << "Error: Message contents mismatch or violation of local order!!!" << std::endl;
                exit(1);
            }
	    if(received_msgs.size() == num_msgs * num_nodes) {//sender_id == node_id && num_received_msgs_map[sender_id] == num_msgs) {
                std::cout << "Local ordering test successful!" << std::endl;
                if(my_rank != 0) {
		    std::cout << "Sending my received messages to leader" << std::endl;
                    std::thread temp([&]() {
			uint32_t num_msgs_sent = 0;
                        uint32_t num_entries;
			RawSubgroup& group_as_subgroup = group->get_subgroup<RawObject>();
			while (num_msgs_sent < received_msgs.size()) {
			  num_entries = std::min(received_msgs.size() - num_msgs_sent, max_msg_size / sizeof(node_id_t) - 50);
			  group_as_subgroup.send(num_entries * sizeof(node_id_t), [&received_msgs, &num_msgs_sent, &num_entries](char* buf){
			     for(uint i = 0; i < num_entries; i++) {
			         if (num_msgs_sent + i >= received_msgs.size()) {
				     break;
				 }
                                 (node_id_t&)buf[sizeof(node_id_t) * i] = received_msgs[num_msgs_sent + i];
                             } 
			  });
			  num_msgs_sent = num_msgs_sent + num_entries;
			}
			    //group_as_subgroup.send(received_msgs.size() * sizeof(node_id_t), [received_msgs](char* buf) {
			    //for(uint i = 0; i < received_msgs.size(); i++) {
                            //    (node_id_t&)buf[sizeof(node_id_t) * i] = received_msgs[i];
                            //}            
			    //});
                    });
                    temp.detach();
                }
            }
        } else {
            if(my_rank == 0) {
	        long long int curr_size = 0;
		std::cout << "Received node " << sender_id << "'s received_msgs" << std::endl;
		std::cout << "Size is: " << size << std::endl;
	        while (curr_size < size) {
		    //std::cout << "curr size is: " << curr_size << std::endl;
		    node_id_t val = *((node_id_t*)buf);
                    buf += sizeof(node_id_t);
		    curr_size += sizeof(node_id_t);
		    //std::cout << "Received messages index for sender id " << sender_id << " is " << received_msgs_index_map[sender_id] << std::endl;
                    node_id_t node = received_msgs[received_msgs_index_map[sender_id]];
		    //std::cout << "Node is: " << node << std::endl;
		    //std::cout << "Val is: " << val << std::endl;    
		    if(node != val) {
		        std::cout << "Error: Violation of global order!!!" << std::endl;
                        exit(1);
                    }
		    received_msgs_index_map[sender_id] += 1;
		}
                //std::cout << "Testing for global ordering" << std::endl;
                // verify the message against received_msgs
                //for(auto node : received_msgs) {
                //    node_id_t val = *((node_id_t*)buf);
                //    buf += sizeof(node_id_t);
                //    if(node != val) {
                //        std::cout << "Error: Violation of global order!!!" << std::endl;
                //        exit(1);
                //    }
                //}
		std::cout << "Received msgs size is: " << received_msgs.size() << std::endl;
		std::cout << "Received msgs index map entry for " << sender_id << std::endl;
		std::cout << received_msgs_index_map[sender_id] << std::endl;
		if (received_msgs_index_map[sender_id] == received_msgs.size()) {
		  finished_nodes.insert(sender_id);
		}
		if(finished_nodes.size() == num_nodes - 1) {
		  done = true;
		}
            }
	    //std::cout << "Received msgs size is: " << received_msgs.size() << std::endl;
	    //std::cout << "Received msgs index map entry for " << sender_id << std::endl;
	    //std::cout << received_msgs_index_map[sender_id] << std::endl;
	    //if (received_msgs_index_map[sender_id] == received_msgs.size()) {
	    //    finished_nodes.insert(sender_id);
	    //}
            //if(finished_nodes.size() == num_nodes - 1) {
            //    done = true;
            //}
        }
        num_received_msgs_map[sender_id] = num_received_msgs_map[sender_id] + 1;
    };

    group = new Group<>(CallbackSet{delivery_callback}, subgroup_info);
    std::cout << "Finished constructing/joining Group" << std::endl;
    RawSubgroup& group_as_subgroup = group->get_subgroup<RawObject>();
    // my_rank = group_as_subgroup.get_my_rank();
    my_rank = -1;
    auto members_order = group->get_members();
    std::cout << "The order of members is :" << std::endl;
    for(uint i = 0; i < num_nodes; ++i) {
      std::cout << members_order[i] << " ";
      if(members_order[i] == node_id) {
	my_rank = i;
      }
    }
    std::cout << std::endl;
    
    ifstream input_file(my_input_file);
    std::cout << "Constructed file handler" << std::endl;
    group_as_subgroup.send(my_input_file.size(), [my_input_file](char* buf){
	my_input_file.copy(buf, my_input_file.size());
    });

    string line;
    uint32_t msg_counter = 0;
    std::cout << "Attempting to send messages" << std::endl;
    while(msg_counter < num_msgs) {
        getline(input_file, line);
        //std::cout << "Sending message: " << line << std::endl;
	//std::cout << "Message size is: " << line.size() << std::endl;
	group_as_subgroup.send(msg_size, [line](char* buf){
	    line.copy(buf, line.size());
	});
        msg_counter = msg_counter + 1;
    }

    input_file.close();
    if (my_rank != 0) {done = true;}
    while(!done) {
    }
    if (my_rank == 0) {
        std::cout << "Global ordering test successful!" << std::endl;
    }
    group->barrier_sync();
    group->leave();
    return 0;
}
