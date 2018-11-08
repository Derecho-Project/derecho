#include <derecho/derecho.h>
#include <derecho/view.h>

#include <vector>
#include <fstream>

using namespace derecho;
using std::vector;
using std::string;
using std::pair;

class Messages : public mutils::ByteRepresentable {
  // vector of maps <node_id, message>
  static uint nodeid = 0;
  vector<map<uint, vector<string>>> msgs;
  std::ifstream inf("node%d.txt", id);

public:
  Messages () : msgs(10, map<uint, vector<string>>){
  }
  Messages (const vector<map<uint,vector<string>>>& msgs) : msgs(msgs){
  }
  Messages(const Messages&) = default;

  // pass in node's ID
  // use std::accumulate to fold instead?

  void send(uint id, string msg){
    msgs.push_back(std::make_pair(id, msg));
    std::cout << "Node " << id << " sent msg " << msg << std::endl;
  }


  // remove param, have id be part of the constructor
  bool verify_contents(uint id){
    string line;
    std::vector<pair<uint, string>>::iterator it = msgs.begin();
    std::ifstream infile("node%d.txt", id);
    while(std::getline(infile, line)){
      for(it; it != msgs.end(); ++it){
        if(it->first == id){
          if(it->second != line){
            std::cout << "Message content error!" << std::endl;
            return false;
          }
          break;
        }
      }
    }
    std::cout << "Pass" << std::endl;
    return true;
  }

  bool verify_order (vector<pair<uint, string>> v) {
    std::vector<pair<uint, string>>::iterator a = msgs.begin();
    std::vector<pair<uint, string>>::iterator b = v.begin();
    while(a != msgs.end() && b != v.end()){
      if(*a != *b){
        std::cout << "Message order error!" << std::endl;
        return false;
      }
      ++a;
      ++b;
    }
    if (a != msgs.end() || b != v.end()){
      std::cout << "Message order error!" << std::endl;
      return false;
    }
    std::cout << "Pass" << std::endl;
    return true;
  }

  // default state
  DEFAULT_SERIALIZATION_SUPPORT(Messages, msgs);

  // what operations you want as part of the subgroup
  REGISTER_RPC_FUNCTIONS(Messages, send, verify_contents, verify_order);
  
};

int main(int argc, char *argv[]) {
    if(argc < 2) {
        std::cout << "Provide the number of nodes as a command line argument" << std::endl;
        exit(1);
    }
    const uint32_t num_nodes = atoi(argv[1]);

    node_id_t my_id;
    ip_addr leader_ip, my_ip;

    std::cout << "Enter my id: " << std::endl;
    std::cin >> my_id;

    std::cout << "Enter my ip: " << std::endl;
    std::cin >> my_ip;

    std::cout << "Enter leader's ip: " << std::endl;
    std::cin >> leader_ip;

    Group<Messages>* group;

    auto delivery_callback = [my_id](subgroup_id_t subgroup_id, node_id_t sender_id, message_id_t index, char* buf, long long int size) {
        // null message filter
        if(size == 0) {
            return;
        }
        std::cout << "In the delivery callback for subgroup_id=" << subgroup_id << ", sender=" << sender_id << std::endl;
        std::cout << "Message: " << buf << std::endl;
        return;
    };

    CallbackSet callbacks{delivery_callback};

    std::map<std::type_index, shard_view_generator_t>
      subgroup_membership_functions{{std::type_index(typeid(TicketBookingSystem)),
				     [num_nodes](const View& view, int&, bool) {
				       auto& members = view.members;
				       auto num_members = members.size();
				       // if (num_members < num_nodes) {
				       // 	 throw subgroup_provisioning_exception();
				       // }
				       subgroup_shard_layout_t layout(num_members);
				       // for (uint i = 0; i < num_members; ++i) {
					 // layout[i].push_back(view.make_subview(vector<uint32_t>{members[i], members[(i+1)%num_members], members[(i+2)%num_members]}));
					 // layout[i].
					 layout[0].push_back(view.make_subview(vector<uint32_t>(members)));
					 // }
				       return layout;
				     }}};

    auto ticket_subgroup_factory = [num_tickets=100u] (PersistentRegistry*) {return std::make_unique<TicketBookingSystem>(num_tickets);};
    
    const unsigned long long int max_msg_size = 200;
    DerechoParams derecho_params{max_msg_size, max_msg_size, max_msg_size};

    SubgroupInfo subgroup_info(subgroup_membership_functions);

    auto view_upcall = [](const View& view) {
			 std::cout << "The members are: " << std::endl;
			 for (auto m : view.members) {
			   std::cout << m << " ";
			 }
			 std::cout << std::endl;
		       };
    
    if(my_id == 0) {
      group = new Group<TicketBookingSystem>(my_id, my_ip, callbacks, subgroup_info, derecho_params, vector<view_upcall_t>{view_upcall}, ticket_subgroup_factory);
    } else {
      group = new Group<TicketBookingSystem>(my_id, my_ip, leader_ip, callbacks, subgroup_info, vector<view_upcall_t>{view_upcall}, ticket_subgroup_factory);
    }

    std::cout << "Finished constructing/joining the group" << std::endl;
    auto group_members = group->get_members();
    uint32_t my_rank = -1;
    std::cout << "Members are" << std::endl;
    for(uint i = 0; i < group_members.size(); ++i) {
        std::cout << group_members[i] << " ";
        if(group_members[i] == my_id) {
          my_rank = i;
        }
    }
    std::cout << std::endl;
    if (my_rank == (uint32_t) -1) {
        exit(1);
    }

    // // all members book a ticket
    // Replicated<TicketBookingSystem>& ticketBookingHandle = group->get_subgroup<TicketBookingSystem>();

    // rpc::QueryResults<bool> results = ticketBookingHandle.ordered_query<RPC_NAME(book)>(my_rank);
    // rpc::QueryResults<bool>::ReplyMap& replies = results.get();
    // for (auto& reply_pair: replies) {
    //     std::cout << "Reply from node " << reply_pair.first << ": " << std::boolalpha << reply_pair.second.get() << std::endl;
    // }
    
    std::cout << "End of main. Waiting indefinitely" << std::endl;
    while(true) {
    }
    return 0;
}
