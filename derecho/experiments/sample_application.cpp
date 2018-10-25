#include <derecho/derecho.h>
#include <derecho/view.h>

#include <vector>

using namespace derecho;
using std::vector;

class TicketBookingSystem : public mutils::ByteRepresentable {
  uint num_tickets;
  // booked[i] = 1, if i^th ticket is booked, otherwise 0
  vector<int> booked;

public:
  TicketBookingSystem (uint num_tickets) : num_tickets(num_tickets), booked(num_tickets, 0){
  }
  TicketBookingSystem (const uint& num_tickets, const vector<int>& booked) : num_tickets(num_tickets), booked(booked){
  }
  TicketBookingSystem(const TicketBookingSystem&) = default;
  
  bool book (uint tid) {
    std::cout << "In function book with argument " << tid << std::endl;
    if (!booked[tid]) {
      booked[tid] = true;
      std::cout << "Ticket booking successful" << std::endl;
      return true;
    }
    else {
      // ticket already booked
      std::cout << "Error: Ticket already booked" << std::endl;
      return false;
    }
  }

  bool cancel (uint tid) {
    if (booked[tid]) {
      booked[tid] = false;
      return true;
    }
    else {
      // ticket is not booked
      return false;
    }
  }

  // default state
  DEFAULT_SERIALIZATION_SUPPORT(TicketBookingSystem, num_tickets, booked);

  // what operations you want as part of the subgroup
  REGISTER_RPC_FUNCTIONS(TicketBookingSystem, book, cancel);
  
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

    Group<TicketBookingSystem>* group;

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
				       if (num_members < num_nodes) {
					 throw subgroup_provisioning_exception();
				       }
				       subgroup_shard_layout_t layout(num_members);
				       for (uint i = 0; i < num_members; ++i) {
					 layout[i].push_back(view.make_subview(vector<uint32_t>{members[i], members[(i+1)%num_members], members[(i+2)%num_members]}));
				       }
				       return layout;
				     }}};

    auto ticket_subgroup_factory = [num_tickets=100u] (PersistentRegistry*) {return std::make_unique<TicketBookingSystem>(num_tickets);};
    
    const unsigned long long int max_msg_size = 200;
    DerechoParams derecho_params{max_msg_size, max_msg_size, max_msg_size};

    SubgroupInfo subgroup_info(subgroup_membership_functions);

    if(my_id == 0) {
      group = new Group<TicketBookingSystem>(my_id, my_ip, callbacks, subgroup_info, derecho_params, {}, ticket_subgroup_factory);
    } else {
      group = new Group<TicketBookingSystem>(my_id, my_ip, leader_ip, callbacks, subgroup_info, {}, ticket_subgroup_factory);
    }

    std::cout << "Finished constructing/joining the group" << std::endl;
    auto group_members = group->get_members();
    // uint32_t my_rank = -1;
    std::cout << "Members are" << std::endl;
    for(uint i = 0; i < group_members.size(); ++i) {
        std::cout << group_members[i] << " ";
        // if(group_members[i] == my_id) {
        //   my_rank = i;
        // }
    }
    std::cout << std::endl;
    // if (my_rank == (uint32_t) -1) {
    //     exit(1);
    // }

    // // all members book a ticket
    // Replicated<TicketBookingSystem>& ticketBookingHandle = group->get_subgroup<TicketBookingSystem>();

    // rpc::QueryResults<bool> results = ticketBookingHandle.ordered_query<RPC_NAME(book)>(my_rank);
    // rpc::QueryResults<bool>::ReplyMap& replies = results.get();
    // for (auto& reply_pair: replies) {
    //     std::cout << "Reply from node " << reply_pair.first << ": " << std::boolalpha << reply_pair.second.get() << std::endl;
    // }

    // rpc::QueryResults<bool> results2 = ticketBookingHandle.ordered_query<RPC_NAME(book)>(my_rank);
    // rpc::QueryResults<bool>::ReplyMap& replies2 = results2.get();
    // for (auto& reply_pair: replies2) {
    //     std::cout << "Reply from node " << reply_pair.first << ": " << std::boolalpha << reply_pair.second.get() << std::endl;
    // }
    
    std::cout << "End of main. Waiting indefinitely" << std::endl;
    while(true) {
    }
    return 0;
}
