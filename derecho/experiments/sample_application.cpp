#include <derecho/derecho.h>
#include <derecho/view.h>

#include <vector>

using namespace derecho;
using std::vector;

int main () {
  node_id_t my_id;
  ip_addr leader_ip, my_ip;

  std::cout << "Enter my id: " << std::endl;
  std::cin >> my_id;

  std::cin >> my_ip;
  std::cin >> leader_ip;

  Group<>* group;

  auto delivery_callback = [my_id] (subgroup_id_t subgroup_id, node_id_t sender_id, message_id_t index, char* buf, long long int size) {
    return; 
  };
  
  CallbackSet callbacks{delivery_callback};

  std::map<std::type_index, shard_view_generator_t> subgroup_membership_functions{{std::type_index(typeid(RawObject)), [] (const View& view, int&, bool) {
	subgroup_shard_layout_t layout;
	vector<node_id_t> members = view.members;
	layout[0].push_back(view.make_subview(members));
	return layout;
      }}};

  unsigned long long int max_msg_size = 100;
  DerechoParams derecho_params{max_msg_size, max_msg_size + 100, max_msg_size};

  SubgroupInfo subgroup_info(subgroup_membership_functions);
  
  if (my_id == 0) {
    group = new Group<>(my_id, my_ip, callbacks, subgroup_info, derecho_params);
  }
  else {
    group = new Group<>(my_id, my_ip, leader_ip, callbacks, subgroup_info);
  }

  while (true) {
    
  }
  return 0;
}
