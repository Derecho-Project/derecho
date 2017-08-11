#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>
#include <string>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <persistent/Persistent.hpp>
#include <mutils-serialization/SerializationSupport.hpp>

/**
 * Non-Persitent Object with vairable sizes
 */
template<unsigned object_size>
class ByteArrayObject: public mutils::ByteRepresentable {
public:
  char pArr[object_size];
  std::string log;

  bool change_state(char new_state[object_size]){
    std::cout<<"copy from:"<<(long long int)new_state<<"."<<std::endl;
    // memcpy(pArr,new_state,object_size);
    std::cout<<"copied"<<std::endl;
    return true;
  }

  void append(const std::string& words) {
    log = words;
  }

  /** Named integers that will be used to tag the RPC methods */
  enum Functions { CHANGE_STATE, APPEND };

  static auto register_functions() {
    return std::make_tuple(derecho::rpc::tag<CHANGE_STATE>(&ByteArrayObject::change_state),
      derecho::rpc::tag<APPEND>(&ByteArrayObject::append));
  }

  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pArr);
  // constructor
  ByteArrayObject(const char * _pArr) {
    memcpy(pArr,_pArr,object_size);
  }
  ByteArrayObject() {
    // the default constructor
  }
};


int main(int argc, char *argv[]) {

  if(argc != 4) {
    std::cout<<"usage:"<<argv[0]<<" <num_of_nodes> <msg_size> <count>"<<std::endl;
    return -1;
  }
  int num_of_nodes = atoi(argv[1]);
  int msg_size = atoi(argv[2]);
  int count = atoi(argv[3]);

  derecho::node_id_t node_id;
  derecho::ip_addr my_ip;
  derecho::ip_addr leader_ip;
  query_node_info(node_id,my_ip,leader_ip);
  long long unsigned int max_msg_size = msg_size+100;//how to decide on the size?
  long long unsigned int block_size = (max_msg_size>1048576)?1048576:max_msg_size;
  derecho::DerechoParams derecho_params{max_msg_size, block_size};

  derecho::CallbackSet callback_set{
    nullptr,//we don't need the stability_callback here
    nullptr//we don't need the persistence_callback either
  };

  derecho::SubgroupInfo subgroup_info{
    {{std::type_index(typeid(ByteArrayObject<1024>)), [num_of_nodes](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
      if(curr_view.num_members < num_of_nodes) {
        std::cout << "not enough members yet:"<<curr_view.num_members<<" < "<<num_of_nodes<<std::endl;
        throw derecho::subgroup_provisioning_exception();
      }
      derecho::subgroup_shard_layout_t subgroup_vector(1);

      std::vector<uint32_t> members(num_of_nodes);
      for(int i=0;i<num_of_nodes;i++){
        members[i] = i;
      }

      subgroup_vector[0].emplace_back(curr_view.make_subview(members));
      next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
      return subgroup_vector;
    }}},
    {std::type_index(typeid(ByteArrayObject<1024>))}
  };

  auto ba_factory = [](PersistentRegistry *) {return std::make_unique<ByteArrayObject<1024>>();};

  std::unique_ptr<derecho::Group<ByteArrayObject<1024>>> group;
  if(my_ip == leader_ip) {
    group = std::make_unique<derecho::Group<ByteArrayObject<1024>>>(
            node_id, my_ip, callback_set, subgroup_info, derecho_params,
            std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
            ba_factory);
  } else {
    group = std::make_unique<derecho::Group<ByteArrayObject<1024>>>(
            node_id, my_ip, leader_ip, callback_set, subgroup_info,
            std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
            ba_factory);
  }

  std::cout << "Finished constructing/joining Group" <<std::endl;

  bool inadequately_provisioned = true;
  while(inadequately_provisioned) {
    try {
      group->get_subgroup<ByteArrayObject<1024>>();
      inadequately_provisioned = false;
    } catch(derecho::subgroup_provisioning_exception& e) {
      inadequately_provisioned = true;
    }
  }

  std::cout << "All members have joined, subgroups are provisioned." <<std::endl;
/*
  if (node_id == 0) {
    derecho::Replicated<ByteArrayObject<1024>>& handle = group->get_subgroup<ByteArrayObject<1024>>();
    char my_array[1024];
    derecho::rpc::QueryResults<bool> results = handle.ordered_query<ByteArrayObject<1024>::CHANGE_STATE>(my_array);
    decltype(results)::ReplyMap& replies = results.get();
    std::cout<<"Got a reply map!"<<std::endl;
    for(auto& ritr:replies) {
      std::cout<<"Reply from node "<< ritr.first <<" was " << std::boolalpha << ritr.second.get()<<std::endl;
    }
  }
*/
//  if (node_id == 1) {
    derecho::Replicated<ByteArrayObject<1024>>& handle = group->get_subgroup<ByteArrayObject<1024>>();
    std::string str_1k(msg_size,'x');

    struct timespec t1,t2;
    clock_gettime(CLOCK_REALTIME,&t1);

    for(int i=0;i<count;i++) {
      //std::cout<<"ordered send:"<<i<<std::endl;
      handle.ordered_send<ByteArrayObject<1024>::APPEND>(str_1k);
    }
    clock_gettime(CLOCK_REALTIME,&t2);
    int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec)*1000000000 + 
      t2.tv_nsec - t1.tv_nsec;
    double msec = (double)nsec/1000000;
    double thp_gbps = ((double)count*msg_size*8)/nsec;
    double thp_ops = ((double)count*1000000000)/nsec;
    std::cout<<"timespan:"<<msec<<" millisecond."<<std::endl;
    std::cout<<"throughput:"<<thp_gbps<<"Gbit/s."<<std::endl;
    std::cout<<"throughput:"<<thp_ops<<"ops."<<std::endl;
//  }

  std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
  while(true){}
}
