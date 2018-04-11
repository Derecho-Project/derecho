#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>
#include <string>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include "bytes_object.h"
#include <persistent/Persistent.hpp>
#include <mutils-serialization/SerializationSupport.hpp>

using std::cout;
using std::endl;
using namespace persistent;

/**
 * Non-Persitent Object with vairable sizes
 */
class ByteArrayObject: public mutils::ByteRepresentable {
public:
  Persistent<Bytes> pers_bytes;
//  Persistent<Bytes,ST_MEM> vola_bytes;

  void change_pers_bytes(const Bytes& bytes) {
    *pers_bytes = bytes;
  }

//  void change_vola_bytes(const Bytes& bytes) {
//    *vola_bytes = bytes;
//  }

  /** Named integers that will be used to tag the RPC methods */
//  enum Functions { CHANGE_PERS_BYTES, CHANGE_VOLA_BYTES };
  enum Functions { CHANGE_PERS_BYTES };

  static auto register_functions() {
    return std::make_tuple(
      derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes));
//      derecho::rpc::tag<CHANGE_VOLA_BYTES>(&ByteArrayObject::change_vola_bytes));
  }

//  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes,vola_bytes);
  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes);
  // constructor
//  ByteArrayObject(Persistent<Bytes> & _p_bytes,Persistent<Bytes,ST_MEM> & _v_bytes):
//  ByteArrayObject(Persistent<Bytes,ST_MEM> & _v_bytes):
  ByteArrayObject(Persistent<Bytes> & _p_bytes):
    pers_bytes(std::move(_p_bytes)) {
//    vola_bytes(std::move(_v_bytes)) {
  }
  // the default constructor
  ByteArrayObject(PersistentRegistry *pr):
    pers_bytes(nullptr,pr) {
//    vola_bytes(nullptr,pr) {
  }
};

int main(int argc, char *argv[]) {

  if(argc < 5) {
    std::cout<<"usage:"<<argv[0]<<" <all|half|one> <num_of_nodes> <msg_size> <count> [window_size=3]"<<std::endl;
    return -1;
  }
  int sender_selector = 0; // 0 for all sender
  if(strcmp(argv[1],"half") == 0)sender_selector=1;
  if(strcmp(argv[1],"one") == 0)sender_selector=2;
  int num_of_nodes = atoi(argv[2]);
  int msg_size = atoi(argv[3]);
  int count = atoi(argv[4]);
  struct timespec t1,t2,t3;

  derecho::node_id_t node_id;
  derecho::ip_addr my_ip;
  derecho::ip_addr leader_ip;
  query_node_info(node_id,my_ip,leader_ip);
  long long unsigned int max_msg_size = msg_size;
  long long unsigned int block_size = get_block_size(msg_size);
  unsigned int window_size = 3;
  if (argc >= 6) {
    window_size = (unsigned int )atoi(argv[5]);
  }
  derecho::DerechoParams derecho_params{max_msg_size, block_size, window_size};
  bool is_sending = true;

  long total_num_messages;
  switch(sender_selector) {
  case 0:
    total_num_messages = num_of_nodes * count;
    break;
  case 1:
    total_num_messages = (num_of_nodes/2) * count;
    break;
  case 2:
    total_num_messages = count;
    break;
  }

  derecho::CallbackSet callback_set{
    nullptr,//we don't need the stability_callback here
    // the persistence_callback either
    [&](derecho::subgroup_id_t subgroup,persistent::version_t ver){
      if(ver == (total_num_messages - 1)){
        if(is_sending) {
          clock_gettime(CLOCK_REALTIME,&t3);
          int64_t nsec = ((int64_t)t3.tv_sec - t1.tv_sec)*1000000000 + 
            t3.tv_nsec - t1.tv_nsec;
          double msec = (double)nsec/1000000;
          double thp_gbps = ((double)count*msg_size*8)/nsec;
          double thp_ops = ((double)count*1000000000)/nsec;
          std::cout<<"(pers)timespan:"<<msec<<" millisecond."<<std::endl;
          std::cout<<"(pers)throughput:"<<thp_gbps<<"Gbit/s."<<std::endl;
          std::cout<<"(pers)throughput:"<<thp_ops<<"ops."<<std::endl;  
          std::cout<<std::flush;
        }
        exit(0);
      }
    }
  };

  derecho::SubgroupInfo subgroup_info{
    {{std::type_index(typeid(ByteArrayObject)), [num_of_nodes,sender_selector](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
      if(curr_view.num_members < num_of_nodes) {
        std::cout << "not enough members yet:"<<curr_view.num_members<<" < "<<num_of_nodes<<std::endl;
        throw derecho::subgroup_provisioning_exception();
      }
      derecho::subgroup_shard_layout_t subgroup_vector(1);

      std::vector<uint32_t> members(num_of_nodes);
      std::vector<int> senders(num_of_nodes,1);
      for(int i=0;i<num_of_nodes;i++){
        members[i] = i;
        switch (sender_selector) {
        case 0: // all senders
          break;
        case 1: // half senders
          if (i <= (num_of_nodes-1)/2)senders[i] = 0;
          break;
        case 2: // one senders
          if (i != (num_of_nodes-1))senders[i] = 0;
          break;
        }
      }

      subgroup_vector[0].emplace_back(curr_view.make_subview(members,derecho::Mode::ORDERED,senders));
      next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
      return subgroup_vector;
    }}},
    {std::type_index(typeid(ByteArrayObject))}
  };

  auto ba_factory = [](PersistentRegistry * pr) {return std::make_unique<ByteArrayObject>(pr);};

  std::unique_ptr<derecho::Group<ByteArrayObject>> group;
  if(my_ip == leader_ip) {
    group = std::make_unique<derecho::Group<ByteArrayObject>>(
            node_id, my_ip, callback_set, subgroup_info, derecho_params,
            std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
            ba_factory);
  } else {
    group = std::make_unique<derecho::Group<ByteArrayObject>>(
            node_id, my_ip, leader_ip, callback_set, subgroup_info,
            std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
            ba_factory);
  }

  std::cout << "Finished constructing/joining Group" <<std::endl;

  uint32_t node_rank = -1;
  auto members_order = group->get_members();
  cout << "The order of members is :" << endl;
  for(uint i = 0; i < (uint32_t)num_of_nodes; ++i) {
      cout << members_order[i] << " ";
      if (members_order[i] == node_id) {
        node_rank = i;
      }
  }
  cout << endl;
  if((sender_selector == 1) && (node_rank <= (uint32_t)(num_of_nodes-1)/2)) is_sending = false;
  if((sender_selector == 2) && (node_rank != (uint32_t)num_of_nodes-1)) is_sending = false;

  std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;
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
  // if (node_id == 0) {
    if(is_sending) {
      derecho::Replicated<ByteArrayObject>& handle = group->get_subgroup<ByteArrayObject>();
      char *bbuf = new char[msg_size];
      bzero(bbuf,msg_size);
      Bytes bs(bbuf,msg_size);

      try{

        clock_gettime(CLOCK_REALTIME,&t1);
        for(int i=0;i<count;i++) {
            handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
        }
        clock_gettime(CLOCK_REALTIME,&t2);

      } catch (uint64_t exp){
        std::cout<<"Exception caught:0x"<<std::hex<<exp<<std::endl;
        return -1;
      }
      int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec)*1000000000 + 
        t2.tv_nsec - t1.tv_nsec;
      double msec = (double)nsec/1000000;
      double thp_gbps = ((double)count*msg_size*8)/nsec;
      double thp_ops = ((double)count*1000000000)/nsec;
      std::cout<<"(send)timespan:"<<msec<<" millisecond."<<std::endl;
      std::cout<<"(send)throughput:"<<thp_gbps<<"Gbit/s."<<std::endl;
      std::cout<<"(send)throughput:"<<thp_ops<<"ops."<<std::endl;
      #ifdef _PERFORMANCE_DEBUG
        (*handle.user_object_ptr)->pers_bytes.print_performance_stat();
      #endif//_PERFORMANCE_DEBUG
    }


  std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
  while(true){}
}
