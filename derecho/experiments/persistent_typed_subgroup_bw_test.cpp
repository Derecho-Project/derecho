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
#include <mutils-serialization/context_ptr.hpp>

using mutils::context_ptr;

//This class is modified from Matt's implementation
struct Bytes : public mutils::ByteRepresentable{

        char *bytes;
        std::size_t size;

        Bytes(const char * b, decltype(size) s)
                :size(s){
            bytes = nullptr;
            if(s>0) {
                bytes = new char[s];
                memcpy(bytes,b,s);
            }
        }
        Bytes() {
          bytes = nullptr;
          size = 0;
        }
        virtual ~Bytes(){
            if(bytes!=nullptr) {
                delete bytes;
            }
        }

        Bytes & operator = (Bytes && other) {
            char *swp_bytes = other.bytes;
            std::size_t swp_size = other.size;
            other.bytes = bytes;
            other.size = size;
            bytes = swp_bytes;
            size = swp_size;
            return *this;
        }

        Bytes & operator = (const Bytes & other) {
            if(bytes!=nullptr) {
                delete bytes;
            }
            size = other.size;
            if(size > 0) {
                bytes = new char[size];
                memcpy(bytes,other.bytes,size);
            } else {
                bytes = nullptr;
            }
            return *this;
        }

        std::size_t to_bytes(char* v) const{
                ((std::size_t*)(v))[0] = size;
                if(size > 0) {
                    memcpy(v + sizeof(size),bytes,size);
                }
                return size + sizeof(size);
        }

        std::size_t bytes_size() const {
                return size + sizeof(size);
        }

        void post_object(const std::function<void (char const * const,std::size_t)>& f) const{
                f((char*)&size,sizeof(size));
                f(bytes,size);
        }

        void ensure_registered(mutils::DeserializationManager&){}

        static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager *, const  char * const v){
            return std::make_unique<Bytes>(v + sizeof(std::size_t),((std::size_t*)(v))[0]);
        }

        static context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager *, const char * const v)  {
                return context_ptr<Bytes>{new Bytes(v + sizeof(std::size_t),((std::size_t*)(v))[0])};
        }

};


/**
 * Non-Persitent Object with vairable sizes
 */
class ByteArrayObject: public mutils::ByteRepresentable {
public:
  Persistent<Bytes> pers_bytes;
  Persistent<Bytes,ST_MEM> vola_bytes;

  void change_pers_bytes(const Bytes& bytes) {
    *pers_bytes = bytes;
  }

  void change_vola_bytes(const Bytes& bytes) {
    *vola_bytes = bytes;
  }

  /** Named integers that will be used to tag the RPC methods */
  enum Functions { CHANGE_PERS_BYTES, CHANGE_VOLA_BYTES };

  static auto register_functions() {
    return std::make_tuple(
      derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes),
      derecho::rpc::tag<CHANGE_VOLA_BYTES>(&ByteArrayObject::change_vola_bytes));
  }

  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes,vola_bytes);
  // constructor
  ByteArrayObject(Persistent<Bytes> & _p_bytes,Persistent<Bytes,ST_MEM> & _v_bytes):
    pers_bytes(std::move(_p_bytes)),
    vola_bytes(std::move(_v_bytes)) {
  }
  // the default constructor
  ByteArrayObject(PersistentRegistry *pr):
    pers_bytes(nullptr,pr),
    vola_bytes(nullptr,pr) {
  }
};

int main(int argc, char *argv[]) {

  if(argc != 5) {
    std::cout<<"usage:"<<argv[0]<<" <pers|vola> <num_of_nodes> <msg_size> <count>"<<std::endl;
    return -1;
  }
  bool is_pers = (strcmp(argv[1],"pers") == 0);
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
  derecho::DerechoParams derecho_params{max_msg_size, block_size};

  derecho::CallbackSet callback_set{
    nullptr,//we don't need the stability_callback here
    // the persistence_callback either
    [&](derecho::subgroup_id_t subgroup,derecho::persistence_version_t ver){
      if(ver == num_of_nodes * count - 1){
        clock_gettime(CLOCK_REALTIME,&t3);
        int64_t nsec = ((int64_t)t3.tv_sec - t1.tv_sec)*1000000000 + 
          t3.tv_nsec - t1.tv_nsec;
        double msec = (double)nsec/1000000;
        double thp_gbps = ((double)count*msg_size*8)/nsec;
        double thp_ops = ((double)count*1000000000)/nsec;
        std::cout<<"(pers)timespan:"<<msec<<" millisecond."<<std::endl;
        std::cout<<"(pers)throughput:"<<thp_gbps<<"Gbit/s."<<std::endl;
        std::cout<<"(pers)throughput:"<<thp_ops<<"ops."<<std::endl;  

      }
    }
  };

  derecho::SubgroupInfo subgroup_info{
    {{std::type_index(typeid(ByteArrayObject)), [num_of_nodes](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
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

  bool inadequately_provisioned = true;
  while(inadequately_provisioned) {
    try {
      group->get_subgroup<ByteArrayObject>();
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
  // if (node_id == 0) {
      derecho::Replicated<ByteArrayObject>& handle = group->get_subgroup<ByteArrayObject>();
      char *bbuf = new char[msg_size];
      bzero(bbuf,msg_size);
      Bytes bs(bbuf,msg_size);

      try{

        clock_gettime(CLOCK_REALTIME,&t1);
        for(int i=0;i<count;i++) {
          if (is_pers) {
            handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
          } else {
            handle.ordered_send<ByteArrayObject::CHANGE_VOLA_BYTES>(bs);
          }
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
  // }


      usleep(2000000);
#ifdef _PERFORMANCE_DEBUG
      if(is_pers){
        (*handle.user_object_ptr)->pers_bytes.print_performance_stat();
      } else {
        (*handle.user_object_ptr)->vola_bytes.print_performance_stat();
      }
#endif//_PERFORMANCE_DEBUG
      usleep(500000);
      exit(0);

  std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
  while(true){}
}
