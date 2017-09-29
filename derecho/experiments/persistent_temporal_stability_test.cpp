#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>
#include <string>
#include <pthread.h>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <persistent/Persistent.hpp>
#include <persistent/util.hpp>
#include <mutils-serialization/SerializationSupport.hpp>
#include <mutils-serialization/context_ptr.hpp>

using mutils::context_ptr;

//the payload is used to identify the user timestamp
typedef struct _payload {
  uint32_t node_rank; // rank of the sender
  uint32_t msg_seqno; // sequence of the message sent by the same sender
  uint64_t tv_sec;    // second
  uint64_t tv_nsec;   // nano second
} PayLoad;

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

  void change_pers_bytes(const Bytes& bytes) {
    *pers_bytes = bytes;
  }

  /** Named integers that will be used to tag the RPC methods */
//  enum Functions { CHANGE_PERS_BYTES, CHANGE_VOLA_BYTES };
  enum Functions { CHANGE_PERS_BYTES };

  static auto register_functions() {
    return std::make_tuple(
      derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes));
  }

  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes);
  // constructor
  ByteArrayObject(Persistent<Bytes> & _p_bytes):
    pers_bytes(std::move(_p_bytes)) {
  }
  // the default constructor
  ByteArrayObject(PersistentRegistry *pr):
    pers_bytes(nullptr,pr) {
  }
};

int main(int argc, char *argv[]) {

#ifdef _DEBUG
   spdlog::set_level(spdlog::level::trace);  
#endif
  if(argc < 6) {
    std::cout<<"usage:"<<argv[0]<<" <all|half|one> <num_of_nodes> <msg_size> <count> <max_ops> [window_size=3]"<<std::endl;
    return -1;
  }
  int sender_selector = 0; // 0 for all sender
  if(strcmp(argv[1],"half") == 0)sender_selector=1;
  if(strcmp(argv[1],"one") == 0)sender_selector=2;
  int num_of_nodes = atoi(argv[2]);
  int msg_size = atoi(argv[3]);
  int count = atoi(argv[4]);
  int max_ops = atoi(argv[5]);
  uint64_t si_us = (1000000l/max_ops);
  unsigned int window_size = 3;
  if (argc >= 7) {
    window_size = atoi(argv[6]);
  }

  derecho::node_id_t node_id;
  derecho::ip_addr my_ip;
  derecho::ip_addr leader_ip;
  query_node_info(node_id,my_ip,leader_ip);
  long long unsigned int max_msg_size = msg_size;
  long long unsigned int block_size = get_block_size(msg_size);
  derecho::DerechoParams derecho_params{max_msg_size, block_size, std::string(), window_size};
  bool is_sending = true;

  derecho::CallbackSet callback_set{
    nullptr,//we don't need the stability_callback here
    // the persistence_callback either
    [&](derecho::subgroup_id_t subgroup,derecho::persistence_version_t ver){
      // TODO: can be used to test the time from send to persistence.
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

  // Local Message Timestamp Array
  uint64_t *local_message_time_us = (uint64_t*)malloc(sizeof(uint64_t)*count);
  if(local_message_time_us == NULL) {
    std::cerr<<"allocate memory error!"<<std::endl;
    return -1;
  }

  // spawn the query thread
  derecho::Replicated<ByteArrayObject>& handle = group->get_subgroup<ByteArrayObject>();
  std::unique_ptr<std::thread> pqt = nullptr;
  uint64_t *query_time_us; // time used for temporal query
  uint64_t *appear_time_us; // when the temporal query appears
  uint64_t *message_time_us; // the latest timestamp
  int num_datapoints = 0; // number of data points
  query_time_us = (uint64_t*)malloc(sizeof(uint64_t)*num_of_nodes*count);
  appear_time_us = (uint64_t*)malloc(sizeof(uint64_t)*num_of_nodes*count);
  message_time_us = (uint64_t*)malloc(sizeof(uint64_t)*num_of_nodes*count);
  if(query_time_us == NULL ||
     appear_time_us == NULL ||
     message_time_us == NULL) {
    std::cerr<<"allocate memory error!"<<std::endl;
    return -1;
  }
  dbg_debug("about to start the querying thread.");
#if defined(_PERFORMANCE_DEBUG) || defined(_DEBUG)
  pqt = std::make_unique<std::thread>([&](){
      struct timespec tqt;
      HLC tqhlc;
      tqhlc.m_logic = 0;
      uint32_t msg_seqno_seen = 0xffffffff;
      uint32_t node_rank_seen = 0xffffffff;
      dbg_debug("query thread is running.");
      bool bQuit = false;
      while(!bQuit){
        clock_gettime(CLOCK_REALTIME,&tqt);
        tqhlc.m_rtc_us = tqt.tv_sec*1e6 + tqt.tv_nsec/1e3;
        while(true){
          try {
            struct timespec at;
            std::unique_ptr<Bytes> bs = (*handle.user_object_ptr)->pers_bytes.get(tqhlc);
            clock_gettime(CLOCK_REALTIME,&at);
            PayLoad *pl = (PayLoad*)(bs->bytes);

            // uint32_t next_seqno = (pl->node_rank < node_rank)?(pl->msg_seqno-1):pl->msg_seqno; // roundrobin
            // if(msg_seqno_seen != next_seqno){
            if(node_rank_seen != pl->node_rank || msg_seqno_seen != pl->msg_seqno){
              query_time_us[num_datapoints] = tqhlc.m_rtc_us;
              appear_time_us[num_datapoints] = (uint64_t)(at.tv_sec*1e6+at.tv_nsec/1e3);
              // message_time_us[num_datapoints] = local_message_time_us[next_seqno];
              message_time_us[num_datapoints] = (uint64_t)(pl->tv_sec*1e6+pl->tv_nsec/1e3);
              num_datapoints++;
              msg_seqno_seen = pl->msg_seqno; 
              node_rank_seen = pl->node_rank;
            }
            
            dbg_trace("msg_seqno_seen={},count={},num_datapoints={}",msg_seqno_seen,count,num_datapoints);
            if (msg_seqno_seen == (uint32_t)(count-1)) {
              std::cout<<"query(us)\tappear(us)\tmessage(us)"<<std::endl;
              for(int i=0;i<num_datapoints;i++){
                std::cout<<query_time_us[i]<<"\t"<<appear_time_us[i]<<"\t"<<message_time_us[i]<<std::endl;
              }
              dbg_trace("quitting...");
              bQuit = true;
            }
            break;
          } catch(unsigned long long exp) {
            dbg_debug("query thread return:{0:x}",exp);
            // query time is too late or too early:
            if (exp == PERSIST_EXP_BEYOND_GSF) {
              usleep(10); // sleep for 10 microseconds.
            } else if (exp == PERSIST_EXP_INV_HLC) {
              usleep(10);
              pthread_yield();
              dbg_trace("give up query with hlc({},{}) because it is too early.",tqhlc.m_rtc_us,tqhlc.m_logic); 
              break;
            } else {
              dbg_warn("unexpected exception({:x})",exp);
            }
          }
      }
    }
    std::cout<<std::flush;
    exit(0);
  });
#endif//_PERFORMANCE_DEBUG || _DEBUG
  dbg_debug("querying thread started.");

  if(is_sending) {
    char *bbuf = new char[msg_size];
    bzero(bbuf,msg_size);
    Bytes bs(bbuf,msg_size);

    try{

      struct timespec start,cur;
      clock_gettime(CLOCK_REALTIME,&start);

#define DELTA_T_US(t1,t2) ((double)(((t2).tv_sec-(t1).tv_sec)*1e6+((t2).tv_nsec-(t1).tv_nsec)*1e-3))  

      for(int i=0;i<count;i++) {
          do {
            pthread_yield();
            clock_gettime(CLOCK_REALTIME,&cur);
          } while ( DELTA_T_US(start,cur) < i*(double)si_us );
          {
              ((PayLoad*)bs.bytes)->node_rank = (uint32_t)node_id;
              ((PayLoad*)bs.bytes)->msg_seqno = (uint32_t)i;
              ((PayLoad*)bs.bytes)->tv_sec    = (uint64_t)cur.tv_sec;
              ((PayLoad*)bs.bytes)->tv_nsec   = (uint64_t)cur.tv_nsec;
              local_message_time_us[i] = ((cur.tv_sec)*1e6 + cur.tv_nsec/1e3);
              handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
          }
      }

    } catch (uint64_t exp){
      std::cout<<"Exception caught:0x"<<std::hex<<exp<<std::endl;
      return -1;
    }
  }


#if defined(_PERFORMANCE_DEBUG) || (_DEBUG)
//      (*handle.user_object_ptr)->vola_bytes.print_performance_stat();
#endif//_PERFORMANCE_DEBUG

  std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
  while(true){}
}
