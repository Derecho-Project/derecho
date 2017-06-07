#ifndef PERSISTENT_HPP
#define PERSISTENT_HPP

#include <sys/types.h>
#include <inttypes.h>
#include <string>
#include <iostream>
#include <memory>
#include <functional>
#include <pthread.h>
#include "HLC.hpp"
#include "PersistException.hpp"
#include "PersistLog.hpp"
#include "FilePersistLog.hpp"
#include "SerializationSupport.hpp"

using namespace mutils;

namespace ns_persistent {

  // #define DEFINE_PERSIST_VAR(_t,_n) DEFINE_PERSIST_VAR(_t,_n,ST_FILE)
  #define DEFINE_PERSIST_VAR(_t,_n,_s) \
    Persistent<_t, _s> _n(# _n)
  #define DECLARE_PERSIST_VAR(_t,_n,_s) \
    extern DEFINE_PERSIST_VAR(_t,_n,_s)

  /* deprecated
  // number of versions in the cache
  #define MAX_NUM_CACHED_VERSION        (1024)
  #define VERSION_HASH(v)               ( (int)((v) & (MAX_NUM_CACHED_VERSION-1)) )

  // invalid version:
  #define INVALID_VERSION (-1ll)

  // read from cache
  #define VERSION_IS_CACHED(v)  (this->m_aCache[VERSION_HASH(v)].ver == (v))
  #define GET_CACHED_OBJ_PTR(v) (this->m_aCache[VERSION_HASH(v)].obj)
  */

  // function types to be registered for create version
  // , persist version, and trim a version
  using VersionFunc = std::function<void(const __int128 &)>;
  using PersistFunc = std::function<void(void)>;
  using TrimFunc = std::function<void(const __int128 &)>;
  using FuncRegisterCallback = std::function<void(VersionFunc,PersistFunc,TrimFunc)>;

  // Persistent represents a variable backed up by persistent storage. The
  // backend is PersistLog class. PersistLog handles only raw bytes and this
  // class is repsonsible for converting it back and forth between raw bytes
  // and ObjectType. But, the serialization/deserialization functionality is
  // actually defined by ObjectType and provided by Persistent users.
  // - ObjectType: user-defined type of the variable it is required to support
  //   serialization and deserialization as follows:
  //   // serialize
  //   void * ObjectType::serialize(const ObjectType & obj, uint64_t *psize)
  //   - obj: obj is the reference to the object to be serialized
  //   - psize: psize is a uint64_t pointer to receive the size of the serialized
  //     data.
  //   - Return value is a pointer to a new malloced buffer with the serialized
  //     //TODO: this may not be efficient for large object...open to be changed.
  //   // deserialize
  //   ObjectType * ObjectType::deserialize(const void *pdata)
  //   - pdata: a buffer of the serialized data
  //   - Return value is a pointer to a new created ObjectType deserialized from
  //     'pdata' buffer.
  // - StorageType: storage type is defined in PersistLog. The value could be
  //   ST_FILE/ST_MEM/ST_3DXP ... I will start with ST_FILE and extend it to
  //   other persistent Storage.
  // TODO:comments
  template <typename ObjectType,
    StorageType storageType=ST_FILE>
  class Persistent{
  public:
      /** The constructor
       * @param func_register_cb Call this to register myself to Replicated<T>
       * @param object_name This name is used for persistent data in file.
       */
      Persistent(FuncRegisterCallback func_register_cb=nullptr,
        const char * object_name = (*Persistent::getNameMaker().make()).c_str())
        noexcept(false) {
         // Initialize log
        this->m_pLog = NULL;
        switch(storageType){
        // file system
        case ST_FILE:
          this->m_pLog = new FilePersistLog(object_name);
          if(this->m_pLog == NULL){
            throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
          }
          break;
        // volatile
        case ST_MEM:
        {
          const string tmpfsPath = "/dev/shm/volatile_t";
          this->m_pLog = new FilePersistLog(object_name, tmpfsPath);
          if(this->m_pLog == NULL){
            throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
          }
          break;
        }
        //default
        default:
          throw PERSIST_EXP_STORAGE_TYPE_UNKNOWN(storageType);
        }
        //register the version creator and persist callback
        if(func_register_cb != nullptr){
          func_register_cb(
            std::bind(&Persistent<ObjectType,storageType>::version,this,std::placeholders::_1),
            std::bind(&Persistent<ObjectType,storageType>::persist,this),
            std::bind(&Persistent<ObjectType,storageType>::trim<const __int128>,this,std::placeholders::_1) //trim by version:(const __int128)
        );
        }
      }
      // destructor: release the resources
      virtual ~Persistent() noexcept(false){
        // destroy the in-memory log
        if(this->m_pLog != NULL){
          delete this->m_pLog;
        }
        //TODO:unregister the version creator and persist callback,
        // if the Persistent<T> is added to the pool dynamically.
      };

      /**
       * * operator to get the memory version
       * @return ObjectType&
       */
      ObjectType& operator * (){
        return this->wrapped_obj;
      }

      // get the latest Value of T. The user lambda will be fed with the latest object
      // zerocopy:this object will not live once it returns.
      // return value is decided by user lambda
      template <typename Func>
      auto get (
        const Func& fun, 
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        return this->getByIndex(-1L,fun,dm);
      };

      // get the latest Value of T, returns a unique pointer to the object
      std::unique_ptr<ObjectType> get ( DeserializationManager *dm=nullptr)
        noexcept(false) {
        return this->getByIndex(-1L,dm);
      };

      // get a version of Value T. the user lambda will be fed with the given object
      // zerocopy:this object will not live once it returns.
      // return value is decided by user lambda
      template <typename Func>
      auto getByIndex (
        int64_t idx, 
        const Func& fun, 
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        return deserialize_and_run<ObjectType>(dm,(char *)this->m_pLog->getEntryByIndex(idx),fun);
      };

      // get a version of value T. returns a unique pointer to the object
      std::unique_ptr<ObjectType> getByIndex(
        int64_t idx, 
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        return from_bytes<ObjectType>(dm,(char const *)this->m_pLog->getEntryByIndex(idx));      
      };

      // get a version of Value T, specified by version. the user lambda will be fed with
      // an object of T.
      // zerocopy: this object will not live once it returns.
      // return value is decided by the user lambda.
      template <typename Func>
      auto get (
        const __int128 & ver,
        const Func& fun,
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        char * pdat = (char*)this->m_pLog->getEntry(ver);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_VERSION;
        }
        return deserialize_and_run<ObjectType>(dm,pdat,fun);
      };

      // get a version of value T. specified version.
      // return a deserialized copy for the variable.
      std::unique_ptr<ObjectType> get(
        const __int128 & ver,
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        char const * pdat = (char const *)this->m_pLog->getEntry(ver);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_VERSION;
        }

        return from_bytes<ObjectType>(dm,pdat);
      }

      template <typename TKey>
      void trim (const TKey &k) noexcept(false) {
        dbg_trace("trim.");
        this->m_pLog->trim(k);
        dbg_trace("trim...done");
      }

      // get a version of Value T, specified by HLC clock. the user lambda will be fed with
      // an object of T.
      // zerocopy: this object will not live once it returns.
      // return value is decided by the user lambda.
      template <typename Func>
      auto get (
        const HLC& hlc,
        const Func& fun,
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        char * pdat = (char*)this->m_pLog->getEntry(hlc);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_HLC;
        }
        return deserialize_and_run<ObjectType>(dm,pdat,fun);
      };

      // get a version of value T. specified by HLC clock.
      std::unique_ptr<ObjectType> get(
        const HLC& hlc,
        DeserializationManager *dm=nullptr)
        noexcept(false) {
        char const * pdat = (char const *)this->m_pLog->getEntry(hlc);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_HLC;
        }

        return from_bytes<ObjectType>(dm,pdat);
      }

      // syntax sugar: get a specified version of T without DSM
      std::unique_ptr<ObjectType> operator [](int64_t idx)
        noexcept(false) {
        return this->getByIndex(idx);
      }

      // syntax sugar: get a specified version of T without DSM
      std::unique_ptr<ObjectType> operator [](const __int128 ver)
        noexcept(false) {
        return this->get(ver);
      }

      // syntax sugar: get a specified version of T without DSM
      std::unique_ptr<ObjectType> operator [](const HLC & hlc)
        noexcept(false) {
        return this->get(hlc);
      }

      // get number of the versions
      virtual int64_t getNumOfVersions() noexcept(false) {
        return this->m_pLog->getLength();
      };

      virtual int64_t getEarliestIndex() noexcept(false) {
        return this->m_pLog->getEarliestIndex();
      }

      // make a version with version and mhlc clock
      virtual void set(const ObjectType &v, const __int128 & ver, const HLC &mhlc) 
        noexcept(false) {
        auto size = bytes_size(v);
        char buf[size];
        bzero(buf,size);
        to_bytes(v,buf);
        this->m_pLog->append((void*)buf,size,ver,mhlc);
      };

      // make a version with version
      virtual void set(const ObjectType &v, const __int128 & ver)
        noexcept(false) {
        HLC mhlc; // generate a default timestamp for it.
        this->set(v,ver,mhlc);
      }

      // make a version
      virtual void version(const __int128 & ver)
        noexcept(false) {
        //TODO: compare if value has been changed?
        this->set(this->wrapped_obj,ver);
      }

      /** persist till version
       * @param ver version number
       * @return the given version to be persisted.
       */
      virtual const __int128 persist()
        noexcept(false){
        return this->m_pLog->persist();
      }

      // internal _NameMaker class
      class _NameMaker{
      public:
        // Constructor
        _NameMaker() noexcept(false):
        m_sObjectTypeName(typeid(ObjectType).name()) {
          this->m_iCounter = 0;
          if (pthread_spin_init(&this->m_oLck,PTHREAD_PROCESS_SHARED) != 0) {
            throw PERSIST_EXP_SPIN_INIT(errno);
          }
        }

        // Destructor
        virtual ~_NameMaker() noexcept(true) {
          pthread_spin_destroy(&this->m_oLck);
        }

        // guess a name
        std::unique_ptr<std::string> make() noexcept(false) {
          int cnt;
          if (pthread_spin_lock(&this->m_oLck) != 0) {
            throw PERSIST_EXP_SPIN_LOCK(errno);
          }
          cnt = this->m_iCounter++;
          if (pthread_spin_unlock(&this->m_oLck) != 0) {
            throw PERSIST_EXP_SPIN_UNLOCK(errno);
          }
          std::unique_ptr<std::string> ret = std::make_unique<std::string>();
          //char * buf = (char *)malloc((strlen(this->m_sObjectTypeName)+13)/8*8);
          char buf[256];
          sprintf(buf,"%s-%d",this->m_sObjectTypeName,cnt);
         // return std::make_shared<const char *>((const char*)buf);
         *ret = buf;
         return ret;
        }

      private:
        int m_iCounter;
        const char *m_sObjectTypeName;
        pthread_spinlock_t m_oLck;
      };

  protected:
      // wrapped objected
      ObjectType wrapped_obj;
      
      // PersistLog
      PersistLog * m_pLog;

      // get the static name maker.
      static _NameMaker & getNameMaker();
  };

  // How many times the constructor was called.
  template <typename ObjectType, StorageType storageType>
  typename Persistent<ObjectType,storageType>::_NameMaker & 
    Persistent<ObjectType,storageType>::getNameMaker() noexcept(false) {
    static Persistent<ObjectType,storageType>::_NameMaker nameMaker;
    return nameMaker;
  }

  template <typename ObjectType>
  class Volatile: public Persistent<ObjectType,ST_MEM>{
  public:
    // constructor: this will guess the objectname form ObjectType
    Volatile (
      FuncRegisterCallback func_register_cb=nullptr,
      const char * object_name = (*Persistent<ObjectType,ST_MEM>::getNameMaker().make()).c_str())
      noexcept(false):
      Persistent<ObjectType,ST_MEM>(func_register_cb,object_name){
    };
    // destructor:
    virtual ~Volatile() noexcept(false){
      // do nothing
    };
  };

  /*
   * PersistentRegistry is a book for all the Persistent<T> or Volatile<T>
   * variables. Replicated<T> class should maintain such a registry to perform
   * the following operations:
   * - makeVersion(const __int128 & ver): create a version 
   * - persist(): persist the existing versions
   * - trim(const __int128 & ver): trim all versions earlier than ver
   */
  class PersistentRegistry{
  public:
    PersistentRegistry() {
      //
    };
    virtual ~PersistentRegistry() {
      this->_registry.clear();
    };
    #define VERSION_FUNC_IDX (0)
    #define PERSIST_FUNC_IDX (1)
    #define TRIM_FUNC_IDX (2)
    void makeVersion(const __int128 & ver) noexcept(false) {
      callFunc<VERSION_FUNC_IDX>(ver);
    };
    void persist() noexcept(false) {
      callFunc<PERSIST_FUNC_IDX>();
    };
    void trim(const __int128 & ver) noexcept(false) {
      callFunc<TRIM_FUNC_IDX>(ver);
    };
    void registerPersist(const VersionFunc &vf,const PersistFunc &pf,const TrimFunc &tf) noexcept(false) {
      //this->_registry.push_back(std::make_tuple<VersionFunc,PersistFunc,TrimFunc>(
      this->_registry.push_back(std::make_tuple(vf,pf,tf));
    };
  protected:
    std::vector<std::tuple<VersionFunc,PersistFunc,TrimFunc>> _registry;
    template<int funcIdx,typename ... Args >
    void callFunc(Args ... args) {
      for (auto itr = this->_registry.begin();
        itr != this->_registry.end(); ++itr) {
        std::get<funcIdx>(*itr)(args ...);
      }
    };
  };
}

#endif//PERSIST_VAR_H
