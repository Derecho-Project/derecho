#pragma once
#ifndef PERSISTENT_HPP
#define PERSISTENT_HPP

#include <sys/types.h>
#include <inttypes.h>
#include <string>
#include <iostream>
#include <memory>
#include <functional>
#include <pthread.h>
#include <map>
#include "HLC.hpp"
#include "PersistException.hpp"
#include "PersistLog.hpp"
#include "FilePersistLog.hpp"
#include "SerializationSupport.hpp"

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
  using VersionFunc = std::function<void(const int64_t &)>;
  using PersistFunc = std::function<const int64_t(void)>;
  using TrimFunc = std::function<void(const int64_t &)>;
  // this function is obsolete, now we use a shared pointer to persistence registry
  // using PersistentCallbackRegisterFunc = std::function<void(const char*,VersionFunc,PersistFunc,TrimFunc)>;

  /*
   * PersistentRegistry is a book for all the Persistent<T> or Volatile<T>
   * variables. Replicated<T> class should maintain such a registry to perform
   * the following operations:
   * - makeVersion(const int64_t & ver): create a version 
   * - persist(): persist the existing versions
   * - trim(const int64_t & ver): trim all versions earlier than ver
   */
  class PersistentRegistry:public mutils::RemoteDeserializationContext{
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
    void makeVersion(const int64_t & ver) noexcept(false) {
      callFunc<VERSION_FUNC_IDX>(ver);
    };
    const int64_t persist() noexcept(false) {
      return callFuncMin<PERSIST_FUNC_IDX,int64_t>();
    };
    void trim(const int64_t & ver) noexcept(false) {
      callFunc<TRIM_FUNC_IDX>(ver);
    };
    void registerPersist(const char* obj_name, const VersionFunc &vf,const PersistFunc &pf,const TrimFunc &tf) noexcept(false) {
      //this->_registry.push_back(std::make_tuple(vf,pf,tf));
      auto tuple_val = std::make_tuple(vf,pf,tf);
      std::size_t key = std::hash<std::string>{}(obj_name);
      auto res = this->_registry.insert(std::pair<std::size_t,std::tuple<VersionFunc,PersistFunc,TrimFunc>>(key,tuple_val));
      if (res.second==false) {
        //override the previous value:
        this->_registry.erase(res.first);
        this->_registry.insert(std::pair<std::size_t,std::tuple<VersionFunc,PersistFunc,TrimFunc>>(key,tuple_val));
      }
    };
    PersistentRegistry(PersistentRegistry &&) = default;
    PersistentRegistry(const PersistentRegistry &) = delete;

  protected:
    //std::vector<std::tuple<VersionFunc,PersistFunc,TrimFunc>> _registry;
    std::map<std::size_t,std::tuple<VersionFunc,PersistFunc,TrimFunc>> _registry;
    template<int funcIdx,typename ... Args >
    void callFunc(Args ... args) {
      for (auto itr = this->_registry.begin();
        itr != this->_registry.end(); ++itr) {
        std::get<funcIdx>(itr->second)(args ...);
      }
    };
    template<int funcIdx,typename ReturnType,typename ... Args>
    ReturnType callFuncMin(Args ... args) {
      ReturnType min_ret = -1; // -1 means invalid value.
      for (auto itr = this->_registry.begin();
        itr != this->_registry.end(); ++itr) {
        ReturnType ret = std::get<funcIdx>(itr->second)(args ...);
        if (itr == this->_registry.begin()) {
          min_ret = ret;
        } else if (min_ret > ret) {
          min_ret = ret;
        }
      }
      return min_ret;
    }
  };

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
  //TODO: Persistent<T> has to be serializable, extending from mutils::ByteRepresentable 
  template <typename ObjectType,
    StorageType storageType=ST_FILE>
  class Persistent: public mutils::ByteRepresentable{
  protected:
      /** initialize from local state.
       *  @param object_name Object name
       */
      inline void initialize_log(const char * object_name)
        noexcept(false){
        // STEP 1: initialize log
        this->m_pLog = nullptr;
        switch(storageType){
        // file system
        case ST_FILE:
          this->m_pLog = std::make_unique<FilePersistLog>(object_name);
          if(this->m_pLog == nullptr){
            throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
          }
          break;
        // volatile
        case ST_MEM:
        {
          const string tmpfsPath = "/dev/shm/volatile_t";
          this->m_pLog = std::make_unique<FilePersistLog>(object_name, tmpfsPath);
          if(this->m_pLog == nullptr){
            throw PERSIST_EXP_NEW_FAILED_UNKNOWN;
          }
          break;
        }
        //default
        default:
          throw PERSIST_EXP_STORAGE_TYPE_UNKNOWN(storageType);
        }
      }
      /** initialize the object from log
       */
      inline void initialize_object_from_log() {
        if (this->getNumOfVersions()>0) {
          // load the object from log.
          this->m_pWrappedObject = std::move(this->getByIndex(this->getLatestIndex()));
        } else { // create a new one;
          this->m_pWrappedObject = std::make_unique<ObjectType>();
        }
      }
      /** register the callbacks.
       */
      inline void register_callbacks() 
        noexcept(false) {
        if(this->m_pRegistry != nullptr){
          this->m_pRegistry->registerPersist(
            this->m_pLog->m_sName.c_str(),
            std::bind(&Persistent<ObjectType,storageType>::version,this,std::placeholders::_1),
            std::bind(&Persistent<ObjectType,storageType>::persist,this),
            std::bind(&Persistent<ObjectType,storageType>::trim<const int64_t>,this,std::placeholders::_1) //trim by version:(const int64_t)
          );
        }
      }
  public:
      /** constructor 1 is for building a persistent<T> locally, load/create a
       * log and register itself to a persistent registry.
       * @param object_name This name is used for persistent data in file.
       * @param persistent_registry A normal pointer to the registry.
       */
      Persistent(
        const char * object_name = nullptr,
        PersistentRegistry * persistent_registry = nullptr)
        noexcept(false)
        : m_pRegistry(persistent_registry) {
        // Initialize log
        initialize_log((object_name==nullptr)?
          (*Persistent::getNameMaker().make()).c_str() : object_name);
        // Initialize object
        initialize_object_from_log();
        // Register Callbacks
        register_callbacks();
      }

      /** constructor 2 is move constructor. It "steals" the resource from
       * another object.
       * @param other The other object.
       */
      Persistent(Persistent && other) noexcept(false) {
        this->m_pWrappedObject = std::move(other.m_pWrappedObject);
        this->m_pLog = std::move(other.m_pLog);
        this->m_pRegistry = other.m_pRegistry;
        register_callbacks(); // this callback will override the previous registry entry.
      }

      /** constructor 3 is for deserialization. It builds a Persistent<T> from
       * the object name, a unique_ptr to the wrapped object, a unique_ptr to
       * the log.
       * @param object_name The name is used for persistent data in file.
       * @param wrapped_obj_ptr A unique pointer to the wrapped object.
       * @param log_ptr A unique pointer to the log.
       * @param persistent_registry A normal pointer to the registry.
       */
      Persistent(
        const char * object_name,
        std::unique_ptr<ObjectType> & wrapped_obj_ptr,
        std::unique_ptr<PersistLog> & log_ptr = nullptr,
        PersistentRegistry * persistent_registry = nullptr)
        noexcept(false)
        : m_pRegistry(persistent_registry) {
        // Initialize log
        if ( log_ptr == nullptr ) {
          initialize_log((object_name==nullptr)?
            (*Persistent::getNameMaker().make()).c_str() : object_name);
        } else {
          this->m_pLog = std::move(log_ptr);
        }
        // Initialize Warpped Object
        if ( wrapped_obj_ptr == nullptr ) {
          initialize_object_from_log();
        } else {
          this->m_pWrappedObject = std::move(wrapped_obj_ptr);
        }
        // Register callbacks
        register_callbacks();
      }

      /** constructor 4, the default copy constructor, is disabled
       */
      Persistent(const Persistent &) = delete;

      // destructor: release the resources
      virtual ~Persistent() noexcept(true){
        // destroy the in-memory log:
        // We don't need this anymore. m_pLog is managed by smart pointer
        // automatically.
        // if(this->m_pLog != NULL){
        //   delete this->m_pLog;
        // }
        //TODO:unregister the version creator and persist callback,
        // if the Persistent<T> is added to the pool dynamically.
      };

      /**
       * * operator to get the memory version
       * @return ObjectType&
       */
      ObjectType& operator * (){
        return *this->m_pWrappedObject;
      }

      // get the latest Value of T. The user lambda will be fed with the latest object
      // zerocopy:this object will not live once it returns.
      // return value is decided by user lambda
      template <typename Func>
      auto get (
        const Func& fun, 
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        return this->getByIndex(-1L,fun,dm);
      };

      // get the latest Value of T, returns a unique pointer to the object
      std::unique_ptr<ObjectType> get ( mutils::DeserializationManager *dm=nullptr)
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
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        return mutils::deserialize_and_run<ObjectType>(dm,(char *)this->m_pLog->getEntryByIndex(idx),fun);
      };

      // get a version of value T. returns a unique pointer to the object
      std::unique_ptr<ObjectType> getByIndex(
        int64_t idx, 
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        return mutils::from_bytes<ObjectType>(dm,(char const *)this->m_pLog->getEntryByIndex(idx));      
      };

      // get a version of Value T, specified by version. the user lambda will be fed with
      // an object of T.
      // zerocopy: this object will not live once it returns.
      // return value is decided by the user lambda.
      template <typename Func>
      auto get (
        const int64_t & ver,
        const Func& fun,
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        char * pdat = (char*)this->m_pLog->getEntry(ver);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_VERSION;
        }
        return mutils::deserialize_and_run<ObjectType>(dm,pdat,fun);
      };

      // get a version of value T. specified version.
      // return a deserialized copy for the variable.
      std::unique_ptr<ObjectType> get(
        const int64_t & ver,
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        char const * pdat = (char const *)this->m_pLog->getEntry(ver);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_VERSION;
        }

        return mutils::from_bytes<ObjectType>(dm,pdat);
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
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        char * pdat = (char*)this->m_pLog->getEntry(hlc);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_HLC;
        }
        return mutils::deserialize_and_run<ObjectType>(dm,pdat,fun);
      };

      // get a version of value T. specified by HLC clock.
      std::unique_ptr<ObjectType> get(
        const HLC& hlc,
        mutils::DeserializationManager *dm=nullptr)
        noexcept(false) {
        char const * pdat = (char const *)this->m_pLog->getEntry(hlc);
        if (pdat == nullptr) {
          throw PERSIST_EXP_INV_HLC;
        }

        return mutils::from_bytes<ObjectType>(dm,pdat);
      }

      // syntax sugar: get a specified version of T without DSM
/*
      std::unique_ptr<ObjectType> operator [](const int64_t idx)
        noexcept(false) {
        return this->getByIndex(idx);
      }
*/

      // syntax sugar: get a specified version of T without DSM
      std::unique_ptr<ObjectType> operator [](const int64_t ver)
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

      virtual int64_t getLatestIndex() noexcept(false) {
        return this->m_pLog->getLatestIndex();
      }

      virtual const int64_t getLastPersisted() noexcept(false) {
        return this->m_pLog->getLastPersisted();
      };

      // make a version with version and mhlc clock
      virtual void set(const ObjectType &v, const int64_t & ver, const HLC &mhlc) 
        noexcept(false) {
        auto size = mutils::bytes_size(v);
        char buf[size];
        bzero(buf,size);
        mutils::to_bytes(v,buf);
        this->m_pLog->append((void*)buf,size,ver,mhlc);
      };

      // make a version with version
      virtual void set(const ObjectType &v, const int64_t & ver)
        noexcept(false) {
        HLC mhlc; // generate a default timestamp for it.
        this->set(v,ver,mhlc);
      }

      // make a version
      virtual void version(const int64_t & ver)
        noexcept(false) {
        //TODO: compare if value has been changed?
        this->set(*this->m_pWrappedObject,ver);
      }

      /** persist till version
       * @param ver version number
       * @return the given version to be persisted.
       */
      virtual const int64_t persist()
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

  public:
      // wrapped objected
      std::unique_ptr<ObjectType> m_pWrappedObject;
      
  protected:
      // PersistLog
      std::unique_ptr<PersistLog> m_pLog;
      // Persistence Registry
      PersistentRegistry* m_pRegistry;

      // get the static name maker.
      static _NameMaker & getNameMaker();

  //serialization supports
  public:
      std::size_t to_bytes(char* ret) const {
          std::size_t sz = 0; 
          // variable name
          sz = mutils::to_bytes(this->m_pLog->m_sName.c_str(),ret+sz);
          // wrapped object
          sz = mutils::to_bytes(*this->m_pWrappedObject,ret+sz);
          // TODO:the persistent log is missing for now. 
          // we should add the log later... a more sophisticated but practical
          // scenario is send a log delta here for a crashed node to catch up.
          return sz;
      }
      std::size_t bytes_size() const {
          return mutils::bytes_size(this->m_pLog->m_sName.c_str()) +
              mutils::bytes_size(*this->m_pWrappedObject);
      }
      void post_object(const std::function<void (char const * const, std::size_t)> &f)
      const {
          mutils::post_object(f,this->m_pLog->m_sName.c_str());
          mutils::post_object(f,*this->m_pWrappedObject);
      }
      // NOTE: we do not set up the registry here. This will only happen in the
      // construction of Replicated<T>
      static std::unique_ptr<Persistent> from_bytes(mutils::DeserializationManager* p, char const *v){
          auto name = mutils::from_bytes<char*>(p,v);
          auto sz_name = mutils::bytes_size(*name);
          auto wrapped_obj = mutils::from_bytes<ObjectType>(p, v + sz_name);
          std::unique_ptr<PersistLog> null_log = nullptr;//TODO: deserialization
          PersistentRegistry & pr = p->template mgr<PersistentRegistry> ();
          return std::make_unique<Persistent>(*name,wrapped_obj,null_log,&pr);
      }
      // derived from ByteRepresentable
      virtual void ensure_registered(mutils::DeserializationManager&){}
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
    /** constructor 1 is for building a persistent<T> locally, load/create a
     * log and register itself to a persistent registry.
     * @param object_name This name is used for persistent data in file.
     * @param persistent_registry A normal pointer to the registry.
     */
    Volatile(
      const char * object_name = nullptr,
      PersistentRegistry * persistent_registry = nullptr) noexcept(false)
      : Persistent<ObjectType,ST_MEM>(object_name,persistent_registry) {}

    /** constructor 2 is move constructor. It "steals" the resource from
     * another object.
     * @param other The other object.
     */
    Volatile(Volatile && other)
      noexcept(false)
      : Persistent<ObjectType,ST_MEM>(other) {}

    /** constructor 3 is for deserialization. It builds a Persistent<T> from
     * the object name, a unique_ptr to the wrapped object, a unique_ptr to
     * the log.
     * @param object_name The name is used for persistent data in file.
     * @param wrapped_obj_ptr A unique pointer to the wrapped object.
     * @param log_ptr A unique pointer to the log.
     * @param persistent_registry A normal pointer to the registry.
     */
    Volatile(
      const char * object_name,
      std::unique_ptr<ObjectType> & wrapped_obj_ptr,
      std::unique_ptr<PersistLog> & log_ptr = nullptr,
      PersistentRegistry * persistent_registry = nullptr)
      noexcept(false)
      : Persistent<ObjectType,ST_MEM>(object_name,wrapped_obj_ptr,log_ptr,persistent_registry) {}

    /** constructor 4, the default copy constructor, is disabled
     */
    Volatile(const Volatile &) = delete;

    // destructor:
    virtual ~Volatile() noexcept(true){
      // do nothing
    };
  };

}

#endif//PERSIST_VAR_H
