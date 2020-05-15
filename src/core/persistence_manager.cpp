/**
 * @file persistence_manager.h
 *
 * @date Jun 20, 2017
 */
#include <derecho/core/detail/persistence_manager.hpp>
#include <derecho/core/detail/view_manager.hpp>
#include <derecho/openssl/signature.hpp>

namespace derecho {

/** Constructor
 * @param objects_map reference to the objects_by_subgroup_id from Group.
 */
PersistenceManager::PersistenceManager(
        std::map<subgroup_id_t, std::reference_wrapper<ReplicatedObject>>& objects_map,
        const persistence_callback_t& _persistence_callback)
        : thread_shutdown(false),
          persistence_callback(_persistence_callback),
          objects_by_subgroup_id(objects_map) {
    // initialize semaphore
    if(sem_init(&persistence_request_sem, 1, 0) != 0) {
        throw derecho_exception("Cannot initialize persistent_request_sem:errno=" + std::to_string(errno));
    }
    // Attempt to load the private key and create a Signer
    try {
        signer = std::make_unique<openssl::Signer>(openssl::load_private_key(getConfString(CONF_DERECHO_PRIVATE_KEY_FILE)),
                                                   openssl::DigestAlgorithm::SHA256);
        signature_size = signer->get_max_signature_size();
    } catch(openssl::file_error& ex) {
        //If the private key file can't be opened, assume signatures are disabled
        dbg_default_info("No private key file found. Persistent version signatures disabled.");
        signer = nullptr;
        signature_size = 0;
    }
}

/** default Destructor
 */
PersistenceManager::~PersistenceManager() {
    sem_destroy(&persistence_request_sem);
}

void PersistenceManager::set_view_manager(ViewManager& view_manager) {
    this->view_manager = &view_manager;
}

std::size_t PersistenceManager::get_signature_size() const {
    return signature_size;
}

/** Start the persistent thread. */
void PersistenceManager::start() {
    this->persist_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "persist");
        do {
            // wait for semaphore
            sem_wait(&persistence_request_sem);
            while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
                ;                                                    // spin
            if(this->persistence_request_queue.empty()) {
                prq_lock.clear(std::memory_order_release);  // release lock
                if(this->thread_shutdown) {
                    break;
                }
                continue;
            }

            subgroup_id_t subgroup_id = std::get<0>(persistence_request_queue.front());
            persistent::version_t version = std::get<1>(persistence_request_queue.front());
            persistence_request_queue.pop();
            prq_lock.clear(std::memory_order_release);  // release lock

            // persist
            try {
                //To reduce the time this thread holds the View lock, put the signature in a local array
                //and copy it into the SST once signing is done. (We could use the SST signatures field
                //directly as the signature array, but that would require holding the lock for longer.)
                unsigned char signature[signature_size];

                auto search = objects_by_subgroup_id.find(subgroup_id);
                if(search != objects_by_subgroup_id.end()) {
                    if(signer) {
                        search->second.get().sign(*signer, signature);
                        //read lock the view
                        SharedLockedReference<View> view_and_lock = view_manager->get_current_view();
                        View& Vc = view_and_lock.get();
                        //update the signature field for this subgroup in the SST
                        gmssst::set(&(Vc.gmsSST->signatures[Vc.gmsSST->get_local_index()][subgroup_id * signature_size]),
                                    signature, signature_size);
                        //push ths signature
                        Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                                       (char*)std::addressof(Vc.gmsSST->signatures[0][subgroup_id * signature_size]) - Vc.gmsSST->getBaseAddress(),
                                       signature_size);
                    }
                    search->second.get().persist(version, signature, signature_size);
                }
                // read lock the view
                SharedLockedReference<View> view_and_lock = view_manager->get_current_view();
                // update the persisted_num in SST
                View& Vc = view_and_lock.get();
                gmssst::set(Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id], version);
                Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                               (char*)std::addressof(Vc.gmsSST->persisted_num[0][subgroup_id]) - Vc.gmsSST->getBaseAddress(),
                               sizeof(long long int));
            } catch(uint64_t exp) {
                dbg_default_debug("exception on persist():subgroup={},ver={},exp={}.", subgroup_id, version, exp);
                std::cout << "exception on persistent:subgroup=" << subgroup_id << ",ver=" << version << "exception=0x" << std::hex << exp << std::endl;
            }

            // callback
            if(this->persistence_callback != nullptr) {
                this->persistence_callback(subgroup_id, version);
            }

            if(this->thread_shutdown) {
                while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
                    ;                                                    // spin
                if(persistence_request_queue.empty()) {
                    prq_lock.clear(std::memory_order_release);  // release lock
                    break;                                      // finish
                }
                prq_lock.clear(std::memory_order_release);  // release lock
            }
        } while(true);
    }};
}

/** post a persistence request */
void PersistenceManager::post_persist_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version) {
    // request enqueue
    while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
        ;                                                    // spin
    persistence_request_queue.push(std::make_tuple(subgroup_id, version));
    prq_lock.clear(std::memory_order_release);  // release lock
    // post semaphore
    sem_post(&persistence_request_sem);
}

/** make a version */
void PersistenceManager::make_version(const subgroup_id_t& subgroup_id,
                                      const persistent::version_t& version, const HLC& mhlc) {
    auto search = objects_by_subgroup_id.find(subgroup_id);
    if(search != objects_by_subgroup_id.end()) {
        search->second.get().make_version(version, mhlc);
    }
}

/** shutdown the thread
 * @wait - wait till the thread finished or not.
 */
void PersistenceManager::shutdown(bool wait) {
    // if(replicated_objects == nullptr) return;  //skip for raw subgroups - NO DON'T

    thread_shutdown = true;
    sem_post(&persistence_request_sem);  // kick the persistence thread in case it is sleeping

    if(wait) {
        this->persist_thread.join();
    }
}
}  // namespace derecho
