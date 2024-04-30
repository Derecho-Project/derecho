/**
 * @date Jun 20, 2017
 */
#include "derecho/core/detail/persistence_manager.hpp"

#include "derecho/core/detail/view_manager.hpp"
#include "derecho/openssl/signature.hpp"
#include "derecho/persistent/detail/logger.hpp"
#include "derecho/utils/timestamp_logger.hpp"

#include <map>
#include <string>
#include <thread>

namespace derecho {

PersistenceManager::PersistenceManager(
        std::map<subgroup_id_t, ReplicatedObject*>& objects_map,
        bool any_signed_objects,
        const persistence_callback_t& user_persistence_callback)
        : persistence_logger(persistent::PersistLogger::get()),
          thread_shutdown(false),
          signature_size(0),
          persistence_callbacks{user_persistence_callback},
          objects_by_subgroup_id(objects_map) {
    // initialize semaphores
    if(sem_init(&persistence_request_sem, 1, 0) != 0) {
        throw derecho_exception("Cannot initialize persistent_request_sem:errno=" + std::to_string(errno));
    }
    if(sem_init(&verification_request_sem, 1, 0) != 0) {
        throw derecho_exception("Cannot initialize verification_request_sem: errno=" + std::to_string(errno));
    }
    if(any_signed_objects) {
        openssl::EnvelopeKey signing_key = openssl::EnvelopeKey::from_pem_private(getConfString(Conf::PERS_PRIVATE_KEY_FILE));
        signature_size = signing_key.get_max_size();
    }
}

PersistenceManager::~PersistenceManager() {
    if(this->persist_thread.joinable()) {
        this->persist_thread.join();
    }
    if(verify_thread.joinable()) {
        verify_thread.join();
    }
    sem_destroy(&persistence_request_sem);
    sem_destroy(&verification_request_sem);
}

void PersistenceManager::set_view_manager(ViewManager& view_manager) {
    this->view_manager = &view_manager;
}

std::size_t PersistenceManager::get_signature_size() const {
    return signature_size;
}

void PersistenceManager::add_persistence_callback(const persistence_callback_t& callback) {
    persistence_callbacks.emplace_back(callback);
}

void PersistenceManager::start() {
    //Initialize this vector now that ViewManager is set up and we know the number of subgroups
    last_persisted_version.resize(view_manager->get_current_view().get().subgroup_shard_views.size(), -1);
    last_verified_version.resize(last_persisted_version.size(), -1);
    //Start the persistence thread
    this->persist_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "persist");
        dbg_debug(persistence_logger, "PersistenceManager thread started");
        while(true) {
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
            ThreadRequest request = persistence_request_queue.front();
            persistence_request_queue.pop();
            prq_lock.clear(std::memory_order_release);  // release lock

            handle_persist_request(request.subgroup_id, request.version);
            if(this->thread_shutdown) {
                while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
                    ;                                                    // spin
                if(persistence_request_queue.empty()) {
                    prq_lock.clear(std::memory_order_release);  // release lock
                    break;                                      // finish
                }
                prq_lock.clear(std::memory_order_release);  // release lock
            }
        }
    }};
    // Start the verification thread
    this->verify_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "verify");
        while(true) {
            // wait for semaphore
            sem_wait(&verification_request_sem);
            while(vrq_lock.test_and_set(std::memory_order_acquire));
            if(verify_request_queue.empty()) {
                vrq_lock.clear(std::memory_order_release);
                if(thread_shutdown) {
                    break;
                }
                continue;
            }
            ThreadRequest request = verify_request_queue.front();
            verify_request_queue.pop();
            vrq_lock.clear(std::memory_order_release);
            handle_verify_request(request.subgroup_id, request.version);
            if(thread_shutdown) {
                while(vrq_lock.test_and_set(std::memory_order_acquire));
                if(verify_request_queue.empty()) {
                    vrq_lock.clear(std::memory_order_release);
                    break;
                }
                vrq_lock.clear(std::memory_order_release);
            }
        }
    }};
}

void PersistenceManager::handle_persist_request(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_debug(persistence_logger, "PersistenceManager: handling persist request for subgroup {} version {}", subgroup_id, version);
    TIMESTAMP_LOG(TimestampLogger::HANDLE_PERSIST_REQUEST_BEGIN, 0, version);
    //If a previous request already persisted a later version (due to batching), don't do anything
    if(last_persisted_version[subgroup_id] >= version) {
        TIMESTAMP_LOG(TimestampLogger::HANDLE_PERSIST_REQUEST_END, 0, version);
        return;
    }
    persistent::version_t persisted_version = version;
    persistent::version_t signed_version = version;
    // persist
    try {
        // To reduce the time this thread holds the View lock, put the signature in a local array
        // and copy it into the SST once signing is done
        uint8_t signature[signature_size];
        memset(signature, 0, signature_size);

        bool object_has_signature = false;
        auto search = objects_by_subgroup_id.find(subgroup_id);
        if(search != objects_by_subgroup_id.end()) {
            // Don't bother doing anything if the object has no persistent fields;
            // persisted_version and signed_version will remain version (the argument)
            if(search->second->is_persistent()) {
                object_has_signature = search->second->is_signed();
                if(object_has_signature) {
                    signed_version = search->second->sign(signature);
                    dbg_trace(persistence_logger, "PersistenceManager: Asked Replicated to sign latest version, version actually signed = {}", signed_version);
                    // Request to persist the same version that was signed, not the "latest available" version,
                    // to avoid persisting a version that still needs to be signed. However, if the argument to
                    // this persistence request is later than the signed version, that means the argument version
                    // definitely does not need a signature (or we would have just signed it) so it's safe to persist.
                    persisted_version = search->second->persist(std::max(signed_version, version));
                    dbg_trace(persistence_logger, "PersistenceManager: Asked Replicated to persist version {}, version actually persisted = {}", std::max(signed_version, version), persisted_version);
                } else {
                    persisted_version = search->second->persist();
                    dbg_trace(persistence_logger, "PersistenceManager: Asked Replicated to persist latest version, version actually persisted = {}", persisted_version);
                }
                assert(persisted_version >= version);
            }
        }
        // Call the local persistence callbacks before updating the SST
        // (as soon as the SST is updated, the global persistence callback may fire)
        for(auto& persistence_callback : persistence_callbacks) {
            if(persistence_callback) {
                persistence_callback(subgroup_id, persisted_version);
            }
        }
        // read lock the view
        SharedLockedReference<View> view_and_lock = view_manager->get_current_view();
        View& Vc = view_and_lock.get();
        dbg_debug(persistence_logger, "PersistenceManager: updating subgroup {} persisted_num to {} (and signed_num to {} if applicable) ", subgroup_id, persisted_version, signed_version);
        // Only update the signature and signed_num in SST if signed_num has in fact advanced
        if(object_has_signature
           && Vc.gmsSST->signed_num[Vc.gmsSST->get_local_index()][subgroup_id] < signed_version) {
            gmssst::set(&(Vc.gmsSST->signatures[Vc.gmsSST->get_local_index()][subgroup_id * signature_size]),
                        signature, signature_size);
            gmssst::set(Vc.gmsSST->signed_num[Vc.gmsSST->get_local_index()][subgroup_id], signed_version);
            Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                           (uint8_t*)(&Vc.gmsSST->signatures[0][subgroup_id * signature_size]) - Vc.gmsSST->getBaseAddress(),
                           signature_size);
            // Put signed_num separately after the signature to ensure the signature arrives first
            Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                           Vc.gmsSST->signed_num,
                           subgroup_id);
        }
        // Update persisted_num in SST
        gmssst::set(Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id], persisted_version);
        Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                       Vc.gmsSST->persisted_num,
                       subgroup_id);
        last_persisted_version[subgroup_id] = persisted_version;
    } catch(persistent::persistent_exception& exp) {
        dbg_debug(persistence_logger, "exception on persist():subgroup={},ver={},what={}.", subgroup_id, version, exp.what());
        std::cout << "exception on persistent:subgroup=" << subgroup_id << ",ver=" << version << "exception message:" << exp.what() << std::endl;
    }
    TIMESTAMP_LOG(TimestampLogger::HANDLE_PERSIST_REQUEST_END, 0, version);
}

void PersistenceManager::handle_verify_request(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_debug(persistence_logger, "PersistenceManager: handling verify request for subgroup {} version {}", subgroup_id, version);
    TIMESTAMP_LOG(TimestampLogger::HANDLE_VERIFY_REQUEST_BEGIN, 0, version);
    // If this request is already obsolete due to batching, don't do anything
    if(last_verified_version[subgroup_id] > version) {
        TIMESTAMP_LOG(TimestampLogger::HANDLE_VERIFY_REQUEST_END, 0, version);
        return;
    }
    // Note: The version parameter has no effect on this function. It just examines the SST and verifies the signatures
    // on whatever versions are posted to the signed_num column by the other members of the shard.
    auto search = objects_by_subgroup_id.find(subgroup_id);
    if(search != objects_by_subgroup_id.end()) {
        ReplicatedObject* subgroup_object = search->second;
        //If signatures are disabled for this subgroup, do nothing
        if(!subgroup_object->is_signed()) {
            return;
        }
        //Read lock the View while reading the SST
        SharedLockedReference<View> view_and_lock = view_manager->get_current_view();
        View& Vc = view_and_lock.get();
        std::vector<uint32_t> shard_member_ranks = Vc.multicast_group->get_shard_sst_indices(subgroup_id);
        persistent::version_t minimum_verified_version = std::numeric_limits<persistent::version_t>::max();
        persistent::version_t my_signed_version = Vc.gmsSST->signed_num[Vc.gmsSST->get_local_index()][subgroup_id];
        // Special case for a shard of size 1: There are no other members to verify, so just advance verified_num to match signed_num
        if(shard_member_ranks.size() == 1) {
            minimum_verified_version = my_signed_version;
        }
        //For each other member of this node's shard, try to verify the signature in its SST row
        for(const uint32_t shard_member_rank : shard_member_ranks) {
            if(shard_member_rank == Vc.gmsSST->get_local_index()) {
                continue;
            }
            // The signature in the other node's "signatures" column corresponds to the version in its "signed_num" column
            const persistent::version_t other_signed_version = Vc.gmsSST->signed_num[shard_member_rank][subgroup_id];
            // If this node hasn't finished signing that version yet, we won't be able to check that the other node's signature
            // matches the local signature. It also means the minimum signed version can't advance past my_signed_version anyway.
            if(other_signed_version > my_signed_version) {
                dbg_debug(persistence_logger, "PersistenceManager: Skipping signature check on version {} from node {} because this node hasn't signed that version yet", other_signed_version, Vc.members[shard_member_rank]);
                continue;
            }
            //Copy out the signature so it can't change during verification
            std::vector<uint8_t> other_signature(signature_size);
            gmssst::set(other_signature.data(),
                        &Vc.gmsSST->signatures[shard_member_rank][subgroup_id * signature_size],
                        signature_size);
            // Retrieve this node's signature on that version
            std::vector<std::uint8_t> my_signature = subgroup_object->get_signature(other_signed_version);
            if(my_signature.size() == 0) {
                dbg_warn(persistence_logger, "PersistenceManager: Could not find a local signature on version {} even though this node's highest signed version is {}", other_signed_version, my_signed_version);
                continue;
            }
            if(my_signature == other_signature) {
                dbg_debug(persistence_logger, "PersistenceManager: Signature for version {} from node {} matched", other_signed_version, Vc.members[shard_member_rank]);
                minimum_verified_version = std::min(minimum_verified_version, other_signed_version);
            } else {
                dbg_warn(persistence_logger, "Signature for version {} from node {} did not match my signature!", other_signed_version, Vc.members[shard_member_rank]);
            }
        }
        //Update verified_num to the lowest version number that successfully verified across all shard members
        if(minimum_verified_version != std::numeric_limits<persistent::version_t>::max()) {
            gmssst::set(Vc.gmsSST->verified_num[Vc.gmsSST->get_local_index()][subgroup_id], minimum_verified_version);
            Vc.gmsSST->put(shard_member_ranks, Vc.gmsSST->verified_num, subgroup_id);
            dbg_debug(persistence_logger, "PersistenceManager: Updated verified_num to {}", minimum_verified_version);
            last_verified_version[subgroup_id] = minimum_verified_version;
        }
    }
    TIMESTAMP_LOG(TimestampLogger::HANDLE_VERIFY_REQUEST_END, 0, version);
}

/** post a persistence request */
void PersistenceManager::post_persist_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version) {
    // request enqueue
    while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
        ;                                                    // spin
    persistence_request_queue.push({subgroup_id, version});
    prq_lock.clear(std::memory_order_release);  // release lock
    // post semaphore
    sem_post(&persistence_request_sem);
    TIMESTAMP_LOG(TimestampLogger::PERSISTENCE_REQUEST_POSTED, 0, version);
}

void PersistenceManager::post_verify_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version) {
    //If signatures are disabled, ignore the request
    if(signature_size == 0) {
        return;
    }
    while(vrq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
        ;                                                    // spin
    verify_request_queue.push({subgroup_id, version});
    vrq_lock.clear(std::memory_order_release);  // release lock
    sem_post(&verification_request_sem);
}

/** make a version */
void PersistenceManager::make_version(const subgroup_id_t& subgroup_id,
                                      const persistent::version_t& version, const HLC& mhlc) {
    TIMESTAMP_LOG(TimestampLogger::PERSISTENCE_MAKE_VERSION_BEGIN, 0, version);
    auto search = objects_by_subgroup_id.find(subgroup_id);
    if(search != objects_by_subgroup_id.end()) {
        search->second->make_version(version, mhlc);
    }
    TIMESTAMP_LOG(TimestampLogger::PERSISTENCE_MAKE_VERSION_END, 0, version);
}

void PersistenceManager::shutdown(bool wait) {
    // if(replicated_objects == nullptr) return;  //skip for raw subgroups - NO DON'T

    dbg_debug(persistence_logger, "PersistenceManager thread shutting down");
    thread_shutdown = true;
    // Wake up the threads in case they are waiting on semaphores
    sem_post(&persistence_request_sem);
    sem_post(&verification_request_sem);

    if(wait) {
        if(this->persist_thread.joinable()) {
            this->persist_thread.join();
        }
        if(verify_thread.joinable()) {
            verify_thread.join();
        }
    }
}
}  // namespace derecho
