/**
 * @file persistence_manager.h
 *
 * @date Jun 20, 2017
 */
#include "derecho/core/detail/persistence_manager.hpp"

#include "derecho/core/detail/view_manager.hpp"
#include "derecho/openssl/signature.hpp"

#include <map>
#include <string>
#include <thread>

namespace derecho {

PersistenceManager::PersistenceManager(
        std::map<subgroup_id_t, ReplicatedObject*>& objects_map,
        bool any_signed_objects,
        const persistence_callback_t& user_persistence_callback)
        : thread_shutdown(false),
          signature_size(0),
          persistence_callbacks{user_persistence_callback},
          objects_by_subgroup_id(objects_map) {
    // initialize semaphore
    if(sem_init(&persistence_request_sem, 1, 0) != 0) {
        throw derecho_exception("Cannot initialize persistent_request_sem:errno=" + std::to_string(errno));
    }
    if(any_signed_objects) {
        openssl::EnvelopeKey signing_key = openssl::EnvelopeKey::from_pem_private(getConfString(CONF_PERS_PRIVATE_KEY_FILE));
        signature_size = signing_key.get_max_size();
        //The Verifier only needs the public key, but we loaded both public and private components from the private key file
        signature_verifier = std::make_unique<openssl::Verifier>(signing_key, openssl::DigestAlgorithm::SHA256);
    }
}

PersistenceManager::~PersistenceManager() {
    sem_destroy(&persistence_request_sem);
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
    //Start the thread
    this->persist_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "persist");
        dbg_default_debug("PersistenceManager thread started");
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

            ThreadRequest request = persistence_request_queue.front();
            persistence_request_queue.pop();
            prq_lock.clear(std::memory_order_release);  // release lock

            if(request.operation == RequestType::PERSIST) {
                handle_persist_request(request.subgroup_id, request.version);
            } else if(request.operation == RequestType::VERIFY) {
                handle_verify_request(request.subgroup_id, request.version);
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

void PersistenceManager::handle_persist_request(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_debug("PersistenceManager: handling persist request for subgroup {} version {}", subgroup_id, version);
    //If a previous request already persisted a later version (due to batching), don't do anything
    if(last_persisted_version[subgroup_id] >= version) {
        return;
    }
    persistent::version_t persisted_version = version;
    // persist
    try {
        //To reduce the time this thread holds the View lock, put the signature in a local array
        //and copy it into the SST once signing is done. (We could use the SST signatures field
        //directly as the signature array, but that would require holding the lock for longer.)
        uint8_t signature[signature_size];

        bool object_has_signature = false;
        auto search = objects_by_subgroup_id.find(subgroup_id);
        if(search != objects_by_subgroup_id.end()) {
            object_has_signature = search->second->is_signed();
            //Update persisted_version to the version actually persisted, which might be greater than the requested version
            persisted_version = search->second->persist(version, signature);
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
        dbg_default_debug("PersistenceManager: updating subgroup {} persisted_num to {}", subgroup_id, persisted_version);
        // update the signature and persisted_num in SST
        View& Vc = view_and_lock.get();
        if(object_has_signature) {
            gmssst::set(&(Vc.gmsSST->signatures[Vc.gmsSST->get_local_index()][subgroup_id * signature_size]),
                        signature, signature_size);
            Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                           (uint8_t*)(&Vc.gmsSST->signatures[0][subgroup_id * signature_size]) - Vc.gmsSST->getBaseAddress(),
                           signature_size);
        }
        gmssst::set(Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id], persisted_version);
        Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_id),
                       Vc.gmsSST->persisted_num,
                       subgroup_id);
        last_persisted_version[subgroup_id] = persisted_version;
    } catch(persistent::persistent_exception& exp) {
        dbg_default_debug("exception on persist():subgroup={},ver={},what={}.", subgroup_id, version, exp.what());
        std::cout << "exception on persistent:subgroup=" << subgroup_id << ",ver=" << version << "exception message:" << exp.what() << std::endl;
    }
}

void PersistenceManager::handle_verify_request(subgroup_id_t subgroup_id, persistent::version_t version) {
    dbg_default_debug("PersistenceManager: handling verify request for subgroup {} version {}", subgroup_id, version);
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
        // Special case for a shard of size 1: There are no other members to verify, so just advance verified_num to match persisted_num
        if(shard_member_ranks.size() == 1) {
            minimum_verified_version = Vc.gmsSST->persisted_num[Vc.gmsSST->get_local_index()][subgroup_id];
        }
        //For each other member of this node's shard, try to verify the signature in its SST row
        for(const uint32_t shard_member_rank : shard_member_ranks) {
            if(shard_member_rank == Vc.gmsSST->get_local_index()) {
                continue;
            }
            //The signature in the other node's "signatures" column should correspond to the version in its "persisted_num" column
            const persistent::version_t other_signed_version = Vc.gmsSST->persisted_num[shard_member_rank][subgroup_id];
            //Copy out the signature so it can't change during verification
            std::vector<uint8_t> other_signature(signature_size);
            gmssst::set(other_signature.data(),
                        &Vc.gmsSST->signatures[shard_member_rank][subgroup_id * signature_size],
                        signature_size);
            assert(other_signed_version >= version);
            assert(subgroup_object->get_minimum_latest_persisted_version() >= other_signed_version);
            bool verification_success = subgroup_object->verify_log(
                    other_signed_version, *signature_verifier, other_signature.data());
            if(verification_success) {
                minimum_verified_version = std::min(minimum_verified_version, other_signed_version);
            } else {
                dbg_default_warn("Verification of version {} from node {} failed! {}", other_signed_version, Vc.members[shard_member_rank], openssl::get_error_string(ERR_get_error(), "OpenSSL error"));
            }
        }
        //Update verified_num to the lowest version number that successfully verified across all shard members
        if(minimum_verified_version != std::numeric_limits<persistent::version_t>::max()) {
            gmssst::set(Vc.gmsSST->verified_num[Vc.gmsSST->get_local_index()][subgroup_id], minimum_verified_version);
            Vc.gmsSST->put(shard_member_ranks, Vc.gmsSST->verified_num, subgroup_id);
        }
    }
}

/** post a persistence request */
void PersistenceManager::post_persist_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version) {
    // request enqueue
    while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
        ;                                                    // spin
    persistence_request_queue.push({RequestType::PERSIST, subgroup_id, version});
    prq_lock.clear(std::memory_order_release);  // release lock
    // post semaphore
    sem_post(&persistence_request_sem);
}

void PersistenceManager::post_verify_request(const subgroup_id_t& subgroup_id, const persistent::version_t& version) {
    //If signatures are disabled, ignore the request
    if(signature_size == 0) {
        return;
    }
    while(prq_lock.test_and_set(std::memory_order_acquire))  // acquire lock
        ;                                                    // spin
    persistence_request_queue.push({RequestType::VERIFY, subgroup_id, version});
    prq_lock.clear(std::memory_order_release);  // release lock
    sem_post(&persistence_request_sem);
}

/** make a version */
void PersistenceManager::make_version(const subgroup_id_t& subgroup_id,
                                      const persistent::version_t& version, const HLC& mhlc) {
    auto search = objects_by_subgroup_id.find(subgroup_id);
    if(search != objects_by_subgroup_id.end()) {
        search->second->make_version(version, mhlc);
    }
}

/** shutdown the thread
 * @wait - wait till the thread finished or not.
 */
void PersistenceManager::shutdown(bool wait) {
    // if(replicated_objects == nullptr) return;  //skip for raw subgroups - NO DON'T

    dbg_default_debug("PersistenceManager thread shutting down");
    thread_shutdown = true;
    sem_post(&persistence_request_sem);  // kick the persistence thread in case it is sleeping

    if(wait) {
        this->persist_thread.join();
    }
}
}  // namespace derecho
