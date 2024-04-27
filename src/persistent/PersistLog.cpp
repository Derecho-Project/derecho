#include "derecho/persistent/detail/PersistLog.hpp"

#include "derecho/openssl/signature.hpp"
#include "derecho/persistent/detail/util.hpp"
#include "derecho/persistent/detail/logger.hpp"

namespace persistent {

PersistLog::PersistLog(const std::string& name, bool enable_signatures)
        : m_sName(name),
          signature_size(enable_signatures
                                 ? openssl::EnvelopeKey::from_pem_private(derecho::getConfString(derecho::Conf::PERS_PRIVATE_KEY_FILE)).get_max_size()
                                 : 0) {
}

PersistLog::~PersistLog() noexcept(true) {
}

#ifndef NDEBUG
void PersistLog::dump_hidx() {
    dbg_trace(PersistLogger::get(), "number of entry in hidx:{}.log_len={}.", hidx.size(), getLength());
    for(auto itr = hidx.cbegin(); itr != hidx.cend(); itr++) {
        dbg_trace(PersistLogger::get(), "hlc({0},{1})->idx({2})", itr->hlc.m_rtc_us, itr->hlc.m_logic, itr->log_idx);
    }
}
#endif  //DERECHO_DEBUG
}  // namespace persistent
