#include <derecho/persistent/detail/PersistLog.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <derecho/utils/logger.hpp>
#include <derecho/openssl/signature.hpp>

namespace persistent {

PersistLog::PersistLog(const std::string& name) noexcept(true) :
    m_sName(name),
    signature_size(openssl::EnvelopeKey::from_pem_private(derecho::getConfString(CONF_PERS_PRIVATE_KEY_FILE)).get_max_size()){
}

PersistLog::~PersistLog() noexcept(true) {
}

#ifndef NDEBUG
void PersistLog::dump_hidx() {
    dbg_default_trace("number of entry in hidx:{}.log_len={}.", hidx.size(), getLength());
    for(auto itr = hidx.cbegin(); itr != hidx.cend(); itr++) {
        dbg_default_trace("hlc({0},{1})->idx({2})", itr->hlc.m_rtc_us, itr->hlc.m_logic, itr->log_idx);
    }
}
#endif  //DERECHO_DEBUG
}  // namespace persistent
