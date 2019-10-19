#include <derecho/persistent/detail/PersistLog.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <derecho/utils/logger.hpp>

namespace persistent {

PersistLog::PersistLog(const std::string& name) noexcept(true) : m_sName(name) {
}

PersistLog::~PersistLog() noexcept(true) {
}

#ifdef DERECHO_DEBUG
void PersistLog::dump_hidx() {
    dbg_default_trace("number of entry in hidx:{}.log_len={}.", hidx.size(), getLength());
    for(auto itr = hidx.cbegin(); itr != hidx.cend(); itr++) {
        dbg_default_trace("hlc({0},{1})->idx({2})", itr->hlc.m_rtc_us, itr->hlc.m_logic, itr->log_idx);
    }
}
#endif  //NDEBUG
}  // namespace persistent
