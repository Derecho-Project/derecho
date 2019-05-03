#include <derecho/persistent/detail/SPDKPersistLog.hpp>

using namespace std;

namespace persistent {
namespace spdk {

SPDKPersistLog::SPDKPersistLog(const std::string& name) noexcept(true) : PersistLog(name) {
    // TODO add verification logic
    // m_currLogMetadata.fields.id

}

virtual void SPDKPersistLog::append(const void* pdata,
                                    const uint64_t& size, const version_t& ver,
                                    const HLC& mhlc) noexcept(false) {

}

} // close spdk namespace
} // close persistent namespace
