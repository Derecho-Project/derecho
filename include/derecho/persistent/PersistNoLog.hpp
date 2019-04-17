#ifndef PERSIST_NO_LOG_HPP
#define PERSIST_NO_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error PersistLog.hpp only works with clang and gnu compilers
#endif

#include "detail/util.hpp"
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <inttypes.h>
#include <map>
#include <set>
#include <stdio.h>
#include <string>

#include <derecho/utils/logger.hpp>

#if __GNUC__ > 7
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace persistent {

/** save object in file
 * @param obj Reference to the object to be persistent
 * @param object_name name of the object
 */
template <typename ObjectType, StorageType storageType = ST_FILE>
void saveNoLogObjectInFile(
        ObjectType& obj,
        const char* object_name) noexcept(false);

/**
 * load data from file
 * @param object_name
 */
template <typename ObjectType, StorageType storageType = ST_FILE>
std::unique_ptr<ObjectType> loadNoLogObjectFromFile(
        const char* object_name,
        mutils::DeserializationManager* dm = nullptr) noexcept(false);

}

#include "detail/PersistNoLog_impl.hpp"

#endif  //PERSIST_NO_LOG_HPP
