#pragma once
#ifndef PERSIST_NO_LOG_HPP
#define PERSIST_NO_LOG_HPP

#if !defined(__GNUG__) && !defined(__clang__)
#error PersistLog.hpp only works with clang and gnu compilers
#endif

#include "derecho/config.h"
#include "derecho/mutils-serialization/SerializationSupport.hpp"
#include "detail/PersistLog.hpp"
#include "detail/util.hpp"
#include <inttypes.h>
#include <map>
#include <set>
#include <stdio.h>
#include <string>

#include "detail/logger.hpp"

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
        const char* object_name);

template <typename ObjectType>
void saveNoLogObjectInMem(
        ObjectType& obj,
        const char* object_name) {
    saveNoLogObjectInFile<ObjectType, ST_MEM>(obj, object_name);
}

/**
 * load data from file
 * @param   object_name object name
 * @param   dm          Pointer to the deserialization manager
 */
template <typename ObjectType, StorageType storageType = ST_FILE>
std::unique_ptr<ObjectType> loadNoLogObjectFromFile(
        const char* object_name,
        mutils::DeserializationManager* dm = nullptr);

template <typename ObjectType>
std::unique_ptr<ObjectType> loadNoLogObjectFromMem(
        const char* object_name,
        mutils::DeserializationManager* dm = nullptr) {
    return loadNoLogObjectFromFile<ObjectType, ST_MEM>(object_name, dm);
}
}

#include "detail/PersistNoLog_impl.hpp"

#endif  //PERSIST_NO_LOG_HPP
