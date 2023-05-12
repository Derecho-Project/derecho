#ifndef PERSIST_NO_LOG_IMPL_HPP
#define PERSIST_NO_LOG_IMPL_HPP

#define _NOLOG_OBJECT_DIR_ ((storageType == ST_MEM) ? getPersRamdiskPath().c_str() : getPersFilePath().c_str())
#define _NOLOG_OBJECT_NAME_ ((object_name == nullptr) ? typeid(ObjectType).name() : object_name)

namespace persistent {

template <typename ObjectType, StorageType storageType>
void saveNoLogObjectInFile(
        ObjectType& obj,
        const char* object_name) {
    char filepath[256];
    char tmpfilepath[260];

    // 0 - create dir
    checkOrCreateDir(std::string(_NOLOG_OBJECT_DIR_));
    // 1 - get object file name
    sprintf(filepath, "%s/%d-%s-nolog", _NOLOG_OBJECT_DIR_, storageType, _NOLOG_OBJECT_NAME_);
    sprintf(tmpfilepath, "%s.tmp", filepath);
    // 2 - serialize
    auto size = mutils::bytes_size(obj);
    uint8_t* buf = new uint8_t[size];
    bzero(buf, size);
    mutils::to_bytes(obj, buf);
    // 3 - write to tmp file
    int fd = open(tmpfilepath, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if(fd == -1) {
        throw persistent_file_error("Failed to open file.", errno);
    }
    ssize_t nWrite = write(fd, buf, size);
    delete[] buf;
    if(nWrite != (ssize_t)size) {
        throw persistent_file_error("Failed to write to file.", errno);
    }
    close(fd);
    // 4 - atomically rename
    if(rename(tmpfilepath, filepath) != 0) {
        throw persistent_file_error("Failed to rename file.", errno);
    }
}

template <typename ObjectType, StorageType storageType>
std::unique_ptr<ObjectType> loadNoLogObjectFromFile(
        const char* object_name,
        mutils::DeserializationManager* dm) {
    char filepath[256];

    // 0 - get object file name
    sprintf(filepath, "%s/%d-%s-nolog", _NOLOG_OBJECT_DIR_, storageType, _NOLOG_OBJECT_NAME_);

    // 0.5 - object file
    if(derecho::getConfBoolean(CONF_PERS_RESET)) {
        if(fs::exists(filepath)) {
            if(!fs::remove(filepath)) {
                dbg_error(PersistLogger::get(), "{} loadNoLogObjectFromFile failed to remove file {}.", _NOLOG_OBJECT_NAME_, filepath);
                throw persistent_file_error("Failed to remove file.", errno);
            }
        }
    }

    // 1 - load file
    checkOrCreateDir(std::string(_NOLOG_OBJECT_DIR_));
    if(!checkRegularFile(filepath)) {
        return std::unique_ptr<ObjectType>{};
    }
    int fd = open(filepath, O_RDONLY);
    struct stat stat_buf;
    if(fd == -1 || (fstat(fd, &stat_buf) != 0)) {
        throw persistent_file_error("Failed to read file.", errno);
    }

    uint8_t* buf = new uint8_t[stat_buf.st_size];
    if(!buf) {
        close(fd);
        throw persistent_file_error("Failed to allocate memory for reading file.", errno);
    }
    if(read(fd, buf, stat_buf.st_size) != stat_buf.st_size) {
        close(fd);
        throw persistent_file_error("Failed to read file.", errno);
    }
    close(fd);

    // 2 - deserialize
    std::unique_ptr<ObjectType> ret = mutils::from_bytes<ObjectType>(dm, buf);
    delete[] buf;

    return ret;
}
}  // namespace persistent

#endif  //PERSIST_NO_LOG_IMPL_HPP
