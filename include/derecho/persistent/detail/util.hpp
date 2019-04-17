#ifndef PERSISTENT_UTIL_HPP
#define PERSISTENT_UTIL_HPP

#include "../PersistException.hpp"
#include <derecho/conf/conf.hpp>
#include <errno.h>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX(a, b) \
    ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
   _a > _b ? _a : _b; })

#define MIN(a, b) \
    ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
   _a < _b ? _a : _b; })

#define HIGH__int128(x) (*((uint64_t*)((uint64_t)(&(x)) + 8)))
#define LOW__int128(x) (*((uint64_t*)&(x)))

//Persistent folder:
inline std::string getPersFilePath() {
    return std::string(derecho::getConfString(CONF_PERS_FS_FILE_PATH));
}

// verify the existence of a folder
// Check if directory exists or not. Create it on absence.
// return error if creating failed
inline void checkOrCreateDir(const std::string& dirPath) noexcept(false) {
    struct stat sb;
    if(stat(dirPath.c_str(), &sb) == 0) {
        if(!S_ISDIR(sb.st_mode)) {
            throw PERSIST_EXP_INV_PATH;
        }
    } else {
        // create it
        if(mkdir(dirPath.c_str(), 0700) != 0) {
            throw PERSIST_EXP_CREATE_PATH(errno);
        }
    }
}

// verify the existence of a regular file
inline bool checkRegularFile(const std::string& file) noexcept(false) {
    struct stat sb;
    bool bRet = true;

    if(stat(file.c_str(), &sb) == 0) {
        if(!S_ISREG(sb.st_mode)) {
            throw PERSIST_EXP_INV_FILE;
        }
    } else {
        bRet = false;
    }
    return bRet;
}

// verify the existence of a sparse file
// Check if directory exists or not. Create it on absence.
// return error if creating failed
inline bool checkOrCreateFileWithSize(const std::string& file, uint64_t size) noexcept(false) {
    bool bCreate = false;
    int fd;

    bCreate = !checkRegularFile(file);

    fd = open(file.c_str(), O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if(fd < 0) {
        throw PERSIST_EXP_CREATE_FILE(errno);
    }

    if(ftruncate(fd, size) != 0) {
        throw PERSIST_EXP_TRUNCATE_FILE(errno);
    }
    close(fd);
    return bCreate;
}

#endif  //UTIL_PERSISTENT_HPP
