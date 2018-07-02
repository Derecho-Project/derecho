#ifndef UTIL_HPP
#define UTIL_HPP

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include "PersistException.hpp"


#ifdef _DEBUG
#include <spdlog/spdlog.h>
#endif//_DEBUG

#ifdef _DEBUG
  inline auto dbgConsole() {
    static auto console = spdlog::stdout_color_mt("console");
    return console;
  }
  #define dbg_trace(...) dbgConsole()->trace(__VA_ARGS__)
  #define dbg_debug(...) dbgConsole()->debug(__VA_ARGS__)
  #define dbg_info(...) dbgConsole()->info(__VA_ARGS__)
  #define dbg_warn(...) dbgConsole()->warn(__VA_ARGS__)
  #define dbg_error(...) dbgConsole()->error(__VA_ARGS__)
  #define dbg_crit(...) dbgConsole()->critical(__VA_ARGS__)
  #define dbg_flush() dbgConsole()->flush()
#else
  #define dbg_trace(...)
  #define dbg_debug(...)
  #define dbg_info(...)
  #define dbg_warn(...)
  #define dbg_error(...)
  #define dbg_crit(...)
  #define dbg_flush()
#endif//_DEBUG

#define MAX(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
   _a > _b ? _a : _b; })

#define MIN(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
   _a < _b ? _a : _b; })

#define HIGH__int128(x) (*((uint64_t*)((uint64_t)(&(x))+8)))
#define LOW__int128(x)  (*((uint64_t*)&(x)))


//Persistent folder:
#define DEFAULT_FILE_PERSIST_PATH (".plog")
#define DEFAULT_RAMDISK_PATH ("/dev/shm/volatile_t")

// verify the existence of a folder
// Check if directory exists or not. Create it on absence.
// return error if creating failed
inline void checkOrCreateDir(const std::string & dirPath)
noexcept(false) {
  struct stat sb;
  if (stat(dirPath.c_str(),&sb) == 0) {
    if (! S_ISDIR(sb.st_mode)) {
      throw PERSIST_EXP_INV_PATH;
    }
  } else {
    // create it
    if (mkdir(dirPath.c_str(),0700) != 0) {
      throw PERSIST_EXP_CREATE_PATH(errno);
    }
  }
}

// verify the existence of a regular file
inline bool checkRegularFile(const std::string & file)
noexcept(false) {
  struct stat sb;
  bool bRet = true;

  if (stat(file.c_str(),&sb) == 0) {
    if(! S_ISREG(sb.st_mode)) {
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
inline bool checkOrCreateFileWithSize(const std::string & file, uint64_t size)
noexcept(false) {
  bool bCreate = false;
  int fd;

  bCreate = !checkRegularFile(file);

  fd = open(file.c_str(), O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
  if (fd < 0) {
    throw PERSIST_EXP_CREATE_FILE(errno);
  }

  if (ftruncate(fd,size) != 0) {
    throw PERSIST_EXP_TRUNCATE_FILE(errno);
  }
  close(fd);
  return bCreate;
}

#endif//UTIL_HPP
