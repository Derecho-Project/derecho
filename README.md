# Derecho
This is the main repository for the Derecho project. It unifies the RDMC, SST, and Derecho modules under a single, easy-to-use repository. 

## Organization
The code for this project is split into modules that interact only through each others' public interfaces. Each module resides in a directory with its own name, which is why the repository's root directory is mostly empty. External dependencies are located in the `third_party` directory, and will be pointers to Git submodules whenever possible to reduce code duplication. 

## Installation
Derecho is a library that helps you build replicated, fault-tolerant services in a datacenter with RDMA networking. Here's how to start using it in your projects.

### Prerequisites
* Linux (other operating systems have insufficient support for RDMA)
* A C++ compiler supporting C++14: GCC 5.4+ or Clang 3.5+
* The following system libraries: `rdmacm` (packaged for Ubuntu as `librdmacm-dev 1.0.21`), and `ibverbs` (packaged for ubuntu as `libibverbs-dev 1.1.8`).
* CMake 2.8.1 or newer, if you want to use the bundled build scripts

### Getting Started
Since this repository uses Git submodules to refer to some bundled dependencies, a simple `git clone` will not actually download all the code. To download a complete copy of the project, run

    git clone --recursive https://github.com/Derecho-Project/derecho-unified.git

Once cloning is complete, `cd` into the `derecho-unified` directory and run `cmake .`. You can now `cd derecho` and type `make` to compile the Derecho library and ensure all the test and experiment files can compile.

To add your own executable (that uses Derecho) to the build system, simply add an executable target to CMakeLists.txt with `derecho` as a "linked library." You can do this either in the top-level CMakeLists.txt or in the CMakeLists.txt inside the "derecho" directory. It will look something like this:

    add_executable(my_project_main my_project_main.cpp)
	target_link_libraries(my_project_main derecho)

To use Derecho in your code, you simply need to include the header `derecho/derecho.h` in your \*.h or \*.cpp files:
   
```cpp
#include "derecho/derecho.h"
```

