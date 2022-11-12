#!/bin/bash
set -eu
export TMPDIR="/var/tmp"
WORKPATH=`mktemp -d`
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"
export CMAKE_PREFIX_PATH=${INSTALL_PREFIX}
export C_INCLUDE_PATH=${INSTALL_PREFIX}/include
export CPLUS_INCLUDE_PATH=${INSTALL_PREFIX}/include
export LIBRARY_PATH=${INSTALL_PREFIX}/lib:${INSTALL_PREFIX}/lib64
export LD_LIBRARY_PATH=${LIBRARY_PATH}

cd ${WORKPATH}
git clone https://github.com/mpmilano/mutils-containers.git
cd mutils-containers
# git checkout e9584168390eb3fac438a443f3bb93ed692e972a
git checkout 3739bf5cd4675a21713bb9838302e590c00eeb97
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
