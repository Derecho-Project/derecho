#!/bin/bash
set -eu
export TMPDIR="/var/tmp"
WORKPATH=`mktemp -d`
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"
export LIBRARY_PATH=${INSTALL_PREFIX}/lib:${INSTALL_PREFIX}/lib64
export LD_LIBRARY_PATH=${LIBRARY_PATH}

cd ${WORKPATH}
git clone https://github.com/mpmilano/mutils-containers.git
cd mutils-containers
# Since there's no version tag or branch we can target, check out a particular commit
# to avoid unexpected changes if there are new commits to master
git checkout e3dd51fdb6dcbe42532250b5d670b3e0f74f015c
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
rm -rf ${WORKPATH}
