#!/bin/bash
set -eu
export TMPDIR=/var/tmp
INSTALL_PREFIX="/usr/local"
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"

WORKPATH=`mktemp -d`
cd ${WORKPATH}
git clone https://github.com/mpmilano/mutils.git
cd mutils
# git checkout f1bdc9f2224a6a85d91acd2703e346c52f0f1a42
# git checkout 2b6f80b60f2b1695bd27dce8dd30cbc3170d7290
git checkout d53e7fa7cd5146fd2995a7f05d5d01d03d872e67
git apply ${SCRIPTPATH}/mutils.patch
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
