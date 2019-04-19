#!/bin/bash
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"

git clone https://github.com/mpmilano/mutils.git
cd mutils
git checkout 7072b25e433148006dfca1dc100eb45050b2db90
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
cd ../..
rm -rf mutils
