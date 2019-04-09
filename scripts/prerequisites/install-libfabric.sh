#!/bin/bash
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"

git clone https://github.com/ofiwg/libfabric.git
cd libfabric
git checkout fcf0f2ec3c7109e06e09d3650564df8d2dfa12b6 
./autogen.sh
./configure --prefix=${INSTALL_PREFIX} --disable-mlx
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
cd ..
rm -rf libfabric
