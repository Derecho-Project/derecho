#!/bin/bash
set -eu
export TMPDIR="/var/tmp"
WORKPATH=`mktemp -d`
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"
cd ${WORKPATH}
git clone https://github.com/gabime/spdlog.git
cd spdlog
git checkout 10e809cf644d55e5bd7d66d02e2604e2ddd7fb48
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} .
make -j `lscpu | grep "^CPU(" | awk '{print $2}'`
make install
