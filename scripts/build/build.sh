#!/bin/bash

# case insensitive for string comparison
shopt -s nocasematch

colorful_print() {
    color_prefix="0;30"
    case $1 in
        "red")
            color_prefix="0;31"
            ;;
        "green")
            color_prefix="0;32"
            ;;
        "orange")
            color_prefix="0;33"
            ;;
        "blue")
            color_prefix="0;34"
            ;;
        "purple")
            color_prefix="0;35"
            ;;
        "cyan")
            color_prefix="0;36"
            ;;
        "lightgray")
            color_prefix="0;37"
            ;;
        "darkgray")
            color_prefix="1;30"
            ;;
        "lightred")
            color_prefix="1;31"
            ;;
        "lightgreen")
            color_prefix="1;32"
            ;;
        "yellow")
            color_prefix="1;33"
            ;;
        "lightblue")
            color_prefix="1;34"
            ;;
        "lightpurple")
            color_prefix="1;35"
            ;;
        "lightcyan")
            color_prefix="1;36"
            ;;
        "white")
            color_prefix="1;37"
            ;;
    esac;
    echo -e "\e[${color_prefix}m $2 $3 $4 $5 $6 $7 $8 $9 \e[0m"
}

if [[ $# -lt 1 ]]; then
    colorful_print orange "USAGE: $0 <Release|Debug|RelWithDebInfo|Benchmark|Clear> [USE_VERBS_API]"
    exit -1
fi

if [[ $1 == "Clear" ]]; then
    rm -rf build-*
    exit 0
fi

build_type=$1
cmake_defs="-DCMAKE_BUILD_TYPE=${build_type}"
build_path="build-${build_type}"

if [[ $2 == "USE_VERBS_API" ]]; then
    cmake_defs="${cmake_defs} -DUSE_VERBS_API=1"
fi

# begin building...
rm -rf ${build_path} 2>/dev/null
mkdir ${build_path}
cd ${build_path}
cmake ${cmake_defs} ..
make -j `nproc` 2>err.log
cd ..
