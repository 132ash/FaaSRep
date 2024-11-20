#!/bin/bash

# 获取脚本所在目录
SCRIPT_PATH=$(readlink -f $0)
BASE_DIR=$(dirname $SCRIPT_PATH)
DEPS_INSTALL_PATH=$BASE_DIR/deps/out

# 创建目标文件目录以及依赖安装目录
mkdir -p "$BASE_DIR/build"
rm -rf ${DEPS_INSTALL_PATH}
mkdir -p ${DEPS_INSTALL_PATH}

# Build abseil-cpp
cd $BASE_DIR/deps/abseil-cpp && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/abseil-cpp/build

# Build tkrzw
cd $BASE_DIR/deps/tkrzw && \
  ./configure --prefix=${DEPS_INSTALL_PATH} --disable-shared \
              --enable-debug=${ENABLE_DEBUG} && \
  make -j$(nproc) && make install && make clean
