#!/bin/bash
# /home/msvoboda/LocalDevel/outlet/outlet/daemon/grpc
PY_PKG='outlet/daemon/grpc'
OUT_DIR=..

mkdir -p $OUT_DIR
python3 -m grpc_tools.protoc -I./proto --python_out=$OUT_DIR --grpc_python_out=$OUT_DIR ./proto/$PY_PKG/Outlet.proto ./proto/$PY_PKG/Node.proto

# disabled since Python support for GRPC+Flatbuffers is currently broken (2020-11-21)
#OUT_DIR=../outlet/daemon/grpc
#flatc --grpc --python -o $OUT_DIR fbs/Outlet.fbs
