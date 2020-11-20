#!/bin/bash
# /home/msvoboda/LocalDevel/outlet/outlet/daemon/grpc
OUT_DIR=..
PY_PKG='outlet/daemon/grpc'
python3 -m grpc_tools.protoc -I./proto --python_out=$OUT_DIR --grpc_python_out=$OUT_DIR ./proto/$PY_PKG/Outlet.proto

