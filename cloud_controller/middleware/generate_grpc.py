#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This command generates cs_pb2.py and cs_pb2_grpc.py files from cs.proto. These files are necessary
for the execution of gRPC client and gRPC control server.
"""
from subprocess import call

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/middleware/middleware.proto",
     shell=True)

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/middleware/ivis.proto",
     shell=True)
call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/architecture.proto",
     shell=True)