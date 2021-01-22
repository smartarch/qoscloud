#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This command generates rs_pb2.py and rs_pb2_grpc.py files from rs.proto. These files are necessary
for the execution of gRPC client and gRPC recognizer server.
"""
from subprocess import call

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. frpyc/rs.proto", shell=True)
