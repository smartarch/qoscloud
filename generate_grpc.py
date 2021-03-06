#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This command generates pb2.py and pb2_grpc.py files from knowledge.proto. These files are necessary
for the execution of knowledge server.
"""
from subprocess import call

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/knowledge/knowledge.proto",
     shell=True)
call("python3 cloud_controller/middleware/generate_grpc.py", shell=True)
call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/assessment/deploy_controller.proto",
     shell=True)
call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/architecture.proto",
     shell=True)
call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/ivis/ivis.proto",
     shell=True)
call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cloud_controller/aggregator/predictor.proto",
     shell=True)
# Apps
call("python3 generate_grpc.py", cwd="./examples/frpyc/", shell=True)
