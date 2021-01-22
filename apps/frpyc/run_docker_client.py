#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Runs image client inside a docker container.
"""
from subprocess import call

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. frpyc/rs.proto", shell=True)

print("Running a frpyc client in a docker container")

call("docker run -ti --rm d3srepo/frpyc-ic", shell=True)
