#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script builds the docker images of image client and recognizer server and pushes them to dockerhub.
"""
from subprocess import call

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. frpyc/rs.proto", shell=True)

print("Build docker images for frpyc")
# Note: grpc stubs are generated inside docker's build process
call("docker build -t edgeclouduser/frpyc-rs -f dockerfiles/frpyc-rs ../..", shell=True)
# call("docker build -t d3srepo/frpyc-ic -f dockerfiles/frpyc-ic ../..", shell=True)

print("Pushing images to DockerHub")

print("Note: `docker login` have to be called before first push")

call("docker push edgeclouduser/frpyc-rs", shell=True)
# call("docker push d3srepo/frpyc-ic", shell=True)
