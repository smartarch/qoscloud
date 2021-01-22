#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script builds the docker images of image client and recognizer server and pushes them to dockerhub.
"""
import sys
from subprocess import call


if len(sys.argv) < 1:
    print(f"You need to provide a name for the container")
    exit(1)

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. frpyc/rs.proto", shell=True)
call("bash run /code/run.sh", shell=True)

print("Build docker images for facerecognition")
# Note: grpc stubs are generated inside docker's build process
call(f"docker build -t {sys.argv[0]} -f dockerfiles/frpyc-rs ../..", shell=True)

print("Pushing images to DockerHub")

print("Note: `docker login` have to be called before first push")

call(f"docker push {sys.argv[0]}", shell=True)
