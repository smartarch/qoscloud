#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script builds the docker images of image client and recognizer server and pushes them to dockerhub.
"""
from subprocess import call

print("Building the default docker image")
call("docker build -t d3srepo/qoscloud-default -f Dockerfile ../..", shell=True)

print("Pushing images to DockerHub")
call("docker push d3srepo/qoscloud-default", shell=True)
