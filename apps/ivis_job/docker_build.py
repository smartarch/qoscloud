#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script builds the docker images of image client and recognizer server and pushes them to dockerhub.
"""
from subprocess import call

print("Build docker images for frpyc")
call("docker build -t dankhalev/ivis-job ../..", shell=True)

print("Pushing images to DockerHub")
call("docker push dankhalev/ivis-job", shell=True)
