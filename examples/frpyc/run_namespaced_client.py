#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Runs image client inside a docker container.
"""
from subprocess import call

import argparse

parser = argparse.ArgumentParser(description='Frpyc client')
parser.add_argument('id', default=None, type=str, help='desired client ID')
args = parser.parse_args()

call("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. frpyc/rs.proto", shell=True)

print("Running a frpyc client in a dedicated network namespace")

call(f"PYTHONPATH=examples/frpyc:. ip netns exec cl{args.id}-rt python3 examples/frpyc/frpyc/client.py -i {args.id} -f", shell=True)
f"PYTHONPATH=examples/frpyc:. ip netns exec cl2-rt python3 examples/frpyc/frpyc/client.py -i 2 -f"
