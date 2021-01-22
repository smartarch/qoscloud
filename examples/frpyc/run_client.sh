#!/usr/bin/env bash

cd ../..

ns="cl${1}-rt"

PYTHONPATH=examples/frpyc:. ip netns exec $ns python3 examples/frpyc/frpyc/client.py -i $1 -f
