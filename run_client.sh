#!/usr/bin/env bash

ns="cl${1}-rt"

PYTHONPATH=apps/frpyc:. ip netns exec $ns python3 apps/frpyc/frpyc/client.py -i $1 -f
