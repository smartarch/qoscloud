#!/bin/bash

export PYTHONPATH=.:predictor/src
python3 cloud_controller/main.py "$@"
