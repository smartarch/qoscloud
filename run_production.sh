#!/bin/bash

export PYTHONPATH=.:predictor/src
python3 cloud_controller/run_cloud_controller.py "$@"
