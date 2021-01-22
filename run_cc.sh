#!/bin/bash

export PYTHONPATH=.:predictor/src
python3 cloud_controller/client_controller/client_controller.py "$@"
