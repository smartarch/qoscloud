#!/bin/bash

export PYTHONPATH=.:predictor/src
python3 cloud_controller/aggregator/performance_data_aggregator.py "$@"
