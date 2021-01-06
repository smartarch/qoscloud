#!/bin/bash

./depltool.py submit application.yaml
./depltool.py status application
./depltool.py get-time 99 application component probe
./depltool.py get-throughput application component probe
./depltool.py submit-requirements requirements.yaml
./depltool.py delete application
