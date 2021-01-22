# A QoS-aware cloud deployment framework

## How to initialize the framework

The following section describes the very first steps that must be followed to run the framework. 
This has to be done before the first run.

1. Make sure two Kubernetes clusters (production and assessment) are set up and running.
2. Run `pip3 install -r requirements.txt` to install the required libraries.
3. Run `python3 ./generate_grpc.py` to generate protocol buffers and gRPC stubs.

## How to use the framework

The controller consists of several processes. 
They can be started in the following way (the commands assume that you are in the root folder):

1. Start the assessment controller process:  `./run_assessment.sh`.
2. Start the cloud controller process:  `./run_production.sh`.
3. Start the performance data aggregator process:  `./run_aggregator.sh`.
4. Start the client controller process: `./run_cc.sh`. 
   This is only necessary if you want to submit applications containing external clients.

In order to deploy an application to the framework, deployment tool has to be used.  
The `depltool.sh` is a command-line tool for communicating with the framework.  

A list of supported commands:

```

./depltool.sh submit application.yaml
./depltool.sh status application
./depltool.sh get-time 99 application component probe
./depltool.sh get-throughput application component probe
./depltool.sh submit-requirements requirements.yaml
./depltool.sh delete application
```

### How to deploy an application

1. Each application is specified with an application descriptor (a YAML file that describes the application). 
   Examples can be found in the demonstrators located in the `examples` folder
2. The framework fetches Docker images from  Docker Hub (or from a different Docker repository. 
   The image may be either public or private. 
   When the image is private, a valid Docker account has to provided (meaning the `username`, `password` and `email` fields have to be filled in the descriptor).
3. The application can be submitted by using the deployment tool.

An example application is located in the `examples/frpyc` directory.
It consists of two components: a client and a recognizer server.
The client sends images to the recognizer, in response it receives the faces recognized in the image.

It can be submitted to the framework with `./depltool.sh submit examples/frpyc/spec.yaml`
