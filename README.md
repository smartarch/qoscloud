# Avocado framework

A proof-of-concept edge cloud system, which makes it possible to minimize and statistically guarantee the latency of a cloud application.

## How to initialize the framework

The following section describes the very first steps that must be followed to run the framework. This has to be done only once in normal cases.

1. Make sure two Kubernetes clusters (production and assessment) are set up and running. An easy way how to do this  is to follow [Kubernetes-Vagrant setup](https://gitlab.d3s.mff.cuni.cz/edge-cloud/vagrant-setup).
   1. The framework expects to find the *kubeconfig* file of the production cluster in the directory `/root/.kube/config`, and *kubeconfig* of the assessment cluster in the directory `/root/.kube/config-assessment`.
2. Run `pip3 install -r requirements.txt` to install the required libraries.
3. Run `python3 ./generate_grpc.py` to generate _GRPC stubs_  and _ProtoBufs_.

## How to use the framework

These lines describe how to use the framework in day to day basis. The framework should be already initialized to work properly. All the following commands have to be executed from the root folder of the project.

The controller consists of several processes which must be run in separate terminals.

1. Start the Client Controller process: `PYTHONPATH=. python3 cloud_controller/client_controller.py`
2. Start the Assessment component process:  `PYTHONPATH=. python3 cloud_controller/assessment/main.py`
3. Start the Adaptation Controller process:  `PYTHONPATH=. python3 cloud_controller/adaptation_controller.py`

In order to deploy an application to the framework, `avocadoctl` has to be used.  The `avocadoctl` is a command-line framework management tool.  A list of possible commands supported by `avocadoctl` follows:

- `avocadoctl -a -f <path to ASD>` Adds a new application, specified by an ASD. If the ASD is  parsed successfully, the command immediately returns; further deployment and probes testing occur in the background and the progress must be checked through the framework logging. If there is a problem with the ASD, the error is announced to the user. 
- `avocadoctl -a -c <hardware configuration name>` Register a new hardware configuration and start the pre-assessment process of that hardware configuration. These results are used in Predictor.
- `avocadoctl -d -n <application name, as it was specified in ASD>` Stops an application that was previously deployed to the framework. The command returns immediately; in a few minutes, all components of an application are killed.
- `avocadoctl -s -n <application name, as it was specified in ASD>` Shows application status in the pre-assessment.

### How to deploy an application

These lines show how to deploy an application to the _Avocado framework_. The application has to consist of at least one _Docker_ images on a server-side and a variable number of clients. Every application component has to implement the _Avocado API_. The API description is in the documentation.

1. Each application is specified with an ASD. The ASD is a YAML file that describes the application. The concrete specification is available in the project documentation.
2. The framework fetches _Docker_ images from  _Docker Hub_. The image may be both public or private. When the image is private, a valid _Docker account_ has to provided (meaning the `username`, `password` and `email` fields have to be filled in ASD).
3. The application could be submitted by using  the `avocadoctl` tool. Let's assume the ASD file is in `apps/frpyc_slup/spec.yaml`.  The  `python3 ./cloud_controller/avocadoctl.py -a -f apps/frpyc_slup/spec.yaml` submits the application.

To delete the application from the cloud you can run `python3 ./cloud_controller/avocadoctl.py -d -f apps/frpyc_slup/spec.yaml`.

## How the framework works on an example

One of the framework-ready applications is located in the `apps/frpyc_slup` directory.
It consists of two components: an unmanaged client and a managed recognizer server.
The client sends images to the recognizer, in response it receives the faces recognized in the image.
The basic idea behind the application is illustrated in Figure 1:

![Figure 1](images/running-example-figure.png)

The role of the Avocado framework (in the scope of this application) is to:
* determine to which node the client has minimum latency
* instantiate recognizers on those nodes that have clients nearby
* connect the clients to the instantiated recognizers.

For the client, this whole process looks  as simple as:
1. Connect to the cloud.
2. Wait for the first address of the recognizer.
3. Communicate with the recognizer.