#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module provides command line interface for submission of new applications to Avocado, removing the applications
from Avocado, checking app status, and registering hardware configurations.
"""

import argparse
import base64
import logging
import sys
from typing import Optional, Dict

import grpc
import yaml

import cloud_controller.architecture_pb2 as arch_pb
import cloud_controller.assessment.deploy_controller_pb2 as deploy_pb
import cloud_controller.assessment.deploy_controller_pb2_grpc as deploy_grpc
from cloud_controller import PREDICTOR_HOST, PREDICTOR_PORT
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorStub
from cloud_controller.assessment import CTL_HOST, CTL_PORT
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployControllerStub
from cloud_controller.middleware.helpers import setup_logging, connect_to_grpc_server

time_periods: Dict[str, int] = {
    "second": 1000,
    "minute": 60 * 1000,
    "hour": 60 * 60 * 1000,
    "day": 24 * 60 * 60 * 1000
}


def check_field_present(yaml_dict, parent, child):
    if child not in yaml_dict[parent]:
        logging.error(f"Incorrect application descriptor format:"
                      f"    the field '{child}' must be present under '{parent}'.")
        exit(1)


def check_value(field: str, value: str, list_):
    if value not in list_:
        logging.error(f"Incorrect application descriptor format:"
                      f"    the field '{field}' can have only the following values: '{list_}'.")
        exit(1)


DOCKER_SECRET = 'dockersecret'
USERNAME = 'username'
PASSWORD = 'password'
EMAIL = 'email'

APP_NAME = 'application'
COMPLETE = 'complete'
COMPONENTS = 'components'
NAME = 'name'
CARDINALITY = 'cardinality'
STATEFULNESS = 'statefulness'
PROBES = 'probes'
CODEFILE = 'codefile'
INPUTFILE = 'inputfile'
ARGS = 'args'
REQUIREMENTS = 'QoSrequirements'
TYPE = 'type'
PROBE = 'probe'
PROBABILITY = 'probability'
REQUESTS = 'requests'
PER = 'per'
TIME = 'time'
TEMPLATE = 'template'
IMAGE = 'image'
CLIENTS = 'clients'
LATENCY = 'latency'
DEPENDENCIES = 'dependencies'


def yaml_to_grpc_architecture(architecture_yaml) -> Optional[arch_pb.Architecture]:
    """
    A helper function that converts a YAML representation of application descriptor to a protobuf representation.
    :param architecture_yaml: A loaded YAML object representing application descriptor to convert.
    :return: a protobuf representation of the application descriptor.
    """
    architecture_grpc = arch_pb.Architecture()

    check_field_present({"document": architecture_yaml}, "document", APP_NAME)
    architecture_grpc.name = architecture_yaml[APP_NAME]
    if DOCKER_SECRET in architecture_yaml:
        check_field_present(architecture_yaml, DOCKER_SECRET, USERNAME)
        check_field_present(architecture_yaml, DOCKER_SECRET, PASSWORD)
        check_field_present(architecture_yaml, DOCKER_SECRET, EMAIL)
        secret_string = f'{{"auths":{{"https://index.docker.io/v1/":{{' \
                        f'"Username":"{architecture_yaml[DOCKER_SECRET][USERNAME]}",' \
                        f'"Password":"{architecture_yaml[DOCKER_SECRET][PASSWORD]}",' \
                        f'"Email":"{architecture_yaml[DOCKER_SECRET][EMAIL]}"}}}}}}'
        encoded_secret = bytes.decode(base64.b64encode(bytes(secret_string, 'utf-8')), 'utf-8')
        architecture_grpc.secret.value = encoded_secret

    if COMPLETE in architecture_yaml:
        architecture_grpc.is_complete = architecture_yaml[COMPLETE]
    else:
        architecture_grpc.is_complete = True

    check_field_present({"document": architecture_yaml}, "document", COMPONENTS)
    for item in architecture_yaml[COMPONENTS]:
        _name = item[NAME]
        check_field_present({COMPONENTS: item}, COMPONENTS, NAME)
        architecture_grpc.components[_name].name = _name
        architecture_grpc.components[_name].type = arch_pb.ComponentType.Value("MANAGED")
        if DEPENDENCIES in item:
            for dependency in item[DEPENDENCIES]:
                architecture_grpc.components[_name].dependsOn.append(dependency)

        if STATEFULNESS in item:
            statefulness = item[STATEFULNESS].upper()
            check_value(STATEFULNESS, statefulness, arch_pb.Statefulness.keys())
            architecture_grpc.components[_name].statefulness = arch_pb.Statefulness.Value(statefulness)
        if TEMPLATE in item:
            _hash = hash(yaml.dump(item[TEMPLATE]))
            _id = str(_hash % (sys.maxsize + 1))
            architecture_grpc.components[_name].id = _id
            architecture_grpc.components[_name].deployment = yaml.dump(item[TEMPLATE])
        if CARDINALITY in item:
            cardinality = item[CARDINALITY].upper()
            check_value(CARDINALITY, cardinality, arch_pb.Cardinality.keys())
            architecture_grpc.components[_name].cardinality = arch_pb.Cardinality.Value(cardinality)
        check_field_present({COMPONENTS: item}, COMPONENTS, PROBES)
        probes: Dict = {}
        for probe in item[PROBES]:
            probe_grpc = architecture_grpc.components[_name].probes.add()
            check_field_present({PROBES: probe}, PROBES, NAME)
            probe_grpc.name = probe[NAME]
            probe_grpc.application = architecture_grpc.name
            probe_grpc.component = _name
            probes[probe_grpc.name] = probe_grpc
            if INPUTFILE in probe:
                with open(probe[INPUTFILE], "r") as file:
                    probe_grpc.config = file.read()
            if CODEFILE in probe:
                with open(probe[INPUTFILE], "r") as file:
                    probe_grpc.code = file.read()
                probe_grpc.type = arch_pb.ProbeType.Value("CODE")
            if ARGS in probe:
                probe_grpc.args = probe[ARGS]
        if REQUIREMENTS in item:
            for req in item[REQUIREMENTS]:
                probe = probes[req[PROBE]]
                requirement_grpc = probe.requirements.add()
                if req[TYPE].lower() == "time":
                    requirement_grpc.time.time = req[TIME]
                    requirement_grpc.time.percentile = req[PROBABILITY]
                elif req[TYPE].lower() == "throughput":
                    requirement_grpc.throughput.requests = req[REQUESTS]
                    requirement_grpc.throughput.per = time_periods[req[PER]]
    if CLIENTS in architecture_yaml:
        for item in architecture_yaml[CLIENTS]:
            check_field_present({CLIENTS: item}, CLIENTS, NAME)
            _name = item[NAME]
            architecture_grpc.components[_name].name = _name
            architecture_grpc.components[_name].type = arch_pb.ComponentType.Value("UNMANAGED")
            check_field_present({CLIENTS: item}, CLIENTS, DEPENDENCIES)
            for dependency in item[DEPENDENCIES]:
                check_field_present({DEPENDENCIES: dependency}, DEPENDENCIES, NAME)
                architecture_grpc.components[_name].dependsOn.append(dependency[NAME])
            if LATENCY in item:
                architecture_grpc.components[_name].latency = item[LATENCY]
            else:
                architecture_grpc.components[_name].latency = True
            if 'UEMPolicy' in item:
                uem_policy = item['UEMPolicy'].upper()
                if uem_policy not in arch_pb.UEMPolicy.keys():
                    logging.info(f"incorrect architecture descriptor: the value of 'UEMPolicy' is wrong. "
                                 f"Supported values: {arch_pb.UEMPolicy.keys()}")
                    return
                architecture_grpc.components[_name].policy = arch_pb.UEMPolicy.Value(uem_policy)
            if 'whitelist' in item:
                whitelist = item['whitelist']
                for ip in whitelist:
                    architecture_grpc.components[_name].whitelist.append(ip)

    return architecture_grpc


def submit_requirements(architecture_grpc: arch_pb.Architecture) -> None:
    """
    Uploads requirements for an app for the application review.
    :param architecture_grpc: protobuf representation of the application requirements
    """
    stub: DeployControllerStub = connect_to_grpc_server(DeployControllerStub, CTL_HOST, CTL_PORT)

    # Send the app for pre-assessment
    response = stub.SubmitRequirements(architecture_grpc)
    # Check results
    if response.rc == deploy_pb.RC_OK:
        print("Application requirements submitted successfully")
    elif response.rc == deploy_pb.RC_NAME_NOT_AVAILABLE:
        print(f"Application {architecture_grpc.name} is not available", file=sys.stderr)
    elif response.rc == deploy_pb.RC_APP_ALREADY_ACCEPTED:
        print(f"Application {architecture_grpc.name} has already been deployed", file=sys.stderr)
    else:
        print(f"An unknown error happened during submission", file=sys.stderr)


def submit_app(architecture_grpc: arch_pb.Architecture) -> None:
    """
    Uploads an app for the pre-assessment. Once pre-assessment is completed, the app will be sent for production
    :param architecture_grpc: protobuf representation of the application descriptor
    """
    stub: DeployControllerStub = connect_to_grpc_server(DeployControllerStub, CTL_HOST, CTL_PORT)

    # Send the app for pre-assessment
    response = stub.SubmitArchitecture(architecture_grpc)
    # Check results
    if response.rc == deploy_pb.RC_OK:
        print("Application %s submitted successfully for pre-assessment" % architecture_grpc.name)
    elif response.rc == deploy_pb.RC_NAME_NOT_AVAILABLE:
        print(f"Name {architecture_grpc.name} is already taken by another application/namespace", file=sys.stderr)
    else:
        print(f"An unknown error happened during submission", file=sys.stderr)


def register_hw_config(name: str) -> None:
    """
    Register a hardware configuration for assessment
    """
    # Open connection
    channel = grpc.insecure_channel(CTL_HOST + ":" + str(CTL_PORT))
    stub = deploy_grpc.DeployControllerStub(channel)

    # Register hw config
    hw_config = deploy_pb.HwConfig(name=name)
    response = stub.RegisterHwConfig(hw_config)
    # Check results
    if response.rc != deploy_pb.RC_OK:
        assert False
    print("HW config %s registered successfully" % name)


def delete_app(name: str) -> None:
    """
    Delete the app with the specified name
    :param name: Name of application
    """
    # Open connection
    channel = grpc.insecure_channel(CTL_HOST + ":" + str(CTL_PORT))
    stub = deploy_grpc.DeployControllerStub(channel)
    # Send request
    delete_request = deploy_pb.AppName(name=name)
    response = stub.DeleteApplication(delete_request)
    # Check results
    if response.rc != deploy_pb.RC_OK:
        if response.rc == deploy_pb.RC_NAME_NOT_AVAILABLE:
            print("Application %s does not exists" % name,
                  file=sys.stderr)
        else:
            assert False
        return
    print("Application %s deleted" % name)


def print_app_status(name: str) -> None:
    """
    Show status of an application
    :param name: Name of the application
    """
    # Open connection
    channel = grpc.insecure_channel(CTL_HOST + ":" + str(CTL_PORT))
    stub = deploy_grpc.DeployControllerStub(channel)
    # Send request
    app_name = deploy_pb.AppName(name=name)
    response = stub.GetApplicationStats(app_name)
    # Check results
    if response.rc != deploy_pb.RC_OK:
        if response.rc == deploy_pb.RC_NAME_NOT_AVAILABLE:
            print("Application %s does not exists" % name,
                  file=sys.stderr)
        else:
            assert False
        return
    print(response.stats)


def get_time(application, component, probe, percentile=None):
    requirements = arch_pb.ApplicationTimingRequirements()
    requirements.name = f"{application}_{component}_{probe}"
    if percentile:
        percentile = int(percentile)
        contract = requirements.contracts.add()
        contract.percentile = percentile
    stub: PredictorStub = connect_to_grpc_server(PredictorStub, PREDICTOR_HOST, PREDICTOR_PORT)
    response = stub.ReportPercentiles(requirements)
    if percentile:
        print(f"Probe {probe}: response time at {percentile} percentile is {response.contracts[0].time}")
    else:
        print(f"Probe {probe}: mean response time is {response.mean}")


def check_args(arg_num: int, error_message: str):
    if len(sys.argv) < arg_num:
        print(error_message, file=sys.stderr)
        exit(1)


if __name__ == "__main__":
    # Prepare logging
    setup_logging()
    command = sys.argv[1]

    if command == "delete":
        check_args(3, "A name of an application has to be provided in order to delete an application")
        delete_app(sys.argv[2])
    elif command == "submit":
        check_args(3, "A path to the application descriptor file has to be provided in order to submit an application")
        architecture_grpc = yaml_to_grpc_architecture(yaml.load(open(sys.argv[2], 'r')))
        if architecture_grpc is None:
            exit(1)
        submit_app(architecture_grpc)
    elif command == "status":
        check_args(3, "A name of application has to be provided")
        print_app_status(sys.argv[2])
    elif command == "get-time":
        check_args(6, "Required format: get-time percentile application component probe")
        get_time(sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[2])
    elif command == "get-throughput":
        check_args(5, "Required format: get-throughput application component probe")
        get_time(sys.argv[2], sys.argv[3], sys.argv[4])
    elif command == "submit-requirements":
        check_args(3, "A path to the application descriptor file has to be provided in order to submit an application")
        architecture_grpc = yaml_to_grpc_architecture(yaml.load(open(sys.argv[2], 'r')))
        if architecture_grpc is None:
            exit(1)
        submit_app(architecture_grpc)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
