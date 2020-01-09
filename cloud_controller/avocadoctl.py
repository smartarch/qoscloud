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
from typing import Optional

import grpc
import yaml

import cloud_controller.architecture_pb2 as arch_pb
import cloud_controller.assessment.deploy_controller_pb2 as deploy_pb
import cloud_controller.assessment.deploy_controller_pb2_grpc as deploy_grpc
from cloud_controller.assessment import CTL_HOST, CTL_PORT
from cloud_controller.middleware.helpers import setup_logging


def yaml_to_grpc_architecture(architecture_yaml) -> Optional[arch_pb.Architecture]:
    """
    A helper function that converts a YAML representation of application descriptor to a protobuf representation.
    :param architecture_yaml: A loaded YAML object representing application descriptor to convert.
    :return: a protobuf representation of the application descriptor.
    """
    architecture_grpc = arch_pb.Architecture()
    architecture_grpc.name = architecture_yaml['name']
    if 'dockersecret' in architecture_yaml:
        secret_string = f'{{"auths":{{"https://index.docker.io/v1/":{{' \
            f'"Username":"{architecture_yaml["dockersecret"]["username"]}",' \
            f'"Password":"{architecture_yaml["dockersecret"]["password"]}",' \
            f'"Email":"{architecture_yaml["dockersecret"]["email"]}"}}}}}}'
        encoded_secret = bytes.decode(base64.b64encode(bytes(secret_string, 'utf-8')), 'utf-8')
        architecture_grpc.secret.value = encoded_secret

    for item in architecture_yaml['components']:
        if 'name' not in item or 'type' not in item:
            logging.info("incorrect architecture descriptor: 'name and 'type' are required fields for every component")
            return
        _name = item['name']
        _type = item['type'].lower()
        architecture_grpc.components[_name].name = _name
        if _type == "managed":
            architecture_grpc.components[_name].type = arch_pb.ComponentType.Value("MANAGED")
        elif _type == "unmanaged":
            architecture_grpc.components[_name].type = arch_pb.ComponentType.Value("UNMANAGED")
        else:
            logging.info("incorrect architecture descriptor: unknown component type: %s" % _type)
            return
        if 'dependsOn' in item:
            for dependency in item['dependsOn']:
                architecture_grpc.components[_name].dependsOn.append(dependency)

        if _type == "managed":
            if 'statefulness' in item:
                statefulness = item['statefulness'].upper()
                if statefulness not in arch_pb.Statefulness.keys():
                    logging.info(f"incorrect architecture descriptor: the value of 'statefulness' is wrong. "
                                 f"Supported values: {arch_pb.Statefulness.keys()}")
                    return
                architecture_grpc.components[_name].statefulness = arch_pb.Statefulness.Value(statefulness)
            if 'template' in item:
                _hash = hash(yaml.dump(item['template']))
                _id = str(_hash % (sys.maxsize + 1))
                architecture_grpc.components[_name].id = _id
                architecture_grpc.components[_name].deployment = yaml.dump(item['template'])
            else:
                logging.info("incorrect architecture descriptor: template not specified")
                return
            if 'cardinality' in item:
                cardinality = item['cardinality']
                if cardinality == '*':
                    architecture_grpc.components[_name].cardinality = arch_pb.Cardinality.Value("MULTIPLE")
                elif cardinality == '1':
                    architecture_grpc.components[_name].cardinality = arch_pb.Cardinality.Value("SINGLE")
                else:
                    logging.info("incorrect architecture descriptor: the cardinality value can be either '*' or '1'")
                    return
            else:
                architecture_grpc.components[_name].cardinality = arch_pb.Cardinality.Value("MULTIPLE")
            if 'probes' in item:
                for probe in item['probes']:
                    probe_grpc = architecture_grpc.components[_name].probes.add()
                    probe_grpc.name = probe['name']
                    probe_grpc.time_limit = probe['limit']
                    probe_grpc.application = architecture_grpc.name
                    probe_grpc.component = _name
            if 'timingRequirements' in item:
                for req in item['timingRequirements']:
                    requirement_grpc = architecture_grpc.components[_name].timingRequirements.add()
                    requirement_grpc.name = req['name']
                    requirement_grpc.probe = req['probe']
                    for limit in req['limits']:
                        limit_grpc = requirement_grpc.limits.add()
                        limit_grpc.probability = limit['probability']
                        limit_grpc.time = limit['time']
        elif _type == "unmanaged":
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


def submit_app(architecture_grpc: arch_pb.Architecture) -> None:
    """
    Uploads an app for the pre-assessment. Once pre-assessment is completed, the app will be sent for production
    :param architecture_grpc: protobuf representation of the application descriptor
    """
    # Open connection
    channel = grpc.insecure_channel(CTL_HOST + ":" + str(CTL_PORT))
    stub = deploy_grpc.DeployControllerStub(channel)

    # Send app for pre-assessment
    response = stub.SubmitArchitecture(architecture_grpc)
    # Check results
    if response.rc != deploy_pb.RC_OK:
        if response.rc == deploy_pb.RC_NAME_NOT_AVAILABLE:
            print("Name %s is already taken by another application/namespace" % architecture_grpc.name,
                  file=sys.stderr)
        else:
            assert False
        return
    print("Application %s submitted successfully for pre-assessment" % architecture_grpc.name)


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


if __name__ == "__main__":
    # Prepare logging
    setup_logging()

    # Parse input args
    parser = argparse.ArgumentParser(description="Avocado's command line interface")
    parser.add_argument('-a', '--add', action='store_true', help='add new application or HW config')
    parser.add_argument('-d', '--delete', action='store_true', help='delete an application')
    parser.add_argument('-s', '--stats', action='store_true', help='show statistics')
    parser.add_argument('-n', '--name', type=str, default=None, help='name of the application')
    parser.add_argument('-f', '--file', type=str, default=None, help='file with application specification')
    parser.add_argument('-c', '--hw_config', type=str, default=None, help='name of HW config')

    args = parser.parse_args()

    # Args check-up
    if [args.add, args.delete, args.stats].count(True) > 1:
        print("You could use only one of --add, --delete, --stats", file=sys.stderr)
        exit(1)

    if not args.add and not args.delete and not args.stats:
        parser.print_help()
        exit(1)

    # Open connection to knowledge

    if args.delete:
        if args.name:
            delete_app(args.name)
        elif args.file:
            architecture_yaml = yaml.load(open(args.file, 'r'))
            delete_app(architecture_yaml['name'])
        else:
            print("A name or an application specification file has to be provided in order to delete an application",
                  file=sys.stderr)
            exit(1)
    elif args.add:
        if args.file:
            architecture_grpc = yaml_to_grpc_architecture(yaml.load(open(args.file, 'r')))
            if architecture_grpc is None:
                exit(1)
            if args.name:
                architecture_grpc.name = args.name
            submit_app(architecture_grpc)
        elif args.hw_config:
            register_hw_config(args.hw_config)
        else:
            print("A path to the application specification file has to be provided ('-f filename')"
                  " or name of HW config (`-c hw_config_name`)", file=sys.stderr)
            exit(1)
    elif args.stats:
        # Args checkup
        if not args.name:
            print("A name of application have to be provided", file=sys.stderr)
            exit(1)

        print_app_status(args.name)
    else:
        assert False
