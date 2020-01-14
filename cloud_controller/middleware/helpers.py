import logging
import logging.config
import os
import sys
import time
from concurrent import futures
from enum import Enum

import grpc
import yaml

from cloud_controller.middleware.middleware_pb2 import Pong
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub

DEFAULT_WORKER_NUMBER = 10
SLEEP_TIME = 10000  # seconds
WAIT_TIME = 0.1  # seconds


def connect_to_grpc_server(stub_class, host, port, block=False, production=True):
    """
    Connects to a gRPC server of a specified type at a specified address
    :param stub_class: A class of the stub of the server to connect to
    :param host: Host of the server
    :param port: Port of the server
    :param block: Whether the function has to block until the server is available
    :return: stub object
    """
    stub, _ = connect_to_grpc_server_with_channel(stub_class, host, port, block, production)
    return stub


def connect_to_grpc_server_with_channel(stub_class, host, port, block=False, production=True):
    """
    Connects to a gRPC server of a specified type at a specified address
    :param stub_class: A class of the stub of the server to connect to
    :param host: Host of the server
    :param port: Port of the server
    :param block: Whether the function has to block until the server is available
    :return: stub object, channel object
    """
    channel = grpc.insecure_channel(host + ":" + str(port))
    stub = stub_class(channel)
    if block:
        channel_ready_future = grpc.channel_ready_future(channel)
        if isinstance(stub, MiddlewareAgentStub):
            while True:
                try:
                    stub.Ping(Pong(production=production))
                    break
                except grpc.RpcError:
                    time.sleep(WAIT_TIME)
        else:
            while not channel_ready_future.done():
                time.sleep(WAIT_TIME)

    logging.info(f"Successfully connected to {stub_class} at {host}:{port}")
    return stub, channel


def start_grpc_server(servicer, adder, host, port, threads=DEFAULT_WORKER_NUMBER, block=False):
    """
    Starts a new gRPC server with specified parameters
    :param servicer: Servicer object to add to the server
    :param adder: Adder function to use for adding servicer object to the server
    :param host: Host to run the server on
    :param port: Port to add to the server
    :param threads: Number of threads in the server
    :param block: Whether the function has to block indefinitely
    :return: the server object
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=threads))
    adder(servicer, server)
    server.add_insecure_port(host + ":" + str(port))
    server.start()
    logging.info(f'Started {type(servicer).__name__} on {host}:{port}')
    if block:
        try:
            while True:
                time.sleep(SLEEP_TIME)
        except KeyboardInterrupt:
            logging.info('^C received, ending')
            server.stop(0)
    else:
        return server


def setup_logging(default_path='logging_info.yaml', default_level=logging.INFO, env_key='LOG_CFG') -> None:
    """
    Setup logging configuration.

    :param default_path: path to logging config file (used if env_key variable is not set)
    :param default_level: config level to use (if config file is not specified)
    :param env_key: environment variable that contains config file location.
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def load_config_from_file(module, config_file) -> bool:
    """
    Loads the values specified in the given config file to a given module.
    :param module: module to load values into
    :param config_file: file to load values from
    :return: True if the load was successful, False if file does not exist. If the file format is incorrect, will throw
    an exception.
    """
    if not os.path.exists(config_file) or not os.path.isfile(config_file):
        print(f"Config file {config_file} does not exist. Using the default settings.")
        return False

    with open(config_file, "r") as file:
        config = yaml.load(file)
        module = sys.modules[module]
        for key, value in config.items():
            setattr(module, key, value)
    print(f"Configuration successfully loaded from {config_file}.")


class OrderedEnum(Enum):
    """
    Base class for enums that can be compared
    """
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented