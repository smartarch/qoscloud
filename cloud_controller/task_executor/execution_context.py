from time import perf_counter
from typing import Optional

import grpc
import logging

from kubernetes import config
from kubernetes.client import CoreV1Api, AppsV1Api

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT
from cloud_controller.execution.mongo_controller import MongoController
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.knowledge_pb2_grpc import ClientControllerInternalStub
from cloud_controller.knowledge.model import CompinPhase, ManagedCompin
from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2 import Pong
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub

PING_TIMEOUT = 2  # Seconds


class ExecutionContext:

    def __init__(self, knowledge: Knowledge):
        self.knowledge: Knowledge = knowledge

    @staticmethod
    def ping_instance_ip(ip: str) -> CompinPhase:
        return ExecutionContext.ping_compin(connect_to_grpc_server(MiddlewareAgentStub, ip, AGENT_PORT))

    @staticmethod
    def ping_compin(stub: MiddlewareAgentStub) -> CompinPhase:
        """
        Checks whether managed compin answers to the ping, and its current phase.
        :param stub: MiddlewareAgentStub of compin to check.
        :return: Current phase of the compin.
        """
        start = perf_counter()
        try:
            pong_future = stub.Ping.future(Pong(), timeout=PING_TIMEOUT)
            while perf_counter() - start < PING_TIMEOUT:
                if pong_future.done():
                    pong = pong_future.result()
                    return CompinPhase(pong.phase)
            pong_future.cancel()
            return CompinPhase.CREATING
        except grpc.RpcError:
            logging.info(f"Ping time: {perf_counter() - start} seconds")
            return CompinPhase.CREATING

    @staticmethod
    def update_instance_phase(instance: ManagedCompin) -> None:
        stub = connect_to_grpc_server(MiddlewareAgentStub, instance.ip, AGENT_PORT)
        instance.phase = ExecutionContext.ping_compin(stub)

    def get_instance_agent(self, app_name: str, component_name: str, instance_id: str) -> Optional[MiddlewareAgentStub]:
        instance = self.knowledge.actual_state.get_compin(app_name, component_name, instance_id)
        return connect_to_grpc_server(MiddlewareAgentStub, instance.ip, AGENT_PORT)

    def get_instance_ip(self, app_name: str, component_name: str, instance_id: str) -> Optional[str]:
        instance = self.knowledge.actual_state.get_compin(app_name, component_name, instance_id)
        return instance.ip


class KubernetesExecutionContext(ExecutionContext):

    def __init__(self, knowledge: Knowledge, kubeconfig_file: str):
        super().__init__(knowledge)
        config.load_kube_config(config_file=kubeconfig_file)
        self.basic_api = CoreV1Api()
        self.extensions_api = AppsV1Api()


class ClientControllerExecutionContext(ExecutionContext):

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self.client_controller: ClientControllerInternalStub = connect_to_grpc_server(
            ClientControllerInternalStub,
            CLIENT_CONTROLLER_HOST,
            CLIENT_CONTROLLER_PORT
        )


class StatefulnessControllerExecutionContext(ExecutionContext):

    def __init__(self, knowledge: Knowledge, mongos_ip: str):
        super().__init__(knowledge)
        self.mongo_controller = MongoController(mongos_ip)
