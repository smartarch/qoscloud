import random
import time
from enum import Enum
from typing import Optional, Dict

import grpc

from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2 import MongoParameters
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub


class ClientStatus(Enum):
    CONNECTED = 1
    DISCONNECTED = 2
    VIRTUAL = 3


class Client:
    """
    Stores the data about a client relevant to the ClientController

    Attributes:
        descriptor          ClientDescriptor protobuf for this client
        context             grpc.ServicerContext of this client, can be used to cancel the connection
        last_call           Timestamp of the last call to the client
        dependency_updates  Pending dependency updates (ones that were not yet sent to the client)
        dependencies        Current values of all client's dependencies
        status              CONNECTED or DISCONNECTED
    """

    def __init__(self, app_name: str, type: str, id: str):
        self.application: str = app_name
        self.type: str = type
        self.id: str = id
        self.hasID: bool = False
        self.persistent_id: Optional[str] = None

        self.ip: Optional[str] = None
        self.imsi: Optional[str] = None
        self.position_x: float = random.randint(0, 100)
        self.position_y: float = random.randint(0, 100)

        self.context: Optional[grpc.ServicerContext] = None
        self.last_call: float = time.perf_counter()

        self.dependency_updates: Dict[str, str] = {}
        self.dependencies: Dict[str, str] = {}

        self.status: ClientStatus = ClientStatus.VIRTUAL

    def set_statefulness_key(self, ip):
        agent: MiddlewareAgentStub = connect_to_grpc_server(MiddlewareAgentStub, ip, AGENT_PORT)
        agent.SetStatefulnessKey(MongoParameters(shardKey=int(self.persistent_id)))

    def virtualize(self) -> None:
        self.status = ClientStatus.VIRTUAL
        self.context = None
        self.persistent_id = None
        self.hasID = False
        self.ip = None
        self.imsi = None

    @staticmethod
    def init_from_pb(descriptor: protocols.ClientDescriptor) -> "Client":
        client = Client(descriptor.application, descriptor.type, descriptor.id)
        client.ip = descriptor.ip
        client.hasID = descriptor.hasID
        client.imsi = descriptor.imsi
        client.position_x = descriptor.position_x
        client.position_y = descriptor.position_y
        client.persistent_id = descriptor.persistent_id
        return client

    def update_from_pb(self, descriptor: protocols.ClientDescriptor):
        assert descriptor.type == self.type
        assert descriptor.application == self.application
        assert descriptor.id == self.id
        self.ip = descriptor.ip
        self.hasID = descriptor.hasID
        self.imsi = descriptor.imsi
        # self.position_x = descriptor.position_x
        # self.position_y = descriptor.position_y
        self.persistent_id = descriptor.persistent_id
        for dependency_ip in self.dependencies.values():
            self.set_statefulness_key(dependency_ip)

    def pb_representation(self) -> protocols.ClientDescriptor:
        return protocols.ClientDescriptor(
            application=self.application,
            type=self.type,
            ip=self.ip,
            hasID=self.hasID,
            id=self.id,
            imsi=self.imsi,
            position_x=self.position_x,
            position_y=self.position_y,
            persistent_id=self.persistent_id
        )