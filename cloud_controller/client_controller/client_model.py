import logging
import math
import random
import time
from enum import Enum
from threading import RLock
from typing import Dict, List, Tuple, Optional

import grpc

from cloud_controller import architecture_pb2 as arch_pb, UEManagementPolicy
from cloud_controller.architecture_pb2 import Architecture
from cloud_controller.client_controller.client_controller import DEFAULT_WAIT_SIGNAL_FREQUENCY
from cloud_controller.client_controller.network_topology import ClientPositionTracker, EuclidClientPositionTracker
from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.knowledge.model import Application, ComponentType
from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.knowledge.user_equipment import UserEquipmentContainer
from cloud_controller.middleware import middleware_pb2 as mw_protocols, AGENT_PORT
from cloud_controller.middleware.agent import MiddlewareAgent
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2 import MongoParameters
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub


class ClientStatus(Enum):
    CONNECTED = 1
    DISCONNECTED = 2
    VIRTUAL = 3


VIRTUAL_COUNT_CONSTANT = 3
VIRTUAL_COUNT_PERCENT = 0.1


def threshold(current_number: int) -> int:
    return VIRTUAL_COUNT_CONSTANT + math.ceil((1 + VIRTUAL_COUNT_PERCENT) * current_number)


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


class ClientModel:
    """
    Contains the data structures used by ClientControllerExternal and ClientControllerInternal.

    Attributes:
        clients                     All clients mapped by application and client ID
        applications                All application protobufs mapped by name
        new_events                  List of ClientDescriptors of client additions and removals that were not yet
                                    reported
        user_equipment              A container holding all registered user equipment
        wait_signal_frequency       A frequency at which the Client Controller has to send WAIT signals to clients
        liveness_check_frequency    A frequency at which the Client Controller has to perform client liveness checks
    """
    def __init__(self, wait_signal_frequency=DEFAULT_WAIT_SIGNAL_FREQUENCY):
        self.clients: Dict[str, Dict[str, Client]] = {}
        self.ids: Dict[str, str] = {}
        self.virtual_clients: Dict[str, Dict[str, List[Client]]] = {}
        self.applications: Dict[str, Architecture] = {}
        self._last_id: int = 0
        self._last_persistent_id: int = 0
        self.new_events: List[protocols.ClientDescriptor] = []
        self.user_equipment: UserEquipmentContainer = UserEquipmentContainer()
        self._lock = RLock()
        self.wait_signal_frequency = wait_signal_frequency  # Seconds
        self.liveness_check_frequency = wait_signal_frequency * 4  # Seconds
        self.network_topology: ClientPositionTracker = EuclidClientPositionTracker()

    def iterate_clients(self):
        for app_name in self.clients:
            for client in self.clients[app_name].values():
                if client.status == ClientStatus.CONNECTED:
                    yield client

    def update_distances(self):
        self.network_topology.update_client_positions(self.iterate_clients())
        for client in self.iterate_clients():
            event = client.pb_representation()
            event.event = protocols.ClientEventType.Value("LOCATION")

    def check_threshold(self, app_name: str, type: str):
        virtual_clients = self.virtual_clients[app_name][type]
        currently_connected = 0
        for client in self.clients[app_name].values():
            if client.type == type:
                currently_connected += 1
        n = threshold(currently_connected)
        while len(virtual_clients) < n:
            self.add_virtual_client(app_name, type)
        while len(virtual_clients) > n:
            self.remove_client(app_name, type)

    def get_virtual_client(self, app_name: str, type: str) -> Client:
        return self.virtual_clients[app_name][type].pop(0)

    def _client_exists(self, application, id_):
        return application in self.clients and id_ in self.clients[application]

    def component_exists(self, application, component):
        return application in self.applications and component in self.applications[application].components

    def _generate_client_id(self):
        self._last_id += 1
        return str(self._last_id)

    def _generate_persistent_id(self):
        self._last_persistent_id += 1
        return str(self._last_persistent_id)

    def _add_event(self, client: Client, connection: bool):
        event_descriptor = client.pb_representation()
        if connection:
            event_descriptor.event = protocols.ClientEventType.Value("CONNECTION")
        else:
            event_descriptor.event = protocols.ClientEventType.Value("DISCONNECTION")
        self.new_events.append(event_descriptor)

    def disconnect_client(self, application: str, id_:str) -> None:
        """
        This method is called when a client liveness check fails. The client is marked as DISCONNECTED, and the
        corresponding client disconnection event scheduled to be sent to the Adaptation Controller.
        :param application: Application the client belongs to.
        :param id_: Client ID
        """
        with self._lock:
            logging.info("Disconnecting a client %s:%s" % (application, id_))
            if not self._client_exists(application, id_):
                logging.info("Client not found")
                return
            client = self.clients[application][id_]
            client.context.cancel()
            client.virtualize()
            logging.info(f"Client with ID {id_} disconnected successfully")

    def remove_client(self, application, type) -> None:
        """
        This method is called when a client liveness check fails. The client is marked as DISCONNECTED, and the
        corresponding client disconnection event scheduled to be sent to the Adaptation Controller.
        :param application: Application the client belongs to.
        :param id_: Client ID
        """
        with self._lock:
            client: Client = self.virtual_clients[application][type].pop(0)
            logging.info("Removing a client %s:%s" % (application, client.id))
            del self.clients[client.application][client.id]
            self._add_event(client, False)
            logging.info(f"Client with ID {client.id} removed successfully")

    def add_virtual_client(self, app_name: str, type: str):
        with self._lock:
            client: Client = Client(app_name, type, self._generate_client_id())
            self.clients[client.application][client.id] = client
            self.virtual_clients[client.application][client.type].append(client)
            self._add_event(client, True)
            logging.info("New client added successfully. ID = %s" % client.id)

    def assign_client(self, client_pb: protocols.ClientDescriptor, context: grpc.ServicerContext):
        """
        This method is called when a new client connects to the Client Controller. It checks whether the the client
        can be accepted, and if all the checks succeed, adds the client to the model. If the client has no ID assigned,
        assignes it a new ID.
        The checks may fail in the following cases:
            (1) The requested application or component name is not known
            (2) The requested ID does not exist
            (3) Client's IP is not allowed to use this application
        Each of these cases returns a specific error code
        :param client_pb: ClientDescriptor of the client
        :param context: grpc.ServicerContext of the client
        :return: error code, client ID, a Client object
        """
        logging.info(f"Adding new client of type {client_pb.application}:{client_pb.type} with ip {client_pb.ip}")
        # Check whether the declared client component exists
        if not self.component_exists(client_pb.application, client_pb.type):
            logging.info("Client type unknown")
            return mw_protocols.ClientResponseCode.Value("UNKNOWN_TYPE"), None, None
        # Check whether this client was allowed by Network Controller to use the app
        ue_policy = self.applications[client_pb.application].components[client_pb.type].policy
        if ue_policy == arch_pb.UEMPolicy.Value(UEManagementPolicy.WHITELIST.name):
            if not self.user_equipment.ue_registered(client_pb.ip):
                logging.info(f"Rejected a client due to unregistered IP: {client_pb.ip}")
                return mw_protocols.ClientResponseCode.Value("IP_NOT_KNOWN"), None, None
            ue = self.user_equipment.get_ue(client_pb.ip)
            if f"{client_pb.application}.{client_pb.type}" not in ue.apps:
                logging.info(f"Rejected a client due to absence of subscription to '{client_pb.application}' app")
                return mw_protocols.ClientResponseCode.Value("NO_APP_SUBSCRIPTION"), None, None
            client_pb.imsi = ue.imsi

        with self._lock:
            if not client_pb.hasID:
                # Assign a new client ID
                client_pb.persistent_id = self._generate_persistent_id()
                client_pb.hasID = True
                self.ids[client_pb.persistent_id] = f"{client_pb.application}.{client_pb.type}"
            elif client_pb.persistent_id not in self.ids or \
                    self.ids[client_pb.persistent_id] != f"{client_pb.application}.{client_pb.type}":
                logging.info("Client ID not found")
                return mw_protocols.ClientResponseCode.Value("ID_NOT_FOUND"), None, None
            client: Client = self.get_virtual_client(client_pb.application, client_pb.type)
            client.update_from_pb(client_pb)
            client.context = context
            client.last_call = time.perf_counter()
            client.status = ClientStatus.CONNECTED
            logging.info("New client connected successfully. ID = %s" % client.persistent_id)
            return mw_protocols.ClientResponseCode.Value("OK"), client.persistent_id, client

    def add_new_application(self, application_pb):
        app = Application.init_from_pb(application_pb)
        app_name = application_pb.name
        self.clients[app_name] = {}
        self.virtual_clients[app_name] = {}
        for component in app.components.values():
            if component.type == ComponentType.UNMANAGED:
                self.virtual_clients[app_name][component.name] = []
        self.applications[app_name] = application_pb
