"""
Client controller module.

Consists of internal and external interfaces to the Client Controller, as well as ClientModel - the data structure
that is shared between these interfaces.

Both interfaces of the Client Controller may be started with start_client_controller function.
"""
import logging
import sys
import time
from enum import Enum
from threading import RLock
from typing import Dict, List, Tuple, Optional, Generator

import argparse
import grpc

import cloud_controller.architecture_pb2 as arch_pb
import cloud_controller.knowledge.knowledge_pb2 as protocols
import cloud_controller.knowledge.knowledge_pb2_grpc as servicers
import cloud_controller.middleware.middleware_pb2 as mw_protocols
import cloud_controller.middleware.middleware_pb2_grpc as mw_servicers
from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT, MAX_CLIENTS
from cloud_controller.architecture_pb2 import Architecture

from cloud_controller.knowledge.user_equipment import UserEquipment, UserEquipmentContainer, UEManagementPolicy
from cloud_controller.middleware import CLIENT_CONTROLLER_EXTERNAL_HOST, CLIENT_CONTROLLER_EXTERNAL_PORT
from cloud_controller.middleware.helpers import setup_logging, start_grpc_server

DEFAULT_WAIT_SIGNAL_FREQUENCY = 5  # Seconds


class ClientStatus(Enum):
    CONNECTED = 1
    DISCONNECTED = 2


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

    def __init__(self, descriptor: protocols.ClientDescriptor, context: grpc.ServicerContext):
        self.descriptor = descriptor
        self.context: grpc.ServicerContext = context
        self.last_call: float = time.perf_counter()
        self.dependency_updates: Dict[str, str] = {}
        self.dependencies: Dict[str, str] = {}
        self.status: ClientStatus = ClientStatus.CONNECTED


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
        self.applications: Dict[str, Architecture] = {}
        self._last_id: int = 0
        self.new_events: List[protocols.ClientDescriptor] = []
        self.user_equipment: UserEquipmentContainer = UserEquipmentContainer()
        self._lock = RLock()
        self.wait_signal_frequency = wait_signal_frequency  # Seconds
        self.liveness_check_frequency = wait_signal_frequency * 4  # Seconds

    def _client_exists(self, application, id_):
        return application in self.clients and id_ in self.clients[application]

    def component_exists(self, application, component):
        return application in self.applications and component in self.applications[application].components

    def _generate_client_id(self):
        self._last_id += 1
        return str(self._last_id)

    def _add_event(self, descriptor: protocols.ClientDescriptor, connection: bool):
        event_descriptor = protocols.ClientDescriptor()
        event_descriptor.CopyFrom(descriptor)
        if connection:
            event_descriptor.event = protocols.ClientEventType.Value("CONNECTION")
        else:
            event_descriptor.event = protocols.ClientEventType.Value("DISCONNECTION")
        self.new_events.append(event_descriptor)

    def remove_client(self, application, id_) -> None:
        """
        This method is called when a client liveness check fails. The client is marked as DISCONNECTED, and the
        corresponding client disconnection event scheduled to be sent to the Adaptation Controller.
        :param application: Application the client belongs to.
        :param id_: Client ID
        """
        with self._lock:
            logging.info("Removing a client %s:%s" % (application, id_))
            if not self._client_exists(application, id_):
                logging.info("Client not found")
                return
            client = self.clients[application][id_]
            client.context.cancel()
            self._add_event(client.descriptor, False)
            client.status = ClientStatus.DISCONNECTED
            client.dependencies = {}
            logging.info(f"Client with ID {id_} removed successfully")

    def add_new_client(self, request: protocols.ClientDescriptor, context: grpc.ServicerContext) -> \
            Tuple[int, Optional[str], Optional[Client]]:
        """
        This method is called when a new client connects to the Client Controller. It checks whether the the client
        can be accepted, and if all the checks succeed, adds the client to the model. If the client has no ID assigned,
        assignes it a new ID.
        The checks may fail in the following cases:
            (1) The requested application or component name is not known
            (2) The requested ID does not exist
            (3) Client's IP is not allowed to use this application
        Each of these cases returns a specific error code
        :param request: ClientDescriptor of the client
        :param context: grpc.ServicerContext of the client
        :return: error code, client ID, a Client object
        """
        with self._lock:
            logging.info(f"Adding new client of type {request.application}:{request.type} with ip {request.ip}")
            # Check whether the declared client component exists
            if not self.component_exists(request.application, request.type):
                logging.info("Client type unknown")
                return mw_protocols.ClientResponseCode.Value("UNKNOWN_TYPE"), None, None

            # Check whether this client was allowed by Network Controller to use the app
            ue_policy = self.applications[request.application].components[request.type].policy
            if ue_policy == arch_pb.UEMPolicy.Value(UEManagementPolicy.WHITELIST.name):
                if not self.user_equipment.ue_registered(request.ip):
                    logging.info(f"Rejected a client due to unregistered IP: {request.ip}")
                    return mw_protocols.ClientResponseCode.Value("IP_NOT_KNOWN"), None, None
                ue = self.user_equipment.get_ue(request.ip)
                if f"{request.application}.{request.type}" not in ue.apps:
                    logging.info(f"Rejected a client due to absence of subscription to '{request.application}' app")
                    return mw_protocols.ClientResponseCode.Value("NO_APP_SUBSCRIPTION"), None, None
                request.imsi = ue.imsi

            if not request.hasID:
                # Assign a new client ID and add the client to the model
                id_: str = self._generate_client_id()
                client = Client(request, context)
                self.clients[request.application][id_] = client
                request.hasID = True
                request.id = id_
                self._add_event(client.descriptor, True)
                logging.info("New client added successfully. ID = %s" % id_)
                return mw_protocols.ClientResponseCode.Value("OK"), id_, client
            elif self._client_exists(request.application, request.id):
                # TODO: Two clients can connect with the same ID. This needs to be fixed
                client = self.clients[request.application][request.id]
                client.last_call = time.perf_counter()
                client.context = context
                if client.status == ClientStatus.DISCONNECTED:
                    client.status = ClientStatus.CONNECTED
                    self._add_event(client.descriptor, True)
                logging.info("Client with ID %s added successfully" % request.id)
                return mw_protocols.ClientResponseCode.Value("OK"), request.id, client
            else:
                logging.info("Client ID not found")
                return mw_protocols.ClientResponseCode.Value("ID_NOT_FOUND"), None, None

    def add_new_application(self, application_pb):
        app_name = application_pb.name
        self.clients[app_name] = {}
        self.applications[app_name] = application_pb


class ClientControllerExternal(mw_servicers.ClientControllerExternalServicer):
    """
    External interface for the clients of Avocado framework to connect to.
    """

    def __init__(self, client_model: ClientModel):
        self._client_model = client_model

    @staticmethod
    def _parse_ip_from_peer(peer: str) -> str:
        if not (peer.startswith("ipv4:") or peer.startswith("ipv6:")):
            raise Exception("Unknown peer format")
        items = peer.split(":")
        return items[1]

    def Connect(self, request: mw_protocols.ClientConnectRequest, context: grpc.ServicerContext) \
            -> Generator[mw_protocols.Command, None, None]:
        """
        Called by a client when it connects to the Avocado framework.
        Preserves a channel with client for the whole time when client is accessible.
        Note that the connection may break from client side.
        :return: stream of commands
        """
        def set_dependency_command():
            command = mw_protocols.Command()
            command.SET_DEPENDENCY.name = dependency
            command.SET_DEPENDENCY.ip = ip
            logging.info(f"Sent address of dependency {dependency} to client "
                         f"{request.application}:{request.clientType}:{id_}. Value = {ip}")
            client.last_call = time.perf_counter()
            return command

        # Get IP from request
        peer: str = context.peer()
        client_ip: str = self._parse_ip_from_peer(peer)

        # Prepare the client descriptor:
        client_descriptor = protocols.ClientDescriptor(
            application=request.application,
            type=request.clientType,
            ip=client_ip
        )
        if request.establishedConnection:
            client_descriptor.hasID = True
            client_descriptor.id = request.id

        rc, id_, client = self._client_model.add_new_client(client_descriptor, context)
        if rc == mw_protocols.ClientResponseCode.Value("OK"):
            if not request.establishedConnection:
                yield mw_protocols.Command(SET_ID=id_)
            else:
                for dependency, ip in client.dependencies.items():
                    yield set_dependency_command()
            while True:
                try:
                    while len(client.dependency_updates) == 0:
                        time.sleep(self._client_model.wait_signal_frequency)
                        client.last_call = time.perf_counter()
                        yield mw_protocols.Command(WAIT=0)
                    dependency, ip = client.dependency_updates.popitem()
                    yield set_dependency_command()
                except Exception:
                    context.cancel()
        else:
            yield mw_protocols.Command(ERROR=rc)
            return


class ClientControllerInternal(servicers.ClientControllerInternalServicer):
    """
    Internal interface through which the Adaptation Controller exchanges information with the Client Controller
    """

    def __init__(self, client_model: ClientModel):
        self._client_model = client_model

    def SetClientDependency(self, request: protocols.ClientDependencyDescription, context):
        """
        Informs that a client's dependecy has to be updated. Client Controller will send the new value of the
        dependency soon after this call.
        Called from PlanExecutor.execute_set_client_dependency().
        """
        if request.application not in self._client_model.clients or request.clientId not in \
                self._client_model.clients[request.application]:
            return protocols.ClientDependencyAck()
        client = self._client_model.clients[request.application][request.clientId]
        client.dependency_updates[request.dependencyAddress.name] = request.dependencyAddress.ip
        client.dependencies[request.dependencyAddress.name] = request.dependencyAddress.ip
        return protocols.ClientDependencyAck()

    def CloseApplicationChannels(self, request, context):
        """
        Informs Client Controller that this application should not be supported anymore. Client Controller will close
        the channels to all the clients that belong to this application, and will delete the corresponding redords.
        """
        if request.name not in self._client_model.clients:
            return protocols.CloseAck()
        for client in self._client_model.clients[request.name].values():
            client.context.cancel()
        del self._client_model.clients[request.name]
        del self._client_model.applications[request.name]
        logging.info("Application %s deleted. All client channels are closed" % request.name)
        return protocols.CloseAck()

    def GetNewClientEvents(self, request, context):
        """
        Returns the stream of all client connection and disconnection events that happened since the last call of this
        method
        """
        while len(self._client_model.new_events) > 0:
            next_event = self._client_model.new_events.pop()
            yield next_event

    def AddNewApplication(self, request, context):
        """
        Adds support for clients belonging to the given application
        """
        self._client_model.add_new_application(request)
        logging.info("Added support for clients belonging to %s application" % request.name)
        return protocols.ArchitectureAck()

    def AddNewUE(self, request, context):
        """
        Adds new user equiment to the model (or updates the old one if UE with the given IP is already present).
        """
        apps: List[str] = []
        for app in request.apps:
            apps.append(app)
        ue = UserEquipment(request.ip, request.imsi, "", apps)
        self._client_model.user_equipment.add_ue(ue)
        logging.info(f"UE added. IMSI: {request.imsi}. IP: {request.ip}. Apps: {request.apps}.")
        return protocols.UEAck()

    def RemoveUE(self, request, context):
        """
        Removes UE from the model. New client connections coming from this UE will not be accepted (for applications
        with WHITELIST UE management policy.
        """
        self._client_model.user_equipment.remove_ue(request.ip)
        logging.info(f"UE removed. IMSI: {request.imsi}. IP: {request.ip}.")
        return protocols.UEAck()


def start_client_controller(wait_signal_frequency=DEFAULT_WAIT_SIGNAL_FREQUENCY) -> None:
    """
    Starts both external and internal Client Controller interfaces. Creates the shared data structure for these
    interfaces. Runs the liveness check thread that checks whether any clients have been disconnected.
    This function does not return.
    May be invoked from a thread or as a separate process.
    """
    setup_logging()
    # A common data structure for external and internal interfaces:
    client_model = ClientModel(wait_signal_frequency)

    internal_server = start_grpc_server(
        servicer=ClientControllerInternal(client_model),
        adder=servicers.add_ClientControllerInternalServicer_to_server,
        host=CLIENT_CONTROLLER_HOST,
        port=CLIENT_CONTROLLER_PORT
    )

    external_server = start_grpc_server(
        servicer=ClientControllerExternal(client_model),
        adder=mw_servicers.add_ClientControllerExternalServicer_to_server,
        host=CLIENT_CONTROLLER_EXTERNAL_HOST,
        port=CLIENT_CONTROLLER_EXTERNAL_PORT,
        threads=MAX_CLIENTS
    )

    try:
        while True:
            time.sleep(client_model.liveness_check_frequency)
            timestamp = time.perf_counter()
            clients_to_delete = []
            for application in client_model.clients:
                for id_ in client_model.clients[application]:
                    client = client_model.clients[application][id_]
                    if client.last_call < timestamp - client_model.liveness_check_frequency and \
                            client.status == ClientStatus.CONNECTED:
                        clients_to_delete.append((application, id_))
            for app, id_ in clients_to_delete:
                logging.info("Cancelling the call for client (%s:%s)" % (app, id_))
                client_model.remove_client(app, id_)
    except KeyboardInterrupt:
        print("ClientController: ^C received, ending")
        external_server.stop(0)
        internal_server.stop(0)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Client controller")
    arg_parser.add_argument("-f", "--wait-signal-freq", type=float, default=5,
                            help="Frequency in seconds after which the WAIT command is send to a connected client.")
    args = arg_parser.parse_args()
    if not 0 < args.wait_signal_freq <= 60:
        print(f"Wait signal frequency {args.wait_signal_freq} seems to be nonsense.", file=sys.stderr)
        exit(1)

    start_client_controller(args.wait_signal_freq)
