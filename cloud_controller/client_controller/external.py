import logging
import time
from typing import Generator

import grpc

from cloud_controller.client_controller.client_model import ClientModel
from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.middleware import middleware_pb2_grpc as mw_servicers, middleware_pb2 as mw_protocols


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
            client_descriptor.persistent_id = request.id

        rc, id_, client = self._client_model.assign_client(client_descriptor, context)
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