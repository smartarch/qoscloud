import logging

import grpc

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT
from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.knowledge_pb2_grpc import ClientControllerInternalStub
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.monitor.monitor import Monitor


class ClientMonitor(Monitor):
    """
    Adds and removes the clients that were flagged for addition or removal by Client Controller
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self.client_controller = connect_to_grpc_server(ClientControllerInternalStub,
                                                        CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT)

    def monitor(self) -> None:
        try:
            for client_pb in self.client_controller.GetNewClientEvents(protocols.ClientsRequest()):
                assert client_pb.application in self._knowledge.applications
                assert client_pb.type in self._knowledge.applications[client_pb.application].components
                if client_pb.event == protocols.ClientEventType.Value("CONNECTION"):
                    self._knowledge.add_client(client_pb.application, client_pb.type, client_pb.id, client_pb.ip,
                                               client_pb.position_x, client_pb.position_y)
                elif client_pb.event == protocols.ClientEventType.Value("DISCONNECTION"):
                    assert self._knowledge.actual_state.compin_exists(client_pb.application, client_pb.type,
                                                                      client_pb.id)
                    self._knowledge.remove_client(client_pb.application, client_pb.type, client_pb.id)
                elif client_pb.event == protocols.ClientEventType.Value("LOCATION"):
                    assert self._knowledge.actual_state.compin_exists(client_pb.application, client_pb.type,
                                                                      client_pb.id)
                    self._knowledge.update_client_position(client_pb.application, client_pb.type, client_pb.id,
                                                           client_pb.position_x, client_pb.position_y)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the client controller. Proceeding without client updates.")