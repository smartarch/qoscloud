import logging

import grpc

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.knowledge_pb2_grpc import ClientControllerInternalStub
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.monitor.monitor import Monitor


class UEMonitor(Monitor):
    """
    Updates User Equipment records on client controller according to the current state of the UserEquipmentContainer
    in Knowledge
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self.client_controller = connect_to_grpc_server(ClientControllerInternalStub,
                                                        CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT)

    def monitor(self) -> None:
        try:
            for ue in self._knowledge.user_equipment.list_new_ue():
                self.client_controller.AddNewUE(ue)
            for ue in self._knowledge.user_equipment.list_removed_ue():
                self.client_controller.RemoveUE(ue)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the client controller. Proceeding without UE updates.")