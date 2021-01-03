import logging
from typing import List

from cloud_controller.client_controller.client_model import ClientModel
from cloud_controller.knowledge import knowledge_pb2_grpc as servicers, knowledge_pb2 as protocols
from cloud_controller.knowledge.user_equipment import UserEquipment


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