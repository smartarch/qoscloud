import logging

import grpc

from cloud_controller import DEFAULT_UE_MANAGEMENT_POLICY
from cloud_controller.architecture_pb2 import UEMPolicy
from cloud_controller.assessment import PUBLISHER_HOST, PUBLISHER_PORT
from cloud_controller.assessment.deploy_controller_pb2 import Empty
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployPublisherStub
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.monitor.monitor import Monitor


class ApplicationMonitor(Monitor):
    """
    Gets the application state updates from the DeployPublisher (new and removed apps) and passes them to Knowledge
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self.publisher = connect_to_grpc_server(DeployPublisherStub, PUBLISHER_HOST, PUBLISHER_PORT)
        self.postponed_apps = []

    def monitor(self) -> None:
        for app_pb in self.postponed_apps:
            self._knowledge.add_application(app_pb)
        while not self._knowledge.delete_apps.empty():
            self._knowledge.delete_application(self._knowledge.delete_apps.get_nowait())
        while not self._knowledge.new_apps.empty():
            self._knowledge.add_application(self._knowledge.new_apps.get_nowait())
        self.postponed_apps = []
        recently_deleted_apps = []
        try:
            for request_pb in self.publisher.DownloadNewRemovals(Empty()):
                self._knowledge.delete_application(request_pb.name)
                recently_deleted_apps.append(request_pb.name)
            for app_pb in self.publisher.DownloadNewArchitectures(Empty()):
                for component in app_pb.components.values():
                    if component.policy == UEMPolicy.Value("DEFAULT"):
                        component.policy = UEMPolicy.Value(DEFAULT_UE_MANAGEMENT_POLICY.name)
                    if component.policy == UEMPolicy.Value("WHITELIST"):
                        for ip in component.whitelist:
                            self._knowledge.user_equipment.add_app_to_ue(ip, f"{app_pb.name}.{component.name}")

                if app_pb.name in recently_deleted_apps:
                    # Recently deleted apps need to wait until the next cycle
                    self.postponed_apps.append(app_pb)
                else:
                    self._knowledge.add_application(app_pb)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the assessment controller. Proceeding without application updates.")