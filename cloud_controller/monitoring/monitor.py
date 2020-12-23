"""
Contains monitor class that is responsible for Monitoring phase of the mapek cycle, as well as FakeMonitor for testing.
"""
import logging
from typing import List, Dict

import grpc
from kubernetes import client

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT, \
    DEFAULT_HARDWARE_ID, DATACENTER_LABEL, HOSTNAME_LABEL, DEFAULT_UE_MANAGEMENT_POLICY, HARDWARE_ID_LABEL
from cloud_controller.architecture_pb2 import UEMPolicy
from cloud_controller.assessment import PUBLISHER_HOST, PUBLISHER_PORT
from cloud_controller.assessment.deploy_controller_pb2 import Empty
from cloud_controller.knowledge.knowledge_pb2_grpc import ClientControllerInternalStub
from cloud_controller.knowledge.model import Node, Namespace
from cloud_controller.knowledge.knowledge import Knowledge
import cloud_controller.knowledge.knowledge_pb2 as protocols
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployPublisherStub
from cloud_controller.middleware.helpers import connect_to_grpc_server


class Monitor:
    """
    Responsible for monitoring the following things:
        (1) State of the kubernetes cluster: nodes, pods, namespaces, labels, datacenters - through K8S API
        (2) Client connections and disconnections - through Client Controller
        (3) Submitted applications and applications requested for deletion - through  DeployPublisher
        (4) User Equipment changes (new and removed UE) - through Knowledge.UserEquipmentContainer

    For instances of adaptation controller that do not provide client support (e.g. pre-assessment), only
    update_cluster_state method should be called.
    """

    def __init__(self, knowledge: Knowledge = None):
        self.knowledge: Knowledge = knowledge
        self.client_controller = connect_to_grpc_server(ClientControllerInternalStub,
                                                        CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT)
        self.publisher = connect_to_grpc_server(DeployPublisherStub, PUBLISHER_HOST, PUBLISHER_PORT)
        self.postponed_apps = []
        logging.info("Cloud Monitor created.")

    def update_cluster_state(self) -> None:
        """
        Fetches information about the current state of the Kubernetes cluster and updates the corresponding data
        structures in the Knowledge.
        """
        core_api = client.CoreV1Api()
        extensions_api = client.AppsV1Api()
        api_nodes_response = core_api.list_node(watch=False)
        # Filter out names of untainted nodes:
        node_names = [item for item in api_nodes_response.items
                      if not item.spec.unschedulable and (not item.spec.taints or len(item.spec.taints) == 0)]
        api_pods_response = core_api.list_pod_for_all_namespaces()
        nodes: Dict[str, Node] = {}
        for item in node_names:
            node_name = item.metadata.labels.pop(HOSTNAME_LABEL)
            data_center = item.metadata.labels.pop(DATACENTER_LABEL, "")
            hardware_id = item.metadata.labels.pop(HARDWARE_ID_LABEL, DEFAULT_HARDWARE_ID)
            pod_list: List[str] = []
            node_ip: str = ""
            for pod in api_pods_response.items:
                if pod.spec.node_name == node_name:
                    pod_list.append(pod.metadata.name)
            for address in item.status.addresses:
                if address.type == "InternalIP":
                    node_ip: str = address.address
            assert node_ip != ""
            node = Node(node_name, hardware_id, node_ip, pod_list, data_center)
            node.labels = item.metadata.labels
            nodes[node_name] = node

        namespaces: Dict[str, Namespace] = {}
        api_ns_response = core_api.list_namespace()
        for item in api_ns_response.items:
            namespace_name = item.metadata.name
            api_deployments_response = extensions_api.list_namespaced_deployment(namespace=namespace_name)
            deployments: List[str] = []
            for deployment in api_deployments_response.items:
                deployments.append(deployment.metadata.name)
            namespaces[namespace_name] = Namespace(namespace_name, deployments, item.status.phase)

        self.knowledge.update_cluster_state(nodes, namespaces)

    def update_ue(self) -> None:
        """
        Updates User Equipment records on client controller according to the current state of the UserEquipmentContainer
        in Knowledge
        """
        try:
            for ue in self.knowledge.user_equipment.list_new_ue():
                self.client_controller.AddNewUE(ue)
            for ue in self.knowledge.user_equipment.list_removed_ue():
                self.client_controller.RemoveUE(ue)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the client controller. Proceeding without UE updates.")

    def update_clients(self) -> None:
        """
        Adds and removes the clients that were flagged for addition or removal by Client Controller
        """
        try:
            for client_pb in self.client_controller.GetNewClientEvents(protocols.ClientsRequest()):
                if client_pb.event == protocols.ClientEventType.Value("CONNECTION"):
                    assert client_pb.application in self.knowledge.applications
                    assert client_pb.type in self.knowledge.applications[client_pb.application].components
                    self.knowledge.add_client(client_pb.application, client_pb.type, client_pb.id, client_pb.ip)
                elif client_pb.event == protocols.ClientEventType.Value("DISCONNECTION"):
                    assert client_pb.application in self.knowledge.applications
                    assert client_pb.type in self.knowledge.applications[client_pb.application].components
                    assert self.knowledge.actual_state.compin_exists(client_pb.application, client_pb.type, client_pb.id)
                    self.knowledge.remove_client(client_pb.application, client_pb.type, client_pb.id)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the client controller. Proceeding without client updates.")

    def update_applications(self) -> None:
        """
        Gets the application state updates from the DeployPublisher (new and removed apps) and passes them to Knowledge
        """
        for app_pb in self.postponed_apps:
            self.knowledge.add_application(app_pb)
        self.postponed_apps = []
        recently_deleted_apps = []
        try:
            for request_pb in self.publisher.DownloadNewRemovals(Empty()):
                self.knowledge.delete_application(request_pb.name)
                recently_deleted_apps.append(request_pb.name)
            for app_pb in self.publisher.DownloadNewArchitectures(Empty()):
                for component in app_pb.components.values():
                    if component.policy == UEMPolicy.Value("DEFAULT"):
                        component.policy = UEMPolicy.Value(DEFAULT_UE_MANAGEMENT_POLICY.name)
                    if component.policy == UEMPolicy.Value("WHITELIST"):
                        for ip in component.whitelist:
                            self.knowledge.user_equipment.add_app_to_ue(ip, f"{app_pb.name}.{component.name}")

                if app_pb.name in recently_deleted_apps:
                    # Recently deleted apps need to wait until the next cycle
                    self.postponed_apps.append(app_pb)
                else:
                    self.knowledge.add_application(app_pb)
        except grpc._channel._Rendezvous:
            logging.info("Could not connect to the assessment controller. Proceeding without application updates.")

class FakeMonitor(Monitor):

    def update_cluster_state(self):
        nodes = {name: Node(name, DEFAULT_HARDWARE_ID, "", []) for name in ["N1", "N2", "N3", "N4", "N5"]}
        self.knowledge.update_cluster_state(nodes, {})
