"""
Contains monitor class that is responsible for Monitoring phase of the mapek cycle, as well as FakeMonitor for testing.
The rest of the  monitoring classes used in the framework are located in their respective modules
"""
import logging
from abc import abstractmethod
from typing import List, Dict

import grpc
from kubernetes import client, config

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT, \
    DEFAULT_HARDWARE_ID, DATACENTER_LABEL, HOSTNAME_LABEL, DEFAULT_UE_MANAGEMENT_POLICY, HARDWARE_ID_LABEL, \
    PRODUCTION_KUBECONFIG
from cloud_controller.architecture_pb2 import UEMPolicy
from cloud_controller.assessment import PUBLISHER_HOST, PUBLISHER_PORT
from cloud_controller.assessment.deploy_controller_pb2 import Empty
from cloud_controller.knowledge.knowledge_pb2_grpc import ClientControllerInternalStub
from cloud_controller.knowledge.model import Node, Namespace
from cloud_controller.knowledge.knowledge import Knowledge
import cloud_controller.knowledge.knowledge_pb2 as protocols
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployPublisherStub
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.task_executor.execution_context import call_k8s_api


class Monitor:
    """
    Carries out the monitoring functionality of this monitor.
    Is called every monitoring phase
    """

    def __init__(self, knowledge: Knowledge):
        self._knowledge: Knowledge = knowledge

    @abstractmethod
    def monitor(self) -> None:
        pass

    def set_knowledge(self, knowledge: Knowledge):
        if self._knowledge is None:
            self._knowledge = knowledge



class TopLevelMonitor(Monitor):
    """
    In the default setup, responsible for monitoring the following things:
        (1) State of the kubernetes cluster: nodes, pods, namespaces, labels, datacenters - through K8S API
        (2) Client connections and disconnections - through Client Controller
        (3) Submitted applications and applications requested for deletion - through  DeployPublisher
        (4) User Equipment changes (new and removed UE) - through Knowledge.UserEquipmentContainer

    For instances of adaptation controller that do not provide client support (e.g. pre-assessment), only
    Kubernetes monitor should be present.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self._monitors: List[Monitor] = []
        logging.info("Cloud Monitor created.")

    def add_monitor(self, monitor: Monitor):
        monitor.set_knowledge(self._knowledge)
        self._monitors.append(monitor)

    def monitor(self):
        for monitor in self._monitors:
            monitor.monitor()


class KubernetesMonitor(Monitor):
    """
    Fetches information about the current state of the Kubernetes cluster and updates the corresponding data
    structures in the Knowledge.
    """

    def __init__(self, knowledge: Knowledge, kubeconfig = PRODUCTION_KUBECONFIG):
        super().__init__(knowledge)
        config.load_kube_config(config_file=kubeconfig)
        self.core_api = client.CoreV1Api()
        self.extensions_api = client.AppsV1Api()

    def monitor(self) -> None:
        api_nodes_response = call_k8s_api(self.core_api.list_node, watch=False)
        # Filter out names of untainted nodes:
        node_names = [item for item in api_nodes_response.items
                      if not item.spec.unschedulable and (not item.spec.taints or len(item.spec.taints) == 0)]
        api_pods_response = call_k8s_api(self.core_api.list_pod_for_all_namespaces)
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
        api_ns_response = call_k8s_api(self.core_api.list_namespace)
        for item in api_ns_response.items:
            namespace_name = item.metadata.name
            api_deployments_response = call_k8s_api(self.extensions_api.list_namespaced_deployment, namespace=namespace_name)
            deployments: List[str] = []
            for deployment in api_deployments_response.items:
                deployments.append(deployment.metadata.name)
            namespaces[namespace_name] = Namespace(namespace_name, deployments, item.status.phase)

        self._knowledge.update_cluster_state(nodes, namespaces)


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


class FakeMonitor(Monitor):

    def monitor(self):
        nodes = {name: Node(name, DEFAULT_HARDWARE_ID, "", []) for name in ["N1", "N2", "N3", "N4", "N5"]}
        self._knowledge.update_cluster_state(nodes, {})
