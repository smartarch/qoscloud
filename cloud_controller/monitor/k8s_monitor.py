from typing import Dict, List

from kubernetes import config, client

from cloud_controller import PRODUCTION_KUBECONFIG, HOSTNAME_LABEL, DATACENTER_LABEL, HARDWARE_ID_LABEL, \
    DEFAULT_HARDWARE_ID
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.cluster_model import Node, Namespace
from cloud_controller.monitor.monitor import Monitor
from cloud_controller.task_executor.execution_context import call_k8s_api


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