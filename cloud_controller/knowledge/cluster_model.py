"""
Contains the classes that are used by the framework to model the state of the the cluster: representations
of nodes, datacenters, and namespaces.
"""

from enum import Enum
from typing import List, Dict

from cloud_controller import MONGOS_LABEL, MONGO_SHARD_LABEL


class Datacenter:
    """
    Represents a datacenter in the cluster. See constructor documentation for attributes.
    """

    def __init__(self, name: str, mongos_ip: str = None, mongos_node: str = None, mongo_shard_name: str = None,
                 nodes: str = None):
        """
        :param name: Name of the datacenter.
        :param mongos_ip: IP of the Mongos instance located in this datacenter.
        :param mongos_node: Name of the node on which the Mongos instance is located in this datacenter.
        :param mongo_shard_name: Name of the MongoDB shard located in this datacenter.
        :param nodes: Nodes of the datacenter (not used currently).
        """
        if nodes is None:
            nodes = []
        self.name: str = name
        self.mongos_ip: str = mongos_ip
        self.mongos_node: str = mongos_node
        self.mongo_shard_name = mongo_shard_name


class Node:
    """
    Represents a node in the cluster. See constructor documentation for attributes.
    """

    def __init__(self, name: str, hardware_id: str, ip: str, pods: List[str], data_center=""):
        """
        :param name: Name of the node.
        :param hardware_id: ID of the node's hardware configuration.
        :param ip: IP address of the node.
        :param pods: List of pods running on the node (determined in the last Monitoring phase).
        :param data_center: Datacenter this node belongs to.
        """
        self.name: str = name
        self.hardware_id: str = hardware_id
        self.ip: str = ip
        self.pods: List[str] = pods
        self.data_center = data_center
        self.labels: Dict[str, str] = {}  # Kubernetes labels of the node (key-value pairs)

    def __str__(self):
        return self.name


def check_datacenters(nodes: Dict[str, Node], datacenters: Dict[str, Datacenter]) -> bool:
    """
    Checks whether the collection of datacenters correctly reflects all the datacenters of the provided nodes.
    :param nodes: Nodes, mapped by name
    :param datacenters: Datacenters, mapped by name
    :return: True if datacenters correspond to the nodes, False otherwise.
    """
    for dc in datacenters.values():
        if dc.mongos_node not in nodes or MONGOS_LABEL not in nodes[dc.mongos_node].labels:
            return False
        if dc.mongo_shard_name not in nodes or MONGO_SHARD_LABEL not in nodes[dc.mongo_shard_name].labels:
            return False
    for node in nodes.values():
        if node.data_center not in datacenters:
            return False
    return True


def construct_datacenters(nodes: Dict[str, Node]) -> Dict[str, Datacenter]:
    """
    Builds the collection of datacenters based on the provided collection of nodes. Uses node labels for that purpose.
    :param nodes: Nodes, mapped by name
    :return: Datacenters, mapped by name
    """
    datacenters = {}
    for node in nodes.values():
        if node.data_center not in datacenters:
            datacenters[node.data_center] = Datacenter(node.data_center)
        if MONGOS_LABEL in node.labels:
            datacenters[node.data_center].mongos_ip = node.ip
            datacenters[node.data_center].mongos_node = node.name
        if MONGO_SHARD_LABEL in node.labels:
            datacenters[node.data_center].mongo_shard_name = node.name
    return datacenters


class NamespacePhase(Enum):
    ACTIVE = 1
    TERMINATING = 2


class Namespace:
    """
     Represents a namespace in the Kubernetes cloud. See constructor documentation for attributes.
    """

    def __init__(self, name: str, deployments: List[str], phase: str):
        """
        :param name: Name of the namespace
        :param deployments: List of the names of deployments currently present in the namespace (updated at the time of
                            last Monitoring phase
        :param phase: Namespace lifecycle phase per Kubernetes specification - either ACTIVE or TERMINATING
        """
        self.name: str = name
        self.deployments: List[str] = deployments
        assert phase in ['Active', 'Terminating']
        self.phase: NamespacePhase = NamespacePhase.ACTIVE if phase == 'Active' else NamespacePhase.TERMINATING


