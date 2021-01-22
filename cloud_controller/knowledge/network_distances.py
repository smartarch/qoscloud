"""
Contains NetworkDistances class that holds the distances between the nodes, clients, and datacenters.
"""
from typing import Dict, FrozenSet, Optional


class NetworkDistances:
    """
    Holds the map of the distances between the nodes, clients, and datacenters.
    """

    def __init__(self):
        self._client_datacenter_distances: Dict[FrozenSet[str], float] = {}
        self._client_node_distances: Dict[FrozenSet[str], float] = {}
        self._distances_between_nodes: Dict[FrozenSet[str], float] = {}

    def add_distance_between_nodes(self, node_a: str, node_b: str, distance: float) -> None:
        self._distances_between_nodes[frozenset({node_a, node_b})] = distance

    def get_distance_between_nodes(self, node_a: str, node_b: str) -> Optional[float]:
        return self._distances_between_nodes.get(frozenset({node_a, node_b}))

    def add_client_datacenter_distance(self, client: str, datacenter: str, distance: float) -> None:
        self._client_datacenter_distances[frozenset({client, datacenter})] = distance

    def get_client_datacenter_distance(self, client: str, datacenter: str) -> Optional[float]:
        return self._client_datacenter_distances.get(frozenset({client, datacenter}))

    def add_client_node_distance(self, client: str, node: str, distance: float) -> None:
        self._client_node_distances[frozenset({client, node})] = distance

    def get_client_node_distance(self, client: str, node: str) -> Optional[float]:
        return self._client_node_distances.get(frozenset({client, node}))

    def merge(self, more_distances: "NetworkDistances"):
        self._client_datacenter_distances = \
            {**self._client_datacenter_distances, **more_distances._client_datacenter_distances}
        self._client_node_distances = \
            {**self._client_node_distances, **more_distances._client_node_distances}
        self._distances_between_nodes = \
            {**self._distances_between_nodes, **more_distances._distances_between_nodes}
