"""
Contains NetworkDistances class that holds the distances between the nodes, clients, and datacenters.
"""
from typing import Dict, FrozenSet, Optional, List


class NetworkDistances:
    """
    Holds the map of the distances between the nodes, clients, and datacenters.
    """

    def __init__(self):
        self._client_datacenter_distances: Dict[FrozenSet[str], float] = {}
        self._client_node_distances: Dict[FrozenSet[str], float] = {}
        self._distances_between_nodes: Dict[FrozenSet[str], float] = {}

    def get_closest_datacenter(self, client: str, datacenters: List[str]) -> str:
        min_distance = self._client_datacenter_distances.get(frozenset({client, datacenters[0]}))
        min_dc = datacenters[0]
        for dc in datacenters:
            distance = self._client_datacenter_distances.get(frozenset({client, datacenters[0]}))
            if distance < min_distance:
                min_distance = distance
                min_dc = dc
        return min_dc

    def add_distance_between_nodes(self, node_a: str, node_b: str, distance: float) -> None:
        self._distances_between_nodes[frozenset({node_a, node_b})] = distance

    def add_client_datacenter_distance(self, client: str, datacenter: str, distance: float) -> None:
        self._client_datacenter_distances[frozenset({client, datacenter})] = distance

    def get_client_datacenter_distance(self, client: str, datacenter: str) -> Optional[float]:
        return self._client_datacenter_distances.get(frozenset({client, datacenter}))

    def add_client_node_distance(self, client: str, node: str, distance: float) -> None:
        self._client_node_distances[frozenset({client, node})] = distance

    def get_client_node_distance(self, client: str, node: str) -> Optional[float]:
        return self._client_node_distances.get(frozenset({client, node}))
