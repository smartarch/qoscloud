"""
Contains different implementations of NetworkTopology, a class that determines distances between clients, data centers,
and nodes.
"""
import math
import random
from abc import ABC, abstractmethod
from subprocess import run
from time import perf_counter

import logging
from shapely.geometry import Point, LineString

import yaml
from typing import Dict, Iterable, FrozenSet, Tuple, List

from cloud_controller import DEFAULT_EUCLID_TOPOLOGY_CONFIG, USE_VIRTUAL_NETWORK_CONTROLLER
from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.knowledge.model import UnmanagedCompin, Node


class NetworkTopology(ABC):
    """
    Base class for all network topology implementations.
    """

    @abstractmethod
    def get_network_distances(self, clients: Iterable[UnmanagedCompin], nodes: Iterable[Node]) -> NetworkDistances:
        """
        Builds and returns a NetworkDistances object that contains the latencies between each client and each data
        center, as well as the distances between nodes.
        :param clients: Iterable of all the clients (managed compins) that the distances should be determined for.
        :param nodes: Iterable of all the nodes in the cluster.
        :return: Resulting NetworkDistances object.
        """
        pass


EUCLID_CLIENT_SPEED = 0.5  # Units per second


class StaticNetworkTopology(NetworkTopology):

    def __init__(self, input_file=DEFAULT_EUCLID_TOPOLOGY_CONFIG):
        self._datacenters: Dict[str, Point] = {}
        self._dc_positions: List[Point] = []
        self._current_dc_position: int = 0
        with open(input_file, "r") as file:
            self._yaml_obj = yaml.load(file)
        for dc in self._yaml_obj["datacenters"]:
            self._dc_positions.append(Point(dc[0], dc[1]))

        assert len(self._dc_positions) > 0

    def _add_datacenter(self, name: str):
        self._datacenters[name] = (self._dc_positions[self._current_dc_position])
        self._current_dc_position += 1
        self._current_dc_position %= len(self._dc_positions)

    def _calculate_distance(self, client: UnmanagedCompin, datacenter: str, network_distances: NetworkDistances) -> None:
        point = Point(client.position_x, client.position_y)
        distance = point.distance(self._datacenters[datacenter])
        network_distances.add_client_datacenter_distance(client.id, datacenter, distance)

    def get_network_distances(self, clients: Iterable[UnmanagedCompin], nodes: Iterable[Node]) -> NetworkDistances:
        nodes = list(nodes)
        for node in nodes:
            if node.data_center not in self._datacenters:
                self._add_datacenter(node.data_center)
        distances = NetworkDistances()
        for client in clients:
            for dc in self._datacenters:
                self._calculate_distance(client, dc, distances)
        return distances


class EuclidNetworkTopology(NetworkTopology):
    """
    This implementation loads a configuration YAML file that defines the positions of the datacenters and the client
    movement paths, and calculates the distances according to the current client and datacenter positions on a plane.
    Each client moves along its track with a speed set in EUCLID_CLIENT_SPEED. The time used to calculate the movement
    is a real (not simulated) wall time.

    The format of a YAML configuration file is the following:

        datacenters:
          - [10, 90]
          - [90, 90]
          - ...
        tracks:
          - [ [40, 20], [60, 20], [80, 30], [90, 50], [80, 70], [60, 80], [40, 80], [20, 70], [10, 50], [20, 30] ]
          - [ [40, 20], [20, 30], [10, 50], [20, 70], [40, 80], [60, 80], [80, 70], [90, 50], [80, 30], [60, 20] ]
          - ...

    The datacenter positions and client tracks are assigned to real data centers and clients cyclically. That means,
    that there can be more clients than the tracks defined, in this case several clients will be put on the same track.
    The client is considered to start its movement when it is first added to the topology data structure.

    Notable attributes:
        _datacenters:   Maps data center name to its position.
        _clients:       Maps client ID to a tuple: (number of the client's track,
                        time when the client started its movement, last calculated position of the client)
    """

    def __init__(self, input_file=DEFAULT_EUCLID_TOPOLOGY_CONFIG):
        self._datacenters: Dict[str, Point] = {}
        self._dc_positions: List[Point] = []
        self._current_dc_position: int = 0
        self._client_tracks: List[LineString] = []
        self._clients: Dict[str, Tuple[int, float, Point]] = {}
        self._current_client_track = 0
        self._last_time: float = perf_counter()
        self._current_time: float = perf_counter()
        self._dc_cfg: Dict[str, Tuple[str, str]] = {}
        self._net_control = USE_VIRTUAL_NETWORK_CONTROLLER
        self.overlay_network = "10.96.0.0/12"
        with open(input_file, "r") as file:
            self._yaml_obj = yaml.load(file)

        for dc in self._yaml_obj["datacenters"]:
            self._dc_positions.append(Point(dc[0], dc[1]))

        for track_yaml in self._yaml_obj["tracks"]:
            assert len(track_yaml) > 0
            track_yaml.append(track_yaml[0])
            self._client_tracks.append(LineString(track_yaml))

        if self._net_control:
            if "dc_config" in self._yaml_obj:
                for cfg in self._yaml_obj["dc_config"]:
                    self._dc_cfg[cfg['name']] = (cfg['abbr'], cfg['router'])
            else:
                logging.warning("dc_config not found in network topology. Network Controller will not work.")
                self._net_control = False

        assert len(self._dc_positions) > 0 and len(self._client_tracks) > 0

    def _add_client(self, id_: str):
        self._clients[id_] = (self._current_client_track, self._current_time,
                              self._client_tracks[self._current_client_track].interpolate(0))
        self._current_client_track += 1
        self._current_client_track %= len(self._client_tracks)

    def _add_datacenter(self, name: str):
        self._datacenters[name] = (self._dc_positions[self._current_dc_position])
        self._current_dc_position += 1
        self._current_dc_position %= len(self._dc_positions)

    def _calculate_distance(self, client_id: str, datacenter: str, network_distances: NetworkDistances) -> None:
        _, _, point = self._clients[client_id]
        distance = point.distance(self._datacenters[datacenter])
        network_distances.add_client_datacenter_distance(client_id, datacenter, distance)
        if self._net_control:
            run(['ip', 'netns', 'exec', f"cl{client_id}-rt", 'tc', 'qdisc', 'change', 'dev',
                 f"cl{client_id}-{self._dc_cfg[datacenter][0]}", 'root', 'netem', 'delay',
                 str(math.ceil(distance)) + 'ms'])
            logging.info(f"Latency between {client_id} and {datacenter} was set to {math.ceil(distance)}.")

    def _update_client_position(self, client_id: str, delta: float):
        track_id, start_time, pos = self._clients[client_id]
        track = self._client_tracks[track_id]
        distance = (self._current_time - start_time) * EUCLID_CLIENT_SPEED
        distance %= track.length
        pos = track.interpolate(distance)
        self._clients[client_id] = (track_id, start_time, pos)

    def get_network_distances(self, clients: Iterable[UnmanagedCompin], nodes: Iterable[Node]) -> NetworkDistances:
        self._current_time = perf_counter()
        delta = self._current_time - self._last_time
        network_distances = NetworkDistances()
        clients = list(clients)
        nodes = list(nodes)
        for node in nodes:
            if node.data_center not in self._datacenters:
                self._add_datacenter(node.data_center)
        for compin in clients:
            # Calculate current client position
            if compin.id not in self._clients:
                self._add_client(compin.id)
            self._update_client_position(compin.id, delta)
            # Add client-datacenter distances
            for datacenter in self._datacenters:
                self._calculate_distance(compin.id, datacenter, network_distances)
            # Add client-node distances
            for node in nodes:
                distance = network_distances.get_client_datacenter_distance(compin.id, node.data_center)
                network_distances.add_client_node_distance(compin.id, node.name, distance)
            if self._net_control:
                closest_dc = None
                min_distance = 1000000000
                for datacenter in self._datacenters:
                    distance = network_distances.get_client_datacenter_distance(compin.id, datacenter)
                    if distance < min_distance:
                        min_distance = distance
                        closest_dc = datacenter
                run(['ip', 'netns', 'exec', f"cl{compin.id}-rt", 'ip', 'route', 'change', self.overlay_network, 'via',
                     self._dc_cfg[closest_dc][1]])
                logging.info(f"Client {compin.id} is now connected to the cloud through {closest_dc}.")

        self._last_time = self._current_time
        return network_distances


MIN_LATENCY = 1
MAX_LATENCY = 60
MAX_CLIENT_SPEED = 1.5  # units per second


class RandomNetworkTopology(NetworkTopology):
    """
    This implementation generates the distances randomly, and then randomly changes them with each call of
    get_network_distances. The change of the distance depends on the time that passed since the last call, and the
    value of MAX_CLIENT_SPEED (i.e. the difference between the last distance and the current distance cannot be larger
    then time_passed * MAX_CLIENT_SPEED).
    """

    def __init__(self):
        self._ping_tables: Dict[str, Dict[str, float]] = {}
        self._node_distances: Dict[FrozenSet[str], float] = {}
        self._last_time: float = perf_counter()
        self._current_time: float = perf_counter()

    def get_network_distances(self, clients: Iterable[UnmanagedCompin], nodes: Iterable[Node]) -> NetworkDistances:
        self._current_time = perf_counter()
        network_distances = NetworkDistances()
        clients = list(clients)
        nodes = list(nodes)
        # Add client-node distances
        for compin in clients:
            ping_table = self._get_client_ping_table(compin.id, nodes)
            for node_name, ping in ping_table.items():
                network_distances.add_client_node_distance(compin.id, node_name, ping)
        # Add node-node distances
        for node_1 in nodes:
            for node_2 in nodes:
                distance = self._get_node_distance(node_1.name, node_2.name)
                network_distances.add_distance_between_nodes(node_1.name, node_2.name, distance)
        # Add client-datacenter distances
        for compin in clients:
            for node in nodes:
                distance = network_distances.get_client_node_distance(compin.id, node.name)
                network_distances.add_client_datacenter_distance(compin.id, node.data_center, distance)
        self._last_time = self._current_time
        return network_distances

    def _get_node_distance(self, node_name_1: str, node_name_2: str) -> float:
        if node_name_1 == node_name_2:
            return 0
        if (node_name_1, node_name_2) not in self._node_distances:
            self._node_distances[frozenset((node_name_1, node_name_2))] = random.randint(MIN_LATENCY, MAX_LATENCY)
        return self._node_distances[frozenset((node_name_1, node_name_2))]

    def _get_client_ping_table(self, client_id: str, nodes: Iterable[Node]) -> Dict[str, float]:
        if client_id in self._ping_tables:
            for node_name in self._ping_tables[client_id]:
                delta = random.uniform(-MAX_CLIENT_SPEED, MAX_CLIENT_SPEED) * (self._last_time - self._current_time)
                ping = self._ping_tables[client_id][node_name] + delta
                ping = max(MIN_LATENCY, min(MAX_LATENCY, ping))
                self._ping_tables[client_id][node_name] = ping
        else:
            ping_table: Dict[str, int] = {}
            for node in nodes:
                ping_table[node.name] = random.randint(MIN_LATENCY, MAX_LATENCY)
            self._ping_tables[client_id] = ping_table
        return self._ping_tables[client_id]
