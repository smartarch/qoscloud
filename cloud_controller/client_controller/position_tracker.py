"""
Contains different implementations of ClientPositionTracker, a class that determines distances between clients
and data centers
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
from cloud_controller.client_controller.client import ClientStatus, Client
from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.knowledge.model import UnmanagedCompin, Node


class ClientPositionTracker(ABC):
    """
    Base class for all position tracker implementations.
    """

    @abstractmethod
    def update_client_positions(self, clients: Iterable[Client]) -> None:
        """
        Builds and returns a NetworkDistances object that contains the latencies between each client and each data
        center, as well as the distances between nodes.
        :param clients: Iterable of all the clients (managed compins) that the distances should be determined for.
        :param nodes: Iterable of all the nodes in the cluster.
        :return: Resulting NetworkDistances object.
        """
        pass


EUCLID_CLIENT_SPEED = 0.5  # Units per second


class EuclidClientPositionTracker(ClientPositionTracker):
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
        self._client_tracks: List[LineString] = []
        self._clients: Dict[str, Tuple[int, float, Point]] = {}
        self._current_client_track = 0
        self._last_time: float = perf_counter()
        self._current_time: float = perf_counter()
        with open(input_file, "r") as file:
            self._yaml_obj = yaml.load(file)

        for track_yaml in self._yaml_obj["tracks"]:
            assert len(track_yaml) > 0
            track_yaml.append(track_yaml[0])
            self._client_tracks.append(LineString(track_yaml))

    def _add_client(self, id_: str):
        self._clients[id_] = (self._current_client_track, self._current_time,
                              self._client_tracks[self._current_client_track].interpolate(0))
        self._current_client_track += 1
        self._current_client_track %= len(self._client_tracks)

    def _update_client_position(self, client_id: str, delta: float):
        track_id, start_time, pos = self._clients[client_id]
        track = self._client_tracks[track_id]
        distance = (self._current_time - start_time) * EUCLID_CLIENT_SPEED
        distance %= track.length
        pos = track.interpolate(distance)
        self._clients[client_id] = (track_id, start_time, pos)

    def update_client_positions(self, clients: Iterable[Client]) -> None:
        self._current_time = perf_counter()
        delta = self._current_time - self._last_time
        clients = [client for client in clients if client.status == ClientStatus.CONNECTED]
        for client in clients:
            # Calculate current client position
            if client.persistent_id not in self._clients:
                self._add_client(client.persistent_id)
            self._update_client_position(client.persistent_id, delta)
            _, _, point = self._clients[client.persistent_id]
            client.position_x, client.position_y = point.x, point.y
        self._last_time = self._current_time
