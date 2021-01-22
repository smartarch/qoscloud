"""
Contains various implementations of the predictor. All of the implementations located in the module are mocks that try
to reflect the properties of a real predictor implementation, but cannot ensure the real-time guarantees that the
real predictor aims for.
"""

import logging
from abc import ABC, abstractmethod
from time import perf_counter
from typing import Dict, Iterable

import yaml

import cloud_controller.knowledge.component
import cloud_controller.knowledge.instance
import cloud_controller.knowledge.cluster_model as model


class Predictor(ABC):
    """
    An interface for all predictor implementations.
    """

    def predict(self, node: model.Node, components_on_node: Dict[cloud_controller.knowledge.component.Component, int]) -> bool:
        return self.predict_(node.hardware_id, {component.id: count for component, count in components_on_node.items()})

    @abstractmethod
    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        """
        Answers the questions whether the given number of instances of the given components can run on a given node.
        "Can run" should mean that the runtime guarantees are kept, but for different mock implementations that can
        mean something else.
        :param node_id: Hardware ID of the node the question is asked for.
        :param components_on_node: A collection of component IDs, mapped to the number of these components.
        :return: True if the given components can run on the node, False otherwise
        """
        pass


class InstancesCountPredictor(Predictor):
    """
    Returns True for all combinations of components of a size less than _options.MAX_COMPINS_ON_NODE.
    """
    def __init__(self, count):
        self.max = count

    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        total_component_count = 0
        for count in components_on_node.values():
            total_component_count += count
        if total_component_count > self.max:
            return False
        else:
            return True


class PredictorObjectModel:

    def __init__(self, id_: str, mem: int, cpu: int, io: int):
        self.id = id_
        self.mem = mem
        self.cpu = cpu
        self.io = io


class StraightforwardPredictorModel(Predictor):
    """
    Uses a YAML configuration file that specifies the "hardware configurations" of nodes, and "hardware requirements"
    of components. These data are used to determine whether the components can run on a node. Both "Hardware configuration" and
    "Hardware requirements" here here means triplets of numbers (memory, CPU, IO). The triplets for components are
    summed together and are compared against the triplet for node, and True is returned only if the node has more
    resources than the sum of components' requirements.

    The configuration file has to have a following format:

        components:
          - id: "server"
            memory: 256
            cpu: 15
            io: 0
          - ...
        nodes:
          - id: "defaultclusternode"
            memory: 4096
            cpu: 200
            io: 10
          - ...
    """

    def __init__(self, filename: str):
        """
        :param filename: name of the file to load the predictor data from.
        """
        self._components: Dict[str, PredictorObjectModel] = {}
        self._nodes: Dict[str, PredictorObjectModel] = {}
        self.calls = 0
        self.time = 0
        with open(filename, "r") as file:
            self._yaml_obj = yaml.load(file)

        for component in self._yaml_obj["components"]:
            id_ = component["id"]
            self._components[id_] = PredictorObjectModel(id_, component["memory"], component["cpu"], component["io"])

        for node in self._yaml_obj["nodes"]:
            id_ = node["id"]
            self._nodes[id_] = PredictorObjectModel(id_, node["memory"], node["cpu"], node["io"])

    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        start_time = perf_counter()
        self.calls += 1
        node_model: PredictorObjectModel = self._nodes[node_id]
        utilization_model = PredictorObjectModel("", 0, 0, 0)
        for component_id, count in components_on_node.items():
            component_model: PredictorObjectModel = self._components[component_id]
            utilization_model.io += component_model.io * count
            utilization_model.mem += component_model.mem * count
            utilization_model.cpu += component_model.cpu * count
        self.time += perf_counter() - start_time
        return utilization_model.io <= node_model.io and \
               utilization_model.cpu <= node_model.cpu and \
               utilization_model.mem <= node_model.mem

    def print_node_utilization_stats(self, nodes: Iterable[model.Node], compins: Iterable[
        cloud_controller.knowledge.instance.ManagedCompin]) -> None:
        """
        Prints statistics of node utilization according to the loaded predictor configuration, i.e. which % of CPU,
        memory, and IO is used on each node for the given compin locations.
        :param nodes: Iterable of the nodes to determine utilization for.
        :param compins: All the managed compins running in the cloud.
        """
        utilizations: Dict[str, PredictorObjectModel] = {}
        for node in nodes:
            utilizations[node.name] = PredictorObjectModel(node.hardware_id, 0, 0, 0)
        for compin in compins:
            component = self._components[compin.component.id]
            node = utilizations[compin.node_name]
            node.io += component.io
            node.mem += component.mem
            node.cpu += component.cpu

        logging.info("Node utilization stats (CPU/memory/IO):")
        for name, utilization in utilizations.items():
            node = self._nodes[utilization.id]
            cpu = str(int(utilization.cpu / node.cpu * 100.0))
            mem = str(int(utilization.mem / node.mem * 100.0))
            io = str(int(utilization.io / node.io * 100.0))
            logging.info(name + ": " + cpu + "% / " + mem + "% / " + io + "%")
