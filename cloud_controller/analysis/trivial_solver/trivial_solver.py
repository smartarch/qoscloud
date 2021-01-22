import logging
import math
from typing import List, Dict, Set, Optional

import cloud_controller.knowledge.model as model
from cloud_controller.analyzer.predictor import Predictor
from cloud_controller.analysis.solver_options import SolverOptions
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.network_distances import NetworkDistances


_logger = logging.getLogger("TrivialSolver")


class TrivialSolver:
    """
    This solver searches for the assignment of components to nodes via trivial backtracking method:
    For every client dependency it searches the nearest node to that client and tries to add this dependency
    on that node. In case of failure it searches for other node.

    Internally it calls predictor for deciding whether given combination of components can be added
    to a certain node.

    Note that this class assumes that all the dependencies for one client have to be placed on one datacenter.
    """

    def __init__(self, knowledge: Knowledge, distances: NetworkDistances, predictor: Predictor,
                 options: SolverOptions = SolverOptions()):
        self._knowledge: Knowledge = knowledge
        self._distances: NetworkDistances = distances
        self.predictor: Predictor = predictor
        self._options: SolverOptions = options

        self._curr_state: model.CloudState = knowledge.actual_state
        self._clients: List[model.UnmanagedCompin] = _get_clients_from_state(self._curr_state)

    def find_assignment(self) -> Optional[model.CloudState]:
        assignment = _ComponentNodeAssignment(predictor=self.predictor,
                                              nodes=list(self._knowledge.nodes.values()),
                                              datacenters=self._knowledge.get_all_datacenters())
        for client in self._clients:
            if client.component.application.name in self._knowledge.applications:
                client_dependencies: List[model.Component] = client.component.get_all_dependencies_flat()
                for dependency in client_dependencies:
                    if not self._try_place_component(dependency, client, assignment):
                        return None

        return assignment.convert_to_cloud_state(self._knowledge)

    def find_assignment_longterm(self) -> Optional[model.CloudState]:
        return self.find_assignment()

    def _try_place_component(self, component: model.Component, client: model.UnmanagedCompin,
                             assignment: "_ComponentNodeAssignment") -> bool:
        """
        Tries to place the component somewhere in the cloud in a way that all the constraints are satisfied.
        :param component: Component to be placed.
        :param client: The client for which the component will be instantiated.
        :param assignment: This is output parameter.
        :return: False if no such placement exists.
        """
        nearest_node_iterator = _NearestNodeIterator(list(self._knowledge.nodes.values()), self._distances, client)
        nearest_node = next(nearest_node_iterator)
        while not assignment.can_component_be_added_to_node(component, nearest_node, client.chain_id):
            try:
                nearest_node = next(nearest_node_iterator)
            except StopIteration:
                return False
        assignment.place_component_on_node(component, nearest_node, client)
        return True

    def _construct_empty_state(self) -> model.CloudState:
        empty_state = model.CloudState()
        for application in self._knowledge.applications.values():
            empty_state.add_application(application)
        return empty_state


class _NearestNodeIterator:
    def __init__(self, nodes: List[model.Node], distances: NetworkDistances, client: model.UnmanagedCompin):
        self._nodes = nodes
        self._distances = distances
        self._client = client
        self._yielded_nodes: List[model.Node] = []

    def __iter__(self) -> "_NearestNodeIterator":
        self._yielded_nodes = []
        return self

    def __next__(self) -> model.Node:
        smallest_distance = math.inf
        nearest_node = None
        for node in self._nodes:
            if node in self._yielded_nodes:
                continue
            distance = self._distances.get_client_node_distance(self._client.id, node.name)
            if distance is None:
                raise Exception(f"No distance specified between client {self._client} and node {node}")
            if distance < smallest_distance:
                smallest_distance = distance
                nearest_node = node

        if nearest_node is not None:
            self._yielded_nodes.append(nearest_node)
            return nearest_node
        else:
            raise StopIteration


class _ComponentNodeAssignment:
    def __init__(self, predictor: Predictor, nodes: List[model.Node], datacenters: Set[str]):
        self._node_assignment: Dict[model.Node, List[model.ManagedCompin]] = {}
        # Map DatacenterID <-> ComponentID <-> [compins]
        self._datacenter_assignment: Dict[str, Dict[str, List[model.ManagedCompin]]] = {}
        self._predictor = predictor
        for node in nodes:
            self._node_assignment[node] = []
        for datacenter in datacenters:
            self._datacenter_assignment[datacenter] = {}

    def place_component_on_node(self, component: model.Component, node: model.Node, client: model.UnmanagedCompin)\
            -> None:
        compin = model.ManagedCompin(component=component,
                                     id_=_generate_compin_id(component, client, node),
                                     node=node.name,
                                     chain_id=client.chain_id)

        self._node_assignment[node].append(compin)

        if not self._datacenter_assignment[node.data_center].get(component.id):
            self._datacenter_assignment[node.data_center][component.id] = []
        self._datacenter_assignment[node.data_center][component.id].append(compin)

    def can_component_be_added_to_node(self, component: model.Component, node: model.Node, client_id: str) -> bool:
        try:
            if not component.is_deployable_on_node(node):
                return False

            if not self._is_previous_dependency_on_same_datacenter(component, node.data_center, client_id):
                return False

            components_on_node = self._get_component_count(node)
            if components_on_node.get(component) is None:
                components_on_node[component] = 0
            components_on_node[component] += 1
            if self._predictor.predict(node, components_on_node):
                return True
            else:
                _logger.debug(f"Predictor failed on node {node} with components {components_on_node}")
                return False
        except KeyError:
            return False

    def _is_previous_dependency_on_same_datacenter(self, component: model.Component, datacenter: str, client_id: str)\
            -> bool:
        previous_dependencies = _get_previous_dependent_components(component)
        for previous_dependency in previous_dependencies:
            dependency_instances = self._datacenter_assignment[datacenter][previous_dependency.id]
            client_previous_dependent_compins = \
                [compin for compin in dependency_instances if compin.chain_id == client_id]
            if len(client_previous_dependent_compins) == 0:
                return False
        return True

    def convert_to_cloud_state(self, knowledge: Knowledge) -> model.CloudState:
        """
        Involves also connecting all the dependencies between clients and components and also between
        components themselves.
        :param knowledge: Input parameter - references are not changed.
        :return:
        """
        def copy_unmanaged_compins_from_curr_state() -> List[model.UnmanagedCompin]:
            """Copies unmanaged compins from curr_state for all applications without any connections."""
            unmanaged_compins = []
            for curr_unmanaged_compin in knowledge.actual_state.list_all_unmanaged_compins():
                unmanaged_compins.append(
                    model.UnmanagedCompin(component=curr_unmanaged_compin.component, id_=curr_unmanaged_compin.id)
                )
            return unmanaged_compins

        def connect_chain(actual_compin: model.Compin, all_compins: List[model.ManagedCompin], chain_id: str) -> None:
            """Recursively connects whole chain of compins. """
            for dependent_component in actual_compin.component.dependencies:
                dependent_compin = self._find_compin(chain_id, dependent_component)
                actual_compin.set_dependency(dependent_compin)
                connect_chain(dependent_compin, all_compins, chain_id)

        def connect_all_compins(clients: List[model.UnmanagedCompin], compins: List[model.ManagedCompin]) -> None:
            for client in clients:
                connect_chain(client, compins, client.chain_id)

        unmanaged_compins = copy_unmanaged_compins_from_curr_state()
        unmanaged_compins = [
            compin for compin in unmanaged_compins if compin.component.application.name in knowledge.applications
        ]
        managed_compins = self._list_all_compins()
        connect_all_compins(unmanaged_compins, managed_compins)
        state = model.CloudState()
        for application in knowledge.applications.values():
            state.add_application(application)
        state.add_instances(unmanaged_compins + managed_compins)
        return state

    def _get_component_count(self, node: model.Node) -> Dict[model.Component, int]:
        component_count: Dict[model.Component, int] = {}
        for compin in self._node_assignment[node]:
            if component_count.get(compin.component) is None:
                component_count[compin.component] = 0
            component_count[compin.component] += 1
        return component_count

    def _find_compin(self, chain_id: str, component_type: model.Component) -> model.ManagedCompin:
        for compins in self._node_assignment.values():
            for compin in compins:
                if compin.chain_id == chain_id and compin.component.name == component_type.name:
                    return compin
        raise Exception("Should find compin")

    def _list_all_compins(self) -> List[model.ManagedCompin]:
        all_compins = []
        for compins in self._node_assignment.values():
            all_compins += compins
        return all_compins


def get_first_managed_component(application: model.Application) -> model.Component:
    """ Returns first component in dependency hierarchy. """
    # TODO: Are the checks before raising exception OK?
    unmanaged_components = list(application.list_unmanaged_components())
    if len(unmanaged_components) > 1:
        raise Exception("More than one client in one application not supported.")
    if len(unmanaged_components[0].dependencies) > 1:
        raise Exception("Client cannot have more than one dependency.")
    return unmanaged_components[0].dependencies[0]


def _get_previous_dependent_components(component: model.Component) -> List[model.Component]:
    """
    Returns all the components that have given component as their dependency.
    Implemented with DFS.
    """
    previous_dependencies: List[model.Component] = []
    first_component = get_first_managed_component(component.application)
    queue = [first_component]
    while len(queue) > 0:
        curr_component = queue.pop(0)
        if component in curr_component.dependencies:
            previous_dependencies.append(curr_component)
        queue += curr_component.dependencies
    return previous_dependencies


def _get_clients_from_state(state: model.CloudState) -> List[model.UnmanagedCompin]:
    clients = []
    for application in state.list_applications():
        clients += list(state.list_unmanaged_compins(application))
    return clients


def _generate_compin_id(component: model.Component, client: model.UnmanagedCompin, node: model.Node) -> str:
    return f"{component.name}-{client.chain_id}-{node.name}"

