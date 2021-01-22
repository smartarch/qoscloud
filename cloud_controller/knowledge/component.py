import hashlib
from enum import Enum
from io import StringIO
from typing import Dict, List, Optional

import yaml

from cloud_controller import architecture_pb2 as protocols
from cloud_controller.knowledge.cluster_model import Node
from cloud_controller.knowledge.probe import Probe


class ComponentType(Enum):
    MANAGED = 1
    UNMANAGED = 2


class Statefulness(Enum):
    NONE = 1
    CLIENT = 2
    COMPONENT = 3
    MONGO = 4


class ComponentCardinality(Enum):
    MULTIPLE = 1
    SINGLE = 2


class Component:
    """
    Represents a single component from application descriptor. See constructor documentation for attributes.
    """

    type_map: Dict = {
        protocols.ComponentType.Value("MANAGED"): ComponentType.MANAGED,
        protocols.ComponentType.Value("UNMANAGED"): ComponentType.UNMANAGED,
    }

    statefulness_map: Dict = {
        protocols.Statefulness.Value("NONE"): Statefulness.NONE,
        protocols.Statefulness.Value("CLIENT"): Statefulness.CLIENT,
        protocols.Statefulness.Value("COMPONENT"): Statefulness.COMPONENT
    }

    def __init__(self, application: "Application", name: str, id_: str, type_: ComponentType,
                 container_spec: str = None, dependencies: List["Component"] = None, probes = None,
                 statefulness: Statefulness = Statefulness.NONE,
                 cardinality: ComponentCardinality = ComponentCardinality.MULTIPLE):
        """
        :param application: Application this component belongs to.
        :param name: Name of the component.
        :param id_: Unique ID of the component (needed for identification of the component with the predictor).
        :param type_: MANAGED or UNMANAGED.
        :param container_spec: YAML specification of a template for the corresponding Kubernetes deployment, converted
                                to string. Valid only for MANAGED components.
        :param dependencies: List of other components this component depends on.
        :param probes: List of probes defined on this component (their string IDs).
        :param statefulness: Statefulness type of the component. Either NONE or MONGO.
        """
        if probes is None:
            probes = []
        assert application is not None
        self._application: "Application" = application
        self._name: str = name
        self._container_spec: str = container_spec
        self._id = hashlib.md5(bytes("", 'utf-8'))
        self._type: ComponentType = type_
        self._probes: List[Probe] = probes
        self._statefulness = statefulness
        self.collection_sharded: bool = False
        self._cardinality = cardinality
        self._dependencies: Dict[str, Component] = {}
        # The following attributes contains all the labels that were found in the Component's deployment descriptor
        # that may influence which nodes can be selected for the deployment of instances of this component.
        self._node_selector_labels: Dict[str, str] = self._parse_node_selector_labels()
        if dependencies is not None:
            for dependency in dependencies:
                self._dependencies[dependency.name] = dependency

    def __str__(self) -> str:
        return f"Component(id={self.id}, type={self.type}, dependencies=[{[str(dep) for dep in self._dependencies]}])"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Component):
            return self.name == other.name and self.id == other.id
        else:
            return False

    @property
    def cardinality(self) -> ComponentCardinality:
        return self._cardinality

    @property
    def full_name(self):
        return f"{self.application.name}_{self.name}"

    @property
    def statefulness(self) -> Statefulness:
        return self._statefulness

    @property
    def is_stateful(self) -> bool:
        return self._statefulness is not Statefulness.NONE

    @property
    def application(self) -> "Application":
        return self._application

    @property
    def name(self) -> str:
        return self._name

    @property
    def container_spec(self) -> str:
        return self._container_spec

    @property
    def id(self) -> str:
        return self._id.hexdigest()

    @property
    def type(self) -> ComponentType:
        return self._type

    @property
    def dependencies(self) -> List["Component"]:
        return list(self._dependencies.values())

    @property
    def probes(self) -> List["Probe"]:
        return self._probes

    def add_probe(self, probe: "Probe"):
        self._probes.append(probe)
        self._id.update(bytes(probe.alias, 'utf-8'))

    def _parse_node_selector_labels(self) -> Dict[str, str]:
        if self._container_spec == "" or self._container_spec is None:
            return {}
        container_yaml = yaml.load(StringIO(self._container_spec))
        try:
            node_selector_spec: Dict[str, str] = container_yaml["spec"]["template"]["spec"]["nodeSelector"]
        except KeyError:
            return {}

        return node_selector_spec

    def is_deployable_on_node(self, node: Node) -> bool:
        """
        :return: True, if the Kubernetes deployment specification of this component does not contain any node selector
                    labels that are not present on the given node (and thus, it can be deployed on that node), False
                    otherwise.
        """
        for selector_key, selector_value in self._node_selector_labels.items():
            node_label_value = node.labels.get(selector_key)
            if node_label_value is None or node_label_value != selector_value:
                return False
        return True

    def add_dependency(self, component: "Component"):
        self._dependencies[component.name] = component

    def get_dependency(self, dependency: str) -> Optional["Component"]:
        if dependency in self._dependencies:
            return self._dependencies[dependency]
        else:
            return None

    @staticmethod
    def init_from_pb(application: "Application", component_pb: protocols.Component) -> "Component":
        """
        Creates a component object from protobuf representation.
        :param application: Application the component should belong to.
        :param component_pb: Protobuf representation of the component.
        """
        probes: List[Probe] = []
        component = Component(
            application=application,
            name=component_pb.name,
            id_=component_pb.name,
            type_=Component.type_map[component_pb.type],
            container_spec=component_pb.deployment,
            probes=probes,
            statefulness=Component.statefulness_map[component_pb.statefulness],
            cardinality=ComponentCardinality(component_pb.cardinality + 1)
        )
        for probe in component_pb.probes:
            component.add_probe(Probe.init_from_pb_direct(probe, component))

        return component

    def get_all_dependencies_in_levels(self) -> List[List["Component"]]:
        """
        :return: all the transitive dependencies of the component. The i-th inner list corresponds to i-th level of
        dependency ie. if A is dependent on B and on C, then the returned list would look like this:
        [[A], [B,C]]. More specifically like this: [[B,C]], because the component on which this method is called, is
        not part of the output.
        """
        dep_list = []
        dependencies = list(self._dependencies.values())
        dep_list.append(dependencies)

        while len(dependencies) > 0:
            next_deps = []
            for dep in dependencies:
                next_deps += dep.dependencies
            if len(next_deps) > 0:
                dep_list.append(next_deps)
            dependencies = next_deps

        return dep_list

    def get_all_dependencies_flat(self) -> List["Component"]:
        """
        :return: A list of all transitive dependencies of this component.
        """
        deps_levels = self.get_all_dependencies_in_levels()
        return [item for sublist in deps_levels for item in sublist]

    def get_dependency_chains(self) -> List[List["Component"]]:
        """
        Does BFS all dependencies and returns them as list of lists ie. chains.
        The self component is not contained in the returned chains.
        """
        chains = self._generate_depencency_chains(stack=[], chains=[[]])
        chains.remove([])
        for chain in chains:
            chain.remove(self)
        return chains

    def _generate_depencency_chains(self, stack: List["Component"], chains: List[List["Component"]])\
            -> List[List["Component"]]:
        stack.append(self)
        if len(self.dependencies) == 0:
            chains.append(stack.copy())

        for dependency in self.dependencies:
            dependency._generate_depencency_chains(stack, chains)

        stack.remove(self)
        return chains