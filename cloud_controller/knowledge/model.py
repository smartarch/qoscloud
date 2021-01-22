import hashlib
from enum import Enum
from io import StringIO
from typing import Dict, Optional, Iterable, List

import yaml

from cloud_controller import architecture_pb2 as protocols, architecture_pb2 as arch_pb
from cloud_controller.knowledge.cluster_model import Node


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


class Application:
    """
    An object representation of an application descriptor. Can be either instantiated directly or converted from
    protobuf representation. See constructor documentation for attributes.

    Attributes:
        _components:        All components of this application, mapped by name.
        _name:              Name of the application.
        _secret:            Docker secret of the application.
        _pb_representation: Protobuf representation of the application.
    """

    def __init__(self, name, secret: str = None, is_complete: bool = True, access_token: str = ""):
        """
        :param name: Application name
        :param secret: Application docker secret (if needed)
        """
        self._components: Dict[str, Component] = {}
        self._name: str = name
        self._secret: Optional[str] = secret
        self._pb_representation: Optional[protocols.Architecture] = None
        self._is_complete: bool = is_complete
        self._access_token: str = access_token
        self.namespace_created: bool = False
        self.namespace_deleted: bool = False
        self.cc_add_completed: bool = False
        self.cc_remove_completed: bool = False
        self.secret_added: bool = False
        self.db_dropped: bool = False

    @property
    def is_complete(self) -> bool:
        return self._is_complete

    @is_complete.setter
    def is_complete(self, is_complete: bool) -> None:
        self._is_complete = is_complete

    @property
    def name(self) -> str:
        return self._name

    @property
    def secret(self) -> Optional[str]:
        return self._secret

    @property
    def access_token(self) -> Optional[str]:
        return self._access_token

    @property
    def components(self) -> Dict[str, Component]:
        return self._components

    def get_component(self, component_id: str) -> Optional[Component]:
        return self._components.get(component_id)

    def get_pb_representation(self) -> Optional[protocols.Architecture]:
        return self._pb_representation

    def list_managed_components(self) -> Iterable[Component]:
        return self._list_components_by_type(ComponentType.MANAGED)

    def list_unmanaged_components(self) -> Iterable[Component]:
        return self._list_components_by_type(ComponentType.UNMANAGED)

    def _list_components_by_type(self, type: ComponentType) -> Iterable[Component]:
        for component in self._components.values():
            if component.type == type:
                yield component

    def add_component(self, component: Component) -> None:
        self._components[component.name] = component

    def add_components(self, components: List[Component]) -> None:
        for component in components:
            self.add_component(component)

    @staticmethod
    def init_from_pb(application_pb: protocols.Architecture) -> "Application":
        """
        Creates an application object from protobuf representation.
        """
        application = Application(
            name=application_pb.name,
            secret=application_pb.secret,
            is_complete=application_pb.is_complete,
            access_token=application_pb.access_token
        )
        for component_name in application_pb.components:
            application.add_component(Component.init_from_pb(application, application_pb.components[component_name]))
        for component_pb in application_pb.components.values():
            for dependency in component_pb.dependsOn:
                application.get_component(component_pb.name).add_dependency(application.get_component(dependency))

        application._pb_representation = application_pb
        return application


class QoSContract:

    def __init__(self):
        pass


class TimeContract(QoSContract):

    def __init__(self, time: int, percentile: float):
        super().__init__()
        self.time = time
        self.percentile = percentile


class ThroughputContract(QoSContract):

    def __init__(self, requests: int, per: int):
        super().__init__()
        self.requests = requests
        self.per = per
        self.mean_request_time = self.per / self.requests


class Probe:

    def __init__(self,
                 name: str,
                 component: Component,
                 requirements: List[QoSContract],
                 code: str = "",
                 config: str = "",
                 signal_set: str = "",
                 execution_time_signal: str = "",
                 run_count_signal: str = "",
                 args: str = ""
    ):
        self.name = name
        self.component = component
        self.requirements = requirements

        self.code: str = code
        self.config: str = config
        self.args: str = args
        self._alias = self.generate_alias(self.name, self.component)

        self.signal_set: str = signal_set
        self.execution_time_signal: str = execution_time_signal
        self.run_count_signal: str = run_count_signal

    @property
    def alias(self) -> str:
        return self._alias

    def generate_alias(self, probe_name: str, component: Component) -> str:
        if self.code is None:
            return f"{component.application.name.upper()}_{component.name.upper()}_{probe_name.upper()}"
        else:
            probe_spec = f"{self.component.container_spec}&&{self.code}&&{self.config}&&{self.args}"
            hash_ = hashlib.md5(bytes(probe_spec, 'utf-8'))
            return f"{hash_.hexdigest()}"

    @staticmethod
    def init_from_pb(probe_pb: arch_pb.Probe, applications: Dict[str, Application]) -> "Probe":
        """
        Creates a probe object from protobuf representation.
        """
        component = applications[probe_pb.application].components[probe_pb.component]
        return Probe.init_from_pb_direct(probe_pb, component)

    @staticmethod
    def construct_requirements(probe_pb: arch_pb.Probe) -> List[QoSContract]:
        requirements = []
        for requirement_pb in probe_pb.requirements:
            type = requirement_pb.WhichOneof('type')
            requirement = None
            if type == "time":
                requirement = TimeContract(requirement_pb.time.time, requirement_pb.time.percentile)
            elif type == "throughput":
                requirement = ThroughputContract(requirement_pb.throughput.requests, requirement_pb.throughput.per)
            assert requirement is not None
            requirements.append(requirement)
        return requirements

    @staticmethod
    def init_from_pb_direct(probe_pb: arch_pb.Probe, component: Component):
        requirements = Probe.construct_requirements(probe_pb)
        for probe in component.probes:
            if probe.name == probe_pb.name:
                probe._alias = probe.generate_alias(probe_pb.name, component)
                probe.requirements = requirements
                return probe
        return Probe(
            name=probe_pb.name,
            component=component,
            requirements=requirements,
            code=probe_pb.code,
            config=probe_pb.config,
            signal_set=probe_pb.signal_set,
            execution_time_signal=probe_pb.execution_time_signal,
            run_count_signal=probe_pb.run_count_signal,
            args=probe_pb.args
        )

    def pb_representation(self, probe_pb: arch_pb.Probe = None) -> arch_pb.Probe:
        if probe_pb is None:
            probe_pb = arch_pb.Probe()
        probe_pb.name = self.name
        probe_pb.application = self.component.application.name
        probe_pb.component = self.component.name
        probe_pb.alias = self.alias
        if self.code != "":
            probe_pb.type = protocols.ProbeType.Value('CODE')
            probe_pb.code = self.code
            probe_pb.config = self.config
            probe_pb.args = self.args
        else:
            probe_pb.type = protocols.ProbeType.Value('PROCEDURE')
        probe_pb.signal_set = self.signal_set
        probe_pb.execution_time_signal = self.execution_time_signal
        probe_pb.run_count_signal = self.run_count_signal
        for requirement in self.requirements:
            requirement_pb = probe_pb.requirements.add()
            if isinstance(requirement, TimeContract):
                requirement_pb.time.time = requirement.time
                requirement_pb.time.percentile = requirement.percentile
            else:
                assert isinstance(requirement, ThroughputContract)
                requirement_pb.throughput.requests = requirement.requests
                requirement_pb.throughput.per = requirement.per
        return probe_pb

    def __str__(self) -> str:
        return f"{self._alias}"