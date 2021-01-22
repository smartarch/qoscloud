"""
Contains the classes that are used by the framework to model the state of the cloud and the cluster: representations
of nodes, datacenters, namespaces, applications, components, compins, etc.
"""

from enum import Enum
from typing import List, Dict, Optional, Any, Iterable, Type, TypeVar
from io import StringIO

import yaml

import cloud_controller.architecture_pb2 as protocols
from cloud_controller import MONGOS_LABEL, MONGO_SHARD_LABEL
from cloud_controller.knowledge.user_equipment import UserEquipment
from cloud_controller.middleware.helpers import OrderedEnum


class ComponentType(Enum):
    MANAGED = 1
    UNMANAGED = 2


class Statefulness(Enum):
    NONE = 1
    MONGO = 3


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


def _assert_not_none_and_return(attr) -> Any:
    assert attr is not None
    return attr


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
        protocols.Statefulness.Value("MONGO"): Statefulness.MONGO
    }

    def __init__(self, application: "Application", name: str, id_: str, type_: ComponentType,
                 container_spec: str = None, dependencies: List["Component"] = None, probes = None,
                 statefulness: Statefulness = Statefulness.NONE):
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
        self._id: str = id_
        self._type: ComponentType = type_
        self._probes: List[str] = probes
        self._statefulness = statefulness
        self._dependencies: Dict[str, Component] = {}
        # The following attributes contains all the labels that were found in the Component's deployment descriptor
        # that may influence which nodes can be selected for the deployment of instances of this component.
        self._node_selector_labels: Dict[str, str] = self._parse_node_selector_labels()
        if dependencies is not None:
            for dependency in dependencies:
                self._dependencies[dependency.name] = dependency

    def __str__(self) -> str:
        return f"Component(id={self.id}, type={self.type}, dependencies=[{[str(dep) for dep in self._dependencies]}])"

    def __hash__(self) -> int:
        return hash(self._name + self._id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Component):
            return self.name == other.name and self.id == other.id
        else:
            return False

    @property
    def statefulness(self) -> Statefulness:
        return self._statefulness

    @property
    def is_stateful(self) -> bool:
        return self._statefulness is not Statefulness.NONE

    @property
    def application(self) -> "Application":
        return _assert_not_none_and_return(self._application)

    @property
    def name(self) -> str:
        return _assert_not_none_and_return(self._name)

    @property
    def container_spec(self) -> str:
        return _assert_not_none_and_return(self._container_spec)

    @property
    def id(self) -> str:
        # In the future, there may be a need for a separate concept of component ID for usage with Predictor.
        # Currently, it is the same thing as component name.
        return _assert_not_none_and_return(self._name)

    @property
    def type(self) -> ComponentType:
        return _assert_not_none_and_return(self._type)

    @property
    def dependencies(self) -> List["Component"]:
        return _assert_not_none_and_return(list(self._dependencies.values()))

    @property
    def probes(self) -> List[str]:
        return self._probes

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
        probes = []
        for probe in component_pb.probes:
            probes.append(probe.name)
        return Component(
            application=application,
            name=component_pb.name,
            id_=component_pb.name,
            type_=Component.type_map[component_pb.type],
            container_spec=component_pb.deployment,
            probes=probes,
            statefulness=Component.statefulness_map[component_pb.statefulness]
        )

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

    def __init__(self, name, secret: str = None):
        """
        :param name: Application name
        :param secret: Application docker secret (if needed)
        """
        self._components: Dict[str, Component] = {}
        self._name: str = name
        self._secret: Optional[str] = secret
        self._pb_representation: Optional[protocols.Architecture] = None

    @property
    def name(self) -> str:
        return _assert_not_none_and_return(self._name)

    @property
    def secret(self) -> Optional[str]:
        return self._secret

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
        application = Application(name=application_pb.name, secret=application_pb.secret)
        for component_name in application_pb.components:
            application.add_component(Component.init_from_pb(application, application_pb.components[component_name]))
        for component_pb in application_pb.components.values():
            for dependency in component_pb.dependsOn:
                application.get_component(component_pb.name).add_dependency(application.get_component(dependency))
        application._pb_representation = application_pb
        return application


class Compin:
    """
    A common base class for managed and unmanaged compins.

    Attributes:
        component:      A component of which this compin is an instance.
        id:             Unique ID of the compin.
        _ip:            IP of the compin. For unmanaged compins, this is the IP of their User Equipment. For managed
                        compins, this is the IP of the corresponding Kubernetes service (and thus, belongs to the
                        Kubernetes overlay network).
        _chain_id:      ID of the client to whose chain this compin belongs. For Unmanaged compins must be equal to id.
        _dependencies:  Instances of ManagedCompin this compin that are currently set as the dependencies of this
                        compin, mapped by name
    """

    def __init__(self, component: Component, id_: str, chain_id: str):
        """
        :param component: A component of which this compin is an instance.
        :param id_: Unique ID of the compin.
        :param chain_id: ID of the client to whose chain this compin belongs. For Unmanaged compins must be equal to id.
        """
        self.component: Component = component
        self.id: str = id_
        self._ip: str = None
        self._chain_id: str = chain_id
        self._dependencies: Dict[str, "ManagedCompin"] = {}

    @property
    def chain_id(self) -> str:
        """ID of the client for which this compin was created"""
        return self._chain_id

    @chain_id.setter
    def chain_id(self, id_: str) -> None:
        self._chain_id = id_

    @property
    def ip(self) -> str:
        return self._ip

    @ip.setter
    def ip(self, ip: str):
        self._ip = ip

    def set_dependency(self, compin: "ManagedCompin") -> None:
        """
        Sets the provided Managed compin as a dependency of this compin. Will raise an exception if this compin does
        not have a dependency on the given managed compin's component.
        """
        if compin.component in self.component.dependencies:
            if compin.component.name in self._dependencies:
                self._dependencies[compin.component.name].dependants.remove(self)
            self._dependencies[compin.component.name] = compin
            compin.dependants.append(self)
        else:
            raise Exception(f"Component {self.component.name} does not have dependency {compin.component.name}")

    def get_dependency(self, dependency: str) -> Optional["ManagedCompin"]:
        if dependency in self._dependencies:
            return self._dependencies[dependency]
        else:
            return None

    def list_dependencies(self) -> List["ManagedCompin"]:
        return list(self._dependencies.values())


class CompinPhase(OrderedEnum):
    """
    There are five possible phases for Managed compins:

        CREATING    Means that the container for this compin is still being created.
        INIT        Container has been created, yet is not able to accept client connections yet.
        READY       Instance can accept the connections and/or already serving a client.
        FINALIZING  The clients of this instance have already received new dependency addresses, and this instance have
                    received FinalizeExecution command.
        FINISHED    The instance is ready to be removed from the cloud.
    """
    CREATING = 0
    INIT = 1
    READY = 2
    FINALIZING = 3
    FINISHED = 4


class ManagedCompin(Compin):
    """
    Represents a managed compin.

    Attributes (in addition to those defined in the Compin class):
        node_name:              Name of the node this compin is running on (or should be running on, if it is not
                                running yet).
        phase:                  Current lifecycle phase of the compin. See docs for CompinPhase
        dependants:             List of compins (both managed and unmanaged) that have this compin as their dependency.
        _is_serving:            True if this compin has ever been set as a (transitive) dependency for an unmanaged
                                compin.
        force_keep:             True means that this compin should not be removed from the cloud even if it does not
                                serve any other compins.
        mongo_init_completed    True if this compin had already received MongoDB parameters. Applies only to
                                Mongo-stateful compins.
    """

    def __init__(self, component: Component, id_: str, node: str, chain_id: str):
        super().__init__(component, id_, chain_id)
        self.node_name: str = node
        self.phase: CompinPhase = CompinPhase.READY
        if component.statefulness == Statefulness.MONGO:
            self.mongo_init_completed = False
        else:
            self.mongo_init_completed = True
        self.dependants: List[Compin] = []
        self._is_serving: bool = False
        self.force_keep: bool = False

    def __str__(self) -> str:
        return f"ManagedCompin(id={self.id}, node_name={self.node_name}, chain_id={self._chain_id})"

    def set_force_keep(self) -> None:
        """
        Sets force_keep on this compin as well as on all of its transitive dependencies.
        """
        self.force_keep = True
        for compin in self.list_dependencies():
            compin.set_force_keep()

    @property
    def is_serving(self) -> bool:
        """
        :return: True if this compin has ever been set as a (transitive) dependency for an unmanaged compin, False
                    otherwise.
        """
        return self._is_serving

    def set_serving(self) -> None:
        """
        Sets this compin, as well as all of its transitive dependencies as serving. Resets force_keep.
        """
        self.force_keep = False
        if not self._is_serving:
            self._is_serving = True
            for dependent_compin in self.list_dependencies():
                dependent_compin.set_serving()

    def disconnect(self) -> None:
        """
        Clears the dependencies and dependants of this compin, sets force_keep and _is_serving to False.
        """
        self._is_serving = False
        self.force_keep = False
        self.dependants.clear()
        self._dependencies.clear()

    def deployment_name(self) -> str:
        """
        :return: The name of a Kubernetes deployment corresponding to this compin.
        """
        return self.component.name + '-' + self.id

    def service_name(self) -> str:
        """
        :return: The name of a Kubernetes service corresponding to this compin.
        """
        return ''.join(ch for ch in self.deployment_name() if ch.isalnum() or ch == '-')

    def get_client(self) -> Optional["UnmanagedCompin"]:
        """
        Searches through the dependent compins recursively, until finds the unmanaged compin for which this instance was
        created
        :return: The unmanaged compin for which this instance was created. None, if no such compin exists anymore
        """
        return self._get_client([])

    def _get_client(self, searched: List[str]) -> Optional["UnmanagedCompin"]:
        for compin in self.dependants:
            if isinstance(compin, UnmanagedCompin):
                assert compin.id == self.chain_id
                return compin
            elif compin.id not in searched:
                assert isinstance(compin, ManagedCompin)
                searched.append(self.id)
                client = compin._get_client(searched)
                if client is not None:
                    assert client.id == self.chain_id
                    return client
        return None


class UnmanagedCompin(Compin):
    """
    Represents an unmanaged compin.

    Attributes (in addition to those defined in the Compin class):
        ue: User Equipment on which this compin runs.
    """

    def __init__(self, component: Component, id_: str, ue: UserEquipment = None):
        super().__init__(component, id_, id_)
        if ue is not None:
            self._ue: UserEquipment = ue
            self.ip = ue.ip

    @property
    def ue(self) -> UserEquipment:
        return self._ue

    def set_dependency(self, compin: "ManagedCompin") -> None:
        """
        In addition to setting the compin as a current dependency, sets that compin (and all transitively dependent
        compins) as serving.
        :param compin: A new compin to set as dependency.
        """
        super().set_dependency(compin)
        compin.set_serving()

    def __str__(self) -> str:
        return f"UnmanagedCompin(id={self.id}, chain_id={self._chain_id})"


X = TypeVar('X')


class CloudState:
    """
    Contains and manages the state of the cloud, i.e. which managed and unmanaged compins are present in the cloud.

    Attributes:
        _state:     A collection of all compins. Maps application_name: {component_name: {compin_ID: Compin} }.
        _chains:    All the chains present in the state, mapped by client ID. A chain here is a list of all managed
                    compins that are related to a single client.
        _clients:   All the unmanaged compins in the state, mapped by ID.
    """

    def __init__(self):
        self._state: Dict[str, Dict[str, Dict[str, Compin]]] = {}
        self._chains: Dict[str, List[ManagedCompin]] = {}
        self._clients: Dict[str, UnmanagedCompin] = {}

    def add_application(self, application: Application) -> None:
        self._state[application.name] = {}
        for component in application.components:
            self._state[application.name][component] = {}

    def remove_application(self, application_name: str) -> None:
        for compin in self.list_unmanaged_compins(application_name):
            del self._chains[compin.id]
            del self._clients[compin.id]
        for compin in self.list_managed_compins(application_name):
            if compin.chain_id in self._chains:
                del self._chains[compin.chain_id]
        del self._state[application_name]

    def contains_application(self, application_name: str) -> bool:
        return application_name in self._state

    def list_applications(self) -> List[str]:
        return list(self._state.keys())

    def list_components(self, application_name: str) -> List[str]:
        if application_name not in self._state:
            return []
        return list(self._state[application_name].keys())

    def list_instances(self, application_name: str, component_name: str) -> List[str]:
        """
        :param application_name: Name of application to list compins for
        :param component_name: Name of component to list compins for
        :return: List of compin IDs with a given component name for a given application.
        """
        if application_name not in self._state or component_name not in self._state[application_name]:
            return []
        return list(self._state[application_name][component_name].keys())

    def get_compin(self, application_name: str, component_name: str, instance_id: str) -> Optional[Compin]:
        """
        :return: A compin with a given application name, component name and ID, if such compin exists in the cloud
                    state, None otherwise.
        """
        if self.compin_exists(application_name, component_name, instance_id):
            return self._state[application_name][component_name][instance_id]
        else:
            return None

    def add_instance(self, compin: Compin) -> None:
        """
        Adds a given compin to the state. The compin's application has to be already present in the cloud state, and
        the compin with that ID should not already exist in the state. Otherwise, will throw an exception.
        """
        assert not self.compin_exists(compin.component.application.name, compin.component.name, compin.id)
        self._state[compin.component.application.name][compin.component.name][compin.id] = compin
        if compin.chain_id not in self._chains:
            self._chains[compin.chain_id] = []
        if isinstance(compin, ManagedCompin):
            self._chains[compin.chain_id].append(compin)
        else:
            assert isinstance(compin, UnmanagedCompin)
            assert compin.id not in self._clients
            self._clients[compin.id] = compin

    def add_instances(self, compins: List[Compin]) -> None:
        for compin in compins:
            self.add_instance(compin)

    def delete_instance(self, application: str, component: str, id_: str) -> None:
        """
        Removes a compin with given parameters from the state. If no such compin is present, will throw an exception.
        """
        assert self.compin_exists(application, component, id_)
        compin = self._state[application][component][id_]
        if isinstance(compin, ManagedCompin):
            assert compin.chain_id in self._chains
            self._chains[compin.chain_id].remove(compin)
        else:
            assert isinstance(compin, UnmanagedCompin)
            assert compin.id in self._chains and compin.id in self._clients
            del self._clients[compin.id]
            for managed_compin in self._chains[compin.id]:
                managed_compin.disconnect()
        if len(self._chains[compin.chain_id]) == 0:
            del self._chains[compin.chain_id]
        del self._state[application][component][id_]

    def set_dependency(self, application: str, component: str, id_: str, dependency: str, dependency_id: str) -> None:
        """
        Sets one compin as a dependency of the other.
        :param application: Application both compins are located in.
        :param component: Component name of a compin for which dependency has to e set.
        :param id_: ID of a compin for which dependency has to e set.
        :param dependency: Component name of a compin that will serve as a dependency
        :param dependency_id: ID of a compin that will serve as a dependency
        """
        dependency_provider = self.get_compin(application, dependency, dependency_id)
        dependent_compin = self.get_compin(application, component, id_)
        assert dependency_provider is not None and dependent_compin is not None
        assert isinstance(dependency_provider, ManagedCompin)
        assert dependency_provider.chain_id == dependent_compin.chain_id
        dependent_compin.set_dependency(dependency_provider)

    def compin_exists(self, application: str, component: str, client_id: str) -> bool:
        """
        Returns true if the compin with specified parameters exists in the model.
        :param application: Application the compin belongs to
        :param component: name of the component
        :param client_id: ID of the compin
        :return: true if the compin exists, false otherwise
        """
        if application in self._state:
            if component in self._state[application]:
                if client_id in self._state[application][component]:
                    return True
        return False

    def get_all_compins(self) -> List[Compin]:
        """
        Returns a list of all unmanaged and managed compins for all applications.
        """
        compins = []
        for app_name in self._state:
            for component_name in self._state[app_name]:
                for compin in self._state[app_name][component_name].values():
                    compins.append(compin)
        return compins

    def list_all_unmanaged_compins(self) -> Iterable[UnmanagedCompin]:
        """
         Iterates through all unmanaged compins across all the applications.
         :return: UnmanagedCompin iterator.
         """
        for application_name in self.list_applications():
            for unmanaged_compin in self.list_unmanaged_compins(application_name):
                yield unmanaged_compin

    def list_all_managed_compins(self) -> Iterable[ManagedCompin]:
        """
         Iterates through all managed compins across all the applications.
         :return: ManagedCompin iterator.
         """
        for application_name in self.list_applications():
            for managed_compin in self.list_managed_compins(application_name):
                yield managed_compin

    def list_managed_compins(self, application_name: str) -> Iterable[ManagedCompin]:
        """
        Iterates through all unmanaged compins in a given application.
        :return: ManagedCompin iterator.
        """
        return self._list_compins_by_type(application_name, ManagedCompin)

    def list_unmanaged_compins(self, application_name: str) -> Iterable[UnmanagedCompin]:
        """
        Iterates through all managed compins in a given application.
        :return: UnmanagedCompin iterator.
        """
        return self._list_compins_by_type(application_name, UnmanagedCompin)

    def _list_compins_by_type(self, application_name: str, type_: Type[X]) -> Iterable[X]:
        """
        Iterates through all compins of a given type in a given application.
        :param application_name: Name of the application to iterate the compins from.
        :param type_: ManagedCompin or UnmanagedCompin.
        :return: Iterator of compins of a given type
        """
        if application_name not in self._state:
            return []
        for component_name in self._state[application_name]:
            for compin_id in self._state[application_name][component_name]:
                compin = self.get_compin(application_name, component_name, compin_id)
                if isinstance(compin, type_):
                    yield compin
