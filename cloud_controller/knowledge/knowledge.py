"""
Contains the Knowledge class: the data structure that manages all the data that are required for the adaptation
process.
"""
import logging
from typing import Dict, Set, Optional, Iterable
from typing import List

from cloud_controller.knowledge.model import Component, UnmanagedCompin, check_datacenters, construct_datacenters, \
    Datacenter
from cloud_controller.knowledge.user_equipment import UserEquipmentContainer, UserEquipment
from cloud_controller.knowledge.model import CloudState, Node, Namespace, Application
from cloud_controller.knowledge.network_topology import NetworkTopology, EuclidNetworkTopology


class Knowledge:
    """
    Contains and manages all the data required to run the adaptation process.

    Attributes:
        applications:       All the applications present in the system, mapped by name.
        actual_state:       The actual CloudState.
        nodes:              All the nodes in the cluster, mapped by name.
        namespaces:         All the Kubernetes namespaces present in the cloud, mapped by name.
        user_equipment:     A UserEquipmentContainer managing all the user equipment connected to the system.
        network_topology:   A NetworkTopology topology object capable of providing the network distances between the
                            clients, nodes, and datacenters.
        datacenters:        All the datacenters in the cluster.
        secrets:            The dockersecrets submitted to the framework, mapped by name of the relevant application.
        _new_clients:       List of the unmanaged compins connected to the framework in the last Monitoring phase.
        client_support      True if this instance of the Adaptation Controller provides support for the client
                            connections (i.e. communicates with the client controller), False otherwise.
    """

    def __init__(self, actual_state: CloudState = None):
        """
        :param actual_state: Initial CloudState to use as the actual state. If None, will create an empty CloudState.
        """
        if actual_state is None:
            actual_state = CloudState()
        self.applications: Dict[str, Application] = {}
        self.components: Dict[str, Component] = {}
        self.actual_state: CloudState = actual_state
        self.nodes: Dict[str, Node] = {}
        self.namespaces: Dict[str, Namespace] = {}
        self.user_equipment: UserEquipmentContainer = UserEquipmentContainer()
        self.network_topology: NetworkTopology = EuclidNetworkTopology()
        self.datacenters: Dict[str, Datacenter] = {}
        self.secrets: Dict[str, str] = {}
        self._new_clients: List[UnmanagedCompin] = []
        self.client_support = True
        self.unique_components_without_resources: List[str] = []
        self.api_endpoint_access_token: Optional[str] = None

    def update_access_token(self, token: str):
        self.api_endpoint_access_token = token

    def no_resources_for_component(self, component_id: str) -> None:
        self.unique_components_without_resources.append(component_id)

    def all_components_scheduled(self) -> None:
        self.unique_components_without_resources.clear()

    def there_are_applications(self) -> bool:
        return len(self.applications) == 0

    def set_network_topology(self, network_topology: NetworkTopology):
        """
        :param network_topology: a new instance of NetworkTopology to use.
        """
        self.network_topology = network_topology

    def update_cluster_state(self, nodes: Dict[str, Node], namespaces: Dict[str, Namespace]) -> None:
        """
        Updates the nodes and namespaces. Based on the provided nodes collection, updates the datacenters.
        :param nodes: Nodes to set, mapped by name.
        :param namespaces: Namespaces to set, mapped by name.
        """
        self.nodes = nodes
        self.namespaces = namespaces
        if not check_datacenters(self.nodes, self.datacenters):
            self.datacenters = construct_datacenters(nodes)
        logging.info(f"New cluster state received. Datacenters: {len(self.datacenters)}. Nodes: {len(self.nodes)}. "
                     f"Namespaces: {len(self.namespaces)}.")

    def get_all_managed_components(self) -> List[Component]:
        """
        :return: List of all Managed components throughout all the applications present in the system.
        """
        managed_components = []
        for application in self.applications.values():
            managed_components += list(application.list_managed_components())
        return managed_components

    def get_all_datacenters(self) -> Set[str]:
        """
        :return: Set of the names of all datacenters in the cluster.
        """
        return set(self.datacenters.keys())

    def add_application(self, app_pb) -> None:
        """
        Adds an application to the Knowledge. Converts the protobuf representation into the object representation. Also,
        processes the application's docker secret.
        :param app_pb: protobuf representation of the application architecture.
        """
        app = Application.init_from_pb(app_pb)
        self.applications[app_pb.name] = app
        # TONOWDO: DEFAULT_MEASURED_RUNS
        for component in self.applications[app_pb.name].components.values():
            self.components[component.id] = component
        if app_pb.HasField("secret"):
            self._add_secret(app_pb.name, app_pb.secret.value)
        logging.info("An architecture for the %s application was processed." % app_pb.name)

    def delete_application(self, name: str) -> None:
        """
        Deletes the application with the specified name from the Knowledge. If the Knowledge instance is located in a
        running Adaptation Controller it will ultimately lead to removal of all the resources related to this
        application form the cloud.
        :param name: name of the application.
        """
        if name in self.applications:
            for component in self.applications[name].components.values():
                del self.components[component.id]
            del self.applications[name]
        logging.info(f"A request for deletion of application {name} was processed.")

    def add_client(self, app_name: str, component_name: str, id_: str, ip: str) -> None:
        """
        Adds an instance of UnmanagedCompin to the Knowledge. Will throw an exception if compin with this ID already
        exists.
        :param app_name: Application of the compin.
        :param component_name: Component of the compin.
        :param id_: Unique ID of the compin.
        :param ip: IP address of the compin's UE.
        """
        assert not self.actual_state.compin_exists(app_name, component_name, id_)
        if not self.user_equipment.ue_registered(ip):
            self.user_equipment.add_ue(UserEquipment(ip), True)
        ue = self.user_equipment.get_ue(ip)
        compin = UnmanagedCompin(
            component=self.applications[app_name].components[component_name],
            id_=id_,
            ue=ue
        )
        self.actual_state.add_instance(compin)
        self._new_clients.append(compin)
        logging.info(f"Client {app_name}-{component_name}-{id_} added successfully")

    def remove_client(self, app_name: str, component_name: str, id_: str) -> None:
        """
        Removes an instance of UnmanagedCompin from the Knowledge. Will throw an exception if compin with this ID does
        not exist.
        :param app_name: Application of the compin.
        :param component_name: Component of the compin.
        :param id_: Unique ID of the compin.
        """
        compin = self.actual_state.get_compin(app_name, component_name, id_)
        assert isinstance(compin, UnmanagedCompin)
        self.actual_state.delete_instance(app_name, component_name, id_)
        logging.info(f"Client {app_name}-{component_name}-{id_} removed successfully")

    def list_new_clients(self) -> Iterable[UnmanagedCompin]:
        """
        :return: Iterable of all the clients (unmanaged compins) that were added to the Knowledge since the last call
                    of this method.
        """
        while len(self._new_clients) > 0:
            yield self._new_clients.pop()

    def _add_secret(self, app: str, secret: str) -> None:
        """
        Adds a docker secret for a given application.
        :param app: Application the secret belongs to
        :param secret: A docker secret for Kubernetes, encoded per Kubernetes requirements and converted to string.
        """
        self.secrets[app] = secret

    def remove_secret(self, app: str) -> Optional[str]:
        """
        :param app: Application the secret belongs to.
        :return: The secret for a given application, if exists, None otherwise.
        """
        if app in self.secrets:
            return self.secrets.pop(app)
        else:
            return None
