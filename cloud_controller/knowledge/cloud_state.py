import itertools
from typing import TypeVar, Dict, List, Optional, Iterable, Type

from cloud_controller.knowledge.application import Application
from cloud_controller.knowledge.component import ComponentType, ComponentCardinality, Component
from cloud_controller.knowledge.instance import Compin, ManagedCompin, UnmanagedCompin

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
        self._chains: Dict[str, Dict[str, ManagedCompin]] = {}
        self._clients: Dict[str, UnmanagedCompin] = {}
        self._applications: Dict[str, Application] = {}

    def add_application(self, application: Application) -> None:
        self._state[application.name] = {}
        self._applications[application.name] = application
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

    def connect_all_chains(self):
        for chain_id in self._chains:
            client = [self._clients[chain_id]] if chain_id in self._clients else []
            for instance in itertools.chain(client, self._chains[chain_id].values()):
                for dependency in instance.component.dependencies:
                    if dependency.cardinality == ComponentCardinality.MULTIPLE:
                        provider = self._chains[chain_id][dependency.name]
                        assert isinstance(provider, ManagedCompin)
                        instance.set_dependency(provider)
                    else:
                        provider = self._state[dependency.application.name][dependency.name][dependency.name]
                        assert isinstance(provider, ManagedCompin)
                        instance.set_dependency(provider)

    def list_instances(self, application_name: str, component_name: str) -> List[str]:
        """
        :param application_name: Name of application to list compins for
        :param component_name: Name of component to list compins for
        :return: List of compin IDs with a given component name for a given application.
        """
        if application_name not in self._state or component_name not in self._state[application_name]:
            return []
        return list(self._state[application_name][component_name].keys())

    def get_instance(self, component: Component, instance_id: str) -> Optional[Compin]:
        """
        :return: An instance of a given component with a given ID, if instance compin exists in the cloud
                    state, None otherwise.
        """
        return self.get_compin(component.application.name, component.name, instance_id)

    def get_compin(self, application_name: str, component_name: str, instance_id: str) -> Optional[Compin]:
        """
        :return: A compin with a given application name, component name and ID, if such compin exists in the cloud
                    state, None otherwise.
        """
        if self.compin_exists(application_name, component_name, instance_id):
            return self._state[application_name][component_name][instance_id]
        else:
            return None

    def get_job_compin(self, id: str):
        return self.get_compin(id, id, id)

    def get_unique_compin(self, component: Component) -> Optional[ManagedCompin]:
        assert component.type == ComponentType.MANAGED
        assert component.cardinality == ComponentCardinality.SINGLE
        if not self.contains_application(component.application.name):
            return None
        if len(self._state[component.application.name][component.name]) == 0:
            return None
        instance = self._state[component.application.name][component.name][component.name]
        assert isinstance(instance, ManagedCompin)
        return instance

    def add_instance(self, compin: Compin) -> None:
        """
        Adds a given compin to the state. The compin's application has to be already present in the cloud state, and
        the compin with that ID should not already exist in the state. Otherwise, will throw an exception.
        """
        assert not self.compin_exists(compin.component.application.name, compin.component.name, compin.id)
        self._state[compin.component.application.name][compin.component.name][compin.id] = compin
        if compin.chain_id not in self._chains:
            self._chains[compin.chain_id] = {}
        if isinstance(compin, ManagedCompin):
            self._chains[compin.chain_id][compin.component.name] = compin
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
            assert compin.component.name in self._chains[compin.chain_id]
            del self._chains[compin.chain_id][compin.component.name]
        else:
            assert isinstance(compin, UnmanagedCompin)
            assert compin.id in self._chains and compin.id in self._clients
            del self._clients[compin.id]
            for managed_compin in self._chains[compin.id].values():
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
        if dependency_provider is not None and dependent_compin is not None:
            assert isinstance(dependency_provider, ManagedCompin)
            if dependency_provider.component.cardinality == ComponentCardinality.MULTIPLE and \
                dependent_compin.component.cardinality == ComponentCardinality.MULTIPLE:
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
                if managed_compin.component.cardinality == ComponentCardinality.MULTIPLE:
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