from typing import Dict, Optional, Iterable, List

from cloud_controller import architecture_pb2 as protocols
from cloud_controller.knowledge.component import Component, ComponentType


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