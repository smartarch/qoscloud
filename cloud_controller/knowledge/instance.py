from typing import Optional, Dict, List

from cloud_controller.knowledge.model import Statefulness, Component
from cloud_controller.knowledge.user_equipment import UserEquipment
from cloud_controller.middleware.helpers import OrderedEnum


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
        self._ip: Optional[str] = None
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
        if component.statefulness != Statefulness.NONE:
            self.mongo_init_completed = False
        else:
            self.mongo_init_completed = True
        self.dependants: List[Compin] = []
        self._is_serving: bool = False
        self.force_keep: bool = False
        self.init_completed: bool = False

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

    def __init__(self, component: Component, id_: str, ue: UserEquipment = None, position_x: float = 0, position_y: float = 0):
        super().__init__(component, id_, id_)
        if ue is not None:
            self._ue: UserEquipment = ue
            self.ip = ue.ip
        self.position_x: float = position_x
        self.position_y: float = position_y

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