"""
This module contains the functions that compare two CloudStates. These functions are used by ExecutionPlanner to
find the differences between the actual state and the desired state. Then, based on these differences it creates
execution plans that aim to bring the actual state to the desired state.
"""
from typing import Tuple, List, Optional

from cloud_controller.knowledge.component import Statefulness
from cloud_controller.knowledge.instance import Compin, CompinPhase, ManagedCompin
from cloud_controller.knowledge.cloud_state import CloudState


def get_compin_diff(application_name, actual_state: CloudState, desired_state: CloudState) -> \
        Tuple[List[ManagedCompin], List[ManagedCompin], List[ManagedCompin], List[ManagedCompin]]:
    """
    Calculates the differences in the compins of a given application present in the two cloud states.
    :param application_name: An application to calculate the difference for
    :param actual_state: CloudState representing the actual state of the cloud
    :param desired_state: CloudState representing the desired state of the cloud
    :return: list of compins present in the desired state but not present in the actual state, list of compins present
                in the actual state but not present in the desired state, and list of compins in the actual state whose
                MongoAgents have not been initialized yet
    """

    create_instances: List[ManagedCompin] = []
    for compin in desired_state.list_managed_compins(application_name):
        if not actual_state.compin_exists(application_name, compin.component.name, compin.id):
            create_instances.append(compin)

    delete_instances: List[ManagedCompin] = []
    mongo_init_instances: List[ManagedCompin] = []
    init_instances: List[ManagedCompin] = []
    for compin in actual_state.list_managed_compins(application_name):
        if not desired_state.compin_exists(application_name, compin.component.name, compin.id):
            if not compin.force_keep:
                delete_instances.append(compin)
        elif not compin.init_completed:
            init_instances.append(compin)
        elif compin.component.statefulness != Statefulness.NONE and not compin.mongo_init_completed:
            mongo_init_instances.append(compin)

    return create_instances, delete_instances, mongo_init_instances, init_instances


def get_dependency_diff(application_name, actual_state: CloudState, desired_state: CloudState) -> \
        List[Tuple[str, str, str, str, Optional[str]]]:
    """
    Calculates the differences in the compins' dependencies of a given application between the two cloud states.
    :param application_name: An application to calculate the difference for
    :param actual_state: CloudState representing the actual state of the cloud
    :param desired_state: CloudState representing the desired state of the cloud
    :return: list of tuples, each containing: component name and ID of the compin in the actual state that has a
                dependency that needs to be updated, name of that dependency, ID of the compin that should serve as
                the new dependency, IP address of the current dependency (if the dependency is set at the moment).
    """
    set_dependencies: List[Tuple[str, str, str, str, Optional[str]]] = []
    for component in desired_state.list_components(application_name):
        for id_ in desired_state.list_instances(application_name, component):
            desired_state_compin: Compin = desired_state.get_compin(application_name, component, id_)
            actual_state_compin: Compin = actual_state.get_compin(application_name, component, id_)
            for dependency in desired_state_compin.list_dependencies():
                if actual_state_compin is None or actual_state_compin.get_dependency(dependency.component.name) is None:
                    set_dependencies.append((component, id_, dependency.component.name, dependency.id, None))
                elif actual_state_compin.get_dependency(dependency.component.name).id != dependency.id:
                    old_dependency_ip = actual_state_compin.get_dependency(dependency.component.name).ip
                    set_dependencies.append(
                        (component, id_, dependency.component.name, dependency.id, old_dependency_ip))
    return set_dependencies
