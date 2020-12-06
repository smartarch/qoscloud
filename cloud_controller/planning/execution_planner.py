"""
This module contains the ExecutionPlanner class which is responsible for the _planning_ phase of the adaptation process.
"""
import logging
from itertools import chain
from typing import Iterable

from cloud_controller import DEBUG
from cloud_controller.knowledge.model import CloudState, NamespacePhase
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.planning.cloud_state_diff import get_application_diff
from cloud_controller.planning.execution_plan_factory import ExecutionPlanFactory
import cloud_controller.knowledge.knowledge_pb2 as protocols


def delete_ns_plan(ns_name: str) -> protocols.ExecutionPlan:
    """
    A helper function that creates a simple plan for deletion of a Kubernetes namespace.
    :param ns_name: namespace to delete
    """
    plan = protocols.ExecutionPlan()
    task = plan.tasks.add()
    task.id = 0
    task.DELETE_NAMESPACE.name = ns_name
    return plan


def empty_plan() -> protocols.ExecutionPlan:
    """
    A helper function that creates an empty ExecutionPlan.
    """
    plan = protocols.ExecutionPlan()
    task = plan.tasks.add()
    task.id = 0
    task.DO_NOTHING = 0
    return plan


class ExecutionPlanner:
    """
    Execution Planner is capable of creating the execution plans based on the differences between the actual and the
    desired states with its plan_changes method. The actual state is taken from the Knowledge. For the different types
    of the execution plans that exist refer to the documentation of ExecutionPlanFactory.

    Attributes:
        knowledge:  reference to the Knowledge
        _factories: a collection of ExecutionPlanFactories for all the present applications, mapped by application name.
    """

    def __init__(self, knowledge: Knowledge):
        self.knowledge: Knowledge = knowledge
        self._factories = {}

    def plan_changes(self, desired_state: CloudState) -> Iterable[protocols.ExecutionPlan]:
        """
        :param desired_state: A desired state of the cloud.
        :return: Execution plans one by one for all the applications.
        """
        #
        return chain(self._apply_app_diff(desired_state), self._redeploy_changed_applications(desired_state))

    def _apply_app_diff(self, desired_state: CloudState) -> Iterable[protocols.ExecutionPlan]:
        """
        :param desired_state: A desired state of the cloud.
        :return: Iterable of all the creation and deletion plans
        """
        create_apps, delete_apps = get_application_diff(self.knowledge.actual_state, desired_state)
        for app_name in create_apps:
            if app_name in self.knowledge.namespaces:
                if self.knowledge.namespaces[app_name].phase == NamespacePhase.TERMINATING:
                    # We cannot create a namespace for the application if there is already a namespace with that name
                    # that is being terminated. This can happen if an application was recently deleted and then
                    # re-submitted
                    logging.info(f"Namespace {app_name} is terminating. Application creation is postponed.")
                    yield empty_plan()
                    continue
                elif self.knowledge.namespaces[app_name].phase == NamespacePhase.ACTIVE:
                    # If there is an _active_ namespace with that name, that means that we've got a problem
                    msg = f"Cannot create application {app_name}: there is already a namespace with that name"
                    logging.error(msg)
                    if DEBUG:
                        # Theoretically, it can mean that there is a bug in Avocado (if it failed to delete a namespace
                        # for a previously submitted application), or a problem in the setup of the cluster. Under
                        # DEBUG, we can raise an exception
                        raise Exception(msg)
                    else:
                        # However, usually that means that someone had created a namespace manually while the Avocado
                        # was running. In production we just delete that namespace.
                        yield delete_ns_plan(app_name)
                        continue
            self._factories[app_name] = ExecutionPlanFactory(app_name, self.knowledge)
            exec_plan = self._factories[app_name].create_application_creation_plan()
            yield exec_plan
        for app_name in delete_apps:
            exec_plan = self._factories[app_name].create_application_deletion_plan()
            del self._factories[app_name]
            yield exec_plan

    def _redeploy_changed_applications(self, desired_state: CloudState) -> Iterable[protocols.ExecutionPlan]:
        """
        :param desired_state: A desired state of the cloud.
        :return: Iterable of redeployment plans for all the applications that require changes in their state.
        """
        for factory in self._factories.values():
            exec_plan = factory.create_redeployment_plan(desired_state)
            if exec_plan is not None:
                yield exec_plan
