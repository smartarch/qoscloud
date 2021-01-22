import logging
from typing import List, Tuple, Optional

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, Compin, ManagedCompin, Statefulness
from cloud_controller.planner.top_planner import Planner
from cloud_controller.planner.cloud_state_diff import get_dependency_diff
from cloud_controller.tasks.client_controller import SetClientDependencyTask
from cloud_controller.tasks.middleware import SetMiddlewareAddressTask
from cloud_controller.tasks.preconditions import dependency_is_set
from cloud_controller.task_executor.registry import TaskRegistry
from cloud_controller.tasks.statefulness import MoveChunkTask


class DependencyPlanner(Planner):

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        super().__init__(knowledge, task_registry)

    def _add_middleware_connection_tasks(
            self,
            app_name:str,
            dependency_diff: List[Tuple[str, str, str, str, Optional[str]]]
    ) -> None:
        """
        :param dependency_diff: A diff produced by get_dependency_diff()
        """
        # Go through dependencies, and create the corresponding "set dependency address" tasks:
        for (dependent_component, id_, dependency_name, dependency_id, old_dependency_ip) in dependency_diff:
            # Retrieve relevant compins from the actual state
            dependent_instance = self.knowledge.actual_state.get_compin(app_name, dependent_component, id_)
            providing_instance = self.knowledge.actual_state.get_compin(app_name, dependency_name, dependency_id)
            if providing_instance is None or dependent_instance is None:
                continue
            assert isinstance(providing_instance, ManagedCompin)

            # Create a task for middleware connection
            if isinstance(dependent_instance, ManagedCompin):
                self._create_task(
                    SetMiddlewareAddressTask(
                        providing_component=providing_instance.component,
                        providing_instance_id=providing_instance.id,
                        dependent_component=dependent_instance.component,
                        dependent_instance_id=dependent_instance.id
                    )
                )
            else:
                self._create_task(
                    SetClientDependencyTask(
                        component=providing_instance.component,
                        instance_id=providing_instance.id,
                        client_component=dependent_instance.component,
                        client_id=dependent_instance.id
                    )
                )
            # self._add_dependency(self._starting_task, middleware_task)
            logging.info(f"Created a task for connection of {dependent_component} with ID {id_} to "
                         f"{dependency_name} with ID {dependency_id}.")

            if providing_instance.component.statefulness == Statefulness.CLIENT:
                dc_name = self.knowledge.nodes[providing_instance.node_name].data_center
                dc = self.knowledge.datacenters[dc_name]
                task = MoveChunkTask(
                    database=app_name,
                    collection=providing_instance.component.name,
                    key=int(providing_instance.chain_id),
                    shard=dc.mongo_shard_name
                )
                task.add_precondition(dependency_is_set, (app_name, dependent_instance.component.name,
                                                          dependent_instance.id, providing_instance.component.name,
                                                          providing_instance.id))
                self._create_task(task)

    def _plan_connection_tasks(self, app_name: str, desired_state: CloudState):
        dependency_diff = get_dependency_diff(app_name, self.knowledge.actual_state, desired_state)
        self._add_middleware_connection_tasks(app_name, dependency_diff)
        logging.info(f"Created the dependency connection tasks for {app_name} application")

    def plan_tasks(self, desired_state: CloudState):
        for app_name in self.knowledge.applications:
            if self.knowledge.actual_state.contains_application(app_name):
                self._plan_connection_tasks(app_name, desired_state)