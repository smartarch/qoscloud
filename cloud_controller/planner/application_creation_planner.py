import logging
from typing import List, Dict, Set

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, Application, Statefulness
from cloud_controller.planner.top_planner import Planner
from cloud_controller.tasks.client_controller import AddApplicationToCCTask
from cloud_controller.tasks.kubernetes import CreateNamespaceTask, CreateDockersecretTask
from cloud_controller.task_executor.registry import TaskRegistry
from cloud_controller.tasks.statefulness import AddAppRecordTask, ShardCollectionTask
from cloud_controller.tasks.task import Task


class ApplicationCreationPlanner(Planner):
    """
    Creates an execution plan for new application, which includes creation of namespace, adding the image pull
    secret to that namespace, adding that application to the Client Controller, and creating and sharding the
    collections for all the Mongo-stateful components.
    """

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        super(ApplicationCreationPlanner, self).__init__(knowledge, task_registry)
        self._tasks_by_app: Dict[str, Set[str]] = {}

    def _complete_planning(self):
        for app in self._tasks_by_app:
            if app not in self.knowledge.applications:
                for task_id in self._tasks_by_app[app]:
                    self.task_registry.cancel_task(task_id)

    def _create_task(self, task: Task, app_name=None):
        super()._create_task(task)
        self._tasks_by_app[app_name].add(task.task_id)

    def _get_application_diff(self, actual_state: CloudState, desired_state: CloudState) -> List[str]:
        """
        Calculates the differences in the application present in the two cloud states.
        :param actual_state: CloudState representing the actual state of the cloud
        :param desired_state: CloudState representing the desired state of the cloud
        :return: list of applications present in the desired state but not present in the actual state
        """
        new_apps = [app for app in desired_state.list_applications() if app not in actual_state.list_applications()]
        return new_apps

    def _cc_operations_required(self, app: Application) -> bool:
        return self.knowledge.client_support and len(list(app.list_unmanaged_components())) > 0

    def _plan_app_creation(self, app_name: str) -> None:
        self._tasks_by_app[app_name] = set()
        app: Application = self.knowledge.applications[app_name]
        self._create_task(AddAppRecordTask(app_name), app_name)
        if not app.namespace_created:
            self._create_task(CreateNamespaceTask(app_name), app_name)
        # self._add_dependency(database_record_task, namespace_task)
        if self._cc_operations_required(app) and not app.cc_add_completed:
            self._create_task(AddApplicationToCCTask(app), app_name)
        secret = self.knowledge.get_secret(app_name)
        if secret is not None and not app.secret_added:
            self._create_task(CreateDockersecretTask(app_name, secret), app_name)
        for component in app.list_managed_components():
            if component.statefulness == Statefulness.CLIENT and not component.collection_sharded:
                self._create_task(ShardCollectionTask(app_name, app_name, component.name), app_name)
                # self._add_dependency(database_record_task, sharding_task)
        logging.info(f"Created the tasks for application {app_name} deployment")

    def plan_tasks(self, desired_state: CloudState):
        create_apps = self._get_application_diff(self.knowledge.actual_state, desired_state)
        for app_name in create_apps:
            self._plan_app_creation(app_name)