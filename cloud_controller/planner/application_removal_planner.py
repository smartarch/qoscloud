import logging
from typing import List

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, Application, Statefulness
from cloud_controller.planner.top_planner import Planner
from cloud_controller.tasks.client_controller import DeleteApplicationFromCCTask
from cloud_controller.tasks.kubernetes import DeleteNamespaceTask
from cloud_controller.task_executor.registry import TaskRegistry
from cloud_controller.tasks.statefulness import DeleteAppRecordTask, DropDatabaseTask


class ApplicationRemovalPlanner(Planner):

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        super().__init__(knowledge, task_registry)

    def _get_application_diff(self, actual_state: CloudState, desired_state: CloudState) -> List[str]:
        """
        Calculates the differences in the application present in the two cloud states.
        :param actual_state: CloudState representing the actual state of the cloud
        :param desired_state: CloudState representing the desired state of the cloud
        :return: list of applications present in the actual state but not present in the desired state
        """
        apps_to_remove = [app for app in actual_state.list_applications() if
                          app not in desired_state.list_applications()]
        return apps_to_remove

    def _cc_operations_required(self, app: Application) -> bool:
        return self.knowledge.client_support and len(list(app.list_unmanaged_components())) > 0

    def _plan_app_deletion(self, app_name: str) -> None:
        """
        Creates the tasks for deleting an application, which includes disconnection of all the clients from
        the Client Controller, deletion of the Kubernetes namespace and deletion of the corresponding Mongo database.
        """
        app: Application = self.knowledge.applications[app_name]
        self._create_task(DeleteNamespaceTask(app_name))

        if self._cc_operations_required(app):
            self._create_task(DeleteApplicationFromCCTask(app_name))
            # self._add_dependency(disconnection_task, namespace_task)
        self._create_task(DeleteAppRecordTask(app_name))
        mongo_components = []
        for component in app.list_managed_components():
            if component.statefulness != Statefulness.NONE:
                mongo_components.append(component.name)
        if len(mongo_components) > 0:
            self._create_task(DropDatabaseTask(app_name, mongo_components))
            # self._add_dependency(db_drop_task, database_record_task)

        logging.info(f"Created a deletion plan for {app_name} application")

    def plan_tasks(self, desired_state: CloudState):
        delete_apps = self._get_application_diff(self.knowledge.actual_state, desired_state)
        for app_name in delete_apps:
            self._plan_app_deletion(app_name)