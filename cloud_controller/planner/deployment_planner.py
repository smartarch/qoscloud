import logging

from cloud_controller import DEFAULT_MEASURED_RUNS
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, ManagedCompin, CompinPhase
from cloud_controller.planner.top_planner import Planner
from cloud_controller.planning.cloud_state_diff import get_compin_diff
from cloud_controller.tasks.instance_management import CreateCompinTask, DeleteInstanceTask
from cloud_controller.tasks.middleware import InitializeInstanceTask, SetMongoParametersTask, FinalizeInstanceTask
from cloud_controller.task_executor.registry import TaskRegistry


class InstanceDeploymentPlanner(Planner):

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        super(InstanceDeploymentPlanner, self).__init__(knowledge, task_registry)

    def plan_tasks(self, desired_state: CloudState):
        for app_name in self.knowledge.applications:
            if self.knowledge.actual_state.contains_application(app_name):
                self._plan_app_redeployments(app_name, desired_state)

    def _plan_app_redeployments(self, app_name: str, desired_state):
        create_instances, delete_instances, mongo_init_instances, init_instances = \
            get_compin_diff(app_name, self.knowledge.actual_state, desired_state)
        for compin in create_instances:
            self._create_compin_creation_task(compin)
        for compin in delete_instances:
            self._create_compin_deletion_task(compin)
        for compin in init_instances:
            self._create_instance_init_task(compin)
        for compin in mongo_init_instances:
            self._add_mongo_init_task(compin)
        logging.info(f"Created a redeployment plan for {app_name} application")

    def _create_instance_init_task(self, compin: ManagedCompin) -> None:
        self.task_registry.add_task(
            InitializeInstanceTask(
                component=compin.component,
                instance_id=compin.id,
                run_count= DEFAULT_MEASURED_RUNS if self.knowledge.client_support else 0,
                access_token=self.knowledge.api_endpoint_access_token,
                production=self.knowledge.client_support
            )
        )
        logging.debug(f"Created instance initialization task for instance {compin.id}.")

    def _create_compin_creation_task(self, compin: ManagedCompin) -> None:
        """
        :param compin: A compin to create the task for
        :param parent_task: A task after which the compin creation task will be executed.
        """
        self.task_registry.add_task(CreateCompinTask(compin.component.name, compin, self.knowledge.client_support))
        logging.info(f"Created tasks for creation of {compin.component.name} on {compin.node_name}. ")

    def _add_mongo_init_task(self, compin: ManagedCompin):
        """
        :param compin: A compin that requires Mongo initialization
        """
        dc_name = self.knowledge.nodes[compin.node_name].data_center
        datacenter = self.knowledge.datacenters[dc_name]
        self.task_registry.add_task(SetMongoParametersTask(
            component=compin.component,
            instance_id=compin.id,
            key=int(compin.chain_id),
            mongos_ip=datacenter.mongos_ip
        ))

    def _create_compin_deletion_task(self, compin: ManagedCompin) -> None:
        """
        :param compin: A compin to create the task for
        :param parent_task: A task after which the compin creation task will be executed.
        """
        if compin.is_serving:
            if compin.phase < CompinPhase.FINALIZING:
                # Create a task for finalizing the instance
                self.task_registry.add_task(FinalizeInstanceTask(compin.component, compin.id))
        if compin.get_client() is not None:
            # Even though this compin is marked for deletion, it still serves the client. We need to wait until the
            # client disconnects before we can delete it.
            return
        self.task_registry.add_task(DeleteInstanceTask(compin.component.application.name, compin))
        logging.debug(f"Created tasks for deletion of compin {compin.id}.")