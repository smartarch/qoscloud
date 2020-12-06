"""
This module contains the ExecutionPlanFactory class, which is responsible for the creation of execution plans for
separate applications
"""
import logging
from typing import List, Tuple, Optional

from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ManagedCompin, CloudState, Compin, \
    Application, Statefulness, CompinPhase
from cloud_controller.planning.execution_plan_factory import ExecutionPlanFactory


class JobExecutionPlanFactory(ExecutionPlanFactory):
    """
    A factory class which creates execution plans for a given application. There are three different types of plans
     that it can create:
        (1) application creation plan - needed only once, after the application was submitted,
        (2) application deletion plan - needed only once, when the application is being deleted,
        (3) redeployment plan - created throughout the existence of the application, every time when there is some
            difference between the actual and the desired state of the application.

    The individual task types are documented with the corresponding methods of the PlanExecutor

    Attributes:
        _execution_plan The execution plan under construction
        _starting_task  The first (initial) task of the current execution plan
        _knowledge      Reference to the Knowledge
        _actual_state   Reference to the actual state
        _application    Reference to the application for which the instance of ExecutionPlanFactory is responsible
    """

    def __init__(self, application_name: str, knowledge: Knowledge):
        """
        :param application_name: The name of an application. This parameter is necessary, because
               an execution plan factory is specific for each application.
        :param knowledge: Reference to knowledge shared across different modules.
        """
        super().__init__(application_name, knowledge)
        self._job_deployed: bool = False

    def create_redeployment_plan(self, desired_state: CloudState) -> Optional[protocols.ExecutionPlan]:
        """
        Creates an execution plan that should bring the actual state of the application to the desired state. The plan
        includes tasks for creation and deletion of the compins, as well as tasks for setting the dependencies between
        the compins. May also include additional tasks for managing the state of the compins.
        :return: None if there is no difference between actual state and desired state, the constructed execution plan
                    otherwise.
        """
        self._new_execution_plan()
        job_id = self._application.name
        as_compin = self._knowledge.actual_state.get_unique_compin(job_id)
        ds_compin = desired_state.get_unique_compin(job_id)

        if as_compin is None and ds_compin is not None:
            self._create_compin_creation_task(ds_compin, self._starting_task)
            self._job_deployed = True
            logging.info(f"Created a job deployment plan with {len(self._execution_plan.tasks)} tasks")
        elif as_compin is not None and as_compin.phase < CompinPhase.READY:
            self._create_instance_init_task(as_compin, self._starting_task)
        else:
            return None

        logging.info(f"Created a job deployment plan with {len(self._execution_plan.tasks)} tasks")
        return self._execution_plan

    def _create_instance_init_task(self, compin: ManagedCompin, parent_task: protocols.Task) -> None:
        init_task = self._add_new_task()
        init_task.INITIALIZE_JOB = self._application.name
        self._add_dependency(parent_task, init_task)
        logging.debug(f"Created job initialization task for job {compin.id}. Task ID: {init_task.id}")
