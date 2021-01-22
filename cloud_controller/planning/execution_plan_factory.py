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
from cloud_controller.planning.cloud_state_diff import get_compin_diff, get_dependency_diff
from cloud_controller.planning.k8s_generators import create_deployment_for_compin, create_service_for_compin


class ExecutionPlanFactory:
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
        self._execution_plan = protocols.ExecutionPlan()
        self._starting_task = None
        self._knowledge: Knowledge = knowledge
        self._actual_state: CloudState = self._knowledge.actual_state
        self._application: Application = self._knowledge.applications[application_name]

    def _new_execution_plan(self) -> None:
        """
        Discards the current execution plan, and starts the new one.
        """
        self._execution_plan = protocols.ExecutionPlan()
        # First we set a name of the namespace the application will be deployed into
        self._execution_plan.namespace = self._application.name
        # all execution plans start with an (empty) root task
        self._starting_task = self._add_new_task()
        self._starting_task.DO_NOTHING = 0

    def _add_new_task(self) -> protocols.Task:
        """
        Adds a new task to the execution plan under construction.
        :return: The newly created task.
        """
        task = self._execution_plan.tasks.add()
        task.id = len(self._execution_plan.tasks) - 1
        return task

    @staticmethod
    def _add_dependency(task_1: protocols.Task, task_2: protocols.Task) -> None:
        """
        Marks task_2 as being dependent on the task_1 (meaning that the execution of the task_2 cannot start before
        task_1 is completed).
        """
        task_1.successors.append(task_2.id)
        task_2.predecessors.append(task_1.id)
        logging.debug(f"Added dependency between {task_1.id} and {task_2.id}")

    def create_application_creation_plan(self) -> protocols.ExecutionPlan:
        """
        Creates an execution plan for new application, which includes creation of namespace, adding the image pull
        secret to that namespace, adding that application to the Client Controller, and creating and sharding the
        collections for all the Mongo-stateful components.
        :return: The constructed execution plan
        """
        self._new_execution_plan()
        database_record_task = self._add_new_task()
        database_record_task.ADD_APP_RECORD = self._application.name
        self._add_dependency(self._starting_task, database_record_task)
        namespace_task = self._add_new_task()
        self._add_dependency(database_record_task, namespace_task)
        namespace_task.CREATE_NAMESPACE.name = self._application.name
        if self._knowledge.client_support:
            cc_task = self._add_new_task()
            cc_task.ADD_APPLICATION_TO_CC.CopyFrom(self._application.get_pb_representation())
            self._add_dependency(namespace_task, cc_task)
        secret = self._knowledge.remove_secret(self._application.name)
        if secret is not None:
            secret_task = self._add_new_task()
            secret_task.CREATE_DOCKER_SECRET = secret
            self._add_dependency(namespace_task, secret_task)
        for component in self._application.list_managed_components():
            if component.statefulness == Statefulness.MONGO:
                sharding_task = self._add_new_task()
                sharding_task.SHARD_COLLECTION.db = self._application.name
                sharding_task.SHARD_COLLECTION.collection = component.name
                self._add_dependency(database_record_task, sharding_task)
        logging.info(f"Created an initial plan with {len(self._execution_plan.tasks)} tasks")
        return self._execution_plan

    def create_application_deletion_plan(self) -> protocols.ExecutionPlan:
        """
        Creates an execution plan for deleting an application, which includes disconnection of all the clients from
        the Client Controller, deletion of the Kubernetes namespace and deletion of the corresponding Mongo database.
        :return: The constructed execution plan
        """
        self._new_execution_plan()
        namespace_task = self._add_new_task()
        namespace_task.DELETE_NAMESPACE.name = self._application.name
        if self._knowledge.client_support:
            disconnection_task = self._add_new_task()
            disconnection_task.DISCONNECT_APPLICATION_CLIENTS = self._application.name
            self._add_dependency(self._starting_task, disconnection_task)
            self._add_dependency(disconnection_task, namespace_task)
        else:
            self._add_dependency(self._starting_task, namespace_task)
        database_record_task = self._add_new_task()
        database_record_task.DELETE_APP_RECORD = self._application.name
        self._add_dependency(namespace_task, database_record_task)

        mongo_components = []
        for component in self._application.list_managed_components():
            if component.statefulness == Statefulness.MONGO:
                mongo_components.append(component.name)
        if len(mongo_components) > 0:
            db_drop_task = self._add_new_task()
            db_drop_task.DROP_DATABASE.db = self._application.name
            for component_name in mongo_components:
                db_drop_task.DROP_DATABASE.collections.append(component_name)
            self._add_dependency(self._starting_task, db_drop_task)
            self._add_dependency(db_drop_task, database_record_task)

        logging.info(f"Created a deletion plan with {len(self._execution_plan.tasks)} tasks")
        return self._execution_plan

    def create_redeployment_plan(self, desired_state: CloudState) -> Optional[protocols.ExecutionPlan]:
        """
        Creates an execution plan that should bring the actual state of the application to the desired state. The plan
        includes tasks for creation and deletion of the compins, as well as tasks for setting the dependencies between
        the compins. May also include additional tasks for managing the state of the compins.
        :return: None if there is no difference between actual state and desired state, the constructed execution plan
                    otherwise.
        """
        self._new_execution_plan()

        create_instances, delete_instances, mongo_init_instances = \
            get_compin_diff(self._application.name, self._actual_state, desired_state)
        for compin in create_instances:
            self._create_compin_creation_task(compin, self._starting_task)
        for compin in delete_instances:
            self._create_compin_deletion_task(compin, self._starting_task)
        for compin in mongo_init_instances:
            self._add_mongo_init_task(compin)

        dependency_diff = get_dependency_diff(self._application.name, self._actual_state, desired_state)
        self._add_middleware_connection_tasks(dependency_diff)
        if len(self._execution_plan.tasks) <= 1:
            # if the execution plan contains only one task (DO_NOTHING), then we don't even need to return it
            return None
        logging.info(f"Created a redeployment plan with {len(self._execution_plan.tasks)} tasks")
        return self._execution_plan

    def _create_compin_creation_task(self, compin: ManagedCompin, parent_task) -> None:
        """
        :param compin: A compin to create the task for
        :param parent_task: A task after which the compin creation task will be executed.
        """
        deployment = create_deployment_for_compin(compin, not self._knowledge.client_support)
        creation_task = self._add_new_task()
        creation_task.CREATE_COMPIN.deployment = deployment
        creation_task.CREATE_COMPIN.service = create_service_for_compin(deployment, compin)
        creation_task.CREATE_COMPIN.component = compin.component.name
        creation_task.CREATE_COMPIN.id = compin.id
        creation_task.CREATE_COMPIN.node = compin.node_name
        creation_task.CREATE_COMPIN.clientId = compin.chain_id
        creation_task.CREATE_COMPIN.force = compin.force_keep

        self._add_dependency(parent_task, creation_task)
        logging.info(f"Created tasks for creation of {compin.component.name} on {compin.node_name}. "
                     f"Task ID: {creation_task.id}")

    def _add_mongo_init_task(self, compin: ManagedCompin):
        """
        :param compin: A compin that requires Mongo initialization
        """
        smp_task = self._add_new_task()
        smp_task.SET_MONGO_PARAMETERS.instanceType = compin.component.name
        smp_task.SET_MONGO_PARAMETERS.instanceId = compin.id
        smp_task.SET_MONGO_PARAMETERS.parameters.db = self._application.name
        smp_task.SET_MONGO_PARAMETERS.parameters.collection = compin.component.name
        smp_task.SET_MONGO_PARAMETERS.parameters.shardKey = int(compin.chain_id)
        dc_name = self._knowledge.nodes[compin.node_name].data_center
        smp_task.SET_MONGO_PARAMETERS.parameters.mongosIp = self._knowledge.datacenters[dc_name].mongos_ip
        self._add_dependency(self._starting_task, smp_task)

    def _create_compin_deletion_task(self, compin: ManagedCompin, parent_task: protocols.Task) -> None:
        """
        :param compin: A compin to create the task for
        :param parent_task: A task after which the compin creation task will be executed.
        """
        if compin.is_serving:
            if compin.phase < CompinPhase.FINALIZING:
                # Create a task for finalizing the instance
                finalize_task = self._add_new_task()
                finalize_task.FINALIZE_EXECUTION.type = compin.component.name
                finalize_task.FINALIZE_EXECUTION.id = compin.id
                self._add_dependency(parent_task, finalize_task)
                parent_task = finalize_task
        if compin.get_client() is not None:
            # Even though this compin is marked for deletion, it still serves the client. We need to wait until the
            # client disconnects before we can delete it.
            return
        deletion_task = self._add_new_task()
        deletion_task.DELETE_COMPIN.component = compin.component.name
        deletion_task.DELETE_COMPIN.id = compin.id
        deletion_task.DELETE_COMPIN.force = not compin.is_serving
        self._add_dependency(parent_task, deletion_task)
        logging.debug(f"Created tasks for deletion of compin {compin.id}. Task ID: {deletion_task.id}")

    def _add_middleware_connection_tasks(self, dependency_diff: List[Tuple[str, str, str, str, Optional[str]]]) -> None:
        """
        :param dependency_diff: A diff produced by get_dependency_diff()
        """
        # Go through dependencies, and create the corresponding "set dependency address" tasks:
        for (dependent_component, id_, dependency_name, dependency_id, old_dependency_ip) in dependency_diff:
            # Retrieve relevant compins from the actual state
            dependent_instance: Compin = self._actual_state.get_compin(self._application.name, dependent_component, id_)
            providing_instance = self._actual_state.get_compin(self._application.name, dependency_name, dependency_id)
            if providing_instance is None:
                continue
            assert isinstance(providing_instance, ManagedCompin)

            # Create a task for middleware connection
            middleware_task = self._add_new_task()
            if isinstance(dependent_instance, ManagedCompin):
                middleware_task.SET_DEPENDENCY_ADDRESS.dependentInstanceType = dependent_instance.component.name
                middleware_task.SET_DEPENDENCY_ADDRESS.dependentInstanceId = dependent_instance.id
                middleware_task.SET_DEPENDENCY_ADDRESS.providingInstanceType = providing_instance.component.name
                middleware_task.SET_DEPENDENCY_ADDRESS.providingInstanceId = providing_instance.id
            else:
                middleware_task.SET_CLIENT_DEPENDENCY.dependency = dependency_name
                middleware_task.SET_CLIENT_DEPENDENCY.clientId = id_
                middleware_task.SET_CLIENT_DEPENDENCY.clientType = dependent_component
                middleware_task.SET_CLIENT_DEPENDENCY.providingInstanceType = providing_instance.component.name
                middleware_task.SET_CLIENT_DEPENDENCY.providingInstanceId = providing_instance.id
            self._add_dependency(self._starting_task, middleware_task)
            logging.info(f"Created a task for connection of {dependent_component} with ID {id_} to "
                         f"{dependency_name} with ID {dependency_id}. Task ID: {middleware_task.id}")

            if providing_instance.component.statefulness == Statefulness.MONGO:
                move_chunk_task = self._add_new_task()
                move_chunk_task.MOVE_CHUNK.db = self._application.name
                move_chunk_task.MOVE_CHUNK.collection = dependency_name
                move_chunk_task.MOVE_CHUNK.key = int(providing_instance.chain_id)
                dc_name = self._knowledge.nodes[providing_instance.node_name].data_center
                move_chunk_task.MOVE_CHUNK.shard = self._knowledge.datacenters[dc_name].mongo_shard_name
                self._add_dependency(middleware_task, move_chunk_task)
