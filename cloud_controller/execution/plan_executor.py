"""
Contains PlanExecutor responsible for executing an individual execution plan
"""
import logging
import time
from multiprocessing.pool import ThreadPool
from time import perf_counter
from typing import Dict, Callable, Iterable, Tuple, List, Union

import grpc
import yaml

import cloud_controller.knowledge.knowledge_pb2 as protocols

from kubernetes import client

from cloud_controller import DEFAULT_SECRET_NAME, SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, IVIS_CORE_IP, IVIS_CORE_PORT
from cloud_controller.execution.mongo_controller import MongoController
from cloud_controller.knowledge.knowledge import Knowledge
import cloud_controller.knowledge.knowledge_pb2_grpc as servicers
from cloud_controller.knowledge.model import ManagedCompin, CompinPhase, UnmanagedCompin, IvisApplication
from cloud_controller.knowledge.user_equipment import UserEquipmentContainer
from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.ivis_pb2 import JobDescriptor
from cloud_controller.middleware.ivis_pb2_grpc import JobMiddlewareAgentStub
from cloud_controller.middleware.middleware_pb2 import Pong, DependencyAddress
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.probe_controller import ProbeController


PING_TIMEOUT = 2  # Seconds


class PlanExecutor:
    """
    Executes all the tasks in the plan in the order defined by their dependencies. Independent tasks are executed in
    parallel, dependent tasks wait for their predecessors to finish.

    Attributes:
        tasks:                  List of tasks to execute
        namespace:              Namespace on which K8S-related tasks have to be executed
        basic_api:              K8S core API
        extensions_api:         K8S Extensions API
        client_controller_stub  Stub of client controller used for client-related tasks
        knowledge               Reference to Knowledge
        mongo_controller        MongoController used for MongoDB tasks
        pool                    ThreadPool used for parallel execution of tasks
        task_handlers           Map of task types mapped to the methods that execute them
    """

    def __init__(self, tasks, namespace, basic_api, extensions_api, client_controller_stub, knowledge, mongo_controller,
                 pool, probe_controller):
        """
        :param tasks: List of tasks to execute
        :param namespace: Namespace on which K8S-related tasks have to be executed
        :param basic_api: K8S core API
        :param extensions_api: K8S Extensions API
        :param client_controller_stub: Stub of client controller used for client-related tasks
        :param knowledge: Reference to Knowledge
        :param mongo_controller: MongoController used for MongoDB tasks
        :param pool: ThreadPool used for parallel execution of tasks
        """
        self.tasks: List[protocols.Task] = tasks
        self.namespace: str = namespace
        self.basic_api: client.CoreV1Api = basic_api
        self.extensions_api: client.AppsV1Api = extensions_api
        self.client_controller_stub: servicers.ClientControllerInternalStub = client_controller_stub
        self.knowledge: Knowledge = knowledge
        self.mongo_controller: MongoController = mongo_controller
        self.pool: ThreadPool = pool
        self.task_handlers: Dict[str, Callable[[protocols.Task], bool]] = {
            'DO_NOTHING': self.do_not_execute,
            'CREATE_DEPLOYMENT': self.execute_create_deployment,
            'UPDATE_DEPLOYMENT': self.execute_update_deployment,
            'DELETE_DEPLOYMENT': self.execute_delete_deployment,
            'CREATE_SERVICE': self.execute_create_service,
            'DELETE_SERVICE': self.execute_delete_service,
            'CREATE_NAMESPACE': self.execute_create_namespace,
            'DELETE_NAMESPACE': self.execute_delete_namespace,
            'SET_DEPENDENCY_ADDRESS': self.execute_set_middleware_address,
            'SET_CLIENT_DEPENDENCY': self.execute_set_client_dependency,
            'DISCONNECT_APPLICATION_CLIENTS': self.execute_disconnect_application_clients,
            'CREATE_DOCKER_SECRET': self.execute_create_docker_secret,
            'DELETE_DOCKER_SECRET': self.execute_delete_docker_secret,
            'FINALIZE_EXECUTION': self.execute_finalize_execution,
            'SHARD_COLLECTION': self.execute_shard_collection,
            'MOVE_CHUNK': self.execute_move_chunk,
            'SET_MONGO_PARAMETERS': self.execute_set_mongo_parameters,
            'DROP_DATABASE': self.execute_drop_database,
            'ADD_APP_RECORD': self.execute_add_app_record,
            'DELETE_APP_RECORD': self.execute_delete_app_record,
            'ADD_APPLICATION_TO_CC': self.execute_add_app_to_cc,
            'DELETE_COMPIN': self.execute_delete_compin,
            'CREATE_COMPIN': self.execute_create_compin,
            'INITIALIZE_JOB': self.execute_initialize_job,
        }
        self.WAIT_BEFORE_RETRY = 2

        self.probe_controller: ProbeController = probe_controller

    def run(self) -> None:
        """
        Executes the plan.
        """
        logging.info(f"Executing an execution plan with {str(len(self.tasks))} tasks for application {self.namespace}")
        self.pool.apply(self.execute_task, (self.tasks[0],))
        logging.info(f"Plan for application {self.namespace}: execution successful")

    def wait_until_compins_are_ready(self, stubs: Iterable[Tuple[MiddlewareAgentStub, int]]) -> None:
        while True:
            try:
                all_ready = True
                for stub, value in stubs:
                    pong = stub.Ping(Pong(production=self.knowledge.client_support))
                    if pong.phase < value:
                        all_ready = False
                        break
                if all_ready:
                    break
            except grpc.RpcError:
                time.sleep(self.WAIT_BEFORE_RETRY)

    def ping_compin(self, stub: Union[MiddlewareAgentStub, JobMiddlewareAgentStub]) -> CompinPhase:
        """
        Checks whether managed compin answers to the ping, and its current phase.
        :param stub: MiddlewareAgentStub of compin to check.
        :return: Current phase of the compin.
        """
        start = perf_counter()
        try:
            pong_future = stub.Ping.future(Pong(production=self.knowledge.client_support), timeout=PING_TIMEOUT)
            while perf_counter() - start < PING_TIMEOUT:
                if pong_future.done():
                    pong = pong_future.result()
                    return CompinPhase(pong.phase)
            pong_future.cancel()
            return CompinPhase.CREATING
        except grpc.RpcError:
            logging.info(f"Ping time: {perf_counter() - start} seconds")
            return CompinPhase.CREATING

    def execute_task(self, task: protocols.Task) -> None:
        """
        Executes the task and all of its successors (if a successor does not have an unfinished predecessor). Uses
        thread pool to run the parallel tasks. If some successor task ends with an exception, raises that exception.
        """
        start = time.perf_counter()
        result = self.task_handlers[task.WhichOneof("task_type")](task)
        duration = time.perf_counter() - start
        logging.info(f"Task {task.WhichOneof('task_type')} with ID {task.id} completed in {duration} seconds. Result: "
                     f"{'Success' if result else 'Failure'}")
        if not result:
            # Task was not completed successfully, cannot proceed to successor tasks
            return
        successor_threads = []
        for successor_id in task.successors:
            successor = self.tasks[successor_id]
            successor.predecessors.remove(task.id)
            if len(successor.predecessors) == 0:
                result = self.pool.apply_async(self.execute_task, (successor,))
                successor_threads.append(result)
        for result in successor_threads:
            result.wait()
        for result in successor_threads:
            if not result.successful():
                result.get()

    def execute_add_app_record(self, task: protocols.Task) -> bool:
        """
        Adds a record about the application to MongoDB. These records are used by Avocado to check which apps are
        present in the cloud after Avocado restart.
        """
        self.mongo_controller.add_document(SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, {
            "type": "app",
            "name": task.ADD_APP_RECORD
        })
        self.knowledge.actual_state.add_application(self.knowledge.applications[self.namespace])
        logging.info(f"Record for app {task.ADD_APP_RECORD} added")
        return True

    def execute_delete_app_record(self, task: protocols.Task) -> bool:
        """
        Deletes a record about the application from MongoDB.
        """
        self.mongo_controller.delete_document(SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, {
            "type": "app",
            "name": task.DELETE_APP_RECORD
        })
        self.knowledge.actual_state.remove_application(self.namespace)
        logging.info(f"Record for app {task.DELETE_APP_RECORD} deleted")
        return True

    def execute_drop_database(self, task: protocols.Task) -> bool:
        """
        Drops a database from MongoDB
        """
        self.mongo_controller.drop_database(
            task.DROP_DATABASE.db,
            task.DROP_DATABASE.collections
        )
        logging.info(f"Database {task.DROP_DATABASE.db} dropped")
        return True


    def execute_shard_collection(self, task: protocols.Task) -> bool:
        """
        Creates and shards a MongoDB collection.
        """
        self.mongo_controller.shard_collection(
            task.SHARD_COLLECTION.db,
            task.SHARD_COLLECTION.collection
        )
        logging.info(f"Collection {task.SHARD_COLLECTION.db}.{task.SHARD_COLLECTION.collection} sharded")
        return True

    def execute_move_chunk(self, task: protocols.Task) -> bool:
        """
        See documentation to MongoController.move_chunk
        """
        self.mongo_controller.move_chunk(
            task.MOVE_CHUNK.db,
            task.MOVE_CHUNK.collection,
            task.MOVE_CHUNK.key,
            task.MOVE_CHUNK.shard
        )
        logging.info(f"Chunk {task.MOVE_CHUNK.key} of collection {task.MOVE_CHUNK.db}.{task.MOVE_CHUNK.collection} "
                     f"moved to shard {task.MOVE_CHUNK.shard}")
        return True

    def execute_set_mongo_parameters(self, task: protocols.Task) -> bool:
        """
        Tries to set mongo parameters for a Managed compin, if a compin have already started. Mongo parameters include
        IP address of a mongos instance, database and collection to use, and a personal shard key.
        :return: True if successful, false if compin does not respond.
        """
        compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.SET_MONGO_PARAMETERS.instanceType,
            task.SET_MONGO_PARAMETERS.instanceId
        )
        assert isinstance(compin, ManagedCompin)
        stub = connect_to_grpc_server(MiddlewareAgentStub, compin.ip, AGENT_PORT)
        phase = self.ping_compin(stub)
        if phase < CompinPhase.INIT:
            return False
        stub.SetMongoParameters(task.SET_MONGO_PARAMETERS.parameters)
        compin.mongo_init_completed = True
        logging.info(f"New mongo parameters were sent to {task.SET_MONGO_PARAMETERS.instanceType}:"
                     f"{task.SET_MONGO_PARAMETERS.instanceId}")
        return True

    def execute_finalize_execution(self, task: protocols.Task) -> bool:
        """
        Sends a FinalizeExecution command to a Managed compin.
        :return: True if successful or if compin had already finished, false if compin does not respond.
        """
        compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.FINALIZE_EXECUTION.type,
            task.FINALIZE_EXECUTION.id
        )
        assert isinstance(compin, ManagedCompin)
        stub = connect_to_grpc_server(MiddlewareAgentStub, compin.ip, AGENT_PORT)
        phase = self.ping_compin(stub)
        compin.phase = phase
        if phase < CompinPhase.INIT:
            return False
        elif compin.phase == CompinPhase.FINISHED:
            return True
        stub.FinalizeExecution(DependencyAddress())
        compin.phase = CompinPhase.FINALIZING
        logging.info(f"Finalize call was sent to the instance at {task.FINALIZE_EXECUTION.TransferFromIp}")
        return True

    def execute_create_docker_secret(self, task: protocols.Task) -> bool:
        """
        Adds a docker secret to the namespace.
        """
        secret = client.V1Secret(
            data={
                ".dockerconfigjson": task.CREATE_DOCKER_SECRET
            },
            metadata=client.V1ObjectMeta(name=DEFAULT_SECRET_NAME),
            type="kubernetes.io/dockerconfigjson"
        )
        self.basic_api.create_namespaced_secret(
            namespace=self.namespace,
            body=secret
        )
        logging.info(f"Docker secret for namespace {self.namespace} created.")
        return True

    def execute_delete_docker_secret(self, _: protocols.Task) -> bool:
        self.basic_api.delete_namespaced_secret(
            namespace=self.namespace,
            name=DEFAULT_SECRET_NAME
        )
        logging.info(f"Docker secret for namespace {self.namespace} deleted.")
        return True

    def execute_set_client_dependency(self, task: protocols.Task) -> bool:
        """
        Tries to set an IP address of a dependency for a client via Client Controller. If all of the client's
        dependencies have already been moved to a new datacenter, reports the full client handover. Will set the
        dependency only if the compin is ready to accept client connections (i.e. is in READY state)
        :return: True if successful, False if compin is not in READY state.
        """
        def check_and_report_handover(client: UnmanagedCompin, server: ManagedCompin):
            """
            Checks whether after setting _server_ to be the new dependency of the _client_, all dependencies of the
            client will be located in the same data center. If that is the case, reports the completed handover.
            """
            if len(client.list_dependencies()) == len(client.component.dependencies):
                new_dc_name = self.knowledge.nodes[server.node_name].data_center
                all_switched = True
                for compin in client.list_dependencies():
                    dc_name = self.knowledge.nodes[compin.node_name].data_center
                    if dc_name != new_dc_name:
                        all_switched = False
                        break
                if all_switched:
                    old_dependency = client.get_dependency(server.component.name)
                    if old_dependency is not None:
                        old_dc_name = self.knowledge.nodes[old_dependency.node_name].data_center
                    else:
                        old_dc_name = ""
                    UserEquipmentContainer.report_handover(client.ip, client.component.application.name,
                                                           client.component.name, old_dc_name, new_dc_name)

        server = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.SET_CLIENT_DEPENDENCY.providingInstanceType,
            task.SET_CLIENT_DEPENDENCY.providingInstanceId
        )
        assert isinstance(server, ManagedCompin)
        client = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.SET_CLIENT_DEPENDENCY.clientType,
            task.SET_CLIENT_DEPENDENCY.clientId
        )
        assert isinstance(client, UnmanagedCompin)
        dependency = protocols.ClientDependencyDescription()
        dependency.dependencyAddress.name = server.component.name
        dependency.dependencyAddress.ip = server.ip
        dependency.application = self.namespace
        dependency.clientType = client.component.name
        dependency.clientId = client.id
        stub = connect_to_grpc_server(MiddlewareAgentStub, server.ip, AGENT_PORT)
        phase = self.ping_compin(stub)
        server.phase = phase
        if phase < CompinPhase.READY:
            return False
        self.client_controller_stub.SetClientDependency(dependency),
        logging.info(f"Dependency {server.component.name} for client {client.id} set to {server.ip}.")

        # If all dependencies are now located in the same DC, we have to report full handover
        check_and_report_handover(client, server)

        # Finally, change the record in the actual state model
        client.set_dependency(server)
        return True

    def execute_disconnect_application_clients(self, task: protocols.Task) -> bool:
        """
        Signals to the client controller that it has to disconnect all the clients in the given application, and delete
        the application from its database.
        """
        self.client_controller_stub.CloseApplicationChannels(
            protocols.ApplicationName(name=task.DISCONNECT_APPLICATION_CLIENTS)
        )
        logging.info(f"Disconnected all clients of application {task.DISCONNECT_APPLICATION_CLIENTS}")
        return True

    def execute_add_app_to_cc(self, task: protocols.Task) -> bool:
        """
        Signals to the client controller that it has to add an application to its database and start accepting new
        clients for that application.
        """
        self.client_controller_stub.AddNewApplication(task.ADD_APPLICATION_TO_CC)
        logging.info(f"Sent application descriptor for {self.namespace} app to Client Controller")
        return True

    def execute_create_deployment(self, task: protocols.Task) -> bool:
        deployment = yaml.load(task.CREATE_DEPLOYMENT.deployment)
        api_response = self.extensions_api.create_namespaced_deployment(
            body=deployment,
            namespace=self.namespace
        )
        logging.info(f"Deployment {task.CREATE_DEPLOYMENT.name} created. status='%s'" % str(api_response.status))
        return True

    def execute_delete_deployment(self, task: protocols.Task) -> bool:
        options = client.V1DeleteOptions()
        options.propagation_policy = 'Background'
        api_response = self.extensions_api.delete_namespaced_deployment(
            name=task.DELETE_DEPLOYMENT,
            namespace=self.namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Deployment {task.DELETE_DEPLOYMENT} deleted. status='%s'" % str(api_response.status))
        return True

    def execute_update_deployment(self, task: protocols.Task) -> bool:
        deployment = yaml.load(task.UPDATE_DEPLOYMENT.deployment)
        api_response = self.extensions_api.patch_namespaced_deployment(
            name=task.UPDATE_DEPLOYMENT.name,
            namespace=self.namespace,
            body=deployment
        )
        logging.info(f"Deployment {task.UPDATE_DEPLOYMENT.name} updated. status='%s'" % str(api_response.status))
        return True

    def execute_create_service(self, task: protocols.Task) -> bool:
        svc_yaml = yaml.load(task.CREATE_SERVICE.service)
        api_response = self.basic_api.create_namespaced_service(namespace=self.namespace, body=svc_yaml)
        compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.CREATE_SERVICE.component,
            task.CREATE_SERVICE.instanceId
        )
        compin.ip = api_response.spec.cluster_ip
        logging.info("Service created. status='%s'" % str(api_response.status))
        return True

    def execute_initialize_job(self, task: protocols.Task) -> bool:
        job_id = task.INITIALIZE_JOB
        compin = self.knowledge.actual_state.get_job_compin(job_id)
        assert compin is not None
        stub: JobMiddlewareAgentStub = connect_to_grpc_server(JobMiddlewareAgentStub, compin.ip, AGENT_PORT)
        phase = self.ping_compin(stub)
        if phase < CompinPhase.INIT:
            return False
        app = self.knowledge.applications[task.INITIALIZE_JOB]
        assert isinstance(app, IvisApplication)
        init_data = JobDescriptor(
            job_id=app.name,
            code=app.code,
            parameters=app.parameters,
            config=app.config,
            minimal_interval=app.interval,
            ivis_core_ip=IVIS_CORE_IP,
            ivis_core_port=IVIS_CORE_PORT,
            access_token=self.knowledge.ivis_access_token,
            signal_set=app.signal_set,
            execution_time_signal=app.execution_time_signal,
            run_count_signal=app.run_count_signal,
            run_count=app.run_count
        )
        stub.InitializeJob(init_data)
        compin.phase = CompinPhase.READY
        logging.info(f"Job {task.INITIALIZE_JOB} was successfully initialized")
        return True

    def execute_create_compin(self, task: protocols.Task) -> bool:
        """
        Creates a Kubernetes deployment and a Kubernetes service for the compin. Adds the compin to the actual state
        with phase=CREATING.
        """
        compin = ManagedCompin(
            component=self.knowledge.applications[self.namespace].components[task.CREATE_COMPIN.component],
            id_=task.CREATE_COMPIN.id,
            node=task.CREATE_COMPIN.node,
            chain_id=task.CREATE_COMPIN.clientId
        )
        if task.CREATE_COMPIN.force:
            compin.set_force_keep()
        compin.phase = CompinPhase.CREATING
        deployment_yaml = yaml.load(task.CREATE_COMPIN.deployment)
        api_response = self.extensions_api.create_namespaced_deployment(body=deployment_yaml, namespace=self.namespace)
        logging.info(f"Deployment {compin.deployment_name()} created")

        service_yaml = yaml.load(task.CREATE_COMPIN.service)
        api_response = self.basic_api.create_namespaced_service(namespace=self.namespace, body=service_yaml)
        compin.ip = api_response.spec.cluster_ip
        logging.info(f"Service {compin.service_name()} created")
        self.knowledge.actual_state.add_instance(compin)
        self.probe_controller.add_compin(compin)
        return True

    def execute_delete_compin(self, task: protocols.Task) -> bool:
        """
        Deletes a Kubernetes deployment and a Kubernetes service for the compin. Deletes the compin from the actual
        state. Will perform these actions only if the compin had already reached the FINISHED state.
        :return: True if successful, False if compin is not in FINISHED state yet.
        """
        compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.DELETE_COMPIN.component,
            task.DELETE_COMPIN.id
        )
        assert isinstance(compin, ManagedCompin)
        self.probe_controller.remove_compin(compin)
        if not task.DELETE_COMPIN.force:
            stub = connect_to_grpc_server(MiddlewareAgentStub, compin.ip, AGENT_PORT)
            phase = self.ping_compin(stub)
            if phase < CompinPhase.FINISHED:
                return False
        # Delete the corresponding deployment:
        options = client.V1DeleteOptions()
        options.propagation_policy = 'Background'
        api_response = self.extensions_api.delete_namespaced_deployment(
            name=compin.deployment_name(),
            namespace=self.namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Deployment {compin.deployment_name()} deleted")
        # Delete the corresponding service:
        api_response = self.basic_api.delete_namespaced_service(
            name=compin.service_name(),
            namespace=self.namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Service {compin.service_name()} deleted")
        self.knowledge.actual_state.delete_instance(self.namespace, compin.component.name, compin.id)
        return True

    def execute_delete_service(self, task: protocols.Task) -> bool:
        options = client.V1DeleteOptions()
        options.propagation_policy = 'Background'
        api_response = self.basic_api.delete_namespaced_service(
            name=task.DELETE_SERVICE,
            namespace=self.namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Service {task.DELETE_SERVICE} deleted. status={api_response.status}")
        return True

    def execute_create_namespace(self, task: protocols.Task) -> bool:
        """
        Creates a Kubernetes namespace in the cluster.
        """
        namespace = client.V1Namespace()
        namespace.metadata = client.V1ObjectMeta()
        namespace.metadata.name = task.CREATE_NAMESPACE.name
        api_response = self.basic_api.create_namespace(namespace)
        logging.info(f'Namespace {task.CREATE_NAMESPACE.name} created.  status=%s' % str(api_response.status))
        return True

    def execute_delete_namespace(self, task: protocols.Task) -> bool:
        """
        Deletes a Kubernetes namespace from the cluster. Deletion of a namespace also deletes all the resources
        associated with it (deployments, services, etc.). Deletion happens in background, thus it may take some time
        until all the resources are cleaned.
        """
        options = client.V1DeleteOptions()
        options.propagation_policy = 'Background'
        api_response = self.basic_api.delete_namespace(
            name=task.DELETE_NAMESPACE.name,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f'Namespace {task.DELETE_NAMESPACE.name} deleted. status={api_response}')
        return True

    def execute_set_middleware_address(self, task: protocols.Task) -> bool:
        """
        Sends the IP address of a dependency to the MiddlewareAgent of the compin. The task will be performed only if
        the dependent compin had already reached the INIT state, and the providing compin had reached READY state.
        :return: True if successful, False if one of the compins is not in a required state.
        """
        providing_compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.SET_DEPENDENCY_ADDRESS.providingInstanceType,
            task.SET_DEPENDENCY_ADDRESS.providingInstanceId
        )
        dependent_compin = self.knowledge.actual_state.get_compin(
            self.namespace,
            task.SET_DEPENDENCY_ADDRESS.dependentInstanceType,
            task.SET_DEPENDENCY_ADDRESS.dependentInstanceId
        )
        assert isinstance(providing_compin, ManagedCompin)
        assert isinstance(dependent_compin, ManagedCompin)
        providing_stub = connect_to_grpc_server(MiddlewareAgentStub, providing_compin.ip, AGENT_PORT)
        dependent_stub = connect_to_grpc_server(MiddlewareAgentStub, dependent_compin.ip, AGENT_PORT)
        phase_providing = self.ping_compin(providing_stub)
        providing_compin.phase = phase_providing
        phase_dependent = self.ping_compin(dependent_stub)
        dependent_compin.phase = phase_dependent
        if phase_providing < CompinPhase.READY or phase_dependent < CompinPhase.INIT:
            return False
        address = DependencyAddress(name=task.SET_DEPENDENCY_ADDRESS.providingInstanceType, ip=providing_compin.ip)
        dependent_stub.SetDependencyAddress(address)
        # Finally, change the record in the actual state model
        self.knowledge.actual_state.set_dependency(self.namespace, dependent_compin.component.name, dependent_compin.id,
                                                   providing_compin.component.name, providing_compin.id)

        logging.info(f"Dependency address for instance at {providing_compin.ip} set to {dependent_compin.ip}.")
        return True

    def do_not_execute(self, _: protocols.Task) -> bool:
        logging.info(f"Execution started.")
        return True


class FakeExecutor(PlanExecutor):
    """
    A fake PlanExecutor that just prints the tasks instead of executing them.
    """

    def __init____init__(self, tasks, namespace, basic_api, extensions_api, cc_stub, knowledge):
        super().__init__(tasks, namespace, basic_api, extensions_api, cc_stub, knowledge)
        self.task_separator = \
            "----------------------------------------------------------------------------------------------------------"
        self.set_separator = \
        """
        ****************************************************************************************************************
        ----------------------------------------------------------------------------------------------------------------
        ----------------------------------------------------------------------------------------------------------------
        ----------------------------------------------------------------------------------------------------------------
        ----------------------------------------------------------------------------------------------------------------
        ----------------------------------------------------------------------------------------------------------------
        ****************************************************************************************************************
        """

    def run(self) -> None:
        print("Executing an execution plan with " + str(len(self.tasks)) + " tasks")
        self.execute_task(self.tasks[0])
        print("execution successful")
        print(self.set_separator)

    def execute_task(self, task: protocols.Task) -> None:
        print(self.task_separator)
        print(task)
        print("Task #%i of type %s completed" % (task.id, task.WhichOneof("task_type")))
        print(self.task_separator)
        for successor_id in task.successors:
            successor = self.tasks[successor_id]
            successor.predecessors.remove(task.id)
            if len(successor.predecessors) == 0:
                self.execute_task(successor)
