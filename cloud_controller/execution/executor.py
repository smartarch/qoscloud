"""
Contains the Executor class responsible for the execution phase.
"""

from multiprocessing.pool import ThreadPool
from typing import Iterable

from kubernetes import client

from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT
from cloud_controller.execution.mongo_controller import MongoController
from cloud_controller.knowledge import knowledge_pb2_grpc as servicers
from cloud_controller.execution.plan_executor import PlanExecutor
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.knowledge_pb2 import ExecutionPlan
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.probe_controller import ProbeController


class Executor:
    """
    Executes the execution plans created by Planner. Can execute the plans sequentially or in parallel (with thread
    pool). For each plan under execution creates an instance of the PlanExecutor class.

    Attributes:
        knowledge           A reference to the Knowledge
        pool                ThreadPool shared by all PlanExecutor instances
        client_controller   A stub of the client controller. Some tasks are delegated to it.
        mongo_controller    Mongo controller, responsible for execution of Mongo-related tasks.
    """

    def __init__(self, knowledge: Knowledge, pool: ThreadPool, mongos_ip: str):
        """
        :param knowledge: A reference to the Knowledge
        :param pool: ThreadPool shared by all PlanExecutor instances
        :param mongos_ip: IP of the mongos instance to connect to
        """
        self.knowledge = knowledge
        self.pool = pool
        self.client_controller = connect_to_grpc_server(servicers.ClientControllerInternalStub,
                                                        CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT)
        self.mongo_controller = MongoController(mongos_ip)
        self.mongo_controller.start()
        self.probe_controller = ProbeController(self.knowledge)
        if self.knowledge.client_support:
            self.probe_controller.start()

    def execute_plan(self, execution_plan: ExecutionPlan) -> None:
        """
        Executes the given execution plan.
        :param execution_plan: plan to execute.
        """
        core_api = client.CoreV1Api()
        extensions_api = client.AppsV1Api()
        plan_executor = PlanExecutor(
            tasks=execution_plan.tasks,
            namespace=execution_plan.namespace,
            basic_api=core_api,
            extensions_api=extensions_api,
            client_controller_stub=self.client_controller,
            knowledge=self.knowledge,
            mongo_controller=self.mongo_controller,
            pool=self.pool,
            probe_controller=self.probe_controller
        )
        plan_executor.run()

    def execute_plans_in_parallel(self, execution_plans: Iterable[ExecutionPlan]) -> int:
        """
        Executes all the provided execution plans in parallel with a thread pool.
        :param execution_plans: plans to execute.
        :return: Number of executed plans.
        """
        core_api = client.CoreV1Api()
        extensions_api = client.AppsV1Api()
        executors = []
        plan_futures = []
        for execution_plan in execution_plans:
            plan_executor = PlanExecutor(
                tasks=execution_plan.tasks,
                namespace=execution_plan.namespace,
                basic_api=core_api,
                extensions_api=extensions_api,
                client_controller_stub=self.client_controller,
                knowledge=self.knowledge,
                mongo_controller=self.mongo_controller,
                pool=self.pool,
                probe_controller=self.probe_controller
            )
            future_result = self.pool.apply_async(plan_executor.run, ())
            executors.append(plan_executor)
            plan_futures.append(future_result)
        for future in plan_futures:
            future.wait()
        for future in plan_futures:
            if not future.successful():
                future.get()
        return len(plan_futures)


class FakeExecutor(Executor):
    """
    A fake Executor for testing. Instead of executing the plans, it just prints them.
    """
    def execute_plan(self, execution_plan):
        print("\n\n\n--------------------EXECUTION PLAN--------------------\n")
        for task in execution_plan.tasks:
            print("--------------------TASK--------------------")
            print(task)
