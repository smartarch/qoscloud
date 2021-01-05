"""
Contains ExtensionManager, a class for creating custom versions of Adaptation Controller.
"""
from enum import Enum
from multiprocessing import Pool, Lock
from typing import Callable, Any

from multiprocessing.pool import ThreadPool

import cloud_controller
from cloud_controller import PRODUCTION_KUBECONFIG, PRODUCTION_MONGOS_SERVER_IP, THREAD_COUNT
from cloud_controller.adaptation_controller import AdaptationController
from cloud_controller.analysis.statistical_predictor import StatisticalPredictor
from cloud_controller.analyzer.constraint import PredictConstraint, RunningNodeConstraint, InstanceDeploymentConstraint, \
    RedeploymentConstraint, ChainInDatacenterConstraint, NodeSeparationConstraint
from cloud_controller.analyzer.csp_analyzer import CSPAnalyzer
from cloud_controller.analyzer.objective_function import DefaultObjectiveFunction
from cloud_controller.knowledge.network_topology import NetworkTopology
from cloud_controller.knowledge.user_equipment import UEManagementPolicy
from cloud_controller.monitoring.monitor import TopLevelMonitor, ApplicationMonitor, ClientMonitor, UEMonitor, \
    KubernetesMonitor, Monitor
from cloud_controller.planner.application_creation_planner import ApplicationCreationPlanner
from cloud_controller.planner.application_removal_planner import ApplicationRemovalPlanner
from cloud_controller.planner.dependency_planner import DependencyPlanner
from cloud_controller.planner.deployment_planner import InstanceDeploymentPlanner
from cloud_controller.planner.top_planner import Planner, TopLevelPlanner
from cloud_controller.task_executor.registry import TaskRegistry
from cloud_controller.task_executor.task_executor import TaskExecutor
from cloud_controller.tasks.instance_management import *
from cloud_controller.tasks.client_controller import *
from cloud_controller.tasks.statefulness import *
from cloud_controller.tasks.kubernetes import *
from cloud_controller.tasks.middleware import *



class _Phase(Enum):
    INIT = 1
    DONE = 2




class ExtensionManager:
    """
    Builds a custom version of Adaptation Controller. Can set various variables, class members and hook methods for
    Adaptation Controller and the objects used inside of it (such as Analyzer, Executor, etc.). This class is one of
    the main ways how to customize Avocado (the other being configuration files).
    """

    def __init__(self):
        self.knowledge = Knowledge()
        self.task_registry = TaskRegistry(self.knowledge)
        self._pool = ThreadPool(processes=THREAD_COUNT)
        self._monitor = None
        self._analyzer = None
        self._planner = None
        self._executor = None
        self._predictor = StatisticalPredictor(self.knowledge)
        self._kubeconfig_file = PRODUCTION_KUBECONFIG
        self._mongos_ip: str = PRODUCTION_MONGOS_SERVER_IP
        self._phase = _Phase.INIT

    def get_default_monitor(self) -> Monitor:
        self._monitor = TopLevelMonitor(self.knowledge)
        self._monitor.add_monitor(ApplicationMonitor(self.knowledge))
        self._monitor.add_monitor(ClientMonitor(self.knowledge))
        self._monitor.add_monitor(UEMonitor(self.knowledge))
        self._monitor.add_monitor(KubernetesMonitor(self.knowledge))
        self._monitor.add_monitor(self.task_registry)
        return self._monitor

    def get_default_analyzer(self) -> CSPAnalyzer:
        self._analyzer = CSPAnalyzer(self.knowledge, self._pool)
        self._analyzer.add_constraint(PredictConstraint(self.knowledge, self._predictor))
        self._analyzer.add_constraint(RunningNodeConstraint(self.knowledge))
        self._analyzer.add_constraint(InstanceDeploymentConstraint())
        self._analyzer.add_constraint(RedeploymentConstraint())
        self._analyzer.add_constraint(ChainInDatacenterConstraint(self.knowledge))
        self._analyzer.add_constraint(NodeSeparationConstraint(self.knowledge))
        self._analyzer.set_objective_function(DefaultObjectiveFunction())
        return self._analyzer

    def get_default_planner(self) -> Planner:
        self._planner = TopLevelPlanner(self.knowledge, self.task_registry)
        self._planner.add_planner(ApplicationCreationPlanner(self.knowledge, self.task_registry))
        self._planner.add_planner(ApplicationRemovalPlanner(self.knowledge, self.task_registry))
        self._planner.add_planner(InstanceDeploymentPlanner(self.knowledge, self.task_registry))
        self._planner.add_planner(DependencyPlanner(self.knowledge, self.task_registry))
        return self._planner

    def get_default_executor(self) -> TaskExecutor:
        self._executor = TaskExecutor(self.knowledge, self.task_registry, self._pool)
        self._executor.add_execution_context(ExecutionContext(self.knowledge))
        self._executor.add_execution_context(KubernetesExecutionContext(self.knowledge, self._kubeconfig_file))
        self._executor.add_execution_context(ClientControllerExecutionContext(self.knowledge))
        self._executor.add_execution_context(StatefulnessControllerExecutionContext(self.knowledge, self._mongos_ip))
        self._executor.add_task_type(CreateInstanceTask, KubernetesExecutionContext)
        self._executor.add_task_type(DeleteInstanceTask, KubernetesExecutionContext)
        self._executor.add_task_type(AddApplicationToCCTask, ClientControllerExecutionContext)
        self._executor.add_task_type(DeleteApplicationFromCCTask, ClientControllerExecutionContext)
        self._executor.add_task_type(SetClientDependencyTask, ClientControllerExecutionContext)
        self._executor.add_task_type(CreateNamespaceTask, KubernetesExecutionContext)
        self._executor.add_task_type(DeleteNamespaceTask, KubernetesExecutionContext)
        self._executor.add_task_type(CreateDockersecretTask, KubernetesExecutionContext)
        self._executor.add_task_type(DeleteDockersecretTask, KubernetesExecutionContext)
        self._executor.add_task_type(SetMiddlewareAddressTask, ExecutionContext)
        self._executor.add_task_type(FinalizeInstanceTask, ExecutionContext)
        self._executor.add_task_type(SetMongoParametersTask, ExecutionContext)
        self._executor.add_task_type(InitializeInstanceTask, ExecutionContext)
        self._executor.add_task_type(DropDatabaseTask, StatefulnessControllerExecutionContext)
        self._executor.add_task_type(AddAppRecordTask, StatefulnessControllerExecutionContext)
        self._executor.add_task_type(DeleteAppRecordTask, StatefulnessControllerExecutionContext)
        self._executor.add_task_type(ShardCollectionTask, StatefulnessControllerExecutionContext)
        self._executor.add_task_type(MoveChunkTask, StatefulnessControllerExecutionContext)
        return self._executor

    def get_default_knowledge(self) -> Knowledge:
        return self.knowledge

    def get_adaptation_ctl(self) -> AdaptationController:
        """
        Returns an instance of Adaptation Controller with all the customizations that were set. After this method
        is called, this instance of ExtensionManager is not usable anymore (all the subsequent method calls will raise
        an exception).
        :return: a customized instance of Adaptation Controller
        """
        def task():
            self._phase = _Phase.DONE
            if self.knowledge is None:
                self.knowledge = self.get_default_knowledge()
            if self._monitor is None:
                self._monitor = self.get_default_monitor()
            if self._analyzer is None:
                self._analyzer = self.get_default_analyzer()
            if self._planner is None:
                self._planner = self.get_default_planner()
            if self._executor is None:
                self._executor = self.get_default_executor()
            return AdaptationController(
                knowledge=self.knowledge,
                monitor=self._monitor,
                analyzer=self._analyzer,
                planner=self._planner,
                executor=self._executor,
            )
        return self._check_and_execute(task)

    def _check_and_execute(self, task: Callable) -> Any:
        if self._phase == _Phase.INIT:
            return task()
        else:
            raise Exception("Extension manager has already returned the object")

    def set_mongos_ip(self, mongos_ip: str) -> None:
        def task():
            self._mongos_ip = mongos_ip
        self._check_and_execute(task)

    def set_monitor(self, monitor: TopLevelMonitor) -> None:
        def task():
            monitor.add_monitor(self.task_registry)
            self._monitor = monitor
        self._check_and_execute(task)

    def set_analyzer(self, analyzer) -> None:
        def task():
            self._analyzer = analyzer
        self._check_and_execute(task)

    def set_planner(self, planner) -> None:
        def task():
            self._planner = planner
        self._check_and_execute(task)

    def set_executor(self, executor) -> None:
        def task():
            self._executor = executor
        self._check_and_execute(task)

    def set_predictor(self, predictor) -> None:
        def task():
            self._predictor = predictor
        self._check_and_execute(task)

    def set_client_support(self, enabled: bool) -> None:
        def task():
            self.knowledge.client_support = enabled
        self._check_and_execute(task)

    def set_network_topology(self, network_topology: NetworkTopology) -> None:
        def task():
            self.knowledge.network_topology = network_topology
        self._check_and_execute(task)

    def set_kubeconfig(self, kubernetes_config: str) -> None:
        def task():
            self._kubeconfig_file = kubernetes_config
        self._check_and_execute(task)

    def set_default_ue_management_policy(self, policy: UEManagementPolicy):
        def task():
            cloud_controller.DEFAULT_UE_MANAGEMENT_POLICY = policy
        self._check_and_execute(task)

    def set_report_handover_hook(self, hook) -> None:
        def task():
            UserEquipmentContainer.report_handover = hook
        self._check_and_execute(task)
