"""
Contains ExtensionManager, a class for creating custom versions of Adaptation Controller.
"""
from enum import Enum
from typing import Type, Callable, Any

import cloud_controller
from cloud_controller import PRODUCTION_KUBECONFIG, DEFAULT_PREDICTOR_CONFIG, PRODUCTION_MONGOS_SERVER_IP, THREAD_COUNT
from cloud_controller.adaptation_controller import AdaptationController
from cloud_controller.analysis.csp_solver.solver import CSPSolver
from cloud_controller.analysis.predictor import StraightforwardPredictorModel
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.network_topology import NetworkTopology
from cloud_controller.knowledge.user_equipment import UserEquipmentContainer, UEManagementPolicy


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
        self._monitor = None
        self._analyzer = None
        self._planner = None
        self._executor = None
        self._solver_class = CSPSolver
        self._predictor = StraightforwardPredictorModel(DEFAULT_PREDICTOR_CONFIG)
        self._kubeconfig_file = PRODUCTION_KUBECONFIG
        self._mongos_ip: str = PRODUCTION_MONGOS_SERVER_IP
        self._thread_count: int = THREAD_COUNT
        self._phase = _Phase.INIT

    def get_adaptation_ctl(self) -> AdaptationController:
        """
        Returns an instance of Adaptation Controller with all the customizations that were set. After this method
        is called, this instance of ExtensionManager is not usable anymore (all the subsequent method calls will raise
        an exception).
        :return: a customized instance of Adaptation Controller
        """
        def task():
            self._phase = _Phase.DONE
            return AdaptationController(
                kubeconfig_file=self._kubeconfig_file,
                knowledge=self.knowledge,
                monitor=self._monitor,
                analyzer=self._analyzer,
                planner=self._planner,
                executor=self._executor,
                solver_class=self._solver_class,
                predictor=self._predictor,
                mongos_ip=self._mongos_ip,
                thread_count=self._thread_count
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

    def set_thread_count(self, thread_count: int) -> None:
        def task():
            self._thread_count = thread_count
        self._check_and_execute(task)

    def set_network_topology(self, network_topology: NetworkTopology) -> None:
        def task():
            self.knowledge.network_topology = network_topology
        self._check_and_execute(task)

    def set_report_handover_hook(self, hook) -> None:
        def task():
            UserEquipmentContainer.report_handover = hook
        self._check_and_execute(task)

    def set_monitor(self, monitor) -> None:
        def task():
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

    def set_solver_class(self, solver_class: Type) -> None:
        def task():
            self._solver_class = solver_class
        self._check_and_execute(task)

    def set_kubeconfig(self, kubernetes_config: str) -> None:
        def task():
            self._kubeconfig_file = kubernetes_config
        self._check_and_execute(task)

    def set_predictor(self, predictor) -> None:
        def task():
            self._predictor = predictor
        self._check_and_execute(task)

    def set_client_support(self, enabled: bool) -> None:
        def task():
            self.knowledge.client_support = enabled
        self._check_and_execute(task)

    def set_default_ue_management_policy(self, policy: UEManagementPolicy):
        def task():
            cloud_controller.DEFAULT_UE_MANAGEMENT_POLICY = policy
        self._check_and_execute(task)
