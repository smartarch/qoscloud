"""
The module contains Adaptation Controller class that runs all the steps of the adaptation (MAPE-K) loop sequentially,
in a single process.
"""
from multiprocessing.pool import ThreadPool
from time import perf_counter
from typing import Iterable, Type

import logging

import threading
from kubernetes import config

from cloud_controller import PRODUCTION_KUBECONFIG, THREAD_COUNT, PRODUCTION_MONGOS_SERVER_IP, DEFAULT_PREDICTOR_CONFIG, \
    PARALLEL_EXECUTION
from cloud_controller.analysis.analyzer import Analyzer
from cloud_controller.analysis.csp_solver.solver import CSPSolver
from cloud_controller.analysis.predictor import Predictor, StraightforwardPredictorModel
from cloud_controller.analysis.predictor_interface.predictor_service import StatisticalPredictor
from cloud_controller.analyzer.csp_analyzer import CSPAnalyzer
from cloud_controller.cleanup import ClusterCleaner
from cloud_controller.execution.executor import Executor
from cloud_controller.ivis.ivis_interface import IvisInterface
from cloud_controller.ivis.ivis_mock import IVIS_INTERFACE_HOST, IVIS_INTERFACE_PORT
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.ivis.ivis_pb2_grpc import add_IvisInterfaceServicer_to_server
from cloud_controller.monitoring.monitor import Monitor, TopLevelMonitor, ApplicationMonitor, KubernetesMonitor, \
    UEMonitor, ClientMonitor
from cloud_controller.planner.top_planner import Planner
from cloud_controller.planning.execution_planner import ExecutionPlanner
from cloud_controller.middleware.helpers import setup_logging, start_grpc_server
from cloud_controller.knowledge import knowledge_pb2 as protocols
from cloud_controller.task_executor.task_executor import TaskExecutor


def log_phase_duration():
    """
    Helper function that logs the duration of a phase (basically, the time passed since the previos call)
    """
    logging.info("Phase duration: %f" % (perf_counter() - log_phase_duration.last_measured_time))
    log_phase_duration.last_measured_time = perf_counter()


log_phase_duration.last_measured_time = perf_counter()


class AdaptationController:
    """
    Runs all four phases of the adaptation loop sequentially. Uses an instance of a specialized class responsible for
    each phase (Monitor, Analyzer, ExecutionPlanner, and Executor respectively). As this is the central class for the
    Avocado framework, it also serves as a main target for customization. This class can be customized with the
    ExtensionManager
    """

    def __init__(self,
                 knowledge: Knowledge,
                 monitor: Monitor,
                 analyzer: CSPAnalyzer,
                 planner: Planner,
                 executor: TaskExecutor,
                 ):
        """
        :param kubeconfig_file: A path to the kubeconfig file to use.
        :param knowledge:       A reference to the Knowledge object to use.
        :param monitor:         Monitor to use. If None, will create default Monitor.
        :param analyzer:        Analyzer to use. If None, will create default Analyzer.
        :param planner:         Planner to use. If None, will create default ExecutionPlanner.
        :param executor:        Executor to use. If None, will create default Executor.
        :param solver_class:    A class of the solver to use.
        :param predictor:       Predictor to use.
        :param mongos_ip:       IP of a Mongos instance for executing any commands on MongoDB.
        :param thread_count:    Number of threads to use for Executor ThreadPool.
        """
        self.knowledge = knowledge
        self.monitor = monitor
        self.analyzer = analyzer
        self.planner = planner
        self.executor = executor
        self.desired_state = None

    def monitoring(self):
        """
        Collects all the data from monitor (for the description of all the data being collected refer to the Monitor
        documentation).
        """
        logging.info("--------------- MONITORING PHASE ---------------")
        self.monitor.monitor()

    def analysis(self):
        """
        Gets the desired state from the Analyzer.
        """
        logging.info("--------------- ANALYSIS PHASE   ---------------")
        self.desired_state = self.analyzer.find_new_assignment()

    def planning(self):
        """
        Receives execution plans from Planner based on the desired state
        """
        logging.info("--------------- PLANNING PHASE   ---------------")
        self.planner.plan_tasks(self.desired_state)

    def execution(self) -> int:
        """
        Executes the plans produced by Planner.
        :return: number of executed plans
        """
        logging.info("--------------- EXECUTION PHASE  ---------------")
        task_count = self.executor.execute_all()
        return task_count

    def run_one_cycle(self) -> None:
        """
        Runs all four phases of the adaptation loop.
        """
        for phase in self.monitoring, self.analysis, self.planning, self.execution:
            phase()
            log_phase_duration()

    def deploy(self):
        """
        Runs adaptation loop until the actual state fully matches desired state.
        """
        plan_count = 1
        while plan_count != 0:
            self.monitoring()
            self.analysis()
            self.planning()
            task_count = self.execution()

    def run(self) -> None:
        """
        Runs the adaption loop until interruption. This method blocks indefinitely.
        """
        logging.info("MAPE-K loop started.")
        try:
            while True:
                self.run_one_cycle()
        except KeyboardInterrupt:
            logging.info('^C received, ending')

