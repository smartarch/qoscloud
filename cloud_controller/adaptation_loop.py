"""
The module contains Adaptation Loop class that runs all the steps of the adaptation (MAPE-K) loop sequentially,
in a single process.
"""
from time import perf_counter

import logging

from cloud_controller.analyzer.csp_analyzer import CSPAnalyzer
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.monitor.monitor import Monitor
from cloud_controller.planner.top_planner import Planner
from cloud_controller.task_executor.task_executor import TaskExecutor


def log_phase_duration():
    """
    Helper function that logs the duration of a phase (basically, the time passed since the previos call)
    """
    logging.info("Phase duration: %f" % (perf_counter() - log_phase_duration.last_measured_time))
    log_phase_duration.last_measured_time = perf_counter()


log_phase_duration.last_measured_time = perf_counter()


class AdaptationLoop:
    """
    Runs all four phases of the adaptation loop sequentially. Uses an instance of a specialized class responsible for
    each phase (Monitor, Analyzer, Planner, and TaskExecutor respectively). As this is the central class for the
    framework, it also serves as a main target for customization. This class can be customized with the
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
        :param knowledge:       A reference to the Knowledge object to use.
        :param monitor:         Monitor to use.
        :param analyzer:        Analyzer to use.
        :param planner:         Planner to use.
        :param executor:        Executor to use.
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
        :return: number of executed tasks
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
        while True:
            self.run_one_cycle()
            if self.executor._registry.task_count() == 0:
                break
            # self.monitoring()
            # self.analysis()
            # self.planning()
            # self.execution()

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

