"""
Contains Analyzer class responsible for the Analysis phase.
"""
import logging
from typing import Optional, List

from multiprocessing import Pool

from multiprocessing.pool import AsyncResult, ThreadPool

from cloud_controller.analyzer.problem import CSPProblem
from cloud_controller.analyzer.objective_function import ObjectiveFunction
from cloud_controller.analyzer.constraint import Constraint
from cloud_controller.analyzer.variables import Variables
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, ManagedCompin


class CSPAnalyzer:
    """
    Determines the desired state of the cloud with the help of the solver and the predictor.
    """

    def __init__(self, knowledge: Knowledge, pool: ThreadPool):
        """
        :param knowledge: Reference to the Knowledge
        :param solver_class: A class of the solver that has to be used. The solver is instantiated internally.
        :param predictor: Reference to the predictor that has to be supplied to the solver.
        :param pool: Thread pool for running long-term searches for the desired state.
        """
        self._knowledge: Knowledge = knowledge
        self._last_desired_state: CloudState = CloudState()
        self._pool: ThreadPool = pool
        self._longterm_result_future: Optional[AsyncResult] = None
        self._constraints: List[Constraint] = []
        self._variables = Variables(self._knowledge)
        self._objective_function: ObjectiveFunction = None
        self._asynch_mode: bool = False

    @property
    def variables(self) -> Variables:
        return self._variables

    def add_constraint(self, constraint: Constraint):
        self._constraints.append(constraint)

    def set_objective_function(self, objective_function: ObjectiveFunction):
        self._objective_function = objective_function

    def _mark_force_keep_compins(self, desired_state: CloudState) -> None:
        """
        Marks the dependencies of newly connected clients with force_keep, meaning that these dependencies should not be
        removed from the cloud until the client connects to them. This is important since we want the dependencies for
        newly connected clients to be instantiated as soon as possible, and thus cannot allow them to be deleted.
        """
        for compin in self._knowledge.list_new_clients():
            # Get the same compin from the desired state:
            compin = desired_state.get_compin(compin.component.application.name, compin.component.name, compin.id)
            if compin is not None:
                for dependency in compin.list_dependencies():
                    dependency.set_force_keep()

    def _solve_csp_problem(self, time_limit: int = None) -> Optional[CloudState]:
        problem = CSPProblem(self._variables, self._constraints, self._objective_function)
        if time_limit is not None:
            problem.set_time_limit(time_limit)
        success = problem.solve()
        if success:
            collector = problem.get_solution_collector()
            return self._variables.convert_to_cloud_state(collector, self._knowledge)
        else:
            return None

    def find_new_assignment(self) -> CloudState:
        """
        Gets current network distances, instantiates a solver, and runs the search for desired state. If the solver
        fails to find a desired state, quickly (default 5 seconds), returns the previous desired state, while starting
        an asynchronous long-term computation of the desired state. The result of that computation will be returned in
        one of the next calls to this method (when the computation is finished).
        :return: The new desired state of the cloud if found, last found desired state otherwise.
        """
        if len(self._knowledge.applications) == 0:
            return CloudState()
        if self._asynch_mode:
            # If a long-term computation of the desired state is already running, we can try to get its result:
            if self._longterm_result_future.ready():
                logging.info("Using the result of a long-term desired state computation.")
                desired_state = self._longterm_result_future.get()
                self._longterm_result_future = None
                self._asynch_mode = False
                if desired_state is not None:
                    self._knowledge.all_instances_scheduled()
                else:
                    desired_state = self._last_desired_state
            elif self._knowledge.reduced_load():
                self._asynch_mode = False
                return self.find_new_assignment()
            else:
                # If the result is still None, we just return the last desired state (for now, until the new assignment
                # is found).
                logging.info("Using previous desired state.")
                desired_state = self._last_desired_state
        else:
            desired_state: CloudState = self._solve_csp_problem()
            if desired_state is None:
                # If solver could not find a result quickly, we can turn to long-term computation:
                self._asynch_mode = True
                if self._longterm_result_future is None:
                    self._longterm_result_future = self._pool.apply_async(self._solve_csp_problem, (CSPProblem.NO_LIMIT,))
                self._knowledge.monitor_reduced_load()
                logging.info("Deployment plan was not found. Some workloads cannot be deployed due to the lack of resources.")
                for component in self._knowledge.components.values():
                    unique_compin = self._knowledge.actual_state.get_unique_compin(component)
                    if unique_compin is None:
                        self._knowledge.no_resources_for_component(component.name)
                # If the result is still None, we just return the last desired state (for now, until the new assignment
                # is found).
                logging.info("Using previous desired state.")
                desired_state = self._last_desired_state
            else:
                self._longterm_result_future = None
                self._asynch_mode = False
                self._knowledge.all_instances_scheduled()

        self._mark_force_keep_compins(desired_state)
        self._log_desired_state(desired_state)
        self._last_desired_state = desired_state
        return desired_state

    @staticmethod
    def _log_desired_state(desired_state: CloudState) -> None:
        """
        Prints out the node-compin assignment from the supplied CloudState, as well as the dependencies of each client.
        """
        logging.info(" --- Current assignment: --- ")
        for app in desired_state.list_applications():
            for component in desired_state.list_components(app):
                for instance in desired_state.list_instances(app, component):
                    compin = desired_state.get_compin(app, component, instance)
                    if isinstance(compin, ManagedCompin):
                        logging.info(f"DP: Component {app}:{component} has to be deployed on node {compin.node_name}")
                    else:
                        logging.info(f"Client {app}:{component}:{instance} has to be connected to components "
                                     f"{list(dependency.id for dependency in compin.list_dependencies())}")
        logging.info(" --------------------------- ")
