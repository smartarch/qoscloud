"""
Contains CSPSolver class, an implementation of Solver that uses constraint satisfaction problem to find a desired state.
"""
import logging
import time
from enum import Enum
from typing import List, Optional

from ortools.constraint_solver.pywrapcp import Solver, IntVar, OptimizeVar, SolutionCollector

import cloud_controller.knowledge.model as model
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.analysis.csp_solver.constraint_adder import ConstraintAdder
from cloud_controller.analysis.csp_solver.objective_function_expression import create_objective_function_expression
from cloud_controller.analysis.csp_solver.variable_adder import VariableAdder
from cloud_controller.analysis.csp_solver.variables import Variables
from cloud_controller.knowledge.knowledge import Knowledge


class _DebugLog(Enum):
    ALL_VARIABLES = 1
    ONLY_SET_VARIABLES = 2
    NO_LOG = 3


DEBUG_LOG = _DebugLog.NO_LOG

_logger = logging.getLogger("CSPSolver")

DEFAULT_TIME_LIMIT = 5
NO_LIMIT = -1


class CSPSolver:
    """
    Finds the desired CloudState via constraint satisfaction method. The search is performed in the find_assignment
    method.
    """
    def __init__(self, knowledge: Knowledge, distances: NetworkDistances, predictor: Predictor,
                 time_limit: float = DEFAULT_TIME_LIMIT):
        """
        :param knowledge: Reference to the Knowledge
        :param distances: Current NetworkDistances
        :param predictor: Predictor to use for the solution
        :param time_limit: Time limit for the solution search. If set to NO_LIMIT, solver searches as long as it takes
        to find the first solution.
        """
        self._knowledge: Knowledge = knowledge
        self._distances: NetworkDistances = distances
        self._predictor = predictor

        self._curr_state: model.CloudState = knowledge.actual_state
        self._nodes: List[str] = list(knowledge.nodes.keys())

        self._solver: Solver = Solver("CSP Solver")
        self._total_cost: IntVar = self._solver.IntVar(0, 100000, "Total cost")

        self._variables: Variables = None
        self._time_limit_ms = int(time_limit * 1000) if time_limit != NO_LIMIT else NO_LIMIT

    @property
    def variables(self) -> Variables:
        assert self._variables is not None
        return self._variables

    @variables.setter
    def variables(self, value) -> None:
        self._variables = value

    def find_assignment(self) -> Optional[model.CloudState]:
        """
        Builds the constraint satisfaction problem, searches for the best solution until the time runs out (defined by
        time_limit), and returns the best solution found (converted to the CloudState).

        For the description of the constraint model of the problem, refer to the ConstraintAdder docs.

        For the description of the objective function that determines which solution is the best, refer to the
        objective_function_expression docs.

        When knowledge.current_state does not contain any applications, the solver just returns
        default cloud state. This is because empty knowledge.current_state means that no
        applications were submitted yet.
        Default cloud state contains just applications, no client compins.

        On the other hand, when there are some applications in knowledge.current_state the
        CSP solver returns cloudstate containing desired deployment of every client compin.
        :return: desired CloudState, if the solution was found, None otherwise
        """
        if self._curr_state_contains_no_apps():
            return self._create_default_cloud_state()

        start = time.perf_counter()
        self._add_variables()
        logging.debug(f"Variables adding time: {(time.perf_counter() - start):.15f}")

        start = time.perf_counter()
        self._add_constraints()
        logging.debug(f"Constraints adding time: {(time.perf_counter() - start):.15f}")

        start = time.perf_counter()
        objective_function = self._add_objective_function()
        logging.debug(f"Objective adding time: {(time.perf_counter() - start):.15f}")

        start = time.perf_counter()
        collector = self._create_minimal_value_solution_collector()
        logging.debug(f"Collector adding time: {(time.perf_counter() - start):.15f}")

        start = time.perf_counter()
        res: bool = self._solve(collector, objective_function)
        logging.info(f"Solution time: {(time.perf_counter() - start):.15f}")

        if res:
            _logger.info("Solution found")
        else:
            _logger.info("Solution not found")
            return None

        self._log_variables(collector)
        self._log_solver_stats()
        return self.variables.convert_to_cloud_state(collector, self._knowledge)

    def find_assignment_longterm(self) -> Optional[model.CloudState]:
        """
        Searches for the first solution without any time limits.
        """
        self._time_limit_ms = NO_LIMIT
        return self.find_assignment()

    def _add_variables(self):
        variable_adder = VariableAdder(self._solver, self._knowledge, self._distances)
        variable_adder.create_and_add_variables()
        self.variables = variable_adder.variables

    def _add_constraints(self):
        constraint_adder = ConstraintAdder(self._solver, self.variables, self._knowledge, self._distances,
                                           self._predictor)
        constraint_adder.add_constraints()

    def _add_objective_function(self) -> OptimizeVar:
        obj_func_expr = create_objective_function_expression(self.variables, self._distances)
        self._solver.Add(self._total_cost == obj_func_expr)
        return self._solver.Minimize(self._total_cost, 1)

    def _create_minimal_value_solution_collector(self) -> SolutionCollector:
        collector: SolutionCollector = self._solver.BestValueSolutionCollector(False)
        collector.Add(self._all_vars())
        collector.AddObjective(self._total_cost)
        return collector

    def _solve(self, collector: SolutionCollector, objective_function: OptimizeVar) -> bool:
        """
        If time limit is set, tries to find the best possible solution in the given time. If there is no limit on the
        search, searches until finds the first solution (it cannot be guaranteed that the solution will be found).
        :return: True if solution was found, false otherwise.
        """
        decision_builder = self._solver.Phase(self._all_vars(), self._solver.CHOOSE_FIRST_UNBOUND,
                                              self._solver.ASSIGN_MIN_VALUE)
        if self._time_limit_ms != NO_LIMIT:
            tl = self._solver.TimeLimit(self._time_limit_ms)
            self._solver.Solve(decision_builder, [collector, objective_function, tl])
            if collector.SolutionCount() > 0:
                return True
            return False
        else:
            self._solver.NewSearch(decision_builder, [collector, objective_function])
            self._solver.NextSolution()
            return True

    def _log_variables(self, collector: SolutionCollector) -> None:
        if DEBUG_LOG == _DebugLog.ALL_VARIABLES:
            self.variables.log_all_variables(collector)
        elif DEBUG_LOG == _DebugLog.ONLY_SET_VARIABLES:
            self.variables.log_variable_values(collector)

    def _log_solver_stats(self) -> None:
        _logger.info(f"Ortools solver statistics:\n"
                      f"\t Number of branches = {self._solver.Branches()}\n"
                      f"\t Number of solutions = {self._solver.Solutions()}\n"
                      f"\t Number of failures = {self._solver.Failures()}")

    def _all_vars(self) -> List[IntVar]:
        return [var.int_var for var in self.variables.get_all_variables()] + [self._total_cost]

    def _create_default_cloud_state(self) -> model.CloudState:
        default_state = model.CloudState()
        for application in self._knowledge.applications.values():
            default_state.add_application(application)
        return default_state

    def _curr_state_contains_no_apps(self) -> bool:
        return len(self._knowledge.applications) == 0

    def _curr_state_contains_no_clients(self) -> bool:
        return len(list([client for client in self._curr_state.list_all_unmanaged_compins()
                            if client.component.application.name in self._knowledge.applications])) == 0
