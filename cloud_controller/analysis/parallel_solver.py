"""
Contains a ParallelSolver class, an implementation of the solver that runs both CSPSolver and TrivialSolver in parallel.
"""

from multiprocessing.pool import ThreadPool, AsyncResult
from typing import Optional

from cloud_controller.analysis.csp_solver.solver import CSPSolver
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analysis.trivial_solver.trivial_solver import TrivialSolver
from cloud_controller.knowledge import model
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.network_distances import NetworkDistances

TIME_LIMIT_TOTAL = 7
TIME_LIMIT_SOLUTION = 5


class ParallelSolver:
    """
    Solver implementation that runs CSPSolver and TrivialSolver in parallel. For the mechanism of the search see
    documentation to the find_assignment method.
    """

    def __init__(self, knowledge: Knowledge, distances: NetworkDistances, predictor: Predictor):
        """
        :param knowledge: Reference to the Knowledge
        :param distances: Current NetworkDistances
        :param predictor: Predictor to use for the solution
        """
        self._knowledge: Knowledge = knowledge
        self._distances: NetworkDistances = distances
        self._predictor: Predictor = predictor
        self._pool: ThreadPool = ThreadPool(processes=2)

    def find_assignment(self) -> Optional[model.CloudState]:
        """
        Tries to find a desired state of the cloud by using both CSPSolver and TrivialSolver. These two solvers are run
        in parallel, with a CSP solver having a limit of TIME_LIMIT_SOLUTION seconds. If the CSP solver returns a
        solution in that time, that solution is used. Otherwise, the sub-optimal solution found by TrivialSolver is
        used. If even TrivialSolver fails to find the solution in that time, None is returned
        """
        _csp_solver = CSPSolver(self._knowledge, self._distances, self._predictor, TIME_LIMIT_SOLUTION)
        csp_result: AsyncResult = self._pool.apply_async(_csp_solver.find_assignment, ())

        trivial_solver = TrivialSolver(self._knowledge, self._distances, self._predictor)
        trivial_result: AsyncResult = self._pool.apply_async(trivial_solver.find_assignment, ())

        csp_result.wait(TIME_LIMIT_TOTAL)
        if csp_result.ready():
            return csp_result.get()
        elif trivial_result.ready() and trivial_result.successful():
            return trivial_result.get()
        else:
            return None

    def find_assignment_longterm(self) -> Optional[model.CloudState]:
        """
        :return: The result of find_assignment_longterm called on CSPSolver.
        """
        _csp_solver = CSPSolver(self._knowledge, self._distances, self._predictor)
        return _csp_solver.find_assignment_longterm()
