"""
Contains the constraint that involves calls to the predictor.
"""
import logging
from typing import List, Dict

from ortools.constraint_solver.pywrapcp import Solver, PyConstraint

import cloud_controller.knowledge.model as model
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analysis.csp_solver.variables import CompNodeVar


_logger = logging.getLogger("CSPSolver.NodePredictConstraint")


class NodePredictConstraint(PyConstraint):
    """
    This class represents an Ortools constraint that internally calls the predictor every time that the values of the
    variables are updated. It controls a set of CompNodeVar that includes all the variable of this type that are
    related to one node. The constraint is considered satisfied for the given values of the controlled variables if
    the predictor returns True for these values.

    The predictor is called when one of the variables that are controlled by this constraint is bound to 1.

    For more information about how custom constraints work in Ortools solver refer to Ortools documentation.
    """

    def __init__(self, solver: Solver, vars_for_node: List[CompNodeVar], node: model.Node, predictor: Predictor):
        """
        :param solver: Or-tools solver this constraint is added to.
        :param vars_for_node: All compin node variables representing compins that may run on this node.
        :param node: Node for which the instance of a constraint is responsible.
        :param predictor: Reference to a predictor.
        """
        PyConstraint.__init__(self, solver)
        _logger.debug(f"Creating NodePredictConstraint for node {node}")
        self._vars: List[CompNodeVar] = vars_for_node
        self._node_id: str = node.hardware_id
        self._predictor: Predictor = predictor

    def Post(self):
        """
        This method is called when the constraint is processed by the solver.
        Its main usage is to attach demons to variables.
        """
        _logger.debug("Post")
        for var in self._vars:
            if not var.int_var.Bound():
                demon = self.Demon(NodePredictConstraint.Update, var)
                var.int_var.WhenBound(demon)

    def InitialPropagate(self):
        """
        This method performs the initial propagation of the constraint.
        It is called just after the post.
        :return:
        """
        _logger.debug("InitialPropagate")
        self._call_predictor()

    def Update(self, var: CompNodeVar):
        """
        Called when given var's value is updated.
        :param var:
        :return:
        """
        if var.int_var.Value() != 0:
            self._call_predictor()

    def _call_predictor(self) -> None:
        """
        Calls the Predictor and asks whether the current compins can run together on the node. "Current compins" means
        all the compins whose variable's value is currently set to 1.
        """
        bound_vars = []
        for var in self._vars:
            if var.int_var.Bound() and var.int_var.Value() == 1:
                bound_vars.append(var)

        if len(bound_vars) == 0:
            return

        components: Dict[str, int] = {}
        for var_ in bound_vars:
            id_ = var_.component_id
            if id_ not in components:
                components[id_] = 1
            else:
                components[id_] += 1

        if not self._predictor.predict_(self._node_id, components):
            # _logger.debug(f"Predictor failed on node {self._node} with components {components}")
            self.solver().Fail()

