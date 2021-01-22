"""
This module contains the class creating the objective function expression. The objective is represented as an integer
expression which is supposed to be added to CSP solver.
The abstract class ObjectiveFunction can be used to implement new objective functions.
"""
import time
from abc import abstractmethod
from typing import Optional, List

import logging
from ortools.constraint_solver.pywrapcp import IntVar, OptimizeVar, Solver, IntExpr

from cloud_controller.analyzer.constraint import Constraint
from cloud_controller.analyzer.variables import Variables

# TODO: extract to the top level
RUNNING_NODE_COST = 1
LATENCY_COST = 10
REDEPLOYMENT_COST = 2


class ObjectiveFunction(Constraint):

    def __init__(self):
        super().__init__()
        self.cost_var: Optional[IntVar] = None
        self.objective: Optional[OptimizeVar] = None

    @abstractmethod
    def expression(self, variables: Variables):
        pass

    def add(self, solver: Solver, variables: Variables):
        self.cost_var: IntVar = solver.IntVar(0, 100000, "Total cost")
        solver.Add(self.cost_var == self.expression(variables))
        self.objective: OptimizeVar = solver.Minimize(self.cost_var, 1)


class DefaultObjectiveFunction(ObjectiveFunction):

    def expression(self, variables: Variables):
        """
            Creates an integer expression the value of which shows the "cost" of a given solution. Based on the value of this
            expression, the solver can judge whether one solution is better than the other. The value of the expression should
            be minimized. The expression consists of three terms with different weights.
                (1) First of all, we aim to minimize the latency between the client and its servers. Thus, we should always
                    prefer the solutions where all the client dependencies are located on the closest server.
                (2) Then, we try to minimize the number of managed compin redeployments. There is no need to change the location
                    of an already deployed compin unless the client moves to another datacenter.
                (3) Finally, we prefer those solutions which use the smallest number of nodes.
            :return: The created expression.
            """
        start = time.perf_counter()
        nodes = self._create_running_nodes_expression(variables)
        logging.debug(f"Nodes objective adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        redeployment = self._create_redeployment_expression(variables)
        logging.debug(f"Redeployment objective adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        latency = self._create_latency_expression(variables)
        logging.debug(f"Latency objective adding time: {(time.perf_counter() - start):.15f}")
        return redeployment + nodes + latency

    def _create_latency_expression(self, variables: Variables) -> IntExpr:
        """
        Creates expression representing the cost of the latencies between clients and the datacenters they are connected to.
        """
        latency_expressions: List[IntExpr] = []
        for tier in variables.client_dc_vars_by_tier:
            latency_expressions.append(tier * (sum([var.int_var for var in variables.client_dc_vars_by_tier[tier]])))
        expr = LATENCY_COST * (sum(latency_expressions))
        logging.debug(f"Client latency expression = {expr}")
        return expr

    def _create_running_nodes_expression(self, variables: Variables) -> IntExpr:
        """
        Creates expression representing the number of running nodes.
        """
        running_node_vars_sum: IntExpr = sum([var.int_var for var in variables.running_node_vars.values()])
        expr = RUNNING_NODE_COST * running_node_vars_sum
        logging.debug(f"Running nodes expression = {expr}")
        return expr

    def _create_redeployment_expression(self, variables: Variables) -> IntExpr:
        """
        Creates expression representing the number of compin redeployments.
        """
        redeploy_vars_sum: IntExpr = sum([var.int_var
                                          for _ in variables.running_vars_by_node_and_compin.values()
                                          for var in _.values()
                                          ])
        expr = REDEPLOYMENT_COST * redeploy_vars_sum
        logging.debug(f"Redeployment expression = {expr}")
        return expr
