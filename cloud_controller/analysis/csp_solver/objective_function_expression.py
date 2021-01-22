"""
This module contains functions for creating an objective function expression. The objective is represented as an integer
expression which is supposed to be added to CSP solver.
"""

import time
from typing import List
import logging

from ortools.constraint_solver.pywrapcp import IntExpr

from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.analysis.csp_solver.variables import Variables

RUNNING_NODE_COST = 1
LATENCY_COST = 10
REDEPLOYMENT_COST = 2

_logger = logging.getLogger("CSPSolver.objective_function_expression")

_variables: Variables = None


def create_objective_function_expression(variables: Variables, distances: NetworkDistances) -> IntExpr:
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
    global _variables
    _variables = variables
    start = time.perf_counter()
    nodes = _create_running_nodes_expression()
    logging.debug(f"Nodes objective adding time: {(time.perf_counter() - start):.15f}")
    start = time.perf_counter()
    redeployment = _create_redeployment_expression()
    logging.debug(f"Redeployment objective adding time: {(time.perf_counter() - start):.15f}")
    start = time.perf_counter()
    latency = _create_latency_expression()
    logging.debug(f"Latency objective adding time: {(time.perf_counter() - start):.15f}")
    return redeployment + nodes + latency


def _create_latency_expression() -> IntExpr:
    """
    Creates expression representing the cost of the latencies between clients and the datacenters they are connected to.
    """
    latency_expressions: List[IntExpr] = []
    for tier in _variables.client_dc_vars_by_tier:
        latency_expressions.append(tier * (sum([var.int_var for var in _variables.client_dc_vars_by_tier[tier]])))
    expr = LATENCY_COST * (sum(latency_expressions))
    _logger.debug(f"Client latency expression = {expr}")
    return expr


def _create_running_nodes_expression() -> IntExpr:
    """
    Creates expression representing the number of running nodes.
    """
    running_node_vars_sum: IntExpr = sum([var.int_var for var in _variables.running_node_vars.values()])
    expr = RUNNING_NODE_COST * running_node_vars_sum
    _logger.debug(f"Running nodes expression = {expr}")
    return expr


def _create_redeployment_expression() -> IntExpr:
    """
    Creates expression representing the number of compin redeployments.
    """
    redeploy_vars_sum: IntExpr = sum([var.int_var
                                        for _ in _variables.running_vars_by_node_and_compin.values()
                                            for var in _.values()
                                      ])
    expr = REDEPLOYMENT_COST * redeploy_vars_sum
    _logger.debug(f"Redeployment expression = {expr}")
    return expr
