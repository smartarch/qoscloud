"""
This module contains classes representing constraints (mostly as boolean expressions) in the solver's CSP.
"""

import logging
import time
from typing import List

from ortools.constraint_solver.pywrapcp import IntVar, Solver

from cloud_controller.analysis.predictor import Predictor
from cloud_controller.knowledge.network_distances import NetworkDistances
from cloud_controller.analysis.csp_solver.variables import Variables, CompNodeVar
from cloud_controller.knowledge import model as model
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Node


class ConstraintAdder:
    """
    Responsible for adding constraints to the solver (via add_constraints method).

    The usage of this class is schematically:
    constraint_adder = ConstraintAdder(solver, variables, knowledge, distances, predictor)
    constraint_adder.add_constraints()
    """
    def __init__(self, solver: Solver, variables: Variables, knowledge: Knowledge, distances: NetworkDistances,
                 predictor: Predictor):
        """
        :param solver: Ortools original solver. The constraints will be added directly to this solver. For more
        information how and why this is done refer to Ortools documentation.
        :param variables: All the variables in the CSP.
        :param knowledge: Reference to the knowledge
        """
        self._solver: Solver = solver
        self._variables: Variables = variables
        self._nodes: List[Node] = list(knowledge.nodes.values())
        self._datacenters: List[str] = list(knowledge.datacenters.keys())
        self._node_names: List[str] = list(knowledge.nodes.keys())
        self._curr_state: model.CloudState = knowledge.actual_state
        self._managed_components: List[model.Component] = knowledge.get_all_managed_components()
        self._clients: List[model.UnmanagedCompin] = list(knowledge.actual_state.list_all_unmanaged_compins())
        self._clients = [client for client in self._clients
                         if client.component.application.name in knowledge.applications]
        self._distances = distances
        self._predictor = predictor

    def add_constraints(self) -> None:
        """
        Adds all the constraints for all the applications. For each type of constraint see the docs to the
        corresponding method.
        """
        start = time.perf_counter()
        self._add_node_separation_constraints()  # DONE
        logging.debug(f"Constraint 0 adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        self._add_redeployment_constraints()  # DONE
        logging.debug(f"Constraint 1 adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        self._add_compin_deployed_exactly_once()  # DONE
        logging.debug(f"Constraint 2 adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        self._add_whole_chain_in_single_datacenter()
        logging.debug(f"Constraint 3 adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        self._add_running_nodes_constraints()
        logging.debug(f"Constraint 4 adding time: {(time.perf_counter() - start):.15f}")
        start = time.perf_counter()
        self._add_predict_constraints()  # DONE
        logging.debug(f"Constraint 5 adding time: {(time.perf_counter() - start):.15f}")

    def _add_node_separation_constraints(self) -> None:
        for node_name in self._node_names:
            # node_role_var = self._variables.node_role_vars[node_name]
            job_vars_for_node: List[IntVar] = [var_.int_var for var_ in self._variables.job_vars_by_node[node_name]]
            vars_for_node: List[IntVar] = [var_.int_var for var_ in self._variables.vars_by_node[node_name]]
            # "":
            if len(job_vars_for_node) != 0 and len(vars_for_node) != 0:
                self._solver.Add(self._solver.Max(vars_for_node) == 0 or self._solver.Max(job_vars_for_node) == 0)
            # self._solver.Add(self._solver.Max(job_vars_for_node) == 0 or node_role_var.int_var == 1)

    def _add_whole_chain_in_single_datacenter(self) -> None:
        """
        Adds a set of constraints that ensure that the whole chain of a client (i.e. all of its dependencies, including
        transitive ones) is located in a single datacenter.
        """
        for datacenter in self._datacenters:
            for compin in self._variables.dc_vars[datacenter]:
                vars_ = [var_.int_var for var_ in self._variables.node_vars_by_dc[datacenter][compin]]
                # The following constraint says: "The compin is located in a data center iff it is located in one of
                # its nodes"

                if len(vars_) > 0:
                    self._solver.Add(self._variables.dc_vars[datacenter][compin].int_var == self._solver.Max(vars_))
            for client in self._clients:
                vars_ = [var_.int_var for var_ in self._variables.dc_vars_by_chain[datacenter][client.id]]
                var_ = self._variables.cdc_vars[datacenter][client.id]
                # "The client's chain is located in a data center iff all of the compins of this chan are located in
                # that datacenter":
                if len(vars_) > 0:
                    self._solver.Add(self._solver.MinEquality(vars_,  var_.int_var))
        for client in self._clients:
            vars_ = [dict_[client.id].int_var for dict_ in self._variables.cdc_vars.values()]
            # "The client's chain has to be located in some datacenter"
            if len(vars_) > 0:
                self._solver.Add(self._solver.Max(vars_) == 1)

    def _add_compin_deployed_exactly_once(self) -> None:
        """
        Adds constraints that ensure that each required compin is present only once.
        """
        for compin in self._variables.vars_by_compin.values():
            # "The compin can be deployed only on one node":
            vars = [var_.int_var for var_ in compin]
            if len(vars) > 0:
                self._solver.Add(self._solver.SumEquality(vars, 1))
        for compin in self._variables.job_vars_by_compin.values():
            # "The compin can be deployed only on one node":
            vars = [var_.int_var for var_ in compin]
            if len(vars) > 0:
                self._solver.Add(self._solver.SumEquality(vars, 1))

    def _add_redeployment_constraints(self) -> None:
        """
        These constraints set RunningCompNodeVars.
        """
        for node_ in self._variables.running_vars_by_node_and_compin:
            for compin_ in self._variables.running_vars_by_node_and_compin[node_]:
                # "A RunningCompNodeVar is equal to 1 iff the corresponding CompNodeVar is equal to 0":
                if node_ in self._variables.vars_by_node_and_compin and \
                        compin_ in self._variables.vars_by_node_and_compin[node_]:
                    self._solver.Add(
                        self._variables.running_vars_by_node_and_compin[node_][compin_].int_var !=
                        self._variables.vars_by_node_and_compin[node_][compin_].int_var
                    )
                # "Jobs cannot be redeployed"
                elif node_ in self._variables.job_vars_by_node_and_compin and \
                        compin_ in self._variables.job_vars_by_node_and_compin[node_]:
                    self._solver.Add(self._variables.job_vars_by_node_and_compin[node_][compin_].int_var == 1)
                # This works, because we create RunningCompNodeVars only for already existing compins (from the actual
                # state

    def _add_predict_constraints(self) -> None:
        """
        Adds constraints that call the predictor. See docs to NodePredictConstraint.
        """
        from cloud_controller.analysis.csp_solver.node_predict_constraint import NodePredictConstraint

        for node in self._nodes:
            vars_for_node: List[CompNodeVar] = self._variables.vars_by_node[node.name]
            if len(vars_for_node) > 0:
                self._solver.Add(NodePredictConstraint(solver=self._solver, vars_for_node=vars_for_node,
                                                   node=node, predictor=self._predictor))
            job_vars_for_node: List[CompNodeVar] = self._variables.job_vars_by_node[node.name]
            if len(job_vars_for_node) > 0:
                self._solver.Add(NodePredictConstraint(solver=self._solver, vars_for_node=job_vars_for_node,
                                                   node=node, predictor=self._predictor))

    def _add_running_nodes_constraints(self) -> None:
        """
        Adds constraints that set the values of RunningNodeVars.
        """
        for node_name in self._node_names:
            running_node_var = self._variables.running_node_vars[node_name]
            vars_for_node: List[IntVar] = [var_.int_var for var_ in self._variables.vars_by_node[node_name]]
            # "A node is running iff there is at least one compin running on that node":
            if len(vars_for_node) > 0:
                self._solver.Add(self._solver.MaxEquality(vars_for_node, running_node_var.int_var))
