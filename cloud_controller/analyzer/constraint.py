"""
This module contains classes representing constraints (mostly as boolean expressions) in the solver's CSP.
"""
from abc import abstractmethod
from typing import List

from ortools.constraint_solver.pywrapcp import Solver, IntVar

from cloud_controller.analysis.csp_solver.node_predict_constraint import NodePredictConstraint
from cloud_controller.analysis.csp_solver.variables import CompNodeVar
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analyzer.variables import Variables
from cloud_controller.knowledge.knowledge import Knowledge


class Constraint:

    def __init__(self):
        pass

    @abstractmethod
    def add(self, solver: Solver, variables: Variables):
        """
        Responsible for adding constraints to the solver.

        The usage of this class is schematically:
        analyzer = CSPAnalyzer()
        constraint = Constraint(solver, variables, knowledge, distances, predictor)
        analyzer.add_constraint(constraint)

        :param solver: Ortools solver instance. The constraints will be added directly to this solver.
        :param variables: The Variables object, representing all the variables in the CSP. They should be added to
        the solver before constraints.
        """
        pass


class PredictConstraint(Constraint):
    """
    Adds the predictor-calling constraints
    See docs to NodePredictConstraint
    """

    def __init__(self, knowledge: Knowledge, predictor: Predictor):
        super().__init__()
        self._knowledge = knowledge
        self._predictor = predictor

    def add(self, solver: Solver, variables: Variables) -> None:

        for node in self._knowledge.nodes.values():
            vars_for_node: List[CompNodeVar] = variables.vars_by_node[node.name]
            if len(vars_for_node) > 0:
                solver.Add(NodePredictConstraint(solver=solver, vars_for_node=vars_for_node,
                                                       node=node, predictor=self._predictor))

            unique_vars_for_node: List[CompNodeVar] = variables.unique_vars_by_node[node.name]
            if len(unique_vars_for_node) > 0:
                solver.Add(NodePredictConstraint(solver=solver, vars_for_node=unique_vars_for_node,
                                                       node=node, predictor=self._predictor))


class NodeSeparationConstraint(Constraint):
    """
    This constraints make it so a particular node will be used only for SINGLE or MULTIPLE instances. Is optional.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__()
        self._knowledge = knowledge

    def add(self, solver: Solver, variables: Variables) -> None:
        for node_name in self._knowledge.nodes:
            # node_role_var = self._variables.node_role_vars[node_name]
            unique_vars_for_node: List[IntVar] = [var_.int_var for var_ in variables.unique_vars_by_node[node_name]]
            vars_for_node: List[IntVar] = [var_.int_var for var_ in variables.vars_by_node[node_name]]
            # "":
            if len(unique_vars_for_node) != 0 and len(vars_for_node) != 0:
                solver.Add(solver.Max(vars_for_node) == 0 or solver.Max(unique_vars_for_node) == 0)
            # self._solver.Add(self._solver.Max(unique_vars_for_node) == 0 or node_role_var.int_var == 1)


class ChainInDatacenterConstraint(Constraint):
    """
    Adds a set of constraints that ensure that the whole chain of a client (i.e. all of its dependencies, including
    transitive ones) is located in a single datacenter.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__()
        self._knowledge = knowledge

    def add(self, solver: Solver, variables: Variables) -> None:
        for datacenter in self._knowledge.datacenters:
            for compin in variables.comp_dc_vars[datacenter]:
                vars_ = [var_.int_var for var_ in variables.node_vars_by_dc[datacenter][compin]]
                # The following constraint says: "The compin is located in a data center iff it is located in one of
                # its nodes"

                if len(vars_) > 0:
                    solver.Add(variables.comp_dc_vars[datacenter][compin].int_var == solver.Max(vars_))
            for client in self._knowledge.actual_state.list_all_unmanaged_compins():
                vars_ = [var_.int_var for var_ in variables.dc_vars_by_chain[datacenter][client.id]]
                var_ = variables.client_dc_vars[datacenter][client.id]
                # "The client's chain is located in a data center iff all of the compins of this chain are located in
                # that datacenter":
                if len(vars_) > 0:
                    solver.Add(solver.MinEquality(vars_,  var_.int_var))
        for client in self._knowledge.actual_state.list_all_unmanaged_compins():
            vars_ = [dict_[client.id].int_var for dict_ in variables.client_dc_vars.values()]
            # "The client's chain has to be located in some datacenter"
            if len(vars_) > 0:
                solver.Add(solver.Max(vars_) == 1)


class InstanceDeploymentConstraint(Constraint):
    """
    Adds constraints that ensure that each required compin is present only once.
    """

    def add(self, solver: Solver, variables: Variables) -> None:
        for compin in variables.vars_by_compin.values():
            # "The compin can be deployed only on one node":
            vars = [var_.int_var for var_ in compin]
            if len(vars) > 0:
                solver.Add(solver.SumEquality(vars, 1))
        for compin in variables.unique_vars_by_compin.values():
            # "The compin can be deployed only on one node":
            vars = [var_.int_var for var_ in compin]
            if len(vars) > 0:
                solver.Add(solver.SumEquality(vars, 1))


class RedeploymentConstraint(Constraint):
    """
    These constraints set RunningCompNodeVars.
    """

    def add(self, solver: Solver, variables: Variables) -> None:
        """
        These constraints set RunningCompNodeVars.
        """
        for node_ in variables.running_vars_by_node_and_compin:
            for compin_ in variables.running_vars_by_node_and_compin[node_]:
                # "A RunningCompNodeVar is equal to 1 iff the corresponding CompNodeVar is equal to 0":
                if node_ in variables.vars_by_node_and_compin and \
                        compin_ in variables.vars_by_node_and_compin[node_]:
                    solver.Add(
                        variables.running_vars_by_node_and_compin[node_][compin_].int_var !=
                        variables.vars_by_node_and_compin[node_][compin_].int_var
                    )
                # "Unique instances cannot be redeployed"
                elif node_ in variables.unique_vars_by_node_and_compin and \
                        compin_ in variables.unique_vars_by_node_and_compin[node_]:
                    solver.Add(variables.unique_vars_by_node_and_compin[node_][compin_].int_var == 1)
                # This works, because we create RunningCompNodeVars only for already existing compins (from the actual
                # state)


class RunningNodeConstraint(Constraint):
    """
    Adds constraints that set the values of RunningNodeVars.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__()
        self._knowledge = knowledge

    def add(self, solver: Solver, variables: Variables) -> None:
        """
        Adds constraints that set the values of RunningNodeVars.
        """
        for node_name in self._knowledge.nodes:
            running_node_var = variables.running_node_vars[node_name]
            vars_for_node: List[IntVar] = [var_.int_var for var_ in variables.vars_by_node[node_name]]
            # "A node is running iff there is at least one compin running on that node":
            if len(vars_for_node) > 0:
                solver.Add(solver.MaxEquality(vars_for_node, running_node_var.int_var))
