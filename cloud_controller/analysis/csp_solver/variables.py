"""
This module contains the data structures representing wrappers for variable types for OR-Tools solver.
"""

import logging
from typing import List, Iterable, Generator, Dict

from ortools.constraint_solver.pywrapcp import IntVar, Solver, SolutionCollector

import cloud_controller.knowledge.model as model
from cloud_controller.knowledge.knowledge import Knowledge

COMPIN_NODE_VAR = "CompNode"
COMPIN_DC_VAR = "CompDC"
RUNNING_NODE_VAR = "Running"
NODE_ROLE_VAR = "Role"

_logger = logging.getLogger("CSPSolver")


class Var:
    """
    Base class for all the variable types.
    """

    def __init__(self, var: IntVar):
        self._var: IntVar = var

    def __str__(self):
        return self._var.Name()

    @property
    def int_var(self) -> IntVar:
        return self._var


class _CompNodeVar(Var):
    """
    Base class for some of the variable types.
    """
    def __init__(self, component_id: str, chain_id: str, node: str, var: IntVar):
        super().__init__(var)
        self._comp_id: str = component_id
        self._chain_id: str = chain_id
        self._node: str = node

    @property
    def node(self) -> str:
        return self._node

    @property
    def component_id(self) -> str:
        return self._comp_id

    @property
    def chain_id(self) -> str:
        return self._chain_id


class CompDCVar(_CompNodeVar):
    """
    Represents Compin-DC variable, de facto wrapper for IntVar. When this variable is
    set to 1, it means that compin with specified component type ID and chain ID is
    supposed to run in a specified data center.
    """
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"{COMPIN_DC_VAR},{component_id},{chain_id},{node}"))


class CompNodeVar(_CompNodeVar):
    """
    Represents Compin-Node variable, de facto wrapper for IntVar. When this variable is
    set to 1, it means that compin with specified component type ID and chain ID is
    supposed to run on specified node.
    """
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"{COMPIN_NODE_VAR},{component_id},{chain_id},{node}"))


class RunningCompNodeVar(_CompNodeVar):
    """
    Represents Compin-Node variable, de facto wrapper for IntVar. When this variable is
    set to 1, it means that compin with specified component type ID and chain ID is
    running on specified node even though it is not supposed to (e.g. it is terminating
    or waiting for client to disconnect.
    """
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"Running{COMPIN_NODE_VAR},{component_id},{chain_id},{node}"))


class RunningNodeVar(Var):
    """
    When this variable is set to 1, it means that there are some compins running on this node currently.
    """
    def __init__(self, solver: Solver, node: str):
        super().__init__(solver.BoolVar(f"{RUNNING_NODE_VAR},{node}"))
        self._node: str = node

    @property
    def node(self) -> str:
        return self._node


class NodeRoleVar(Var):
    """
    When this variable is set to 1, it means that this node is allocated for unique instances only, otherwise for regular
    compins only.
    """
    def __init__(self, solver: Solver, node: str):
        super().__init__(solver.BoolVar(f"{NODE_ROLE_VAR},{node}"))
        self._node: str = node

    @property
    def node(self) -> str:
        return self._node


class Variables:
    """
    Container for all variables in constraint satisfaction problem.
    """

    def __init__(
                 self,
                 comp_node_vars: List[CompNodeVar],
                 cn_vars_by_compin,
                 cn_vars_by_node,
                 cn_vars_by_dc,
                 dc_vars_by_chain,
                 dc_vars,
                 cdc_vars,
                 running_node_vars: Dict[str, RunningNodeVar],
                 cn_vars_by_node_and_compin,
                 rcn_vars_by_node_and_compin,
                 client_dc_vars_by_tier,
                 unique_vars_by_compin,
                 unique_vars_by_node,
                 unique_vars_by_node_and_compin,
                 unique_vars,
    ):
        self.comp_node_vars: List[CompNodeVar] = comp_node_vars
        self.vars_by_compin: Dict[str, List[CompNodeVar]] = cn_vars_by_compin
        self.vars_by_node: Dict[str, List[CompNodeVar]] = cn_vars_by_node
        self.vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = cn_vars_by_node_and_compin
        self.running_vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = rcn_vars_by_node_and_compin
        self.client_dc_vars_by_tier: Dict[int, List[CompDCVar]] = client_dc_vars_by_tier

        self.node_vars_by_dc: Dict[str, Dict[str, List[CompNodeVar]]] = cn_vars_by_dc

        self.dc_vars: Dict[str, Dict[str, CompDCVar]] = dc_vars
        self.dc_vars_by_chain: Dict[str, Dict[str, List[CompDCVar]]] = dc_vars_by_chain
        self.running_node_vars: Dict[str, RunningNodeVar] = running_node_vars
        self.cdc_vars: Dict[str, Dict[str, CompDCVar]] = cdc_vars
        self._collector: SolutionCollector = None

        self.unique_vars_by_compin: Dict[str, List[CompNodeVar]] = unique_vars_by_compin
        self.unique_vars_by_node: Dict[str, List[CompNodeVar]] = unique_vars_by_node
        self.unique_vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = unique_vars_by_node_and_compin
        self.unique_vars: List[CompNodeVar] = unique_vars

    def get_all_variables(self) -> List[Var]:
        """
        :return: All the variables present in the container. The variables that are stored in several different
        collections at the same time are not repeated.
        """
        var_list = []
        for tier_ in reversed(range(len(self.client_dc_vars_by_tier))):
            var_list += self.client_dc_vars_by_tier[tier_]
        for node_vars in self.running_vars_by_node_and_compin.values():
            for var_ in node_vars.values():
                var_list.append(var_)
        var_list += self.comp_node_vars
        var_list += self.unique_vars
        for dc in self.dc_vars_by_chain.values():
            for lst in dc.values():
                var_list += lst
        var_list += list(self.running_node_vars.values())

        return var_list

    def convert_to_cloud_state(self, collector: SolutionCollector, knowledge: Knowledge) -> model.CloudState:
        """
        Constructs the cloud state based on the values of variables. Called after a solution have been found in order
        to construct the desired state.
        :param collector: Collector to check the values of the variables against.
        :param knowledge: Reference to Knowledge.
        :return: The constructed CloudState
        """
        def iterate_set_vars(vars: Iterable[Var]) -> Generator[Var, None, None]:
            for var in vars:
                if self._var_value(var) != 1:
                    continue
                else:
                    yield var

        def find_component(component_id: str) -> model.Component:
            """ Find a component in all applications. """
            for application in knowledge.applications.values():
                component = application.get_component(component_id)
                if component is not None:
                    return component
            raise Exception(f"Component with ID={component_id} not found in Knowledge")

        def create_managed_compins_for_comp_node_vars() -> List[model.ManagedCompin]:
            managed_compins = []
            var: CompNodeVar
            for var in iterate_set_vars(self.comp_node_vars):
                component = find_component(var.component_id)
                managed_compins.append(
                    model.ManagedCompin(component=component,
                                        id_=generate_managed_compin_id(component, var),
                                        node=var.node,
                                        chain_id=var.chain_id))
            for var in iterate_set_vars(self.unique_vars):
                component = knowledge.components[var.component_id]
                managed_compins.append(model.ManagedCompin(
                    component=component,
                    id_=var.component_id,
                    node=var.node,
                    chain_id=var.chain_id
                ))
            return managed_compins

        def copy_unmanaged_compins_from_curr_state() -> List[model.UnmanagedCompin]:
            """Copies unmanaged compins from curr_state for all applications without any connections."""
            unmanaged_compins = []
            for curr_unmanaged_compin in knowledge.actual_state.list_all_unmanaged_compins():
                if curr_unmanaged_compin.component.application.name in knowledge.applications:
                    unmanaged_compins.append(
                        model.UnmanagedCompin(component=curr_unmanaged_compin.component,
                                              id_=curr_unmanaged_compin.id))
            return unmanaged_compins

        def generate_managed_compin_id(component: model.Component, var: CompNodeVar) -> str:
            return f"{component.name}-{var.chain_id}-{var.node}"

        def find_compin(compins: List[model.ManagedCompin], chain_id: str, component_type: model.Component) \
                -> model.ManagedCompin:
            for compin in compins:
                if compin.chain_id == chain_id and compin.component.name == component_type.name:
                    return compin
            raise Exception("Should find compin")

        def connect_chain(actual_compin: model.Compin, all_compins: List[model.ManagedCompin], chain_id: str) -> None:
            """Recursively connects whole chain of compins. """
            for dependent_component in actual_compin.component.dependencies:
                dependent_compin = find_compin(all_compins, chain_id, dependent_component)
                actual_compin.set_dependency(dependent_compin)
                connect_chain(dependent_compin, all_compins, chain_id)

        def connect_all_compins(clients: List[model.UnmanagedCompin], compins: List[model.ManagedCompin]) -> None:
            for client in clients:
                connect_chain(client, compins, client.chain_id)

        self._collector = collector
        unmanaged_compins = copy_unmanaged_compins_from_curr_state()
        managed_compins = create_managed_compins_for_comp_node_vars()
        connect_all_compins(unmanaged_compins, managed_compins)

        state = model.CloudState()
        for application in knowledge.applications.values():
            state.add_application(application)
        state.add_instances(unmanaged_compins + managed_compins)
        return state

    def _var_value(self, var) -> int:
        assert self._collector is not None

        if isinstance(var, Var):
            var = var.int_var

        best_solution: int = self._collector.SolutionCount() - 1
        return self._collector.Value(best_solution, var)

    def log_variable_values(self, collector: SolutionCollector) -> None:
        """
        Prints variables only if they equal 1.
        """

        def log_var_if_not_zero(var: IntVar, name: str) -> None:
            if self._var_value(var) == 0:
                pass
            else:
                _logger.debug(f"\t  {name} = {self._var_value(var)}")

        def log_set_vars(vars: Iterable[Var], name: str) -> None:
            _logger.debug(f"{name}:")
            for var in vars:
                log_var_if_not_zero(var.int_var, str(var))

        self._collector = collector

        _logger.debug("=== VARIABLE VALUES ===")

        log_set_vars(self.comp_node_vars, "CompNodeVars")
        for vars_ in self.running_vars_by_node_and_compin.values():
            log_set_vars(vars_.values(), "RunningCompNodeVars")
        for vars_ in self.cdc_vars.values():
            log_set_vars(vars_.values(), "ClientDCVars")
        for vars_ in self.dc_vars.values():
            log_set_vars(vars_.values(), "CompDCVars")
        log_set_vars(self.running_node_vars.values(), "RunningNodeVars")
        _logger.debug("========================")

    def log_all_variables(self, collector: SolutionCollector) -> None:
        """
        Prints all the variables, even if they are set to 0.
        """

        def log_var(var, name: str) -> None:
            _logger.debug(f"\t  {name} = {self._var_value(var)}")

        def log_vars(vars: Iterable[Var], name: str) -> None:
            _logger.debug(f"{name}:")
            for var in vars:
                log_var(var, str(var))

        self._collector = collector

        _logger.debug("=== ALL VARIABLES ======")
        log_vars(self.comp_node_vars, "CompNodeVars")
        for vars_ in self.running_vars_by_node_and_compin.values():
            log_vars(vars_.values(), "RunningCompNodeVars")
        for vars_ in self.cdc_vars.values():
            log_vars(vars_.values(), "ClientDCVars")
        for vars_ in self.dc_vars.values():
            log_vars(vars_.values(), "CompDCVars")
        log_vars(self.running_node_vars.values(), "RunningNodeVars")
        _logger.debug("========================")
