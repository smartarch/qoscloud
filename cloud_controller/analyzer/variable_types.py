"""
This module contains the data structures representing wrappers for variable types for OR-Tools solver.
"""

import logging

from ortools.constraint_solver.pywrapcp import IntVar, Solver

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
    def __init__(self, component_id: str, chain_id: str, node: str, var: IntVar, app_name: str, component_name: str):
        super().__init__(var)
        self._comp_id: str = component_id
        self._chain_id: str = chain_id
        self._node: str = node
        self._app_name: str = app_name
        self._component_name: str = component_name

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def component_name(self) -> str:
        return self._component_name

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
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str, app_name: str, component_name: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"{COMPIN_DC_VAR},{component_id},{chain_id},{node}"),
                         app_name=app_name, component_name=component_name)


class CompNodeVar(_CompNodeVar):
    """
    Represents Compin-Node variable, de facto wrapper for IntVar. When this variable is
    set to 1, it means that compin with specified component type ID and chain ID is
    supposed to run on specified node.
    """
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str, app_name: str, component_name: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"{COMPIN_NODE_VAR},{component_id},{chain_id},{node}"),
                         app_name=app_name, component_name=component_name)


class RunningCompNodeVar(_CompNodeVar):
    """
    Represents Compin-Node variable, de facto wrapper for IntVar. When this variable is
    set to 1, it means that compin with specified component type ID and chain ID is
    running on specified node even though it is not supposed to (e.g. it is terminating
    or waiting for client to disconnect.
    """
    def __init__(self, solver: Solver, component_id: str, chain_id: str, node: str, app_name: str, component_name: str):
        super().__init__(component_id=component_id, chain_id=chain_id, node=node,
                         var=solver.BoolVar(f"Running{COMPIN_NODE_VAR},{component_id},{chain_id},{node}"),
                         app_name=app_name, component_name=component_name)


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
