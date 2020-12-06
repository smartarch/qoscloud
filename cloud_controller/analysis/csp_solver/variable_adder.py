"""
This module contains a class that creates all the necessary variables for the OR-Tools solver model.
It models the transition between current state of the cluster to the desired state.
"""

import logging
import time
from typing import List, Dict, Tuple

from ortools.constraint_solver.pywrapcp import Solver

from cloud_controller.analysis.csp_solver.variables import Variables, CompNodeVar, CompDCVar, RunningNodeVar, \
    RunningCompNodeVar, NodeRoleVar
from cloud_controller.knowledge import model as model
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ComponentCardinality
from cloud_controller.knowledge.network_distances import NetworkDistances


class VariableAdder:
    """
    Class that creates the variables that should be added to the OR-Tools solver.
    """

    def __init__(self, solver: Solver, knowledge: Knowledge, distances: NetworkDistances):
        """
        :param solver: Reference to OR-Tools solver
        :param knowledge: We need nodes, clients, and datacenters from the knowledge.
        :param distances: current NetworkDistances
        """
        self._solver: Solver = solver
        self._knowledge: Knowledge = knowledge
        self._applications: List[model.Application] = list(knowledge.applications.values())
        self._nodes: List[model.Node] = list(knowledge.nodes.values())
        self._datacenters: List[str] = list(knowledge.datacenters.keys())
        self._curr_state: model.CloudState = knowledge.actual_state
        self._managed_components: List[model.Component] = knowledge.get_all_managed_components()
        self._clients: List[model.UnmanagedCompin] = list(knowledge.actual_state.list_all_unmanaged_compins())
        self._clients = [client for client in self._clients
                         if client.component.application.name in self._knowledge.applications]
        self._variables: Variables = None
        self._tiers: Dict[str, Dict[str, int]] = {}  # client: datacenter: tier
        self._calculate_tiers(distances)

    def _calculate_tiers(self, distances: NetworkDistances):
        """
        The datacenter that is closest to user is considered to be Tier 0. The next closest datacenter is Tier 1, and
        so on. This method determines which datacenter belongs to which tier for each client.
        :param distances: NetworkDistances between clients and datacenters.
        """
        for client in self._clients:
            self._tiers[client.id] = {}
            tuples: List[Tuple[float, str]] = []
            for dc in self._datacenters:
                distance = distances.get_client_datacenter_distance(client.id, dc)
                tuples.append((distance, dc))
            tier_ = 0
            for _, dc in sorted(tuples):
                self._tiers[client.id][dc] = tier_
                tier_ += 1

    @property
    def variables(self) -> Variables:
        assert self._variables is not None
        return self._variables

    def create_and_add_variables(self) -> None:
        """
        Creates all the necessary variables for the CSP.
        """
        comp_node_vars = self._create_compin_node_variables()
        self._variables = Variables(
            comp_node_vars=comp_node_vars,
            cn_vars_by_compin=self.vars_by_compin,
            cn_vars_by_node=self.vars_by_node,
            cn_vars_by_dc=self.node_vars_by_dc,
            dc_vars_by_chain=self.dc_vars_by_chain,
            dc_vars=self.comp_dc_vars,
            cdc_vars=self.client_dc_vars,
            running_node_vars=self._create_running_node_variables(),
            cn_vars_by_node_and_compin=self.vars_by_node_and_compin,
            rcn_vars_by_node_and_compin=self.running_vars_by_node_and_compin,
            client_dc_vars_by_tier=self.client_dc_vars_by_tier,

            job_vars_by_compin=self.job_vars_by_compin,
            job_vars_by_node=self.job_vars_by_node,
            job_vars_by_node_and_compin=self.job_vars_by_node_and_compin,
            job_vars=self.job_vars,
        )

    def _create_compin_node_variables(self) -> List[CompNodeVar]:
        """
        Creates variables that represent three different things:
            (1) Assignment of a specific compin to a specific node. These variables are in a form "A1N1" where A1 is a
                compin and N1 is a node, and when this variable is equal to 1, it means that compin A1 should be running
                on node N1 in the desired state.
            (2) Assignment of a specific compin to a specific datacenter. The same meaning, but for datacenters
            (3) Assignment of a specific _client_ to a specific datacenter. Means that all of its compins are assigned
                to the same datacenter
            (4) Already existing compin-node assignment. Means that there is already a compin running on that node,
                that should not be there in the desired state.
        All the variables are added to several data structures, each data structure is then used for formulation of
        different constraints.
        :return: List of all the variables of type (1). The rest are set into class members.
        """
        start = time.perf_counter()
        self.vars_by_compin: Dict[str, List[CompNodeVar]] = {}  # compin : [var]
        self.vars_by_node: Dict[str, List[CompNodeVar]] = {}  # node : [var]
        self.vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = {}  # node : compin : var
        self.running_vars_by_node_and_compin: Dict[str, Dict[str, RunningCompNodeVar]] = {}  # node : compin : var

        self.node_vars_by_dc: Dict[str, Dict[str, List[CompNodeVar]]] = {}  # DC : compin : [var]
        self.comp_dc_vars: Dict[str, Dict[str, CompDCVar]] = {}  # DC : compin : var
        self.dc_vars_by_chain: Dict[str, Dict[str, List[CompDCVar]]] = {}  # DC : client : [var]
        self.client_dc_vars: Dict[str, Dict[str, CompDCVar]] = {}  # DC : client : var
        self.client_dc_vars_by_tier: Dict[int, List[CompDCVar]] = {}  # Tier: [var]

        self.job_vars_by_compin: Dict[str, List[CompNodeVar]] = {}  # compin : [var]
        self.job_vars_by_node: Dict[str, List[CompNodeVar]] = {}  # node : [var]
        self.job_vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = {}  # node : compin : var
        self.job_vars: List[CompNodeVar] = []

        for datacenter in self._datacenters:
            self.node_vars_by_dc[datacenter] = {}
            self.comp_dc_vars[datacenter] = {}
            self.dc_vars_by_chain[datacenter] = {}
            self.client_dc_vars[datacenter] = {}
            for client in self._clients:
                all_client_dependencies: List[model.Component] = client.component.get_all_dependencies_flat()
                self.dc_vars_by_chain[datacenter][client.id] = []
                for dependency in all_client_dependencies:
                    compin_name = f"{dependency.name}-{client.id}"
                    self.vars_by_compin[compin_name] = []
                    self.node_vars_by_dc[datacenter][compin_name] = []
        for i in range(len(self._datacenters)):
            self.client_dc_vars_by_tier[i] = []

        for node in self._nodes:
            self.vars_by_node[node.name] = []
            self.vars_by_node_and_compin[node.name] = {}
            self.running_vars_by_node_and_compin[node.name] = {}

            self.job_vars_by_node[node.name] = []
            self.job_vars_by_node_and_compin[node.name] = {}

        for compin in self._knowledge.actual_state.list_all_managed_compins():
            if compin.component.application.name in self._knowledge.applications:
                lvar_ = RunningCompNodeVar(self._solver, component_id=compin.component.id, chain_id=compin.chain_id,
                                           node=compin.node_name)
                compin_name = f"{compin.component.name}-{compin.chain_id}"
                self.running_vars_by_node_and_compin[compin.node_name][compin_name] = lvar_

        comp_node_vars: List[CompNodeVar] = []
        for client in self._clients:
            all_client_dependencies: List[model.Component] = client.component.get_all_dependencies_flat()
            for dependency in all_client_dependencies:
                compin_name = f"{dependency.name}-{client.id}"
                for node in self._nodes:
                    if dependency.is_deployable_on_node(node):
                        var_ = CompNodeVar(self._solver, component_id=dependency.id, chain_id=client.id, node=node.name)
                        comp_node_vars.append(var_)
                        self.vars_by_node[node.name].append(var_)
                        self.node_vars_by_dc[node.data_center][compin_name].append(var_)
                        self.vars_by_compin[compin_name].append(var_)
                        self.vars_by_node_and_compin[node.name][compin_name] = var_
                for datacenter in self._datacenters:
                    var_ = CompDCVar(self._solver, component_id=dependency.id, chain_id=client.id, node=datacenter)
                    self.comp_dc_vars[datacenter][compin_name] = var_
                    self.dc_vars_by_chain[datacenter][client.id].append(var_)
            for datacenter in self._datacenters:
                var_ = CompDCVar(self._solver, component_id="", chain_id=client.id, node=datacenter)
                self.client_dc_vars[datacenter][client.id] = var_
                self.client_dc_vars_by_tier[self._tiers[client.id][datacenter]].append(var_)

        for component in self._knowledge.components:
            if self._knowledge.components[component].cardinality == ComponentCardinality.SINGLE:
                self.job_vars_by_compin[component] = []
                for node in self._nodes:
                    var_ = CompNodeVar(self._solver, component_id=component, chain_id=component, node=node.name)
                    self.job_vars_by_compin[component].append(var_)
                    self.job_vars_by_node[node.name].append(var_)
                    self.job_vars_by_node_and_compin[node.name][component] = var_
                    self.job_vars.append(var_)
        self.node_role_vars: Dict[str, NodeRoleVar] = {}
        for node in self._nodes:
            self.node_role_vars[node.name] = NodeRoleVar(self._solver, node.name)

        logging.debug(f"Compin node variables adding time: {(time.perf_counter() - start):.15f}")
        return comp_node_vars

    def _create_running_node_variables(self) -> Dict[str, RunningNodeVar]:
        running_node_vars: Dict[str, RunningNodeVar] = {}
        for node in self._nodes:
            running_node_vars[node.name] = RunningNodeVar(self._solver, node.name)
        return running_node_vars
