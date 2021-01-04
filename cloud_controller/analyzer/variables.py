import logging
import time
from itertools import chain
from typing import List, Tuple, Dict, Iterable, Generator

from ortools.constraint_solver.pywrapcp import SolutionCollector

from cloud_controller.analysis.csp_solver.variables import Var, CompNodeVar, RunningCompNodeVar, CompDCVar, NodeRoleVar, \
    RunningNodeVar
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState, UnmanagedCompin, Component, ComponentCardinality, \
    ManagedCompin, Compin
from cloud_controller.knowledge.network_distances import NetworkDistances


class Variables:

    def __init__(self, knowledge: Knowledge):
        self._knowledge = knowledge
        self._all_vars: List[Var] = []

    @property
    def all_vars(self) -> List[Var]:
        return self._all_vars

    def _create_instances(self, state: CloudState, collector: SolutionCollector):
        best_solution_index: int = collector.SolutionCount() - 1
        for var in chain(self.comp_node_vars, self.unique_vars):
            if collector.Value(best_solution_index, var.int_var) == 1:
                component = self._knowledge.components[var.component_id]
                state.add_instance(
                    ManagedCompin(
                        component=component,
                        id_=self.generate_managed_compin_id(component, var),
                        node=var.node,
                        chain_id=var.chain_id
                    )
                )

    def _add_clients(self, state: CloudState):
        """Copies unmanaged compins from curr_state for all applications without any connections."""
        for curr_unmanaged_compin in self._knowledge.actual_state.list_all_unmanaged_compins():
            if curr_unmanaged_compin.component.application.name in self._knowledge.applications:
                state.add_instance(
                    UnmanagedCompin(
                        component=curr_unmanaged_compin.component,
                        id_=curr_unmanaged_compin.id,
                        position_x=curr_unmanaged_compin.position_x,
                        position_y=curr_unmanaged_compin.position_y
                    )
                )

    @staticmethod
    def generate_managed_compin_id(component: Component, var: CompNodeVar) -> str:
        if component.cardinality == ComponentCardinality.MULTIPLE:
            return f"{component.name}-{var.chain_id}-{var.node}"
        elif component.cardinality == ComponentCardinality.SINGLE:
            return var.component_id

    def convert_to_cloud_state(self, collector: SolutionCollector, knowledge: Knowledge) -> CloudState:
        """
        Constructs the cloud state based on the values of variables. Called after a solution have been found in order
        to construct the desired state.
        :param collector: Collector to check the values of the variables against.
        :param knowledge: Reference to Knowledge.
        :return: The constructed CloudState
        """

        self._knowledge = knowledge
        state = CloudState()
        for application in knowledge.applications.values():
            state.add_application(application)
        self._add_clients(state)
        self._create_instances(state, collector)
        state.connect_all_chains()

        return state

    def _calculate_tiers(self, distances: NetworkDistances):
        """
        The datacenter that is closest to user is considered to be Tier 0. The next closest datacenter is Tier 1, and
        so on. This method determines which datacenter belongs to which tier for each client.
        :param distances: NetworkDistances between clients and datacenters.
        """
        for client in self._clients:
            self._tiers[client.id] = {}
            tuples: List[Tuple[float, str]] = []
            for dc in self._knowledge.datacenters:
                distance = distances.get_client_datacenter_distance(client.id, dc)
                tuples.append((distance, dc))
            tier_ = 0
            for _, dc in sorted(tuples):
                self._tiers[client.id][dc] = tier_
                tier_ += 1

    def clear(self):
        self._all_vars: List[Var] = []
        self._distances = self._knowledge.network_topology.get_network_distances(
            self._knowledge.actual_state.list_all_unmanaged_compins(),
            self._knowledge.nodes.values()
        )
        self._clients: List[UnmanagedCompin] = list(self._knowledge.actual_state.list_all_unmanaged_compins())
        self._clients = [client for client in self._clients
                         if client.component.application.name in self._knowledge.applications]
        self._tiers: Dict[str, Dict[str, int]] = {}  # client: datacenter: tier
        self._calculate_tiers(self._distances)

        self.comp_node_vars: List[CompNodeVar] = []
        self.vars_by_compin: Dict[str, List[CompNodeVar]] = {}  # compin : [var]
        self.vars_by_node: Dict[str, List[CompNodeVar]] = {}  # node : [var]
        self.vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = {}  # node : compin : var
        self.running_vars_by_node_and_compin: Dict[str, Dict[str, RunningCompNodeVar]] = {}  # node : compin : var

        self.node_vars_by_dc: Dict[str, Dict[str, List[CompNodeVar]]] = {}  # DC : compin : [var]
        self.comp_dc_vars: Dict[str, Dict[str, CompDCVar]] = {}  # DC : compin : var
        self.dc_vars_by_chain: Dict[str, Dict[str, List[CompDCVar]]] = {}  # DC : client : [var]
        self.client_dc_vars: Dict[str, Dict[str, CompDCVar]] = {}  # DC : client : var
        self.client_dc_vars_by_tier: Dict[int, List[CompDCVar]] = {}  # Tier: [var]

        self.unique_vars_by_compin: Dict[str, List[CompNodeVar]] = {}  # compin : [var]
        self.unique_vars_by_node: Dict[str, List[CompNodeVar]] = {}  # node : [var]
        self.unique_vars_by_node_and_compin: Dict[str, Dict[str, CompNodeVar]] = {}  # node : compin : var
        self.unique_vars: List[CompNodeVar] = []

        self.node_role_vars: Dict[str, NodeRoleVar] = {}
        self.running_node_vars: Dict[str, RunningNodeVar] = {}

        for datacenter in self._knowledge.datacenters:
            self.node_vars_by_dc[datacenter] = {}
            self.comp_dc_vars[datacenter] = {}
            self.dc_vars_by_chain[datacenter] = {}
            self.client_dc_vars[datacenter] = {}
            for client in self._clients:
                all_client_dependencies: List[Component] = client.component.get_all_dependencies_flat()
                self.dc_vars_by_chain[datacenter][client.id] = []
                for dependency in all_client_dependencies:
                    compin_name = f"{dependency.name}-{client.id}"
                    self.vars_by_compin[compin_name] = []
                    self.node_vars_by_dc[datacenter][compin_name] = []
        for i in range(len(self._knowledge.datacenters)):
            self.client_dc_vars_by_tier[i] = []

        for node_name in self._knowledge.nodes:
            self.vars_by_node[node_name] = []
            self.vars_by_node_and_compin[node_name] = {}
            self.running_vars_by_node_and_compin[node_name] = {}

            self.unique_vars_by_node[node_name] = []
            self.unique_vars_by_node_and_compin[node_name] = {}

    def add(self, solver):
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

        for compin in self._knowledge.actual_state.list_all_managed_compins():
            if compin.component.application.name in self._knowledge.applications:
                var_ = RunningCompNodeVar(solver, component_id=compin.component.id, chain_id=compin.chain_id,
                                           node=compin.node_name)
                self.all_vars.append(var_)
                compin_name = f"{compin.component.name}-{compin.chain_id}"
                self.running_vars_by_node_and_compin[compin.node_name][compin_name] = var_

        for client in self._clients:
            all_client_dependencies: List[Component] = client.component.get_all_dependencies_flat()
            for dependency in all_client_dependencies:
                compin_name = f"{dependency.name}-{client.id}"
                for node in self._knowledge.nodes.values():
                    if dependency.is_deployable_on_node(node):
                        var_ = CompNodeVar(solver, component_id=dependency.id, chain_id=client.id, node=node.name)
                        self.all_vars.append(var_)
                        self.comp_node_vars.append(var_)
                        self.vars_by_node[node.name].append(var_)
                        self.node_vars_by_dc[node.data_center][compin_name].append(var_)
                        self.vars_by_compin[compin_name].append(var_)
                        self.vars_by_node_and_compin[node.name][compin_name] = var_
                for datacenter in self._knowledge.datacenters:
                    var_ = CompDCVar(solver, component_id=dependency.id, chain_id=client.id, node=datacenter)
                    self.all_vars.append(var_)
                    self.comp_dc_vars[datacenter][compin_name] = var_
                    self.dc_vars_by_chain[datacenter][client.id].append(var_)
            for datacenter in self._knowledge.datacenters:
                var_ = CompDCVar(solver, component_id="", chain_id=client.id, node=datacenter)
                self.all_vars.append(var_)
                self.client_dc_vars[datacenter][client.id] = var_
                self.client_dc_vars_by_tier[self._tiers[client.id][datacenter]].append(var_)

        for component in self._knowledge.components:
            if self._knowledge.components[component].cardinality == ComponentCardinality.SINGLE:
                self.unique_vars_by_compin[component] = []
                for node_name in self._knowledge.nodes:
                    var_ = CompNodeVar(solver, component_id=component, chain_id=component, node=node_name)
                    self.all_vars.append(var_)
                    self.unique_vars_by_compin[component].append(var_)
                    self.unique_vars_by_node[node_name].append(var_)
                    self.unique_vars.append(var_)
        for node_name in self._knowledge.nodes:
            var_ = NodeRoleVar(solver, node_name)
            self.all_vars.append(var_)
            self.node_role_vars[node_name] = var_
            var_ = RunningNodeVar(solver, node_name)
            self.all_vars.append(var_)
            self.running_node_vars[node_name] = var_

        logging.debug(f"Compin node variables adding time: {(time.perf_counter() - start):.15f}")