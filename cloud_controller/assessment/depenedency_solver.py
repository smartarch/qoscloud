#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This class takes care of scenario dependencies
"""

from abc import ABC, abstractmethod
from collections import deque
from enum import IntEnum
from typing import Dict, List, Deque, Set, Tuple

from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.model import CloudState, Component, ManagedCompin, Compin, Probe


class WorkloadType(IntEnum):
    NORMAL = 0
    BENCHMARK_CONTROLLED = 1
    BENCHMARK_BACKGROUND = 2


ProbeCompinPlanTuple = Tuple[Probe, str, WorkloadType]  # Probe, Compin ID, WorkloadType

Resources = Dict[str, List[str]]  # { hardware_confing_id: [node_names] }


class DependencySolver(ABC):

    @abstractmethod
    def request(self, scenario: Scenario) -> Dict[str, int]:
        pass

    @abstractmethod
    def append_scenario_to_cloud_state(self, scenario: Scenario, resources: Resources, state: CloudState) -> \
            List[ProbeCompinPlanTuple]:
        pass


class MasterSlaveSolver(DependencySolver):
    last_compin_id = 0
    last_chain_id = 0

    def request(self, scenario: Scenario) -> Dict[str, int]:
        if len(scenario.controlled_probe.component.dependencies) > 0:
            return {scenario.hw_id: 2}

        for probe in scenario.background_probes:
            if len(probe.component.dependencies) > 0:
                return {scenario.hw_id: 2}

        return {scenario.hw_id: 1}

    def append_scenario_to_cloud_state(self, scenario: Scenario, resources: Resources, state: CloudState) -> \
            List[ProbeCompinPlanTuple]:
        # Extract nodes name from resources
        assert len(resources) == 1
        assert 1 <= len(resources[scenario.hw_id]) <= 2

        master_node = resources[scenario.hw_id][0]
        slave_node: str = "none"
        if len(resources[scenario.hw_id]) > 1:
            slave_node = resources[scenario.hw_id][1]

        # Resolve dependencies
        cmd_plans: List[ProbeCompinPlanTuple] = []
        # Main probe
        ctl_compin_id = self._add_component_to_cloud_state(state, scenario.controlled_probe.component, master_node,
                                                           slave_node)
        cmd_plans.append((scenario.controlled_probe, ctl_compin_id, WorkloadType.BENCHMARK_CONTROLLED))

        # Background probes
        for probe in scenario.background_probes:
            compin_id = self._add_component_to_cloud_state(state, probe.component, master_node, slave_node)
            cmd_plans.append((probe, compin_id, WorkloadType.BENCHMARK_BACKGROUND))

        return cmd_plans

    @staticmethod
    def _add_component_to_cloud_state(cloud_state: CloudState, main_component: Component, master_node: str,
                                      slave_node: str) -> str:
        """
        Adds new component to cloud state (with its dependencies) and returns created component instance's ID
        """

        MasterSlaveSolver.last_chain_id += 1
        chain_id = str(MasterSlaveSolver.last_chain_id)
        # Insert application to cloud state
        if not cloud_state.contains_application(main_component.application.name):
            cloud_state.add_application(main_component.application)

        # Prepare main instance
        main_instance = ManagedCompin(main_component,
                                      "main%d" % MasterSlaveSolver.last_compin_id,
                                      master_node,
                                      chain_id)
        MasterSlaveSolver.last_compin_id += 1
        cloud_state.add_instance(main_instance)

        # Dependencies
        # Finds all component that are need by main component
        to_explore: Deque[Component] = deque()
        to_explore.append(main_component)
        explored: Set[Component] = set()
        instances: Dict[Component, Compin] = {main_component: main_instance}
        while len(to_explore) > 0:
            actual = to_explore.pop()
            explored.add(actual)

            for dependency in actual.dependencies:
                # Skip explored components
                if dependency in explored:
                    # Set dependency to
                    instances[actual].set_dependency(instances[dependency])
                    continue
                # Append component to plan
                dependency_instance = ManagedCompin(dependency,
                                                    "dep%d" % MasterSlaveSolver.last_compin_id,
                                                    slave_node,
                                                    chain_id)
                instances[actual].set_dependency(dependency_instance)
                MasterSlaveSolver.last_compin_id += 1
                # Set dependency
                cloud_state.add_instance(dependency_instance)
                # Plan exploration dependency's dependencies
                to_explore.append(dependency)
        assert len(to_explore) == 0
        return main_instance.id
