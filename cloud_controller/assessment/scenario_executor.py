#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module that execute scenarios in MAPE-K cycle
"""
import logging
import threading
import time
from collections import deque
from typing import List, Optional, Dict, Deque, Set, Tuple

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller import middleware
from cloud_controller.assessment.depenedency_solver import DependencySolver, WorkloadType, Resources, \
    ProbeCompinPlanTuple
from cloud_controller.assessment.deploy_controller import AppJudge
from cloud_controller.assessment.al_wrapper import AdaptationLoopWrapper
from cloud_controller.assessment.model import Scenario
from cloud_controller.assessment.result_storage import ResultStorage
from cloud_controller.assessment.scenario_planner import ScenarioPlanner, FailureReason
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Probe
from cloud_controller.knowledge.instance import Compin
from cloud_controller.knowledge.cloud_state import CloudState
from cloud_controller.middleware.helpers import connect_to_grpc_server_with_channel
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub

logger = logging.getLogger("SE")


class ProbeExecutionException(Exception):

    def __init__(self, *args: object) -> None:
        self.reason: Optional[FailureReason] = None
        super().__init__(*args)


class ScenarioExecutor:

    def __init__(self, knowledge: Knowledge, planner: ScenarioPlanner, solver: DependencySolver,
                 mapek_wrapper: AdaptationLoopWrapper, judge: AppJudge, multi_thread=False):
        # Other classes
        self._knowledge = knowledge
        self._planner = planner
        self._solver = solver
        self._mapek_wrapper = mapek_wrapper
        self._judge = judge
        # MAPE-K
        self._cloud_state = CloudState()
        # Working threads
        self._multi_thread = multi_thread
        self._lock = threading.RLock()
        self._running: Set[Scenario] = set()

        # Collect resources
        self._max_resources: Dict[str, int] = {}  # { hw_id: count_of_nodes }
        self._free_resources: Dict[str, Deque[str]] = {}  # { hw_id: [node_names] }
        for node in self._knowledge.nodes.values():
            # Add hw id if not exists
            if node.hardware_id not in self._max_resources.keys():
                self._max_resources[node.hardware_id] = 0
                self._free_resources[node.hardware_id] = deque()

            # Add node to resources
            self._max_resources[node.hardware_id] += 1
            self._free_resources[node.hardware_id].append(node.name)

    @staticmethod
    def _start_probe(ip: str, probe: Probe, warm_up_cycles: int, measured_cycles: int,
                     cpu_events: List[str] = None, reporting_enabled: bool = False) -> None:
        # Open connection
        probe_msg = mw_protocols.ProbeDescriptor(name=probe.name)
        measure_msg = mw_protocols.ProbeMeasurement(probe=probe_msg, warmUpCycles=warm_up_cycles,
                                                    measuredCycles=measured_cycles, reporting_enabled=reporting_enabled)
        if cpu_events is not None:
            for cpu_event in cpu_events:
                measure_msg.cpuEvents.append(cpu_event)

        stub, channel = connect_to_grpc_server_with_channel(MiddlewareAgentStub, ip, middleware.AGENT_PORT, True, production=False)
        reply = stub.MeasureProbe(measure_msg)
        channel.close()

        if reply.result != mw_protocols.ProbeCallResult.OK:
            result_str = mw_protocols.ProbeCallResult.Result.Name(reply.result)
            exception = ProbeExecutionException(f"Received {result_str} from {ip}")

            if reply.result == mw_protocols.ProbeCallResult.CPU_EVENT_NOT_SUPPORTED:
                exception.reason = FailureReason.CPU_EVENTS_NOT_SUPPORTED
            elif reply.result == mw_protocols.ProbeCallResult.IO_EVENT_NOT_SUPPORTED:
                exception.reason = FailureReason.IO_EVENTS_NOT_SUPPORTED

            raise exception
        else:
            logger.info(f"Measurement finished in {reply.executionTime}ms")

    @staticmethod
    def _set_probe_workload(ip: str, probe: Optional[Probe]) -> None:
        # Open connection
        if probe is not None:
            # Start new probe workload
            probe_msg = mw_protocols.ProbeDescriptor(name=probe.name)
            workload_msg = mw_protocols.ProbeWorkload(probe=probe_msg)
        else:
            # Stop any probe workload
            workload_msg = mw_protocols.ProbeWorkload(none=True)

        stub, channel = connect_to_grpc_server_with_channel(MiddlewareAgentStub, ip, middleware.AGENT_PORT, True, production=False)
        reply = stub.SetProbeWorkload(workload_msg)
        channel.close()

        if reply.result != mw_protocols.ProbeCallResult.OK:
            result_str = mw_protocols.ProbeCallResult.Result.Name(reply.result)
            raise ProbeExecutionException(f"Received {result_str} from {ip}")

    def start_probe(self, compin: Compin, probe: Probe, warm_up_cycles: int, measured_cycles: int,
                    cpu_events: List[str], reporting_enabled: bool) -> None:
        self._start_probe(compin.ip, probe, warm_up_cycles=warm_up_cycles, measured_cycles=measured_cycles,
                          cpu_events=cpu_events, reporting_enabled=reporting_enabled)

    def start_probe_workload(self, compin: Compin, probe: Probe) -> None:
        self._set_probe_workload(compin.ip, probe)

    def stop_probe_workload(self, compin: Compin) -> None:
        self._set_probe_workload(compin.ip, None)

    def measure_scenario(self, scenario: Scenario, cmd_plans: List[ProbeCompinPlanTuple]) -> None:
        """
        Deploys and evaluate one scenarion with specific resources
        """
        # Start background workload
        logger.debug("%s: Starting background components", scenario)
        ctl_compin_id = None
        for probe, compin_id, workload in cmd_plans:
            if workload == WorkloadType.BENCHMARK_BACKGROUND:
                compin = self._knowledge.actual_state.get_compin(probe.component.application.name, probe.component.name,
                                                                 compin_id)
                self.start_probe_workload(compin, probe)
            elif workload == WorkloadType.BENCHMARK_CONTROLLED:
                assert ctl_compin_id is None
                ctl_compin_id = compin_id
        assert ctl_compin_id is not None

        # Measure controlled probe
        logger.info("%s: Starting probe measurement on controlled component", scenario)
        ctl_compin = self._knowledge.actual_state.get_compin(scenario.controlled_probe.component.application.name,
                                                             scenario.controlled_probe.component.name, ctl_compin_id)
        # We enable reporting only for isolation scenarios:
        reporting_enabled = len(scenario.background_probes) == 0
        self.start_probe(ctl_compin, scenario.controlled_probe, scenario.warm_up_cycles, scenario.measured_cycles,
                         scenario.cpu_events, reporting_enabled=reporting_enabled)

        # Stops rest of compins
        for probe, compin_id, workload in cmd_plans:
            if workload == WorkloadType.BENCHMARK_BACKGROUND:
                compin = self._knowledge.actual_state.get_compin(probe.component.application.name, probe.component.name,
                                                                 compin_id)
                self.stop_probe_workload(compin)

        # Collect probe results
        logger.info("%s: Collecting probe results", scenario)
        ResultStorage.collect_scenario(ctl_compin, scenario)

    def are_requirements_available(self, requirements: Dict[str, int]) -> bool:
        with self._lock:
            for hw_config, needed in requirements.items():
                if len(self._free_resources[hw_config]) < needed:
                    if self._max_resources[hw_config] < needed:
                        logger.error("Scenario needs %d nodes of hw %s but cluster contains only %d nodes",
                                     needed, hw_config, self._max_resources[hw_config])
                    return False
            return True

    def run(self) -> None:
        # Auto cleanup
        needs_cleanup = False

        while True:
            needs_waiting = True

            with self._lock:
                scenario = self._planner.fetch_scenario()

            # Has something to do
            if scenario is not None:
                requirements = self._solver.request(scenario)
                if self.are_requirements_available(requirements):
                    # Deploy scenario
                    provided, cmd_plans = self.deploy_scenario(scenario, requirements)

                    # Start scenario and than stop it
                    if self._multi_thread:
                        thread = threading.Thread(target=self._scenario_worker,
                                                  args=(scenario, provided, cmd_plans),
                                                  name=f"SE-scenario: {scenario}")
                        thread.start()
                    else:
                        self._scenario_worker(scenario, provided, cmd_plans)

                    # Register for auto cleanup
                    needs_cleanup = True
                    needs_waiting = False

            # Waits some time until some change arrives
            if needs_waiting:
                time.sleep(1)

            # Cleanup cluster if needed
            with self._lock:
                if needs_cleanup and needs_waiting and len(self._running) == 0:
                    self._mapek_wrapper.deploy(self._cloud_state)
                    needs_cleanup = False
                    logger.info("Nothing running remained, so cluster was cleaned")

    def deploy_scenario(self, scenario: Scenario, requirements: Dict[str, int]) \
            -> Tuple[Resources, List[ProbeCompinPlanTuple]]:
        with self._lock:
            self._running.add(scenario)

            # Reserve resources
            provided: Resources = {}
            for hw_config, needed in requirements.items():
                provided[hw_config] = needed * [""]
                for i in range(needed):
                    provided[hw_config][i] = self._free_resources[hw_config].popleft()

            logger.info("%s: Deploying to %s", scenario, provided)

            # Update cloud state for current scenario
            cmd_plans = self._solver.append_scenario_to_cloud_state(scenario, provided, self._cloud_state)

            # Deploy cloud state to cluster
            self._mapek_wrapper.deploy(self._cloud_state)

        return provided, cmd_plans

    def _scenario_worker(self, scenario: Scenario, provided: Resources,
                         cmd_plans: List[ProbeCompinPlanTuple]) -> None:
        logger.info("%s: Measuring", scenario)
        try:
            # Execute scenario
            self.measure_scenario(scenario, cmd_plans)

            # Notify planner
            self._planner.on_scenario_done(scenario)
        except ProbeExecutionException as exception:
            logger.warning("%s: Measuring failed %s", scenario, exception)
            assert exception.reason is not None
            # Notify planner
            self._planner.on_scenario_failed(scenario, exception.reason)

        # Notify judge
        logger.info("%s: Judging", scenario)
        self._judge.notify_scenario_finished(scenario)

        # Clear provided nodes from components
        logger.info("%s: Cleaning up resources", scenario)
        with self._lock:
            provided_nodes: List[str] = []
            for nodes in provided.values():
                provided_nodes += nodes
            # Clean all compins on provided nodes
            scenario_apps = {probe.component.application.name for probe in scenario.background_probes}
            scenario_apps.add(scenario.controlled_probe.component.application.name)

            compins_to_delete: List[Compin] = []
            for app in scenario_apps:
                for compin in self._cloud_state.list_managed_compins(app):
                    if compin.node_name in provided_nodes:
                        compins_to_delete.append(compin)
            for compin in compins_to_delete:
                self._cloud_state.delete_instance(compin.component.application.name, compin.component.name, compin.id)

            # Return resources
            for hw_config, nodes in provided.items():
                for node in nodes:
                    self._free_resources[hw_config].append(node)

            self._running.remove(scenario)

        logger.info("%s: Finished", scenario)


class FakeScenarioExecutor(ScenarioExecutor):

    def __init__(self, knowledge: Knowledge, planner: ScenarioPlanner, solver: DependencySolver,
                 judge: AppJudge):
        super().__init__(knowledge, planner, solver, EmptyMapekWrapper(), judge)

    def measure_scenario(self, scenario: Scenario, resources: Resources) -> None:
        pass


class EmptyMapekWrapper:
    def deploy(self, cloud_state: CloudState) -> None:
        pass
