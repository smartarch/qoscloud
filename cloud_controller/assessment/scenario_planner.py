#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This classes plan evaluation of new applications and hw configs
"""
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from threading import Lock
from typing import List, Set

from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application, Probe

logger = logging.getLogger("SP")


class JudgeResult(IntEnum):
    ACCEPTED = 1
    REJECTED = 2
    NEEDS_DATA = 3


class FailureReason(IntEnum):
    CPU_EVENTS_NOT_SUPPORTED = 1
    IO_EVENTS_NOT_SUPPORTED = 2


class ScenarioPlanner(ABC):

    @abstractmethod
    def register_app(self, app: Application) -> None:
        pass

    @abstractmethod
    def unregister_app(self, app: Application) -> None:
        pass

    @abstractmethod
    def register_hw_config(self, app: Application) -> None:
        pass

    @abstractmethod
    def fetch_scenarios(self) -> List[Scenario]:
        pass

    def on_scenario_done(self, scenario: Scenario) -> None:
        pass

    def on_scenario_failed(self, scenario: Scenario, reason: FailureReason) -> None:
        pass

    @abstractmethod
    def judge_app(self, app: Application) -> JudgeResult:
        pass


class FakeScenarioPlanner(ScenarioPlanner):
    """
    Creates one scenario per probe (without any background workload), accepts everything,
    tests only on default hw config, ignores scenario failures
    """

    def __init__(self, default_hw_id: str):
        self._default_hw_id = default_hw_id
        self._plan: List[Scenario] = []
        self._lock = Lock()

    def register_app(self, app: Application) -> None:
        logger.info("App %s registered" % app.name)

        for component in app.list_managed_components():
            for probe in component.probes:
                with self._lock:
                    self._plan.append(
                        Scenario(controlled_probe=probe, background_probes=[],
                                 hw_id=self._default_hw_id))

    def unregister_app(self, app: Application) -> None:
        with self._lock:
            self._plan = [scenario for scenario in self._plan if not has_probes_from_app(scenario, app)]

    def register_hw_config(self, name: str) -> None:
        logger.info("HW config %s received, ignoring..." % name)

    def fetch_scenarios(self) -> List[Scenario]:
        with self._lock:
            return self._plan.copy()

    def on_scenario_done(self, scenario: Scenario) -> None:
        with self._lock:
            self._plan.remove(scenario)

    def on_scenario_failed(self, scenario: Scenario, reason: FailureReason) -> None:
        logger.error("Scenario %s failed on %s", scenario, reason)
        self.on_scenario_done(scenario)

    def judge_app(self, app: Application) -> JudgeResult:
        return JudgeResult.ACCEPTED


class SimpleScenarioPlanner(ScenarioPlanner):
    """
    Tests each probe alone than test pairs (first as measured, second as background workload), accepts everything,
    tests only on all hw_configs, ignores scenario failures
    """

    def __init__(self, knowledge: Knowledge):
        self._knowledge = knowledge
        self._hw_ids: Set[str] = set()
        self.register_hw_config("")
        self._probes: List[Probe] = []
        self._plan: List[Scenario] = []
        self._lock = Lock()

    def register_app(self, app: Application) -> None:
        with self._lock:
            for ctl_component in app.list_managed_components():
                for ctl_probe in ctl_component.probes:
                    # Test all components alone
                    self._probes.append(ctl_probe)

                    for hw_config in self._hw_ids:
                        scenario = Scenario(ctl_probe, [], hw_config)
                        self._plan.append(scenario)

                    # Test all pairs of components
                    for background_probe in self._probes:
                        for hw_config in self._hw_ids:
                            scenario = Scenario(ctl_probe, [background_probe], hw_config)
                            self._plan.append(scenario)

    def unregister_app(self, app: Application) -> None:
        with self._lock:
            self._probes = [probe for probe in self._probes if probe.component.application != app]
            self._plan = [scenario for scenario in self._plan if not has_probes_from_app(scenario, app)]

    def register_hw_config(self, name: str) -> None:
        num_configs = len(self._hw_ids)
        self._hw_ids = set((node.hardware_id for node in self._knowledge.nodes.values()))
        # Check if some new configs were detected
        assert len(self._hw_ids) > num_configs
        logger.info("New node hw config registered")

    def fetch_scenarios(self) -> List[Scenario]:
        with self._lock:
            return self._plan.copy()

    def on_scenario_done(self, scenario: Scenario) -> None:
        with self._lock:
            self._plan.remove(scenario)

    def on_scenario_failed(self, scenario: Scenario, reason: FailureReason) -> None:
        logger.error("Scenario %s failed on %s", scenario, reason)
        self.on_scenario_done(scenario)

    def judge_app(self, app: Application) -> JudgeResult:
        return JudgeResult.ACCEPTED


def has_probes_from_app(scenario: Scenario, app: Application) -> bool:
    if scenario.controlled_probe.component.application.name == app.name:
        return True

    for probe in scenario.background_probes:
        if probe.component.application.name == app.name:
            return True

    return False
