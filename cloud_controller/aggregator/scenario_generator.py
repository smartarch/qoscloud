import random
from collections import Counter
from enum import Enum
from typing import Tuple, Dict, Set, List, Optional

from cloud_controller import DEFAULT_HARDWARE_ID, MAX_ARITY
from cloud_controller.aggregator.multipredictor import MultiPredictor, PredictorUpdater
from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.probe import Probe


class MeasuringPhase(Enum):
    ISOLATION = 1
    COMBINATIONS = 2
    REGISTERED = 3


class ScenarioGenerator:

    def __init__(self, _predictor: MultiPredictor):
        import typing
        self._combination_counter: typing.Counter[Tuple[str, str, int]] = Counter()
        self._probes: Dict[str, Probe] = {}
        self._measured_combinations: Dict[str, Set[str]] = {}
        self._isolation_scenarios: List[str] = []
        self._combination_scenarios: List[Tuple[str, List[str]]] = []
        self._predictor_updater = PredictorUpdater(_predictor)
        if _predictor is not None:
            self._predictor_updater.start()
        self.next_scenario_id = 0
        self._INITIAL_SCENARIOS_COUNT = 4

    def register_probe(self, probe: Probe) -> None:
        self._probes[probe.alias] = probe
        if probe.alias not in self._measured_combinations:
            self._measured_combinations[probe.alias] = set()
        if self._bg_load_id([]) not in self._measured_combinations[probe.alias]:
            self._isolation_scenarios.append(probe.alias)
        for i in range(1, self._INITIAL_SCENARIOS_COUNT):
            load = self.generate_random_load(i)
            if self._bg_load_id(load) not in self._measured_combinations[probe.alias]:
                self._combination_scenarios.append((probe.alias, load))

    def load_datafile(self, hw_id: str, probe_id: str, bg_probe_ids: List[str], filename: str) -> None:
        if probe_id not in self._measured_combinations:
            self._measured_combinations[probe_id] = set()
        self._measured_combinations[probe_id].add(self._bg_load_id(bg_probe_ids))
        self._predictor_updater.provide_file(hw_id, filename)
        if len(self._measured_combinations[probe_id]) == self._INITIAL_SCENARIOS_COUNT or \
                self._predictor_updater.file_count >= 10:
            self._predictor_updater.update_predictor()

    def scenario_completed(self, scenario: Scenario) -> None:
        return self.load_datafile(
            hw_id=scenario.hw_id,
            probe_id=scenario.controlled_probe.alias,
            bg_probe_ids=[probe.alias for probe in scenario.background_probes],
            filename=scenario.filename_data
        )

    def generate_random_load(self, probe_count) -> List[str]:
        bg_probes: List[str] = []
        for i in range(probe_count):
            bg_probe_id = random.choice(list(self._probes))
            bg_probes.append(bg_probe_id)
        bg_probes.sort()
        return bg_probes

    def increase_count(self, hw_id: str, probe_id: str, arity: int) -> None:
        if 1 < arity <= MAX_ARITY:
            if (hw_id, probe_id, arity) not in self._combination_counter:
                self._combination_counter[(hw_id, probe_id, arity)] = 1
            else:
                self._combination_counter[(hw_id, probe_id, arity)] += 1

    @staticmethod
    def _bg_load_id(bg_load: List[str]):
        bg_load.sort()
        return "-".join(bg_load)

    def _create_scenario(self, probe_id: str, bg_probes: List[str], hw_id: str):
        scenario: Scenario = Scenario(
            controlled_probe=self._probes[probe_id],
            background_probes=[self._probes[bg_probe_id] for bg_probe_id in bg_probes],
            hw_id=hw_id,
            scenario_id=str(self.next_scenario_id),
            app_name=self._probes[probe_id].component.application.name
        )
        self.next_scenario_id += 1
        return scenario

    def next_scenario(self) -> Optional[Scenario]:
        if len(self._isolation_scenarios) > 0:
            probe_id = self._isolation_scenarios.pop(0)
            return self._create_scenario(probe_id, [], DEFAULT_HARDWARE_ID)
        if len(self._combination_scenarios) > 0:
            probe_id, bg_load = self._combination_scenarios.pop(0)
            return self._create_scenario(probe_id, bg_load, DEFAULT_HARDWARE_ID)
        if len(self._combination_counter) > 0:
            (hw_id, probe_id, arity), count = self._combination_counter.most_common(1)[0]
            del self._combination_counter[(hw_id, probe_id, arity)]
            bg_probes: List[str] = []
            for i in range(arity - 1):
                bg_probe_id = random.choice(list(self._probes))
                bg_probes.append(bg_probe_id)
            if self._bg_load_id(bg_probes) in self._measured_combinations[probe_id]:
                return self.next_scenario()
            return self._create_scenario(probe_id, bg_probes, hw_id)
        return None
