import random
import threading
import time
from collections import Counter
from enum import Enum
from threading import RLock
from typing import Dict, List, Tuple, Iterator, Set, Optional

import logging

import cloud_controller.analysis.predictor_interface.predictor_pb2 as predictor_pb
from cloud_controller import DEFAULT_HARDWARE_ID, PREDICTOR_HOST, PREDICTOR_PORT, THROUGHPUT_ENABLED, \
    THROUGHPUT_PERCENTILES
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorServicer, \
    add_PredictorServicer_to_server, PredictorStub
from cloud_controller.architecture_pb2 import ApplicationTimingRequirements
from cloud_controller.assessment.model import Scenario
from cloud_controller.analysis.predictor_interface.statistical_predictor import PercentilePredictor
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application, Probe, TimeContract, ThroughputContract
from cloud_controller.middleware.helpers import start_grpc_server, connect_to_grpc_server, setup_logging
import predictor

MAX_ARITY = 5


class MeasuringPhase(Enum):
    ISOLATION = 1
    COMBINATIONS = 2
    REGISTERED = 3


class StatisticalPredictor(Predictor):

    def __init__(self, knowledge: Knowledge):
        self.knowledge: Knowledge = knowledge
        self._predictor_service = connect_to_grpc_server(PredictorStub, PREDICTOR_HOST, PREDICTOR_PORT)

    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        assignment = predictor_pb.Assignment(hw_id=node_id)
        for component_id, count in components_on_node.items():
            component = self.knowledge.components[component_id]
            for probe in component.probes:
                ct_ = assignment.components.add()
                ct_.component_id = probe.alias
                ct_.count = count
        return self._predictor_service.Predict(assignment).result


class MultiPredictor:

    def __init__(self):
        self.lock = RLock()
        self.percentiles: List[float] = []
        if THROUGHPUT_ENABLED:
            self.percentiles.extend(THROUGHPUT_PERCENTILES)
        self.hw_ids: List[str] = [DEFAULT_HARDWARE_ID]
        self._predictors: Dict[Tuple[str, float], predictor.Predictor] = {}
        for hw_id in self.hw_ids:
            for percentile in self.percentiles:
                self._add_predictor(hw_id, percentile)

    def _add_predictor(self, hw_id: str, percentile:float) -> None:
        self._predictors[(hw_id, percentile)] = self._create_predictor(hw_id, percentile)

    def add_hw_id(self, hw_id: str):
        for percentile in self.percentiles:
            self._add_predictor(hw_id, percentile)

    def add_percentile(self, percentile: float):
        for hw_id in self.hw_ids:
            self._add_predictor(hw_id, percentile)

    def predict_time(self, hw_id: str, combination: List[str], time_limit: int, percentile: float) -> bool:
        with self.lock:
            verdict, _ = self._predictors[(hw_id, percentile)].predict_combination(comb=combination, time_limit=time_limit)
        return verdict is not None and verdict

    def predict_throughput(self, hw_id: str, combination: List[str], max_value: int) -> bool:
        assert THROUGHPUT_ENABLED and len(THROUGHPUT_PERCENTILES) > 0
        total_time = 0.0
        previous = 0.0
        time = None
        for percentile in THROUGHPUT_PERCENTILES:
            with self.lock:
                _, prediction = self._predictors[(hw_id, percentile)].predict_combination(comb=combination, time_limit=0)
            time = prediction.combined
            if time is None:
                return False
            total_time += (percentile - previous) * time
            previous = percentile
        total_time += (100 - previous) * time

        return total_time < max_value * 100 # we multiply it by 100 since the percentiles are 1 to 100, not 0 to 1.

    @staticmethod
    def _create_predictor(hw_id: str, percentile: float) -> predictor.Predictor:
        _predictor = predictor.Predictor(nodetype=hw_id, percentile=percentile)
        _predictor.assign_headers("headers.json")
        _predictor.assign_groundtruth("groundtruth.json")
        _predictor.assign_user_boundary("user_boundary.json")

        from clustering_alg import MeanShift
        from clustering_score import VMeasure
        from distance import AveragePairCorrelation
        from normalizer import MinMax
        from optimizer import SimAnnealing
        _predictor.configure(
            clustering_alg=MeanShift(),
            clustering_score=VMeasure(),
            distance=AveragePairCorrelation(),
            normalizer=MinMax(),
            optimizer=SimAnnealing(),
            boundary_percentage=140)
        return _predictor

    def provide_new_files(self, files: Dict[str, List[str]]) -> None:
        for ((hw_id, _), predictor) in self._predictors.items():
            if hw_id in files:
                for filename in files[hw_id]:
                    predictor.provide_data_matrix(filename)
                predictor.prepare_predictor()


class PredictorUpdater:

    def __init__(self, predictor_: MultiPredictor):
        self._files: Dict[str, List[str]] = {}
        self._update_time: bool = False
        self._predictor: MultiPredictor = predictor_
        self._file_count: int = 0
        self._lock = RLock()

    @property
    def file_count(self) -> int:
        return self._file_count

    def start(self) -> None:
        threading.Thread(target=self._run, args=(), daemon=True).start()

    def _run(self) -> None:
        if self._update_time:
            with self._lock:
                self._update_time = False
                files = self._files
                self._files = {}
                self._file_count = 0
            self._predictor.provide_new_files(files)

        else:
            time.sleep(1)

    def provide_file(self, hw_id: str, filename:str):
        with self._lock:
            if hw_id not in self._files:
                self._files[hw_id] = []
            self._files[hw_id].append(filename)
            self._file_count += 1

    def update_predictor(self) -> None:
        with self._lock:
            self._update_time = True


class ScenarioGenerator:

    def __init__(self, _predictor: MultiPredictor):
        import typing
        self._combination_counter: typing.Counter[Tuple[str, str, int]] = Counter()
        self._probes: Dict[str, Probe] = {}
        self._measured_combinations: Dict[str, Set[str]] = {}
        self._isolation_scenarios: List[str] = []
        self._combination_scenarios: List[Tuple[str, List[str]]] = []
        self._predictor_updater = PredictorUpdater(_predictor)
        self._predictor_updater.start()
        self.next_scenario_id = 0
        self._INITIAL_SCENARIOS_COUNT = 4

    def register_probe(self, probe: Probe) -> None:
        self._probes[probe.alias] = probe
        self._measured_combinations[probe.alias] = set()
        self._isolation_scenarios.append(probe.alias)
        for i in range(1, self._INITIAL_SCENARIOS_COUNT):
            self._combination_scenarios.append((probe.alias, self.generate_random_load(i)))

    def scenario_completed(self, scenario: Scenario) -> None:
        probe_id = scenario.controlled_probe.alias
        bg_probe_ids: List[str] = [probe.alias for probe in scenario.background_probes]
        self._measured_combinations[probe_id].add(self._bg_load_id(bg_probe_ids))
        self._predictor_updater.provide_file(scenario.hw_id, scenario.filename_data)
        if len(self._measured_combinations[probe_id]) == self._INITIAL_SCENARIOS_COUNT or \
            self._predictor_updater.file_count >= 10:
            self._predictor_updater.update_predictor()

    def generate_random_load(self, probe_count):
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
                bg_probe_id = random.choice(self._probes)
                bg_probes.append(bg_probe_id)
            if self._bg_load_id(bg_probes) in self._measured_combinations[probe_id]:
                return self.next_scenario()
            return self._create_scenario(probe_id, bg_probes, hw_id)
        return None


class PredictorService(PredictorServicer):

    def __init__(self):
        self._single_process_predictor = PercentilePredictor()
        self._predictor = MultiPredictor()
        self.applications: Dict[str, Application] = {}
        self._probes_by_component: Dict[str, Set[str]] = {}
        self._probes_by_id: Dict[str, Probe] = {}
        self._scenario_generator = ScenarioGenerator(self._predictor)
        self._lock = RLock()

    def assignment_from_pb(self, assignment_pb: predictor_pb.Assignment) -> Tuple[str, Dict[str, int]]:
        assignment: Dict[str, int] = {}
        for component_pb in assignment_pb.components:
            assert component_pb.component_id in self._probes_by_component
            assignment[component_pb.component_id] = component_pb.count
        return assignment_pb.hw_id, assignment

    def generate_combinations(self, assignment: Dict[str, int]) -> Iterator[List[str]]:
        def generate_probe_combinations(probes: List[str], size: int, combination: List[str]) -> List[str]:
            if len(combination) == size:
                yield combination
                return
            if len(probes) == 0:
                return
            for i in range(size + 1):
                if len(combination) + i <= size:
                    for probe_combination in generate_probe_combinations(probes[1:], size, combination + [probes[0]] * i):
                        yield probe_combination

        def generate_component_combinations(
                components: List[Tuple[str, int]],
                combination: List[str],
                main_component: str
        ) -> List[str]:
            if len(components) == 0:
                yield combination
                return
            component, count = components[0]
            if component == main_component:
                count = count - 1
            probes = list(self._probes_by_component[component])
            for probe_combination in generate_probe_combinations(probes, count, []):
                for full_combination in generate_component_combinations(components[1:], combination + probe_combination,
                                                                        main_component):
                    yield full_combination

        for component in assignment:
            for full_combination in generate_component_combinations(list(assignment.items()), [], component):
                for probe_id in self._probes_by_component[component]:
                    yield [probe_id] + full_combination

    def Predict(self, request: predictor_pb.Assignment, context):
        if len(request.components) == 1 and request.components[0].count == 1 and request.hw_id == DEFAULT_HARDWARE_ID:
            assert request.components[0].component_id in self._probes_by_component
            return predictor_pb.Prediction(result=True)
        hw_id, assignment = self.assignment_from_pb(request)
        for combination in self.generate_combinations(assignment=assignment):
            probe = self._probes_by_id[combination[0]]
            for requirement in probe.requirements:
                prediction: bool = False
                if isinstance(requirement, TimeContract):
                    prediction = self._predictor.predict_time(
                        hw_id=hw_id,
                        combination=combination,
                        time_limit=requirement.time,
                        percentile=requirement.percentile
                    )
                elif isinstance(requirement, ThroughputContract):
                    prediction = self._predictor.predict_throughput(
                        hw_id=hw_id,
                        combination=combination,
                        max_value=requirement.mean_request_time
                    )
                if not prediction:
                    self._scenario_generator.increase_count(hw_id, combination[0], len(combination))
                    return predictor_pb.Prediction(result=False)
        return predictor_pb.Prediction(result=True)

    def RegisterApp(self, request, context):
        app = Application.init_from_pb(request)
        with self._lock:
            self.applications[app.name] = app
            for component in app.components.values():
                for probe in component.probes:
                    self._register_probe(probe)
                    self._scenario_generator.register_probe(probe)
        return predictor_pb.RegistrationAck()

    def UnregisterApp(self, request, context):
        # TODO: implement UnregisterApp
        return predictor_pb.RegistrationAck()

    def RegisterHwConfig(self, request, context):
        # TODO: implement RegisterHwConfig
        # TODO: implement RegisterPercentile
        hw_id: str = request.name
        self._predictor.add_hw_id(hw_id)
        return predictor_pb.RegistrationAck()

    def FetchScenarios(self, request, context):
        with self._lock:
            scenario = self._scenario_generator.next_scenario()
            if scenario is None:
                yield
            logging.info(f"Sending scenario description for scenario {scenario.id_}")
            yield scenario.pb_representation()

    def ReportPercentiles(self, request, context):
        response = ApplicationTimingRequirements()
        response.name = request.name
        for percentile in request.contracts:
            time = self._single_process_predictor.running_time_at_percentile(request.name, percentile.percentile)
            contract = response.contracts.add()
            contract.time = time
            contract.percentile = percentile.percentile
        context.mean = self._single_process_predictor.mean_running_time(request.name)
        return response

    def JudgeApp(self, request, context):
        app = Application.init_from_pb(request)
        # The application has to be already registered before, otherwise we cannot judge it
        assert app.name in self.applications
        with self._lock:
            # All isolation measurements need to be finished before we can judge the app.
            # if self._measuring_phases[app.name] == MeasuringPhase.ISOLATION:
            # Check every QoS requirement one-by-one:
            for component in app.components.values():
                for probe in component.probes:
                    if self._single_process_predictor.has_probe(probe.alias):
                        return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("NEEDS_DATA"))
                    for requirement in probe.requirements:
                        prediction = False
                        if isinstance(requirement, TimeContract):
                            prediction = self._single_process_predictor.predict_time(
                                probe_name=probe.alias,
                                time_limit=requirement.time,
                                percentile=requirement.percentile
                            )
                        elif isinstance(requirement, ThroughputContract):
                            prediction = self._single_process_predictor.predict_throughput(
                                probe_name=probe.alias,
                                max_mean_time=requirement.mean_request_time
                            )
                        if not prediction:
                            return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("REJECTED"))
            # Application is accepted now
            # However, some QoS requirements may have been added between app registration and app evaluation.
            # Thus, we re-register all the probes to include these requirements
            self.applications[app.name] = app
            for component in app.components.values():
                self._probes_by_component[probe.component.id] = set()
                for probe in component.probes:
                    assert probe.alias in self._probes_by_id
                    self._register_probe(probe)
                    self._probes_by_component[probe.component.id].add(probe.alias)
        return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("ACCEPTED"))

    def OnScenarioDone(self, request, context):
        scenario: Scenario = Scenario.init_from_pb(request.scenario, self.applications)
        app_name = scenario.controlled_probe.component.application.name
        logging.info(f"Received ScenarioDone notification for scenario {scenario.id_} of app {app_name}")
        with self._lock:
            # Remove scenario from the list of to-be-done scenarios
            self._scenario_generator.scenario_completed(scenario)
            if len(scenario.background_probes) == 0:
                self._single_process_predictor.add_probe(scenario.controlled_probe.alias, scenario.filename_data)
        return predictor_pb.CallbackAck()

    def _register_probe(self, probe: Probe) -> None:
        self._probes_by_id[probe.alias] = probe


if __name__ == "__main__":
    setup_logging()
    start_grpc_server(PredictorService(), add_PredictorServicer_to_server, PREDICTOR_HOST, PREDICTOR_PORT, block=True)