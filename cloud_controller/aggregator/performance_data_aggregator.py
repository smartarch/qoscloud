from threading import RLock
from typing import Dict, List, Tuple, Iterator, Set

import logging

import cloud_controller.aggregator.predictor_pb2 as predictor_pb
from cloud_controller import DEFAULT_HARDWARE_ID, PREDICTOR_HOST, PREDICTOR_PORT
from cloud_controller.aggregator.multipredictor import MultiPredictor
from cloud_controller.aggregator.predictor_pb2_grpc import PredictorServicer, \
    add_PredictorServicer_to_server
from cloud_controller.aggregator.scenario_generator import ScenarioGenerator
from cloud_controller.architecture_pb2 import ApplicationTimingRequirements
from cloud_controller.assessment.model import Scenario
from cloud_controller.aggregator.measurement_aggregator import MeasurementAggregator
from cloud_controller.knowledge.model import Application, Probe, TimeContract, ThroughputContract
from cloud_controller.middleware.helpers import start_grpc_server, setup_logging


class PerformanceDataAggregator(PredictorServicer):

    def __init__(self):
        self._single_process_predictor = MeasurementAggregator()
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
    start_grpc_server(PerformanceDataAggregator(), add_PredictorServicer_to_server, PREDICTOR_HOST, PREDICTOR_PORT, block=True)