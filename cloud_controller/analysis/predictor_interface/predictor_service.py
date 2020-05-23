import math
from enum import Enum
from threading import RLock
from typing import Dict, List

import logging

import cloud_controller.analysis.predictor_interface.predictor_pb2 as predictor_pb
from cloud_controller import DEFAULT_HARDWARE_ID, GLOBAL_PERCENTILE, PREDICTOR_HOST, PREDICTOR_PORT
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorServicer, \
    add_PredictorServicer_to_server, PredictorStub
from cloud_controller.architecture_pb2 import Architecture, ApplicationType
from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application, Probe, IvisApplication
from cloud_controller.middleware.helpers import start_grpc_server, connect_to_grpc_server, setup_logging
import predictor


class MeasuringPhase(Enum):
    INIT = 1
    ADVANCED = 2
    COMPLETED = 3


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
                ct_.time_limit = math.ceil(probe.time_limit)
        return self._predictor_service.Predict(assignment).result
        # TODO: PROBLEM: no way to specify different percentiles for each prediction


class PredictorService(PredictorServicer):

    def __init__(self):
        self._predictor = self._create_predictor()
        self.probes: Dict[str, List[Probe]] = {}
        self._jobs: Dict[str, Probe] = {}
        self._scenarios_by_app: Dict[str, List[str]] = {}
        self._scenarios_by_id: Dict[str, Scenario] = {}
        self._measuring_phases: Dict[str, MeasuringPhase] = {}

        self._probes_by_id: Dict[str, Probe] = {}
        self._last_scenario_id: int = 0
        self._lock = RLock()

    def _create_predictor(self) -> predictor.Predictor:
        _predictor = predictor.Predictor(nodetype=DEFAULT_HARDWARE_ID, percentile=GLOBAL_PERCENTILE)
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

    def Predict(self, request, context):
        assignment = {}
        for component in request.components:
            assignment[component.component_id] = (component.time_limit, component.count)
        prediction = self._predictor.predict(assignment)
        return predictor_pb.Prediction(result=prediction)

    def _add_scenario(self, probe: Probe, bg_load: List[Probe], app_name: str):
        scenario = Scenario(probe, bg_load, DEFAULT_HARDWARE_ID,
                            scenario_id=str(self._last_scenario_id), app_name=app_name)
        self._last_scenario_id += 1
        self._scenarios_by_app[app_name].append(scenario.id_)
        self._scenarios_by_id[scenario.id_] = scenario

    def RegisterApp(self, request, context):
        with self._lock:
            app = Application.init_from_pb(request)
            self._scenarios_by_app[app.name] = []
            self._measuring_phases[app.name] = MeasuringPhase.INIT
            if request.type == ApplicationType.Value("REGULAR"):
                self.probes[app.name] = []
                for component in app.components.values():
                    for probe in component.probes:
                        self._register_probe(probe)
                        self.probes[app.name].append(probe)
                        for bg_load in [], [probe], [probe, probe]:
                            self._add_scenario(probe, bg_load, app.name)
            elif isinstance(app, IvisApplication):
                self._add_scenario(app.probe, [], app.name)
                # TODO: implement a better strategy for jobs assessment
                # for probe_1 in self._jobs.values():
                #     self._add_scenario(app.probe, [probe_1], app.name)
                #     self._add_scenario(probe_1, [app.probe], app.name)
                #     for probe_2 in self._jobs.values():
                #         if probe_2.name != probe_1.name:
                #             self._add_scenario(app.probe, [probe_1, probe_2], app.name)
                #             self._add_scenario(probe_1, [app.probe, probe_2], app.name)
                self._jobs[app.name] = app.probe
        return predictor_pb.RegistrationAck()

    def UnregisterApp(self, request, context):
        # TODO: implement UnregisterApp
        return predictor_pb.RegistrationAck()

    def RegisterHwConfig(self, request, context):
        # TODO: implement RegisterHwConfig
        return predictor_pb.RegistrationAck()

    def FetchScenarios(self, request, context):
        with self._lock:
            for list in self._scenarios_by_app.values():
                for scenario_id in list:
                    logging.info(f"Sending scenario description for scenario {scenario_id}")
                    yield self._scenarios_by_id[scenario_id].pb_representation(predictor_pb.Scenario())

    def JudgeApp(self, request, context):
        if len(self._scenarios_by_app[request.name]) == 0:
            self._measuring_phases[request.name] = MeasuringPhase.COMPLETED
        with self._lock:
            if self._measuring_phases[request.name] != MeasuringPhase.COMPLETED:
                return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("NEEDS_DATA"))
            self._predictor.prepare_predictor()
            if request.name in self.probes:
                for probe in self.probes[request.name]:
                    prediction = self._predictor.predict({probe.alias: (math.ceil(probe.time_limit), 1)})
                    if prediction is None:
                        raise Exception("Predictor needs data for single process prediction!")
                    elif prediction is False:
                        return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("REJECTED"))
            else:
                probe = self._jobs[request.name]
                prediction = self._predictor.predict({probe.alias: (math.ceil(probe.time_limit), 1)})
                if prediction is None:
                    raise Exception("Predictor needs data for single process prediction!")
                elif prediction is False:
                    return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("REJECTED"))
            return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("ACCEPTED"))

    def OnScenarioDone(self, request, context):
        with self._lock:
            self._predictor.provide_data_matrix(request.scenario.filename)
            # Remove scenario from the list of to-be-done scenarios
            app = request.scenario.application
            logging.info(f"Received ScenarioDone notification for scenario {request.scenario.id} of app {app}")
            assert request.scenario.id in self._scenarios_by_app[app]
            self._scenarios_by_app[app].remove(request.scenario.id)
            # If there are no more scenarios for this app, proceed to the next measurement phase
            if len(self._scenarios_by_app[app]) == 0:
                if self._measuring_phases[app] == MeasuringPhase.INIT:
                    if self._get_new_scenarios():
                        self._measuring_phases[app] = MeasuringPhase.ADVANCED
                    else:
                        self._measuring_phases[app] = MeasuringPhase.COMPLETED
                else:
                    assert self._measuring_phases[app] == MeasuringPhase.ADVANCED
                    self._measuring_phases[app] = MeasuringPhase.COMPLETED
        return predictor_pb.CallbackAck()

    def OnScenarioFailure(self, request, context):
        # TODO: implement OnScenarioFailure
        return super().OnScenarioFailure(request, context)

    def _register_probe(self, probe: Probe) -> None:
        self._probes_by_id[probe.alias] = probe

    def _get_new_scenarios(self) -> bool:
        measurements = []  # self._predictor.get_requested_measurements()
        if len(measurements) == 0:
            return False
        for hw_id in measurements:
            for plan in measurements[hw_id]:
                controlled_probe = self._probes_by_id[plan[0]]
                background_probes = []
                for probe_name in plan[1:]:
                    background_probes.append(self._probes_by_id[probe_name])
                scenario = Scenario(controlled_probe, background_probes, hw_id, scenario_id=str(self._last_scenario_id))
                self._last_scenario_id += 1
                self._scenarios_by_id[scenario.id_] = scenario
                app_name = controlled_probe.component.application.name
                self._scenarios_by_app[app_name].append(scenario.id_)
        return True


if __name__ == "__main__":
    setup_logging()
    start_grpc_server(PredictorService(), add_PredictorServicer_to_server, PREDICTOR_HOST, PREDICTOR_PORT, block=True)