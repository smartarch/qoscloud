from enum import Enum
from typing import Dict, List

import cloud_controller.analysis.predictor_interface.predictor_pb2 as predictor_pb
from cloud_controller import DEFAULT_HARDWARE_ID, GLOBAL_PERCENTILE
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorServicer
from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application, Probe
from src import predictor


class MeasuringPhase(Enum):
    INIT = 1
    ADVANCED = 2
    COMPLETED = 3


# TODO: locking
class PredictorService(PredictorServicer, Predictor):

    def __init__(self, knowledge: Knowledge):
        self._predictor = predictor.Predictor()
        self._predictor.register_nodetype(DEFAULT_HARDWARE_ID, "data/default.header")
        self._predictor.register_percentile(DEFAULT_HARDWARE_ID, GLOBAL_PERCENTILE)
        self.probes: Dict[str, List[Probe]] = {}
        self._initial_scenarios: Dict[str, List[str]] = {}
        self._advanced_scenarios: Dict[str, List[str]] = {}
        self._scenarios_by_id: Dict[str, Scenario] = {}
        self._measuring_phases: Dict[str, MeasuringPhase] = {}
        self.knowledge: Knowledge = knowledge

        self._probes_by_id: Dict[str, Probe] = {}
        self._last_scenario_id: int = 0

    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        assignment = {}
        for component_id, count in components_on_node.items():
            component = self.knowledge.components[component_id]
            for probe in component.probes:
                assignment[probe.alias] = (probe.time_limit, count)
        self._predictor.predict(node_id, assignment, GLOBAL_PERCENTILE)
        # TODO: PROBLEM: no way to specify the number of instances of the same process
        # TODO: PROBLEM: no way to specify different percentiles for each prediction

    def RegisterApp(self, request, context):
        app = Application.init_from_pb(request)
        self.probes[app.name] = []
        self._initial_scenarios[app.name] = []
        self._advanced_scenarios[app.name] = []
        self._measuring_phases[app.name] = MeasuringPhase.INIT
        for component in app.components.values():
            for probe in component.probes:
                self._register_probe(probe)
                self.probes[app.name].append(probe)
                scenario = Scenario(probe, [], DEFAULT_HARDWARE_ID, scenario_id=str(self._last_scenario_id))
                self._last_scenario_id += 1
                self._initial_scenarios[app.name].append(scenario.id_)
                self._scenarios_by_id[scenario.id_] = scenario
        return predictor_pb.RegistrationAck()

    def UnregisterApp(self, request, context):
        # TODO: implement UnregisterApp
        return predictor_pb.RegistrationAck()

    def RegisterHwConfig(self, request, context):
        # TODO
        self._predictor.register_nodetype(request.name, f"data/{request.name}")
        return predictor_pb.RegistrationAck()

    def FetchScenarios(self, request, context):
        from itertools import chain
        for list in chain(self._initial_scenarios.values(), self._advanced_scenarios.values()):
            for scenario_id in list:
                yield self._scenarios_by_id[scenario_id].pb_representation(predictor_pb.Scenario())

    def JudgeApp(self, request, context):
        if self._measuring_phases[request.name] != MeasuringPhase.COMPLETED:
            return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("NEEDS_DATA"))
        self._predictor.preprocess_data()
        for probe, proc_name in self.probes[request.name]:
            prediction = self._predictor.predict(DEFAULT_HARDWARE_ID, {proc_name: probe.time_limit}, GLOBAL_PERCENTILE)
            if prediction is None:
                return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("NEEDS_DATA"))
            elif prediction is False:
                return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("REJECTED"))
        return predictor_pb.JudgeReply(result=predictor_pb.JudgeResult.Value("ACCEPTED"))

    def OnScenarioDone(self, request, context):
        # TODO: scenario IDs
        app = request.scenario.controlled_probe.application
        if request.scenario.id in self._initial_scenarios[app]:
            self._initial_scenarios[app].remove(request.scenario.id)
            if len(self._initial_scenarios[app]) == 0:
                self._get_new_scenarios()
        else:
            assert request.scenario.id in self._advanced_scenarios[app]
            self._advanced_scenarios[app].remove(request.scenario.id)
            if len(self._advanced_scenarios[app]) == 0:
                self._predictor.preprocess_data()
        self._predictor.provide_measurements({request.scenario.hw_id: [request.scenario.filename]})
        return predictor_pb.CallbackAck()

    def OnScenarioFailure(self, request, context):
        # TODO: implement OnScenarioFailure
        return super().OnScenarioFailure(request, context)

    def _register_probe(self, probe: Probe) -> None:
        self._probes_by_id[probe.alias] = probe

    def _get_new_scenarios(self):
        self._predictor.preprocess_data()
        measurements = self._predictor.get_requested_measurements()
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
                self._advanced_scenarios[app_name].append(scenario.id_)
