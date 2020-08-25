from typing import List

import logging

import requests

from cloud_controller import IVIS_CORE_IP, IVIS_CORE_PORT
from cloud_controller.analysis.predictor_interface.predictor_pb2 import ScenarioRequest, ScenarioCompletion, \
    ScenarioOutcome
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorStub
from cloud_controller.architecture_pb2 import ApplicationTimingRequirements
from cloud_controller.assessment.deploy_controller_pb2 import HwConfig
from cloud_controller.assessment.model import Scenario, RunningTimeContract
from cloud_controller.assessment.scenario_planner import ScenarioPlanner, JudgeResult, FailureReason
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application


class PredictorScenarioPlanner(ScenarioPlanner):

    def __init__(self, knowledge: Knowledge, predictor: PredictorStub):
        self._predictor_stub: PredictorStub = predictor
        self._knowledge: Knowledge = knowledge

    def register_app(self, app: Application) -> None:
        app_pb = app.get_pb_representation()
        assert app_pb is not None
        self._predictor_stub.RegisterApp(app_pb)

    def unregister_app(self, app: Application) -> None:
        app_pb = app.get_pb_representation()
        assert app_pb is not None
        self._predictor_stub.UnregisterApp(app_pb)

    def register_hw_config(self, name: str) -> None:
        self._predictor_stub.RegisterHwConfig(HwConfig(name=name))

    def fetch_scenarios(self) -> List[Scenario]:
        scenarios: List[Scenario] = []
        for scenario_pb in self._predictor_stub.FetchScenarios(ScenarioRequest()):
            scenario = Scenario.init_from_pb(scenario_pb, self._knowledge.applications)
            logging.info(f"Received description for scenario {scenario.id_}")
            scenarios.append(scenario)
        print("Fetching successful")
        return scenarios

    def judge_app(self, app_name: str, contracts: List[RunningTimeContract]) -> JudgeResult:
        requirements = ApplicationTimingRequirements(name=app_name)
        for contract in contracts:
            requirement = requirements.contracts.add()
            requirement.time = contract.time
            requirement.percentile = contract.percentile
        reply = self._predictor_stub.JudgeApp(requirements)
        assert 0 < reply.result < 4
        return JudgeResult(reply.result)

    def on_scenario_done(self, scenario: Scenario) -> None:
        scenario_pb = self._compose_scenario_completion(scenario)
        scenario_pb.outcome = ScenarioOutcome.Value("SUCCESS")
        logging.info(f"Sending ScenarioDone notification for scenario {scenario_pb.scenario.id}")
        self._predictor_stub.OnScenarioDone(scenario_pb)

    def on_scenario_failed(self, scenario: Scenario, reason: FailureReason) -> None:
        scenario_pb = self._compose_scenario_completion(scenario)
        scenario_pb.outcome = ScenarioOutcome.Value(reason.name)
        self._predictor_stub.OnScenarioFailure(scenario_pb)

    def on_app_evaluated(self, app_name: str):
        request = ApplicationTimingRequirements(name=app_name)
        for percentile in [50, 80, 90, 95]:
            contract = request.contracts.add()
            contract.percentile = percentile
        response = self._predictor_stub.ReportPercentiles(request)
        headers = {
            "Content-Type": "application/json",
            "access-token": self._knowledge.ivis_access_token
        }
        payload = {"jobId": app_name}
        for contract in response.contracts:
            payload[f"percentile_{contract.percentile}"] = contract.time
        requests.post(
            f"http://{IVIS_CORE_IP}:{IVIS_CORE_PORT}/ccapi/job-stats",
            headers=headers, json=payload
        )

    def _compose_scenario_completion(self, scenario: Scenario):
        scenario_completion_pb = ScenarioCompletion()
        scenario_pb = scenario_completion_pb.scenario
        scenario.pb_representation(scenario_pb)  # scenario_pb is an in-out argument
        return scenario_completion_pb
