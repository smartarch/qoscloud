from typing import List

import logging

import requests

from cloud_controller import API_ENDPOINT_IP, API_ENDPOINT_PORT
from cloud_controller.analysis.predictor_interface.predictor_pb2 import ScenarioRequest
from cloud_controller.analysis.predictor_interface.predictor_pb2_grpc import PredictorStub
from cloud_controller.architecture_pb2 import ApplicationTimingRequirements
from cloud_controller.assessment.deploy_controller_pb2 import HwConfig
from cloud_controller.assessment.model import Scenario
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

    def judge_app(self, application: Application) -> JudgeResult:
        reply = self._predictor_stub.JudgeApp(application.get_pb_representation())
        assert 0 < reply.result < 4
        return JudgeResult(reply.result)

    def on_scenario_done(self, scenario: Scenario) -> None:
        logging.info(f"Sending ScenarioDone notification for scenario {scenario.id_}")
        self._predictor_stub.OnScenarioDone(scenario.pb_representation())

    def on_scenario_failed(self, scenario: Scenario, reason: FailureReason) -> None:
        logging.error("Scenario %s failed on %s", scenario, reason)

    def on_app_evaluated(self, app_name: str):
        request = ApplicationTimingRequirements(name=app_name)
        for percentile in [50, 80, 90, 95]:
            contract = request.contracts.add()
            contract.percentile = percentile
        response = self._predictor_stub.ReportPercentiles(request)
        headers = {
            "Content-Type": "application/json",
            "access-token": self._knowledge.api_endpoint_access_token
        }
        payload = {"instanceId": app_name}
        for contract in response.contracts:
            payload[f"percentile_{contract.percentile}"] = contract.time
        requests.post(
            f"http://{API_ENDPOINT_IP}:{API_ENDPOINT_PORT}/ccapi/instance-stats",
            headers=headers, json=payload
        )
