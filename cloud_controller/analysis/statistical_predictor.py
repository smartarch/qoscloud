from typing import Dict

from cloud_controller import PREDICTOR_HOST, PREDICTOR_PORT
from cloud_controller.aggregator import predictor_pb2 as predictor_pb
from cloud_controller.aggregator.predictor_pb2_grpc import PredictorStub
from cloud_controller.analysis.predictor import Predictor
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.middleware.helpers import connect_to_grpc_server


class StatisticalPredictor(Predictor):

    def __init__(self):
        self._predictor_service = connect_to_grpc_server(PredictorStub, PREDICTOR_HOST, PREDICTOR_PORT)

    def predict_(self, node_id: str, components_on_node: Dict[str, int]) -> bool:
        assignment = predictor_pb.Assignment(hw_id=node_id)
        for component_id, count in components_on_node.items():
            ct_ = assignment.components.add()
            ct_.component_id = component_id
            ct_.count = count
        return self._predictor_service.Predict(assignment).result
