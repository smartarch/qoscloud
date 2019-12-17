import cloud_controller.analysis.predictor_pb2 as predictor_pb
from cloud_controller.analysis.predictor_pb2_grpc import PredictorServicer


class PredictorService(PredictorServicer):
    def RegisterApp(self, request, context):
        return predictor_pb.RegistrationAck()

    def UnregisterApp(self, request, context):
        return predictor_pb.RegistrationAck()

    def RegisterHwConfig(self, request, context):
        return predictor_pb.RegistrationAck()

    def FetchScenarios(self, request, context):
        return super().FetchScenarios(request, context)

    def JudgeApp(self, request, context):
        return super().JudgeApp(request, context)

    def OnScenarioDone(self, request, context):
        return super().OnScenarioDone(request, context)

    def OnScenarioFailure(self, request, context):
        return super().OnScenarioFailure(request, context)
