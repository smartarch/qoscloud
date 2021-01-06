import time

from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2 import RunParameters
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub
from cloud_controller.middleware.user_agents import ClientAgent

if __name__ == "__main__":
    agent = ClientAgent("facerecognition3", "client")
    agent.start()
    ip = None
    while ip is None:
        ip = agent.get_dependency_address("recognizer")
    mwa: MiddlewareAgentStub = connect_to_grpc_server(MiddlewareAgentStub, ip, AGENT_PORT)
    for i in range(5):
        print(mwa.RunProbe(RunParameters(
            instance_id="recognizer",
            run_id=str(i),
            probe_id="recognize"
        )))
        print(f"Executed run {i}")
        time.sleep(2)