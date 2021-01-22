from cloud_controller.middleware import AGENT_HOST, AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2 import InstanceConfig, CODE, RunParameters
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub

mwa = connect_to_grpc_server(MiddlewareAgentStub, AGENT_HOST, AGENT_PORT)

cfg = InstanceConfig(
    instance_id="id",
    api_endpoint_ip="0.0.0.0",
    api_endpoint_port=8282,
    production=False
)

probe = cfg.probes.add()
probe.name = "detect"
probe.type = CODE
probe.code = """
from subprocess import call

call(f"bash /code/run.sh", shell=True)"""

mwa.InitializeInstance(cfg)

mwa.RunProbe(RunParameters(instance_id="id", run_id="0", probe_id="detect"))
