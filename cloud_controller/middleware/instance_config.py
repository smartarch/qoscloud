import json
from typing import Callable, Optional, Dict

from elasticsearch import Elasticsearch

from cloud_controller.middleware import middleware_pb2 as mw_protocols


class ProbeConfig:

    def __init__(self, name: str, signal_set: str, et_signal: str, rc_signal: str, run_count: int):
        self.name = name
        self.signal_set = signal_set
        self.execution_time_signal = et_signal
        self.run_count_signal = rc_signal
        self.run_count = run_count

    def submit_running_time(self, time: float, report_service: Elasticsearch) -> None:
        time *= 1000
        self.run_count += 1
        doc = {
            self.execution_time_signal: time,
            self.run_count_signal: self.run_count
        }
        report_service.index(index=self.run_count, doc_type='_doc', body=doc)


class CallableProbe(ProbeConfig):

    def __init__(self, name: str, signal_set: str, et_signal: str, rc_signal: str, run_count: int, procedure: Callable):
        super(CallableProbe, self).__init__(name, signal_set, et_signal, rc_signal, run_count)
        self.procedure: Callable = procedure


class RunnableProbe(ProbeConfig):

    def __init__(self, name: str, signal_set: str, et_signal: str, rc_signal: str, run_count: int, code: str, config: str, args: str):
        super(RunnableProbe, self).__init__(name, signal_set, et_signal, rc_signal, run_count)
        self.filename = f"./{self.name}.py"
        with open(self.filename, "w") as code_file:
            code_file.write(code)
        self._config = json.loads(config)
        self.args = args
        # self._elasticsearch: Optional[Elasticsearch] = None

    def config(self) -> str:
        return json.dumps(self._config)

    def update_state(self, state: Optional[str] = None) -> None:
        if state is not None and state != "":
            self._config['state'] = json.loads(state)
        else:
            self._config['state'] = None

    def set_es_ip(self, ip: str) -> None:
        self._config['es']['host'] = ip


class InstanceConfig:

    def __init__(self, instance_id: str, api_endpoint_ip: str, api_endpoint_port: int, access_token:str,
                 production: bool):
        self.instance_id: str = instance_id
        self.api_endpoint_url: str = f"http://{api_endpoint_ip}:{api_endpoint_port}/ccapi"
        self.access_token: str = access_token
        self.production: bool = production
        self.probes: Dict[str, ProbeConfig] = {}
        self.reporting_enabled: bool = True

    @staticmethod
    def init_from_pb(config_pb, procedures: Dict[str, Callable]) -> "InstanceConfig":
        config = InstanceConfig(
            config_pb.instance_id,
            config_pb.api_endpoint_ip,
            config_pb.api_endpoint_port,
            config_pb.access_token,
            config_pb.production
        )
        for probe_pb in config_pb.probes:
            if probe_pb.type == mw_protocols.ProbeType.Value('PROCEDURE'):
                assert probe_pb.name in procedures
                probe = CallableProbe(
                    name=probe_pb.name,
                    signal_set=probe_pb.signal_set,
                    et_signal=probe_pb.execution_time_signal,
                    rc_signal=probe_pb.run_count_signal,
                    run_count=probe_pb.run_count,
                    procedure=procedures[probe_pb.name]
                )

            else:
                assert probe_pb.type == mw_protocols.ProbeType.Value('CODE')
                probe = RunnableProbe(
                    name=probe_pb.name,
                    signal_set=probe_pb.signal_set,
                    et_signal=probe_pb.execution_time_signal,
                    rc_signal=probe_pb.run_count_signal,
                    run_count=probe_pb.run_count,
                    code=probe_pb.code,
                    config=probe_pb.config,
                    args=probe_pb.args
                )
                probe.set_es_ip(config_pb.api_endpoint_ip)
            config.probes[probe.name] = probe
        return config