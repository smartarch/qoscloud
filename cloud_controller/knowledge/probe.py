import hashlib
from typing import List, Dict

from cloud_controller import architecture_pb2 as arch_pb, architecture_pb2 as protocols
from cloud_controller.knowledge.application import Application
from cloud_controller.knowledge.component import Component


class QoSContract:

    def __init__(self):
        pass


class TimeContract(QoSContract):

    def __init__(self, time: int, percentile: float):
        super().__init__()
        self.time = time
        self.percentile = percentile


class ThroughputContract(QoSContract):

    def __init__(self, requests: int, per: int):
        super().__init__()
        self.requests = requests
        self.per = per
        self.mean_request_time = self.per / self.requests


class Probe:

    def __init__(self,
                 name: str,
                 component: Component,
                 requirements: List[QoSContract],
                 code: str = "",
                 config: str = "",
                 signal_set: str = "",
                 execution_time_signal: str = "",
                 run_count_signal: str = "",
                 args: str = ""
    ):
        self.name = name
        self.component = component
        self.requirements = requirements

        self.code: str = code
        self.config: str = config
        self.args: str = args
        self._alias = self.generate_alias(self.name, self.component)

        self.signal_set: str = signal_set
        self.execution_time_signal: str = execution_time_signal
        self.run_count_signal: str = run_count_signal

    @property
    def alias(self) -> str:
        return self._alias

    def generate_alias(self, probe_name: str, component: Component) -> str:
        if self.code is None:
            return f"{component.application.name.upper()}_{component.name.upper()}_{probe_name.upper()}"
        else:
            probe_spec = f"{self.component.container_spec}&&{self.code}&&{self.config}&&{self.args}"
            hash_ = hashlib.md5(bytes(probe_spec, 'utf-8'))
            return f"{hash_.hexdigest()}"

    @staticmethod
    def init_from_pb(probe_pb: arch_pb.Probe, applications: Dict[str, Application]) -> "Probe":
        """
        Creates a probe object from protobuf representation.
        """
        component = applications[probe_pb.application].components[probe_pb.component]
        return Probe.init_from_pb_direct(probe_pb, component)

    @staticmethod
    def construct_requirements(probe_pb: arch_pb.Probe) -> List[QoSContract]:
        requirements = []
        for requirement_pb in probe_pb.requirements:
            type = requirement_pb.WhichOneof('type')
            requirement = None
            if type == "time":
                requirement = TimeContract(requirement_pb.time.time, requirement_pb.time.percentile)
            elif type == "throughput":
                requirement = ThroughputContract(requirement_pb.throughput.requests, requirement_pb.throughput.per)
            assert requirement is not None
            requirements.append(requirement)
        return requirements

    @staticmethod
    def init_from_pb_direct(probe_pb: arch_pb.Probe, component: Component):
        requirements = Probe.construct_requirements(probe_pb)
        for probe in component.probes:
            if probe.name == probe_pb.name:
                probe._alias = probe.generate_alias(probe_pb.name, component)
                probe.requirements = requirements
                return probe
        return Probe(
            name=probe_pb.name,
            component=component,
            requirements=requirements,
            code=probe_pb.code,
            config=probe_pb.config,
            signal_set=probe_pb.signal_set,
            execution_time_signal=probe_pb.execution_time_signal,
            run_count_signal=probe_pb.run_count_signal,
            args=probe_pb.args
        )

    def pb_representation(self, probe_pb: arch_pb.Probe = None) -> arch_pb.Probe:
        if probe_pb is None:
            probe_pb = arch_pb.Probe()
        probe_pb.name = self.name
        probe_pb.application = self.component.application.name
        probe_pb.component = self.component.name
        probe_pb.alias = self.alias
        if self.code != "":
            probe_pb.type = protocols.ProbeType.Value('CODE')
            probe_pb.code = self.code
            probe_pb.config = self.config
            probe_pb.args = self.args
        else:
            probe_pb.type = protocols.ProbeType.Value('PROCEDURE')
        probe_pb.signal_set = self.signal_set
        probe_pb.execution_time_signal = self.execution_time_signal
        probe_pb.run_count_signal = self.run_count_signal
        for requirement in self.requirements:
            requirement_pb = probe_pb.requirements.add()
            if isinstance(requirement, TimeContract):
                requirement_pb.time.time = requirement.time
                requirement_pb.time.percentile = requirement.percentile
            else:
                assert isinstance(requirement, ThroughputContract)
                requirement_pb.throughput.requests = requirement.requests
                requirement_pb.throughput.per = requirement.per
        return probe_pb

    def __str__(self) -> str:
        return f"{self._alias}"