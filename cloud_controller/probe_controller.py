import time
from functools import reduce
from multiprocessing.pool import ThreadPool, ApplyResult
from typing import List, Tuple, Dict, Optional

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ManagedCompin
from cloud_controller.middleware import AGENT_PORT, middleware_pb2
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub

# TODO: calling probes and collecting results

# TODO: calculating statistics:
#   for each instance
#   for components
#   for system at large

# TODO: choosing which compins to measure

# TODO: orchestrating measurement scenarios


class RuntimeMeasurementScenario:

    def __init__(self):
        self.workloads: List[Tuple[ManagedCompin, str, int]] = []


class ScenarioFactory:

    def add_compin(self, compin: ManagedCompin) -> None:
        pass

    def remove_compin(self, compin: ManagedCompin) -> None:
        pass

    def next_scenario(self) -> RuntimeMeasurementScenario:
        pass


class SingleNodeScenarioFactory(ScenarioFactory):
    # TODO: as of now it assumes one probe per component. Fix this!

    def __init__(self):
        self.current_node: str = ""
        self.compins_by_node: Dict[str, List[ManagedCompin]] = {}
        self.nodes: List[str] = []
        self.current_node_index: int = 0
        self.DEFAULT_ITERATION_COUNT = 10

    def add_compin(self, compin: ManagedCompin) -> None:
        assert len(compin.component.probes) == 1
        if compin.node_name not in self.compins_by_node:
            self.compins_by_node[compin.node_name] = []
            self.nodes.append(compin.node_name)
        self.compins_by_node[compin.node_name].append(compin)

    def remove_compin(self, compin: ManagedCompin) -> None:
        assert compin.node_name in self.compins_by_node
        assert compin in self.compins_by_node[compin.node_name]
        self.compins_by_node[compin.node_name].remove(compin)
        if len(self.compins_by_node[compin.node_name]) == 0:
            del self.compins_by_node[compin.node_name]
            self.nodes.remove(compin.node_name)

    def next_scenario(self) -> Optional[RuntimeMeasurementScenario]:
        if len(self.nodes) == 0:
            return None
        if self.current_node_index >= len(self.nodes):
            self.current_node_index = 0

        scenario = RuntimeMeasurementScenario()
        for compin in self.compins_by_node[self.nodes[self.current_node_index]]:
            probe_name = compin.component.probes[0].name
            scenario.workloads.append((compin, probe_name, self.DEFAULT_ITERATION_COUNT))

        self.current_node_index += 1
        return scenario


class StatisticsCollector:

    # TODO: as of now it assumes one probe per component. Fix this!
    # TODO: improve performance of statistics calculation
    def __init__(self):
        self.compin_data: Dict[str, List[float]] = {}
        self.component_data: Dict[Tuple[str, str], List[float]] = {}
        self.time_limits: Dict[Tuple[str, str], float] = {}
        self.compin_time_limits: Dict[str, float] = {}

    def process_data(self, compin: ManagedCompin, data: List[str]):
        component_id = (compin.component.application.name, compin.component.name)
        if compin.id not in self.compin_data:
            self.compin_data[compin.id] = []
            assert len(compin.component.probes) == 1
            self.compin_time_limits[compin.id] = compin.component.probes[0].time_limit
        if component_id not in self.component_data:
            self.component_data[component_id] = []
            assert len(compin.component.probes) == 1
            self.time_limits[component_id] = compin.component.probes[0].time_limit
        for line in data:
            items = line.split(';')
            assert len(items) >= 3
            execution_time = float(items[2])
            self.compin_data[compin.id].append(execution_time)
            self.component_data[component_id].append(execution_time)

    def get_compin_stats(self) -> List[Tuple[str, float]]:
        stats = []
        for compin_id, data in self.compin_data.items():
            time_limit = self.compin_time_limits[compin_id]
            success_count = reduce((lambda x, y: x + 1 if y < time_limit else x), [0] + data)
            success_percentage = success_count / len(data)
            stats.append((compin_id, success_percentage))
        return stats


    def get_component_stats(self) -> List[Tuple[Tuple[str, str], float]]:
        stats = []
        for component_id, data in self.component_data.items():
            time_limit = self.time_limits[component_id]
            success_count = reduce((lambda x, y: x + 1 if y < time_limit else x), [0] + data)
            success_percentage = success_count / len(data)
            stats.append((component_id, success_percentage))
        return stats

    def get_global_stats(self) -> float:
        total_count = reduce((lambda x, y: x + len(y)), [[]] + list(self.compin_data.values()))
        compin_stats = self.get_compin_stats()
        total_successes = reduce((lambda x, y: x + y[1] * len(self.compin_data[y[0]])), [0] + compin_stats)
        return total_successes / total_count


class ProbeController:

    def __init__(self, knowledge: Knowledge):
        self._knowledge = knowledge
        self._pool: ThreadPool = ThreadPool(processes=32)
        self._factory: ScenarioFactory = SingleNodeScenarioFactory()
        self._compin_threads: Dict[str, Tuple[ManagedCompin, ApplyResult]] = {}
        self.statistics_collector = StatisticsCollector()
        self.MEASUREMENT_HEADER = "start_time;end_time;elapsed"

    def measure_workload(self, compin: ManagedCompin, probe: str, cycles: int) -> List[str]:
        stub: MiddlewareAgentStub = connect_to_grpc_server(MiddlewareAgentStub, compin.ip, AGENT_PORT)
        measure_msg = middleware_pb2.ProbeMeasurement(
            probe=middleware_pb2.ProbeDescriptor(name=probe),
            warmUpCycles=0,
            measuredCycles=cycles
        )
        result = stub.MeasureProbe(measure_msg)
        if result.result != middleware_pb2.ProbeCallResult.Result.Value("OK"):
            # TODO: propagate this exception to the highest level
            raise Exception("Error in measurements")

        data: List[str] = []
        for row in stub.CollectProbeResults(measure_msg.probe):
            if row.WhichOneof("resultType") == "header":
                assert row.header.strip() == self.MEASUREMENT_HEADER
            elif row.WhichOneof("resultType") == "row":
                data.append(row.row)
        return data

    def run(self) -> None:
        """
        Measurement thread.
        """
        while True:
            scenario: RuntimeMeasurementScenario = self._factory.next_scenario()
            self._pool = ThreadPool(processes=len(scenario.workloads))
            for compin, probe, cycles in scenario.workloads:
                result = self._pool.apply_async(self.measure_workload, (compin, probe, cycles))
                self._compin_threads[compin.ip] = (compin, result)
            for compin, result in self._compin_threads.values():
                result.wait()
                data = result.get()
                self.statistics_collector.process_data(compin, data)
            time.sleep(1)

    def add_compin(self, compin: ManagedCompin) -> None:
        """
        Notifies the controller about new compin available for measurement. If this compin fits the criteria for the
        current measurement scenario, starts measuring its probes right away.
        :param compin: ManagedCompin to add
        """
        self._factory.add_compin(compin)

    def remove_compin(self, compin: ManagedCompin) -> None:
        """
        Notifies the controller that the given compin is going to be deleted. If this compin is currently being
        measured, stops the measurement.
        :param compin: ManagedCompin to remove
        """
        self._factory.remove_compin(compin)
