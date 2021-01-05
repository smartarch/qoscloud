"""
This module contains the Agents: classes that integrate the managed and unmanaged component instances into the Avocado
framework.
"""
import logging
import os
from typing import Optional, Callable, Dict, Generator, List, Tuple, Any

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller.middleware.instance_config import InstanceConfig
from cloud_controller.middleware.interpreter import Interpreter
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentServicer
from cloud_controller.middleware.mongo_agent import MongoAgent
from cloud_controller.middleware.probe_monitor import ProbeMonitor
from cloud_controller.middleware.measurement_collectors import DataCollector, IOEventsNotSupportedException, \
    CPUEventsNotSupportedException

PYTHON_EXEC = "/bin/python3"

IVIS_HOST = "0.0.0.0"
IVIS_PORT = 8082
ELASTICSEARCH_PORT = 9200

NO_SHARDING = -1


class MiddlewareAgent(MiddlewareAgentServicer):
    """
    Provides the interface through which Avocado framework manages the managed components. Instantiated inside
    ServerAgent
    """

    def __init__(self, dependency_map, lock, update_call, finalize_call, initialize_call,
                 probes: Dict[str, Callable[[], None]], standalone: bool = False):
        """
        :param dependency_map: a map of dependencies that will be updated on each dependency change
        :param lock: a lock to hold while setting a dependency value
        :param update_call: A callback function which is called when an update of a dependency occurs.
        :param finalize_call: A callback function which is called after the Server's client receives the new dependency
                address, and thus will soon disconnect from this instance
        :param initialize_call: A callback function which is called when the instance receives its initial state.
        :param probe_monitor: A ProbeMonitor for this instance
        """
        self._dependency_addresses = dependency_map
        self._dict_lock = lock
        self._finalize_call = finalize_call
        self._initialize_call = initialize_call
        self._update_call = update_call
        self.phase = mw_protocols.Phase.Value('INIT')
        self._mongo_agent: Optional[MongoAgent] = None
        self._probes: Dict[str, Callable] = probes
        self._probe_monitor = None
        self._config: Optional[InstanceConfig] = None
        self._interpreter: Optional[Interpreter] = None

        self._ready_reported: bool = standalone
        self._finished_reported: bool = standalone

    def set_ready(self):
        with self._dict_lock:
            if self._ready_reported:
                self.phase = mw_protocols.Phase.Value('READY')
            else:
                self._ready_reported = True

    def set_finished(self):
        with self._dict_lock:
            if self._finished_reported:
                self.phase = mw_protocols.Phase.Value('FINISHED')
            else:
                self._finished_reported = True

    def run_as_probe(self, probe_name: str, procedure: Callable, args: Tuple = ()) -> Optional[Any]:
        return self._interpreter.run_as_probe(probe_name, procedure, args)

    def register_probe(self, probe_name: str, procedure: Callable) -> None:
        self._probes[probe_name] = procedure

    def SetDependencyAddress(self, request, context):
        """
        Updates the address of a compin's dependency
        """
        with self._dict_lock:
            self._dependency_addresses[request.name] = request
        if self._update_call is not None:
            self._update_call(request.name, request.ip)
        logging.info(f"Address of dependency {request.name} set to {request.ip}")
        return mw_protocols.AddressAck()

    def FinalizeExecution(self, request, context):
        """
        Signalizes that the client of this instance had received a new dependency address, and thus will soon
        disconnect from this instance. Sets the phase to FINALIZING.
        """
        if self.phase < mw_protocols.Phase.Value('FINISHED'):
            self.phase = mw_protocols.Phase.Value('FINALIZING')
            logging.info(f"Phase was switched to FINALIZING")
            with self._dict_lock:
                if self._interpreter.current_process is None:
                    self.set_finished()
        if self._finalize_call is not None:
            self._finalize_call()
        return mw_protocols.AddressAck()

    def InitializeState(self, request, context):
        """
        Initializes the instance with the provided data.
        This method is not used by Avocado framework anymore, but remains here since it ay be of use in the future.
        """
        if self._initialize_call is not None:
            self._initialize_call(request.data)
        return mw_protocols.StateAck()

    def RunProbe(self, request, context):
        with self._dict_lock:
            if self.phase == self.phase == mw_protocols.Phase.Value('READY'):
                logging.info(f"Running probe {request.probe_id} with state {request.state}")
                self._interpreter.run_probe(request.probe_id, request.run_id, request.state)
            else:
                logging.info(f"Cannot run probe: the instance is not in the correct lifecycle phase")
        return mw_protocols.RunAck()

    def InitializeInstance(self, request, context):
        self._config = InstanceConfig.init_from_pb(request, self._probes)
        self._interpreter = Interpreter(self._config, self, request.api_endpoint_ip, ELASTICSEARCH_PORT)
        if not self._config.production:
            self._probe_monitor = ProbeMonitor(interpreter=self._interpreter)
        self.set_ready()
        logging.info("Instance initialized")
        return mw_protocols.InitAck()

    def Ping(self, request, context):
        """
        This method is used to check whether the instance is already running and to get its current phase
        :return: Current phase of the instance
        """
        return mw_protocols.Pong(phase=self.phase)

    def SetMongoParameters(self, request, context):
        """
        Sets the parameters for MongoAgents and instantiates it. The parameters include IP of Mongos instance,
        user's shard key, database and collection to use.
        """
        if self._mongo_agent is None:
            self._mongo_agent = MongoAgent(request.mongosIp, request.db, request.collection, request.shardKey != NO_SHARDING)
        else:
            self._mongo_agent._set_mongos_ip(request.mongosIp)
        return mw_protocols.MongoParametersAck()

    def SetStatefulnessKey(self, request, context):
        """
        Sets the shard key for MongoAgent.
        """
        if self._mongo_agent is not None:
            self._mongo_agent._set_shard_key(request.shardKey)
        return mw_protocols.MongoParametersAck()

    @property
    def mongo_agent(self) -> Optional[MongoAgent]:
        if self._mongo_agent is None or self._mongo_agent._shard_key == None:
            return None
        return self._mongo_agent

    def _check_measurement_errors(self, probe_name: str):
        if self._config.production:
            logging.error(f"Probe measurement in production is not supported")
            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.ERROR)
        # Probe name
        if probe_name not in self._config.probes:
            logging.error(f"Probe {probe_name} not found")
            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.ERROR)
        return None

    def MeasureProbe(self, measurement: mw_protocols.ProbeMeasurement, context) -> mw_protocols.ProbeCallResult:
        self._config.reporting_enabled = measurement.reporting_enabled
        error = self._check_measurement_errors(measurement.probe.name)
        if error:
            return error
        # Execute
        try:
            if len(measurement.cpuEvents) == 0:
                cpu_events: Optional[List[str]] = None
            else:
                cpu_events: Optional[List[str]] = measurement.cpuEvents[:]
            time = self._probe_monitor.execute_probe(measurement.probe.name, measurement.warmUpCycles,
                                                     measurement.measuredCycles, cpu_events)

            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.Value("OK"), executionTime=time)
        except IOEventsNotSupportedException:
            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.Value("IO_EVENT_NOT_SUPPORTED"))
        except CPUEventsNotSupportedException:
            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.Value("CPU_EVENT_NOT_SUPPORTED"))

    def SetProbeWorkload(self, workload: mw_protocols.ProbeWorkload, context) -> mw_protocols.ProbeCallResult:
        self._config.reporting_enabled = workload.reporting_enabled
        if workload.WhichOneof("newWorkload") == "probe":
            # Probe name
            error = self._check_measurement_errors(workload.probe.name)
            if error:
                return error
            # Start
            if self._probe_monitor.has_workload:
                self._probe_monitor.stop_probe_workload()
            self._probe_monitor.start_probe_workload(workload.probe.name, workload.probe.name)
        else:
            if self._config.production:
                logging.error(f"Probe measurement in production is not supported")
                return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.ERROR)
            # Check status
            if not self._probe_monitor.has_workload:
                return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.NOTHING_RUNNING)
            self._probe_monitor.stop_probe_workload()

        return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.OK)

    def CollectProbeResults(self, probe: mw_protocols.ProbeDescriptor, context) \
            -> Generator[mw_protocols.ProbeFullResult, None, None]:
        error = self._check_measurement_errors(probe.name)
        if error:
            yield mw_protocols.ProbeFullResult(nothing=True)
            return
        # Return data
        header_file = DataCollector.get_results_header_file(probe.name)
        if not os.path.exists(header_file):
            logging.error(f"Nothing measured for {probe.name}")
            yield mw_protocols.ProbeFullResult(nothing=True)
            return
        with open(header_file, "r") as header_stream:
            for line in header_stream:
                yield mw_protocols.ProbeFullResult(header=line)

        data_file = DataCollector.get_results_data_file(probe.name)
        with open(data_file, "r") as data_stream:
            for line in data_stream:
                yield mw_protocols.ProbeFullResult(row=line[:-1])


