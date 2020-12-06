"""
This module contains the Agents: classes that integrate the managed and unmanaged component instances into the Avocado
framework.
"""
import json
import logging
import os
import threading
from enum import Enum
from threading import Thread
import time
from abc import ABC, abstractmethod
from subprocess import Popen, PIPE
from threading import RLock
from typing import Optional, Callable, Dict, Generator, List

import requests
from elasticsearch import Elasticsearch

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller.middleware import CLIENT_CONTROLLER_EXTERNAL_HOST, CLIENT_CONTROLLER_EXTERNAL_PORT, \
    AGENT_PORT, AGENT_HOST
from cloud_controller.middleware.helpers import connect_to_grpc_server, start_grpc_server, OrderedEnum
from cloud_controller.middleware.ivis_pb2 import RunStatus, RunJobAck
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentServicer, \
    add_MiddlewareAgentServicer_to_server, ClientControllerExternalStub
from cloud_controller.middleware.mongo_agent import MongoAgent
from cloud_controller.middleware.probe_monitor import ProbeMonitor, DataCollector, IOEventsNotSupportedException, \
    CPUEventsNotSupportedException

PYTHON_EXEC = "/bin/python3"

IVIS_HOST = "0.0.0.0"
IVIS_PORT = 8082

class Request(Enum):
    SUCCESS = 1
    FAIL = 2
    RUNTIME = 3
    STATE = 4

request_names = {
    Request.SUCCESS: "/on-success",
    Request.FAIL: "/on-fail",
    Request.RUNTIME: "/run-request",
    Request.STATE: "/job-state/"
}


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

    def __init__(self, name: str, signal_set: str, et_signal: str, rc_signal: str, run_count: int, code: str, config: str):
        super(RunnableProbe, self).__init__(name, signal_set, et_signal, rc_signal, run_count)
        self.filename = f"./{self.name}.py"
        with open(self.filename, "w") as code_file:
            code_file.write(code)
        self._config = json.loads(config)
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
                    config=probe_pb.config
                )
                probe.set_es_ip(config_pb.api_endpoint_ip)
            config.probes[probe.name] = probe
        return config


class Interpreter:

    def __init__(self, config: InstanceConfig, agent: "MiddlewareAgent", es_host, es_port):
        self._config: InstanceConfig = config
        self._agent: MiddlewareAgent = agent
        self._wait_thread: Optional[Thread] = None
        self._requests_thread: Optional[Thread] = None
        self._process: Optional[Popen] = None

        self._current_process: Optional[str] = None
        self._last_run_start_time: float = 0
        self._elasticsearch = Elasticsearch([{'host': es_host, 'port': int(es_port)}])
        self._measurement_iteration_number = 0

    @property
    def current_process(self) -> Optional[str]:
        return self._current_process

    def run_measurement(self, probe_name: str):
        logging.info(f"Running run {self._measurement_iteration_number}")
        probe = self._config.probes[probe_name]
        if isinstance(probe, RunnableProbe):
            state = self._send_request(Request.STATE)
        else:
            state = None
        self.run_probe(probe_name, f"run{self._measurement_iteration_number}", json.dumps(state))
        while self._current_process is not None:
            time.sleep(.01)
        self._measurement_iteration_number += 1

    def run_probe(self, probe_name: str, run_id: str, state: str) -> None:
        if self._current_process is not None:
            self._report_already_running(run_id)
            return
        self._current_process = probe_name
        self._last_run_start_time = time.perf_counter()
        probe = self._config.probes[probe_name]
        if isinstance(probe, RunnableProbe):
            probe.update_state(state)
            self._run_python_interpreter(probe)
        elif isinstance(probe, CallableProbe):
            self._wait_thread: Thread = Thread(target=self._wait_for_process, args=(probe,), daemon=True)
            self._wait_thread.start()

    def _report_already_running(self, run_id: str):
        run_status = {
            'config': "",
            'jobId': self._config.instance_id,
            'runId': run_id,
            'startTime': time.perf_counter(),
            'endTime': time.perf_counter(),
            'output': "",
            'error': "The job is already running.",
            'returnCode': -1,
            'status': RunStatus.Status.Value('FAILED')
        }
        logging.info(f"Cannot run the job. {run_status['error']}")
        self._send_request(Request.FAIL, run_status)

    def _run_python_interpreter(self, probe: RunnableProbe):
        fdr, fdw = os.pipe()
        self._process = Popen([PYTHON_EXEC, probe.filename, str(fdw)], universal_newlines=True,
                              stderr=PIPE, stdout=PIPE, stdin=PIPE, pass_fds=(fdw,))
        self._process.stdin.write(f"{probe.config()}\n")
        self._process.stdin.flush()

        self._wait_thread: Thread = Thread(target=self._wait_for_process, args=(probe,), daemon=True)
        self._wait_thread.start()
        self._requests_thread: Thread = Thread(target=self._process_runtime_requests, args=(fdr, self._process.stdin),
                                               daemon=True)
        self._requests_thread.start()

    def _send_request(self, request: Request, payload=None):
        headers = {
            "Content-Type": "application/json",
            "access-token": self._config.access_token
        }
        if request == Request.STATE:
            return requests.get(
                f"{self._config.api_endpoint_url}{request_names[request]}{self._config.instance_id}",
                headers=headers
            ).json()
        elif request == Request.RUNTIME:
            return requests.post(
                f"{self._config.api_endpoint_url}{request_names[request]}",
                headers=headers, json=payload
            ).json()
        else:
            requests.post(
                f"{self._config.api_endpoint_url}{request_names[request]}",
                headers=headers, json=payload
            )

    def _wait_for_process(self, probe: ProbeConfig):
        assert self._current_process is not None
        run_status = {
            'config': "",
            'jobId': self._config.instance_id,
            'runId': self._current_process,
            'startTime': self._last_run_start_time
        }
        if isinstance(probe, RunnableProbe):
            while self._process.poll() is None:
                time.sleep(0.1)

            run_status['output'] = self._process.stdout.read()
            run_status['error'] = self._process.stderr.read()
            run_status['returnCode'] = self._process.returncode
            success = self._process.returncode == 0
        else:
            assert isinstance(probe, CallableProbe)
            try:
                probe.procedure()
                success = True
            except Exception as e:
                run_status['error'] = str(e)
                success = False

        run_status['endTime'] = time.perf_counter()
        if success:
            run_status['status'] = RunStatus.Status.Value('COMPLETED')
            logging.info(f"Run completed successfully. STDOUT: {run_status['output']}")
            self._send_request(Request.SUCCESS, run_status)
        else:
            run_status['status'] = RunStatus.Status.Value('FAILED')
            logging.info(f"Run failed. STDERR: {run_status['error']}")
            self._send_request(Request.FAIL, run_status)
        probe.submit_running_time(run_status['endTime'] - self._last_run_start_time, self._elasticsearch)
        if self._agent.phase == mw_protocols.Phase.Value('FINALIZING'):
            self._agent.set_finished()
        else:
            self._current_process = None

    def _process_runtime_requests(self, fdr, stdin):
        # TODO: kill the thread at process end
        fr = os.fdopen(fdr)
        while not fr.closed:
            line = fr.readline()
            print(f"Processing a runtime request: {line}")
            response = self._send_request(Request.RUNTIME, {
                'jobId': self._config.instance_id,
                'request': line
            })
            print(f"Writing a runtime response: {response['response']}")
            stdin.write(f"{response['response']}\n")
            stdin.flush()


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
        self._probes = probes
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
        return RunAck()

    def InitializeInstance(self, request, context):
        self._config = InstanceConfig.init_from_pb(request, self._probes)
        self._interpreter = Interpreter(self._config, self)
        if not self._config.production:
            self._probe_monitor = ProbeMonitor(interpreter=self._interpreter)
        self.set_ready()
        logging.info("Job initialized")
        return InitAck()

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
            self._mongo_agent = MongoAgent(request.mongosIp, request.shardKey, request.db, request.collection)
        else:
            self.mongo_agent._set_mongos_ip(request.mongosIp)
        return mw_protocols.MongoParametersAck()

    @property
    def mongo_agent(self) -> Optional[MongoAgent]:
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


class Agent(ABC):
    """
    A common base for Client and Server agents. Manages the compin's dependencies
    """

    def __init__(self):
        self._dependency_lock = RLock()
        self._dependencies: Dict[str, str] = {}

    def start(self) -> None:
        threading.Thread(target=self._run, args=()).start()

    @abstractmethod
    def _run(self) -> None:
        pass

    def get_dependency_address(self, name: str) -> Optional[str]:
        """
        :param name: name of the dependency to get
        :return: current IP address of the dependency, or None if the dependency was not set yet.
        """
        with self._dependency_lock:
            if name in self._dependencies:
                return self._dependencies[name]
            else:
                return None


class Phase(OrderedEnum):
    INIT = 1
    READY = 2
    FINALIZING = 3
    FINISHED = 4


class ServerAgent(Agent):
    """
    This class has to be instantiated and started (via start method) in each MANAGED component instance in order to
    integrate it properly with the the Avocado framework.
    """

    def __init__(self, probes: Dict[str, Callable[[], None]],
                 update_call: Callable = None,
                 finalize_call: Callable = None,
                 initialize_call: Callable = None):
        """
        :param update_call: A callback function which is called when an update of a dependency occurs.
        :param finalize_call: A callback function which is called after the Server's client receives the new dependency
                address, and thus will soon disconnect from this instance
        :param initialize_call: A callback function which is called when the instance receives its initial state.
        """
        super().__init__()
        self._mw_agent = MiddlewareAgent(self._dependencies, self._dependency_lock, update_call,
                                         finalize_call, initialize_call, probes)
        self._ready_called = False
        self._finished_called = False

    def _run(self) -> None:
        start_grpc_server(self._mw_agent, add_MiddlewareAgentServicer_to_server, AGENT_HOST, AGENT_PORT, 10, True)

    def get_mongo_agent(self) -> Optional[MongoAgent]:
        """
        :return: The MongoAgent of this component instance, if it is present, None otherwise. None can be returned in
        two cases: (1) the MongoAgent was not initialized yet (than it will return the non-None value after the
        initialization is complete), (2) this component is not stateful
        """
        return self._mw_agent.mongo_agent

    def set_ready(self) -> None:
        """
        Signalizes that the compin is ready to accept the incoming connections from the clients. The Avocado framework
        will direct the clients towards an instance only after it calls this method.
        """
        if not self._ready_called:
            self._mw_agent.set_ready()
            self._ready_called = True
            logging.info(f"Phase was switched to READY")

    def set_finished(self) -> None:
        """
        Signalizes that the compin had finished its execution. The Avocado framework will not delete this instance
        until this method is called except for several special cases:
            (1) No client ever received address of this instance as its dependency.
            (2) The client for which this component was created closed its connection to the Avocado framework.
        """
        if not self._finished_called:
            self._mw_agent.set_finished()
            self._finished_called = True
            logging.info(f"Phase was switched to FINISHED")

    def get_phase(self) -> Phase:
        return Phase(self._mw_agent.phase)


class ClientAgent(Agent):
    """
    This class has to be instantiated and started (via start method) in each UNMANAGED component instance in order to
    integrate it properly with the the Avocado framework.
    """

    def __init__(self, application: str, type_: str, id_: str = None, update_call: Callable = None):
        """
        :param application: Name of the application.
        :param type_: Name of the component.
        :param update_call: A callback function which will be called when an update of a dependency occurs.
        """
        super().__init__()
        self._application = application
        self._type = type_
        self._id = id_
        self._stop = False
        self._update_call = update_call
        self._connected = False

    def get_id(self) -> Optional[str]:
        return self._id

    def connected(self) -> bool:
        return self._connected

    def _run(self) -> None:
        client_controller = connect_to_grpc_server(ClientControllerExternalStub,
                                                   CLIENT_CONTROLLER_EXTERNAL_HOST, CLIENT_CONTROLLER_EXTERNAL_PORT)
        request = mw_protocols.ClientConnectRequest(application=self._application, clientType=self._type)
        request.establishedConnection = False
        while True:
            try:
                if self._stop:
                    return
                if self._id is not None:
                    request.establishedConnection = True
                    request.id = self._id
                for command in client_controller.Connect(request):
                    command_type = command.WhichOneof("commandType")
                    if command_type == 'ERROR':
                        self._connected = False
                        logging.info("Received an error from Client Controller")
                        if command.ERROR == mw_protocols.ClientResponseCode.Value("IP_NOT_KNOWN"):
                            logging.info("This UE was not authorized to use this application")
                        elif command.ERROR == mw_protocols.ClientResponseCode.Value("NO_APP_SUBSCRIPTION"):
                            logging.info("You are not subscribed to this application")
                        elif command.ERROR == mw_protocols.ClientResponseCode.Value("ID_NOT_FOUND"):
                            logging.info(f"ID {self._id} is not recognized by CC. Resetting ID.")
                            request.establishedConnection = False
                            self._id = None
                        break
                    elif command_type == 'SET_DEPENDENCY':
                        logging.info("Received new address for %s dependency. Value = %s" %
                                     (command.SET_DEPENDENCY.name, command.SET_DEPENDENCY.ip))
                        with self._dependency_lock:
                            self._dependencies[command.SET_DEPENDENCY.name] = command.SET_DEPENDENCY.ip
                        if self._update_call is not None:
                            self._update_call(command.SET_DEPENDENCY.name, command.SET_DEPENDENCY.ip)
                        self._connected = True
                    elif command_type == 'SET_ID':
                        logging.info(f"Received ID {command.SET_ID} from Client Controller")
                        self._id = command.SET_ID
                        self._connected = True
                    elif command_type == 'WAIT':
                        logging.info("WAIT received")
                        self._connected = True
                    if self._stop:
                        self._connected = False
                        return
                time.sleep(1)
            except Exception as e:
                self._connected = False
                logging.exception("")
                time.sleep(1)

    def stop(self) -> None:
        """
        Signalizes to the agent that it has to close its connection to the Client Controller (if the connection is open),
        or that it has to give up the attempts to connect to the Client Controller (if the connection is not yet
        established)
        """
        self._connected = False
        self._stop = True
