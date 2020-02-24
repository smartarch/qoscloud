"""
This module contains the Agents: classes that integrate the managed and unmanaged component instances into the Avocado
framework.
"""
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from threading import RLock
from typing import Optional, Callable, Dict, Generator, List

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller.middleware import CLIENT_CONTROLLER_EXTERNAL_HOST, CLIENT_CONTROLLER_EXTERNAL_PORT, \
    AGENT_PORT, AGENT_HOST
from cloud_controller.middleware.helpers import connect_to_grpc_server, start_grpc_server, OrderedEnum
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentServicer, \
    add_MiddlewareAgentServicer_to_server, ClientControllerExternalStub
from cloud_controller.middleware.mongo_agent import MongoAgent
from cloud_controller.middleware.probe_monitor import ProbeMonitor, DataCollector, IOEventsNotSupportedException, \
    CPUEventsNotSupportedException


class MiddlewareAgent(MiddlewareAgentServicer):
    """
    Provides the interface through which Avocado framework manages the managed components. Instantiated inside
    ServerAgent
    """

    def __init__(self, dependency_map, lock, update_call, finalize_call, initialize_call,
                 probes: Dict[str, Callable[[], None]]):
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
        self._production = None

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

    def Ping(self, request, context):
        """
        This method is used to check whether the instance is already running and to get its current phase
        :return: Current phase of the instance
        """
        if self._probe_monitor is None:
            self._production = request.production
            self._probe_monitor = ProbeMonitor(production=self._production)
            logging.info(f"Production: {self._production}")
            for name, exe in self._probes.items():
                self._probe_monitor.add_probe(name, exe)
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

    def MeasureProbe(self, measurement: mw_protocols.ProbeMeasurement, context) -> mw_protocols.ProbeCallResult:
        # Probe name
        if not self._probe_monitor.has_probe(measurement.probe.name):
            logging.error(f"Probe {measurement.probe.name} not found")
            return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.ERROR)

        # Execute
        try:
            if len(measurement.cpuEvents) == 0:
                cpu_events: Optional[List[str]] = None
            else:
                cpu_events: Optional[List[str]] = measurement.cpuEvents[:]
            if not self._production:
                self._probe_monitor.start_probe_workload(measurement.probe.name)
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
            if not self._probe_monitor.has_probe(workload.probe.name):
                logging.error(f"Probe {workload.probe.name} not found")
                return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.ERROR)

            # Start
            if self._probe_monitor.has_workload:
                self._probe_monitor.stop_probe_workload()
            self._probe_monitor.start_probe_workload(workload.probe.name, workload.probe.name)
        else:
            # Check status
            if not self._probe_monitor.has_workload:
                return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.NOTHING_RUNNING)
            self._probe_monitor.stop_probe_workload()

        return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.OK)

    def CollectProbeResults(self, probe: mw_protocols.ProbeDescriptor, context) \
            -> Generator[mw_protocols.ProbeFullResult, None, None]:
        # Probe name
        if not self._probe_monitor.has_probe(probe.name):
            logging.error(f"Probe {probe.name} not found")
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
        if self._mw_agent.phase == mw_protocols.Phase.Value('INIT'):
            self._mw_agent.phase = mw_protocols.Phase.Value('READY')
            logging.info(f"Phase was switched to READY")

    def set_finished(self) -> None:
        """
        Signalizes that the compin had finished its execution. The Avocado framework will not delete this instance
        until this method is called except for several special cases:
            (1) No client ever received address of this instance as its dependency.
            (2) The client for which this component was created closed its connection to the Avocado framework.
        """
        if self._mw_agent.phase != mw_protocols.Phase.Value('FINISHED'):
            self._mw_agent.phase = mw_protocols.Phase.Value('FINISHED')
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
