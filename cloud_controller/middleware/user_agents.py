import logging
import threading
import time
from abc import ABC, abstractmethod
from threading import RLock
from typing import Dict, Optional, Callable, Tuple, Any

from cloud_controller.middleware import AGENT_HOST, AGENT_PORT, CLIENT_CONTROLLER_EXTERNAL_HOST, \
    CLIENT_CONTROLLER_EXTERNAL_PORT, middleware_pb2 as mw_protocols
from cloud_controller.middleware.middleware_agent import MiddlewareAgent
from cloud_controller.middleware.helpers import OrderedEnum, start_grpc_server, connect_to_grpc_server
from cloud_controller.middleware.middleware_pb2_grpc import add_MiddlewareAgentServicer_to_server, \
    ClientControllerExternalStub
from cloud_controller.middleware.mongo_agent import MongoAgent


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


class ComponentAgent(Agent):
    """
    This class has to be instantiated and started (via start method) in each MANAGED component instance in order to
    integrate it properly with the the Avocado framework.
    """

    def __init__(self, probes: Dict[str, Callable[[], None]] = None,
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

    def run_as_probe(self, probe_name: str, procedure: Callable, args: Tuple = ()) -> Optional[Any]:
        return self._mw_agent.run_as_probe(probe_name, procedure, args)

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

    def register_probe(self, probe_name: str, callable: Callable):
        self._mw_agent.register_probe(probe_name, callable)

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