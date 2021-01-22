import json
import logging
import os
import time
from enum import Enum
from subprocess import Popen, PIPE
from threading import Thread
from typing import Optional, Any, Callable, Tuple

import requests
from elasticsearch import Elasticsearch

from cloud_controller.middleware import middleware_pb2 as mw_protocols
from cloud_controller.middleware.instance_config import InstanceConfig, RunnableProbe, CallableProbe, ProbeConfig

PYTHON_EXEC = "/usr/bin/python3"


class Request(Enum):
    SUCCESS = 1
    FAIL = 2
    RUNTIME = 3
    STATE = 4


request_names = {
    Request.SUCCESS: "/on-success",
    Request.FAIL: "/on-fail",
    Request.RUNTIME: "/run-request",
    Request.STATE: "/instance-state/"
}


class Interpreter:
    """
    This class is responsible for executing the probes on the instance on which it is instantiated.
    It can run both procedure and code probes, and can report their running times. It is also
    responsible for ensuring that two probes do not run concurrently.
    """

    def __init__(self, config: InstanceConfig, agent, es_host, es_port):
        self._config: InstanceConfig = config
        self._agent = agent
        self._wait_thread: Optional[Thread] = None
        self._requests_thread: Optional[Thread] = None
        self._process: Optional[Popen] = None

        self._current_process: Optional[str] = None
        self._last_run_start_time: float = 0
        self._elasticsearch = Elasticsearch([{'host': es_host, 'port': int(es_port)}])
        self._measurement_iteration_number = 0
        self._result: Optional[Any] = None
        self._ivis_server_available: bool = self._config.access_token != ""

    @property
    def current_process(self) -> Optional[str]:
        """
        :return: the name of the probe that is currently running, can be None
        """
        return self._current_process

    def run_measurement(self, probe_name: str) -> None:
        """
        Executes a measured run of the specified probe.
        :param probe_name: the name of the probe to measure.
        """
        logging.info(f"Running run {self._measurement_iteration_number}")
        probe = self._config.probes[probe_name]
        if isinstance(probe, RunnableProbe) and self._ivis_server_available:
            state = self._send_request(Request.STATE, probe=probe)
        else:
            state = None
        self.run_probe(probe_name, f"run{self._measurement_iteration_number}", json.dumps(state))
        while self._current_process is not None:
            time.sleep(.01)
        self._measurement_iteration_number += 1

    def run_as_probe(self, probe_name: str, procedure: Callable, args: Tuple = ()) -> Optional[Any]:
        """
        Executes the specified callable procedure with the specified arguments. This execution is
        reported as a run of the specified probe, along with its running time.
        :return: the returned value of the called procedure.
        """
        if self._current_process is not None:
            self._report_already_running(probe_name, probe_name)
            return
        self._current_process = probe_name
        self._last_run_start_time = time.perf_counter()
        assert probe_name in self._config.probes
        probe = self._config.probes[probe_name]
        assert isinstance(probe, CallableProbe)
        self._wait_for_process(probe, procedure, args)
        return self._result

    def run_probe(self, probe_name: str, run_id: str, state: str) -> None:
        """
        Runs the procedure or the code corresponding to the specified probe.
        """
        if self._current_process is not None:
            self._report_already_running(probe_name, run_id)
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

    def _report_already_running(self, probe_name: str, run_id: str):
        run_status = {
            'config': "",
            'instanceId': probe_name,
            'runId': run_id,
            'startTime': time.perf_counter(),
            'endTime': time.perf_counter(),
            'output': "",
            'error': "This instance is already running a probe.",
            'returnCode': -1,
        }
        logging.info(f"Cannot run the probe. {run_status['error']}")
        if self._ivis_server_available:
            self._send_request(Request.FAIL, run_status)

    def _run_python_interpreter(self, probe: RunnableProbe):
        fdr, fdw = os.pipe()
        self._process = Popen([PYTHON_EXEC, probe.filename, str(fdw), probe.args], universal_newlines=True,
                              stderr=PIPE, stdout=PIPE, stdin=PIPE, pass_fds=(fdw,))
        self._process.stdin.write(f"{probe.config()}\n")
        self._process.stdin.flush()

        self._wait_thread: Thread = Thread(target=self._wait_for_process, args=(probe,), daemon=True)
        self._wait_thread.start()
        if self._ivis_server_available:
            self._requests_thread: Thread = Thread(target=self._process_runtime_requests, args=(fdr, self._process.stdin, probe.name),
                                                   daemon=True)
            self._requests_thread.start()

    def _send_request(self, request: Request, payload=None, probe=None):
        headers = {
            "Content-Type": "application/json",
            "access-token": self._config.access_token
        }
        if request == Request.STATE:
            return requests.get(
                f"{self._config.api_endpoint_url}{request_names[request]}{probe.name}",
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

    def _call_procedure(self, procedure: Callable, args: Tuple = ()):
        try:
            result = procedure(args)
            return result
        except Exception as e:
            return e

    def _wait_for_process(self, probe: ProbeConfig, procedure: Callable = None, args: Tuple = ()):
        assert self._current_process is not None
        run_status = {
            'config': "",
            'instanceId': probe.name,
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
            if procedure is not None:
                self._result = self._call_procedure(procedure, args)
            else:
                self._result = self._call_procedure(probe.procedure)
            success = not isinstance(self._result, Exception)
            if not success:
                run_status['error'] = str(self._result)
            else:
                run_status['output'] = str(self._result)
        run_status['endTime'] = time.perf_counter()
        if success:
            logging.info(f"Run completed successfully. STDOUT: {run_status['output']}")
            if self._ivis_server_available:
                self._send_request(Request.SUCCESS, run_status)
        else:
            logging.info(f"Run failed. STDERR: {run_status['error']}")
            if self._ivis_server_available:
                self._send_request(Request.FAIL, run_status)
        if self._ivis_server_available and self._config.reporting_enabled:
            probe.submit_running_time(run_status['endTime'] - self._last_run_start_time, self._elasticsearch)
        self._current_process = None
        if self._agent.phase == mw_protocols.Phase.Value('FINALIZING'):
            self._agent.set_finished()

    def _process_runtime_requests(self, fdr, stdin, id):
        fr = os.fdopen(fdr)
        while not fr.closed:
            line = fr.readline()
            print(f"Processing a runtime request: {line}")
            response = self._send_request(Request.RUNTIME, {
                'instanceId': id,
                'request': line
            })
            print(f"Writing a runtime response: {response['response']}")
            stdin.write(f"{response['response']}\n")
            stdin.flush()