#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
from enum import Enum
from subprocess import Popen, PIPE

from threading import Thread
from time import perf_counter
from typing import Optional, Dict, List, Generator

import logging

import json

from cloud_controller.middleware import AGENT_PORT, AGENT_HOST
from cloud_controller.middleware.helpers import start_grpc_server, setup_logging
from cloud_controller.middleware.ivis_pb2 import InitJobAck, RunJobAck, RunStatus, InstanceStatus, RunParameters
from cloud_controller.middleware.ivis_pb2_grpc import JobMiddlewareAgentServicer, \
    add_JobMiddlewareAgentServicer_to_server
from cloud_controller.middleware.middleware_pb2 import Phase, Pong
from cloud_controller.middleware.probe_monitor import IOEventsNotSupportedException, CPUEventsNotSupportedException, \
    DataCollector, ProbeMonitor

import cloud_controller.middleware.middleware_pb2 as mw_protocols
import requests

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


# TODO: locking
class JobAgent(JobMiddlewareAgentServicer):

    def __init__(self):
        self._job_file: str = "./job.py"
        self._job_id: Optional[int] = None
        self._parameters: Optional[str] = None
        self._config: Optional[Dict] = None
        self._minimal_interval: int = 0

        self._current_process: Optional[str] = None
        self._last_run_start_time: float = 0
        self._runs: Dict[str, RunStatus] = {}
        self._wait_thread: Optional[Thread] = None
        self._requests_thread: Optional[Thread] = None
        self._process: Optional[Popen] = None
        self._phase: Phase = Phase.Value('INIT')
        self._ivis_ip = IVIS_HOST
        self._ivis_core_url = f"http://{IVIS_HOST}:{IVIS_PORT}/ccapi"
        self._access_token: str = ""

        self._probe_monitor = None

    def send_request(self, request: Request, payload=None):
        headers = {
            "Content-Type": "application/json",
            "access-token": self._access_token
        }
        if request == Request.STATE:
            return requests.get(
                f"{self._ivis_core_url}{request_names[request]}{self._job_id}",
                headers=headers
            ).json()
        elif request == Request.RUNTIME:
            return requests.post(
                f"{self._ivis_core_url}{request_names[request]}",
                headers=headers, json=payload
            ).json()
        else:
            requests.post(
                f"{self._ivis_core_url}{request_names[request]}",
                headers=headers, json=payload
            )

    def wait_for_process(self):
        assert self._current_process is not None
        run_status = {}
        run_status['config'] = ""
        run_status['jobId'] = self._job_id
        run_status['runId'] = self._current_process
        run_status['endTime'] = perf_counter()
        run_status['startTime'] = self._last_run_start_time

        while self._process.poll() is None:
            time.sleep(0.1)

        run_status['output'] = self._process.stdout.read()
        run_status['error'] = self._process.stderr.read()
        run_status['returnCode'] = self._process.returncode
        if self._process.returncode == 0:
            run_status['status'] = RunStatus.Status.Value('COMPLETED')
            logging.info(f"Run completed successfully. STDOUT: {run_status['output']}")
            self.send_request(Request.SUCCESS, run_status)
        else:
            run_status['status'] = RunStatus.Status.Value('FAILED')
            logging.info(f"Run failed. STDERR: {run_status['error']}")
            self.send_request(Request.FAIL, run_status)
        self._runs[self._current_process] = run_status
        self._current_process = None

    def process_runtime_requests(self, fdr, stdin):
        # TODO: kill the thread at process end
        fr = os.fdopen(fdr)
        while not fr.closed:
            line = fr.readline()
            print(f"Processing a runtime request: {line}")
            response = self.send_request(Request.RUNTIME, {
                'jobId': self._job_id,
                'request': line
            })
            print(f"Writing a runtime response: {response['response']}")
            stdin.write(f"{response['response']}\n")
            stdin.flush()

    def InitializeJob(self, request, context):
        self._job_id = request.job_id
        self._parameters = request.parameters
        self._ivis_ip = request.ivis_core_ip
        self._config = json.loads(request.config)
        self._config['es']['host'] = self._ivis_ip
        self._minimal_interval = request.minimal_interval
        self._ivis_core_url = f"http://{self._ivis_ip}:{request.ivis_core_port}/ccapi"
        self._access_token = request.access_token
        with open(self._job_file, "w") as code_file:
            code_file.write(request.code)
        self._phase = Phase.Value('READY')
        logging.info("Job initialized")
        return InitJobAck()

    def RunJob(self, request, context):
        if self._current_process is not None:
            run_status = {}
            run_status['config'] = ""
            run_status['jobId'] = self._job_id
            run_status['runId'] = request.run_id
            run_status['startTime'] = perf_counter()
            run_status['endTime'] = perf_counter()
            run_status['output'] = ""
            run_status['error'] = "The job is already running."
            run_status['returnCode'] = -1
            run_status['status'] = RunStatus.Status.Value('FAILED')
            logging.info(f"Cannot run the job. {run_status['error']}")
            self.send_request(Request.FAIL, run_status)
            self._runs[self._current_process] = run_status
            return RunJobAck()

        self._current_process = request.run_id
        logging.info(f"Running job with state {request.state}")
        self._last_run_start_time = perf_counter()
        if request.state is not None and request.state != "":
            self._config['state'] = json.loads(request.state)
        else:
            self._config['state'] = None

        fdr, fdw = os.pipe()
        self._process = Popen([PYTHON_EXEC, self._job_file, str(fdw)], universal_newlines=True,
                              stderr=PIPE, stdout=PIPE, stdin=PIPE, pass_fds=(fdw,))
        self._process.stdin.write(f"{json.dumps(self._config)}\n")
        self._process.stdin.flush()

        self._wait_thread: Thread = Thread(target=self.wait_for_process, args=())
        self._wait_thread.start()
        self._requests_thread: Thread = Thread(target=self.process_runtime_requests, args=(fdr, self._process.stdin))
        self._requests_thread.start()


        return RunJobAck()

    def compose_run_status(self, run_id: str, full_status: bool) -> RunStatus:
        if run_id == self._current_process:
            return RunStatus(
                status=RunStatus.Status.Value('RUNNING'),
                run_id=run_id,
                start_time=self._last_run_start_time
            )
        elif run_id in self._runs:
            if full_status:
                return self._runs[run_id]
            else:
                full_status = self._runs[run_id]
                return RunStatus(
                    status=full_status.status,
                    run_id=full_status.run_id,
                    start_time=full_status.start_time,
                    end_time=full_status.end_time
                )
        else:
            return RunStatus()

    def GetRunStatus(self, request, context):
        return self.compose_run_status(request.run_id, request.full_status)

    def GetInstanceStatus(self, request, context):
        instance_status = InstanceStatus()
        run_status: RunStatus
        final_status: RunStatus
        if self._current_process is not None:
            run_status = self.compose_run_status(self._current_process, request.full_status)
            instance_status.current_run.CopyFrom(run_status)
        for run_id in self._runs.keys():
            run_status = self.compose_run_status(run_id, request.full_status)
            if run_status.status == RunStatus.Status.Value('COMPLETED'):
                final_status = instance_status.completed_runs.add()
            else:
                assert run_status.status == RunStatus.Status.Value('FAILED')
                final_status = instance_status.failed_runs.add()
            final_status.CopyFrom(run_status)

        return instance_status

    def _run_as_probe(self):
        logging.info(f"Running run {self.internal_run_number}")
        while self._current_process is not None:
            time.sleep(.01)
        state = self.send_request(Request.STATE)
        self.RunJob(RunParameters(job_id=self._job_id, run_id=f"run{self.internal_run_number}",
                                  state=json.dumps(state)), None)
        self.internal_run_number += 1

    def Ping(self, request, context):
        if not request.production and self._probe_monitor is None:
            self._production = request.production
            self.internal_run_number = 0
            self._probe_monitor = ProbeMonitor(production=self._production)
            logging.info(f"Production: {self._production}")
            self._probe_monitor.add_probe("job", self._run_as_probe)
        return Pong(phase=self._phase)

    def MeasureProbe(self, measurement: mw_protocols.ProbeMeasurement, context) -> mw_protocols.ProbeCallResult:
        logging.info(f"Starting measurements, {measurement.measuredCycles} cycles")
        try:
            if len(measurement.cpuEvents) == 0:
                cpu_events: Optional[List[str]] = None
            else:
                cpu_events: Optional[List[str]] = measurement.cpuEvents[:]
            time = self._probe_monitor.execute_probe("job", measurement.warmUpCycles,
                                                     measurement.measuredCycles, cpu_events)

            return mw_protocols.ProbeCallResult(
                result=mw_protocols.ProbeCallResult.Result.Value("OK"), executionTime=time)
        except IOEventsNotSupportedException:
            return mw_protocols.ProbeCallResult(
                result=mw_protocols.ProbeCallResult.Result.Value("IO_EVENT_NOT_SUPPORTED"))
        except CPUEventsNotSupportedException:
            return mw_protocols.ProbeCallResult(
                result=mw_protocols.ProbeCallResult.Result.Value("CPU_EVENT_NOT_SUPPORTED"))

    def SetProbeWorkload(self, workload: mw_protocols.ProbeWorkload, context) -> mw_protocols.ProbeCallResult:
        if workload.WhichOneof("newWorkload") == "probe":
            # Start
            if self._probe_monitor.has_workload:
                self._probe_monitor.stop_probe_workload()
            self._probe_monitor.start_probe_workload("job", None)
        else:
            # Check status
            if not self._probe_monitor.has_workload:
                return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.NOTHING_RUNNING)
            self._probe_monitor.stop_probe_workload()

        return mw_protocols.ProbeCallResult(result=mw_protocols.ProbeCallResult.Result.OK)

    def CollectProbeResults(self, probe: mw_protocols.ProbeDescriptor, context) \
            -> Generator[mw_protocols.ProbeFullResult, None, None]:
        # Return data
        header_file = DataCollector.get_results_header_file("job")
        if not os.path.exists(header_file):
            logging.error(f"Nothing measured for job")
            yield mw_protocols.ProbeFullResult(nothing=True)
            return
        with open(header_file, "r") as header_stream:
            for line in header_stream:
                yield mw_protocols.ProbeFullResult(header=line)

        data_file = DataCollector.get_results_data_file("job")
        with open(data_file, "r") as data_stream:
            for line in data_stream:
                yield mw_protocols.ProbeFullResult(row=line[:-1])


if __name__ == "__main__":
    setup_logging()
    start_grpc_server(JobAgent(), add_JobMiddlewareAgentServicer_to_server, AGENT_HOST, AGENT_PORT, block=True)
