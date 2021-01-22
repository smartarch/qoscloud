#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This classes collect probe results from Kubernetes pods
"""
import os
from multiprocessing import Lock
from typing import Tuple, Iterable, Optional, Dict

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller import middleware, HEADERFILE_EXTENSION, DATAFILE_EXTENSION, RESULTS_PATH
from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.model import Probe
from cloud_controller.knowledge.instance import Compin
from cloud_controller.middleware.helpers import connect_to_grpc_server_with_channel
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub


class ProbeCollectingException(Exception):
    pass


class UnexpectedHeaderException(Exception):
    pass


class ResultStorage:
    _file_locks: Dict[str, Lock] = {}
    _collection_lock = Lock()

    @staticmethod
    def _collect_probe_results(ip: str, probe: Probe) -> Iterable[Tuple[Optional[str], Optional[str]]]:
        # Open connection
        stub, channel = connect_to_grpc_server_with_channel(MiddlewareAgentStub, ip, middleware.AGENT_PORT, True, production=False)
        probe_msg = mw_protocols.ProbeDescriptor(name=probe.name)

        for reply in stub.CollectProbeResults(probe_msg):
            if reply.HasField("header"):
                yield reply.header, None
            elif reply.HasField("row"):
                yield None, reply.row
            else:
                assert reply.HasField("nothing")
                channel.close()
                raise ProbeCollectingException(f"Probe results for {probe.name} not found on {ip}")

        channel.close()

    @staticmethod
    def collect_scenario(compin: Compin, scenario: Scenario) -> None:
        # Create results folder if needed
        folder = scenario.get_folder()
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Collect results to scenario specific folder
        header_path, data_path = scenario.get_results_path()

        with ResultStorage._collection_lock:
            # Lock files against simultaneous access
            if header_path not in ResultStorage._file_locks.keys():
                ResultStorage._file_locks[header_path] = Lock()

        with ResultStorage._file_locks[header_path]:
            # Check header if scenario leads to extend existing results
            existing_header: Optional[str] = "run;iteration;start_time;end_time;elapsed;ref-cycles;instructions;cache-references;cache-misses;branch-instructions;branch-misses;PAPI_L1_DCM;rw_completed;rw_merged;rw_sectors;io_in_progress;io_time;weighted_io_time;scale"

            # Save results to file
            header_lines = 0
            with open(header_path, "w+") as header_file, open(data_path, "w+") as data_file:
                for header_line, data_line in ResultStorage._collect_probe_results(compin.ip,
                                                                                   scenario.controlled_probe):
                    if header_line is not None:
                        header_line = header_line.strip() + ';scale'
                        header_file.write(header_line + '\n')
                        header_lines += 1
                        # Check header for conflict
                        if existing_header is not None and existing_header != header_line:
                            raise UnexpectedHeaderException(f"Expected header {existing_header} "
                                                            f"but received {header_line} on {scenario}")
                    if data_line is not None:
                        data_file.write(data_line + '\n')
            assert header_lines == 1
