#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This classes collect probe results from Kubernetes pods
"""
import os
from multiprocessing import Lock
from typing import Tuple, Iterable, Optional, Dict

import cloud_controller.middleware.middleware_pb2 as mw_protocols
from cloud_controller import middleware
from cloud_controller.assessment import RESULTS_PATH
from cloud_controller.assessment.model import Scenario
from cloud_controller.knowledge.model import Compin, Probe
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
    def get_folder(probe: Probe, hw_config: str) -> str:
        return RESULTS_PATH + "/" + probe.component.application.name + "/" + hw_config + "/"

    @staticmethod
    def _get_fs_probe_name(probe: Probe) -> str:
        # TODO
        return probe.alias # f"{probe.component.name}_{probe.name}"

    @staticmethod
    def get_results_path(scenario: Scenario) -> Tuple[str, str]:
        """
        Returns path to header and data file for selected scenario
        """
        folder = ResultStorage.get_folder(scenario.controlled_probe, scenario.hw_id)
        file = "-".join(ResultStorage._get_fs_probe_name(probe)
                        for probe in [scenario.controlled_probe] + scenario.background_probes)
        path = folder + '/' + file
        return path + ".header", path + ".csv"

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
        folder = ResultStorage.get_folder(scenario.controlled_probe, scenario.hw_id)
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Collect results to scenario specific folder
        header_path, data_path = ResultStorage.get_results_path(scenario)

        with ResultStorage._collection_lock:
            # Lock files against simultaneous access
            if header_path not in ResultStorage._file_locks.keys():
                ResultStorage._file_locks[header_path] = Lock()

        with ResultStorage._file_locks[header_path]:
            # Check header if scenario leads to extend existing results
            # TODO: fix the following 4 lines
            existing_header: Optional[str] = "run;iteration;start_time;end_time;elapsed;ref-cycles;instructions;cache-references;cache-misses;branch-instructions;branch-misses;PAPI_L1_DCM;rw_completed;rw_merged;rw_sectors;io_in_progress;io_time;weighted_io_time;scale"
            # if os.path.isfile(header_path):
            #     with open(header_path) as header_file:
            #         existing_header = header_file.readline().strip()

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
