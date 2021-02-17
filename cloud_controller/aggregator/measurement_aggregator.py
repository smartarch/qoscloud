import math
from typing import Dict, List, Iterable, Tuple

import logging

import os

from elasticsearch import Elasticsearch

from cloud_controller import DATAFILE_EXTENSION, RESULTS_PATH, API_ENDPOINT_IP, DEFAULT_HARDWARE_ID
from cloud_controller.assessment.model import Scenario
from cloud_controller.middleware.instance_config import ELASTICSEARCH_PORT


class MeasurementAggregator:
    """
    Loads response time data from measurement data files. Processes these data in order to be able
    to answer the questions about response times of probes at different percentiles and their mean
    response times.

    This module is used for two purposes:

        (1) For the application review process (verifying that the QoS requirements are realistic.

        (2) For responding to the questions about response times and throughput coming from the
        cloud controller in cases where the combination in question was already measured directly.
        This is faster than making predictions based on the statistical model, but can only be
        done for the measured combinations.
    """

    def __init__(self):
        self._measurements: Dict[str, List[int]] = {}
        self._mean_running_times: Dict[str, int] = {}
        self._elasticsearch = Elasticsearch([{'host': API_ENDPOINT_IP, 'port': int(ELASTICSEARCH_PORT)}])

    def report_measurements(self, probe_alias: str, signal_set: str, execution_time_signal: str, run_count_signal: str):
        run_count = 0
        filename = f"{RESULTS_PATH}/{DEFAULT_HARDWARE_ID}/{probe_alias}{DATAFILE_EXTENSION}"
        with open(filename, "r") as file:
            while True:
                line = file.readline()
                lines = line.split(';', 5)
                if len(lines) < 4:
                    break
                start = int(lines[2])
                end = int(lines[3])
                running_time = end - start
                run_count += 1
                doc = {
                    execution_time_signal: running_time,
                    run_count_signal: run_count
                }
                self._elasticsearch.index(index=signal_set, doc_type='_doc', body=doc)
        logging.info(f"{run_count} measurements for {probe_alias} were reported to "
                     f"{signal_set}:{execution_time_signal}:{run_count_signal}")


    def load_existing_measurements(self) -> Iterable[Tuple[str, str, List[str], str]]:
        """
        Loads the measurements from all the measurement data files stored in the system.
        :return: generator of hw_id, probe_id, bg_probe_ids, filename
        """
        if os.path.exists(RESULTS_PATH):
            for dirname in os.listdir(RESULTS_PATH):
                for filename in os.listdir(f"{RESULTS_PATH}/{dirname}"):
                    if filename.endswith(DATAFILE_EXTENSION):
                        _name = filename[:(len(filename) - len(DATAFILE_EXTENSION))]
                        _probes = _name.split("-")
                        measurement_name = self.compose_measurement_name(dirname, _probes)
                        full_filename = f"{RESULTS_PATH}/{dirname}/{filename}"
                        self.process_measurement_file(measurement_name, full_filename)
                        yield dirname, _probes[0], _probes[1:], full_filename

    @staticmethod
    def compose_measurement_name_from_scenario(scenario: Scenario):
        return MeasurementAggregator.compose_measurement_name(
            scenario.hw_id,
            [scenario.controlled_probe.alias] + [probe.alias for probe in scenario.background_probes]
        )

    @staticmethod
    def compose_measurement_name(hw_id: str, probes: List[str]) -> str:
        name = f"{hw_id}@"
        for probe_name in probes:
            name = f"{name}&{probe_name}"
        return name

    def has_measurement(self, name: str):
        return name in self._measurements

    def process_measurement_file(self, name: str, filename: str) -> None:
        """
        Loads a measurement file, processes the response time values in it.
        :param name: Name of the measurement
        :param filename: path to the file
        """
        self._measurements[name] = []
        total_running_time: int = 0
        with open(filename, "r") as file:
            while True:
                line = file.readline()
                lines = line.split(';', 5)
                if len(lines) < 4:
                    break
                start = int(lines[2])
                end = int(lines[3])
                running_time = end - start
                self._measurements[name].append(running_time)
                total_running_time += running_time
        self._mean_running_times[name] = math.ceil(total_running_time // len(self._measurements[name]))
        self._measurements[name].sort()
        logging.info(f"Measurement data file {filename} loaded successfully.\n"
                     f"Records: {len(self._measurements[name])}. Mean: {self._mean_running_times[name]}."
                     f"Median: {self.running_time_at_percentile(name, 50.0)}")

    def add_measurement(self, name: str, running_time: int) -> None:
        for i in range(len(self._measurements[name])):
            if self._measurements[name][i] > running_time:
                self._measurements[name].insert(i, running_time)
                break

    def predict_time(self, probe_name: str, time_limit: int, percentile: float) -> bool:
        """
        Returns true if the response time of the main probe in the measurement at the specified
        percentile is lower than the specified limit.
        :param probe_name: measurement name
        """
        logging.info(f"Predicting running time for process {probe_name} at {percentile} percentile. Requirement: {time_limit}")
        if self.running_time_at_percentile(probe_name, percentile) < time_limit:
            return True
        return False

    def predict_throughput(self, probe_name: str, max_mean_time: int) -> bool:
        """
        Returns true if the mean response time of the main probe in the measurement
        is lower than the specified limit.
        :param probe_name: measurement name
        """
        logging.info(f"Predicting mean running time for process {probe_name}. Requirement: {max_mean_time}")
        if self.mean_running_time(probe_name) < max_mean_time:
            return True
        return False

    def running_time_at_percentile(self, probe_name: str, percentile: float) -> int:
        time_array = self._measurements[probe_name]
        index = math.ceil(len(time_array) * percentile / 100) - 1
        logging.info(f"Predicted running time of {time_array[index]} for process {probe_name} at {percentile} percentile.")
        return time_array[index]
    
    def mean_running_time(self, name: str) -> int:
        logging.info(f"Predicted mean running time of {self._mean_running_times[name]} for process {name}.")
        return self._mean_running_times[name]
