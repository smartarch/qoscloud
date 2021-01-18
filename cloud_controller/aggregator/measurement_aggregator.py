import math
from typing import Dict, List, Iterable, Tuple

import logging

import os

from cloud_controller import DATAFILE_EXTENSION, DEFAULT_HARDWARE_ID, RESULTS_PATH
from cloud_controller.assessment.model import Scenario


class MeasurementAggregator:

    def __init__(self):
        self._measurements: Dict[str, List[int]] = {}
        self._mean_running_times: Dict[str, int] = {}

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
        if self.running_time_at_percentile(probe_name, percentile) < time_limit:
            return True
        return False

    def predict_throughput(self, probe_name: str, max_mean_time: int) -> bool:
        if self.mean_running_time(probe_name) < max_mean_time:
            return True
        return False

    def running_time_at_percentile(self, probe_name: str, percentile: float) -> int:
        time_array = self._measurements[probe_name]
        index = math.ceil(len(time_array) * percentile / 100) - 1
        logging.info(f"Predicted running time of {time_array[index]} for process {probe_name} at {percentile} percentile.")
        return time_array[index]
    
    def mean_running_time(self, name: str) -> int:
        return self._mean_running_times[name]
