import math
from typing import Dict, List

import logging


class PercentilePredictor:

    def __init__(self):
        self._measurements: Dict[str, List[int]] = {}
        self._mean_running_times: Dict[str, int] = {}

    def has_probe(self, name:str):
        return name in self._measurements

    def add_probe(self, name: str, filename: str) -> None:
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

    def add_measurement(self, name: str, running_time: int) -> None:
        for i in range(len(self._measurements[name])):
            if self._measurements[name][i] > running_time:
                self._measurements[name].insert(i, running_time)
                break

    def predict_time(self, probe_name: str, time_limit: int, percentile: int) -> bool:
        if self.running_time_at_percentile(probe_name, percentile) < time_limit:
            return True
        return False

    def predict_throughput(self, probe_name: str, max_mean_time: int) -> bool:
        if self._mean_running_times[probe_name] < max_mean_time:
            return True
        return False

    def running_time_at_percentile(self, probe_name: str, percentile: int) -> int:
        time_array = self._measurements[probe_name]
        index = math.ceil(len(time_array) * percentile / 100) - 1
        logging.info(f"Predicted running time of {time_array[index]} for process {probe_name} at {percentile} percentile.")
        return time_array[index]
    
    def mean_running_time(self, name: str) -> int:
        return self._mean_running_times[name]
