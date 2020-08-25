import math
from typing import Dict, List

import logging


class PercentilePredictor:

    def __init__(self):
        self._jobs: Dict[str, List[int]] = {}

    def add_job(self, name: str, filename: str):
        self._jobs[name] = []
        with open(filename, "r") as file:
            while True:
                line = file.readline()
                lines = line.split(';', 5)
                if len(lines) < 4:
                    break
                start = int(lines[2])
                end = int(lines[3])
                running_time = end - start
                self._jobs[name].append(running_time)
        self._jobs[name].sort()

    def add_measurement(self, name: str, running_time: int):
        for i in range(len(self._jobs[name])):
            if self._jobs[name][i] > running_time:
                self._jobs[name].insert(i, running_time)

    def predict(self, job_name: str, time_limit: int, percentile: int):
        if self.predict_time(job_name, percentile) < time_limit:
            return True
        return False

    def predict_time(self, job_name: str, percentile: int):
        time_array = self._jobs[job_name]
        index = math.ceil(len(time_array) * percentile / 100) - 1
        logging.info(f"Predicted running time of {time_array[index]} for process {job_name} at {percentile} percentile.")
        return time_array[index]
