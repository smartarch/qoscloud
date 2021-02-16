#!/usr/bin/env python3
import time
from threading import Thread
from typing import List, Optional

from cloud_controller.middleware.interpreter import Interpreter
from cloud_controller.middleware.measurement_collectors import DataCollector


class ProbeMonitor:
    """
    Collects probes data and controls probe measurement process. The results are saved in the instance
    container until the assessment controller requests them.
    """

    def __init__(self, interpreter: Interpreter):
        self._workload_enabled = False
        self._interpreter = interpreter
        self._workload_thread = None

    def execute_probe(self, probe_name: str, warm_up_cycles: int, measured_cycles: int,
                      cpu_events: Optional[List[str]] = None) -> int:
        collector = DataCollector(probe_name, cpu_events)

        # Warm up
        if warm_up_cycles > 0:
            self._interpreter.set_reporting(False)
            for _ in range(warm_up_cycles):
                self._interpreter.run_measurement(probe_name)
            self._interpreter.set_reporting(True)
        # Measured
        start = time.perf_counter() * 1000
        for _ in range(measured_cycles):
            collector.before_iteration()
            self._interpreter.run_measurement(probe_name)
            collector.after_iteration()
        collector.finish()

        return round(time.perf_counter() * 1000 - start)

    @property
    def has_workload(self) -> bool:
        return self._workload_enabled

    def _workload(self, probe_name: str):
        while self._workload_enabled:
            self._interpreter.run_measurement(probe_name)

    def start_probe_workload(self, probe_name: str) -> None:
        assert not self._workload_enabled
        self._workload_enabled = True
        self._workload_thread = Thread(target=self._workload, args=(probe_name,))
        self._workload_thread.start()

    def stop_probe_workload(self) -> None:
        assert self._workload_enabled
        self._workload_enabled = False
        self._workload_thread.join()


