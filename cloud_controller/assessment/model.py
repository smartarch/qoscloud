#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This classes store apps architecture for assessment and avocadoctl
"""

from enum import IntEnum
from threading import Lock
from typing import Dict, List, Optional

from dataclasses import dataclass

import cloud_controller.architecture_pb2 as arch_pb
from cloud_controller.knowledge.model import Component


class AppStatus(IntEnum):
    RECEIVED = 1
    REJECTED = 2
    ACCEPTED = 3
    PUBLISHED = 4


class AppEntry:
    def __init__(self, architecture: arch_pb.Architecture):
        self.architecture = architecture
        self._name: str = architecture.name
        self.status = AppStatus.RECEIVED

    @property
    def name(self) -> str:
        return self._name


class AppDatabase:
    def __init__(self):
        self._apps: Dict[str, AppEntry] = {}
        self._update_lock = Lock()
        self._app_removal_cache: List[str] = []

    def __contains__(self, item: object) -> bool:
        return self._apps.__contains__(item)

    def __getitem__(self, app_name: str) -> AppEntry:
        return self._apps[app_name]

    def add_app(self, architecture: arch_pb.Architecture) -> None:
        with self._update_lock:
            entry = AppEntry(architecture)
            self._apps[architecture.name] = entry

    def remove_app(self, name: str) -> None:
        with self._update_lock:
            status: AppStatus = self._apps[name].status

            if status is AppStatus.PUBLISHED:
                # Publish the removal to other parts of framework
                self._app_removal_cache.append(name)

            # Delete app from db
            del self._apps[name]

    def publish_new_architectures(self) -> List[arch_pb.Architecture]:
        architectures: List[arch_pb.Architecture] = []

        with self._update_lock:
            # Foreach app
            for app in self._apps.values():
                if app.status == AppStatus.ACCEPTED:
                    # Append to list
                    architectures.append(app.architecture)

                    # Update app status
                    app.status = AppStatus.PUBLISHED
                    app.architecture = None

        return architectures

    def publish_new_removals(self) -> List[str]:
        with self._update_lock:
            tmp = self._app_removal_cache
            self._app_removal_cache = []
        return tmp

    def update_app_status(self, app_name: str, new_status: AppStatus) -> None:
        with self._update_lock:
            self._apps[app_name].status = new_status

    def get_app_status(self, app_name: str) -> Optional[AppStatus]:
        with self._update_lock:
            if app_name in self._apps:
                return self._apps[app_name].status
            else:
                return None

    def print_stats(self, app_name: str) -> str:
        # Basic info
        status = "App name: %s\nApp status: %s\n" % (app_name, str(self._apps[app_name].status))
        return status


@dataclass
class Probe:
    component: Component
    name: str
    wait_per_request: int = 0

    def __str__(self) -> str:
        return f"{self.component.name}/{self.name}"


class Scenario:
    def __init__(self, controlled_probe: Probe, background_probes: List[Probe], hw_id: str, warm_up_cycles: int = 100,
                 measured_cycles: int = 400, cpu_events=None):
        self.controlled_probe = controlled_probe
        self.background_probes = background_probes
        self.hw_id = hw_id
        self.warm_up_cycles = warm_up_cycles
        self.measured_cycles = measured_cycles
        if cpu_events is None:
            self.cpu_events = []  # ["JVM:compilations"]
        else:
            self.cpu_events = cpu_events

    def __str__(self) -> str:
        # Main component
        msg = "%s [" % self.controlled_probe

        # Background components
        msg += ", ".join(str(probe) for probe in self.background_probes)
        msg += "]"

        return msg
