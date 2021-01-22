"""
Contains monitor class that is responsible for Monitoring phase of the mapek cycle, as well as FakeMonitor for testing.
The rest of the  monitoring classes used in the framework are located in their respective modules
"""
import logging
from abc import abstractmethod
from typing import List

from cloud_controller import DEFAULT_HARDWARE_ID
from cloud_controller.knowledge.cluster_model import Node
from cloud_controller.knowledge.knowledge import Knowledge


class Monitor:
    """
    Carries out the monitoring functionality of this monitor.
    Is called every monitoring phase
    """

    def __init__(self, knowledge: Knowledge):
        self._knowledge: Knowledge = knowledge

    @abstractmethod
    def monitor(self) -> None:
        pass

    def set_knowledge(self, knowledge: Knowledge):
        if self._knowledge is None:
            self._knowledge = knowledge



class TopLevelMonitor(Monitor):
    """
    In the default setup, responsible for monitoring the following things:
        (1) State of the kubernetes cluster: nodes, pods, namespaces, labels, datacenters - through K8S API
        (2) Client connections and disconnections - through Client Controller
        (3) Submitted applications and applications requested for deletion - through  DeployPublisher
        (4) User Equipment changes (new and removed UE) - through Knowledge.UserEquipmentContainer

    For instances of adaptation controller that do not provide client support (e.g. pre-assessment), only
    Kubernetes monitor should be present.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self._monitors: List[Monitor] = []
        logging.info("Cloud Monitor created.")

    def add_monitor(self, monitor: Monitor):
        monitor.set_knowledge(self._knowledge)
        self._monitors.append(monitor)

    def monitor(self):
        for monitor in self._monitors:
            monitor.monitor()


class FakeMonitor(Monitor):

    def monitor(self):
        nodes = {name: Node(name, DEFAULT_HARDWARE_ID, "", []) for name in ["N1", "N2", "N3", "N4", "N5"]}
        self._knowledge.update_cluster_state(nodes, {})
