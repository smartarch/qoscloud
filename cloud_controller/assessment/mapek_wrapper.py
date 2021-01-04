#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This classes provides interface around production MAPE-K cycle
"""
import logging
from typing import Optional

from cloud_controller import ASSESSMENT_KUBECONFIG, ASSESSMENT_MONGOS_SERVER_IP
from cloud_controller.cleanup import ClusterCleaner
from cloud_controller.extension_manager import ExtensionManager
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState
from cloud_controller.monitoring.monitor import TopLevelMonitor, KubernetesMonitor

logger = logging.getLogger("MW")


class MapekWrapper:
    """
    Deploys ComponentInstanceChains to cloud
    """

    def __init__(self):
        # Assessment analyzer
        self._analyzer = StaticAnalyzer()

        # Default MAPE-K loop controller
        extension_manager = ExtensionManager()
        extension_manager.set_client_support(False)
        extension_manager.set_analyzer(self._analyzer)
        monitor = TopLevelMonitor(extension_manager.knowledge)
        extension_manager.set_kubeconfig(ASSESSMENT_KUBECONFIG)
        monitor.add_monitor(KubernetesMonitor(extension_manager.knowledge, ASSESSMENT_KUBECONFIG))
        extension_manager.set_monitor(monitor)
        extension_manager.set_mongos_ip(ASSESSMENT_MONGOS_SERVER_IP)
        self._adaptation_ctl = extension_manager.get_adaptation_ctl()
        ClusterCleaner(ASSESSMENT_MONGOS_SERVER_IP).cleanup()
        # Load cloud data
        self._adaptation_ctl.monitoring()
        logger.info("Cloud status updated")

    def get_knowledge(self) -> Knowledge:
        return self._adaptation_ctl.knowledge

    def deploy(self, cloud_state: CloudState) -> None:
        """
        Deploy new cloud state to cloud (and removes the old one)
        """
        self._analyzer.set_new_cloud_state(cloud_state)
        self._adaptation_ctl.deploy()
        self._analyzer.reset_cloud_state()
        logger.info("New cloud state deployed")


class StaticAnalyzer:
    """
    Provides saved CloudState to AssessmentPlanner
    """

    def __init__(self):
        self._cloud_state: Optional[CloudState] = None

    def set_new_cloud_state(self, cloud_state: CloudState) -> None:
        """
        Save CloudState for next MAPE-K look
        """
        assert self._cloud_state is None
        self._cloud_state = cloud_state

    def reset_cloud_state(self) -> None:
        assert self._cloud_state is not None
        self._cloud_state = None

    def find_new_assignment(self) -> Optional[CloudState]:
        assert self._cloud_state is not None

        cloud_state = self._cloud_state

        return cloud_state
