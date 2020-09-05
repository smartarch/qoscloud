#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module provides backend for avocadoctl and end-point for MAPE-K monitor phase
"""

import logging
import time
from concurrent import futures
from multiprocessing import Lock
from typing import List, Iterable

import grpc

# grpc
from threading import Thread

import cloud_controller.architecture_pb2 as arch_pb
# Deploy controller grpc
import cloud_controller.assessment.deploy_controller_pb2 as deploy_pb
import cloud_controller.assessment.deploy_controller_pb2_grpc as deploy_grpc
from cloud_controller import RESERVED_NAMESPACES
from cloud_controller.assessment import CTL_HOST, CTL_PORT
# Deploy updater grpc
from cloud_controller.assessment import PUBLISHER_HOST, PUBLISHER_PORT
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployControllerServicer
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployPublisherServicer
from cloud_controller.assessment.model import AppDatabase, Scenario, AppStatus
from cloud_controller.assessment.scenario_planner import ScenarioPlanner, JudgeResult
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Application, RunningTimeContract
from cloud_controller.middleware.helpers import start_grpc_server
from cloud_controller.middleware.ivis_pb2 import AccessTokenAck

logger = logging.getLogger("DC")


class DeployController(DeployControllerServicer):

    def __init__(self, knowledge: Knowledge, app_db: AppDatabase, scenario_pln: ScenarioPlanner,
                 app_judge: "AppJudge"):
        self._knowledge = knowledge
        self._app_db = app_db
        self._scenario_pln = scenario_pln
        self._app_judge = app_judge

    def UpdateAccessToken(self, request, context):
        if self._knowledge.there_are_jobs():
            logging.error(f"Cannot update the access token due to the jobs already deployed")
            return AccessTokenAck(success=False)
        self._knowledge.update_access_token(request.token)
        logging.info(f"Access token was updated successfully")
        return AccessTokenAck(success=True)

    def SubmitArchitecture(self, architecture: arch_pb.Architecture, context) -> deploy_pb.DeployReply:
        # Check app name for conflicts
        if architecture.name in RESERVED_NAMESPACES or architecture.name in self._app_db:
            logger.error("An app %s was refused (name conflict)" % architecture.name)
            return deploy_pb.DeployReply(rc=deploy_pb.RC_NAME_NOT_AVAILABLE)

        # Adds app to db
        self._app_db.add_app(architecture)
        self._knowledge.add_application(architecture)

        # Submit app for benchmarks
        app = self._knowledge.applications[architecture.name]
        self._scenario_pln.register_app(app)

        self._app_judge.notify_new_application(app)

        # Sends reply
        logger.info("An app %s was successfully received" % architecture.name)
        return deploy_pb.DeployReply(rc=deploy_pb.RC_OK)

    def SubmitRequirements(self, request, context) -> deploy_pb.DeployReply:
        status = self._app_db.get_app_status(request.name)
        if status == AppStatus.MEASURED:
            contracts = []
            for contract in request.contracts:
                contracts.append(RunningTimeContract(contract.time, contract.percentile))
            self._app_db.add_contracts(request.name, contracts)
            self._app_judge.judge_and_rule(request.name)
        return deploy_pb.DeployReply(rc=deploy_pb.RC_OK)

    def RegisterHwConfig(self, hw_config: deploy_pb.HwConfig, context) -> deploy_pb.DeployReply:
        # Register HW config
        self._scenario_pln.register_hw_config(hw_config.name)

        # Sends reply
        return deploy_pb.DeployReply(rc=deploy_pb.RC_OK)

    def DeleteApplication(self, app_name: deploy_pb.AppName, context) -> deploy_pb.DeployReply:
        # Check if app exists
        if app_name.name not in self._app_db:
            logger.error("Cannot delete app %s" % app_name.name)
            return deploy_pb.DeployReply(rc=deploy_pb.RC_NAME_NOT_AVAILABLE)

        # Removes app from db
        app = self._knowledge.applications[app_name.name]
        self._scenario_pln.unregister_app(app)
        self._app_db.remove_app(app_name.name)
        self._knowledge.delete_application(app_name.name)

        # Sends reply
        logger.info("Application %s marked for removal" % app_name.name)
        return deploy_pb.DeployReply(rc=deploy_pb.RC_OK)

    def GetApplicationStats(self, app_name: deploy_pb.AppName, context) -> deploy_pb.AppStats:
        # Check if app exists
        if app_name.name not in self._app_db:
            logger.error("Cannot show info about %s (name not found)" % app_name.name)
            return deploy_pb.AppStats(rc=deploy_pb.RC_NAME_NOT_AVAILABLE)

        # Finds details
        app_stats = self._app_db.print_stats(app_name.name)

        # Sends reply
        return deploy_pb.AppStats(rc=deploy_pb.RC_OK, status=self._app_db.get_app_status(app_name.name))


class DeployPublisher(DeployPublisherServicer):

    def __init__(self, app_db: AppDatabase):
        self.__app_db = app_db

    def DownloadNewArchitectures(self, request: deploy_pb.Empty, context) -> Iterable[arch_pb.Architecture]:
        architectures: List[arch_pb.Architecture] = self.__app_db.publish_new_architectures()

        for architecture in architectures:
            yield architecture
        if len(architectures) > 0:
            logger.info("Architectures published")

    def DownloadNewRemovals(self, request: deploy_pb.Empty, context) -> Iterable[deploy_pb.AppName]:
        removals: List[str] = self.__app_db.publish_new_removals()

        for removal in removals:
            yield deploy_pb.AppName(name=removal)
        if len(removals) > 0:
            logger.info("Removals published")


class AppJudge:

    def __init__(self, app_db: AppDatabase, planner: ScenarioPlanner):
        self._app_db = app_db
        self._planner = planner
        self._lock = Lock()

    def _has_remaining_scenarios(self, app_name: str) -> bool:
        # Checks plans
        for scenario in self._planner.fetch_scenarios():
            if scenario.controlled_probe.component.application.name == app_name:
                return True

        return False

    def judge_and_rule(self, app_name: str) -> None:
        judgement = self._planner.judge_app(app_name, self._app_db.get_contracts(app_name))
        if judgement == JudgeResult.ACCEPTED:
            self._app_db.update_app_status(app_name, AppStatus.ACCEPTED)
            logger.info("App %s accepted", app_name)
        elif judgement == JudgeResult.REJECTED:
            self._app_db.update_app_status(app_name, AppStatus.REJECTED)
            logger.warn("App %s rejected", app_name)
        elif judgement == JudgeResult.NEEDS_DATA:
            logger.warn("App %s needs more data for judgement", app_name)
            # Planner found out that needs more data
            assert self._has_remaining_scenarios(app_name)
        else:
            raise NotImplementedError("Unsupported judgement %s" % judgement)

    def notify_new_application(self, app: Application):
        with self._lock:
            status = self._app_db.get_app_status(app.name)
            if status == AppStatus.RECEIVED:
                # Check if app could be judged
                if not self._has_remaining_scenarios(app.name):
                    self._planner.on_app_evaluated(app.name)
                    self._app_db.update_app_status(app.name, AppStatus.MEASURED)
                    if app.name in self._app_db.contracts:
                        self.judge_and_rule(app.name)

    def notify_scenario_finished(self, scenario: Scenario) -> None:
        app = scenario.controlled_probe.component.application
        self.notify_new_application(app)


def start_publisher_server(app_db: AppDatabase) -> None:
    thread = Thread(
        target=start_grpc_server,
        args=(
            DeployPublisher(app_db),
            deploy_grpc.add_DeployPublisherServicer_to_server,
            PUBLISHER_HOST,
            PUBLISHER_PORT
        ),
        kwargs={'block': True}
    )
    thread.start()


def start_controller_server(knowledge: Knowledge, app_db: AppDatabase, scenario_pln: ScenarioPlanner,
                  app_judge: AppJudge) -> None:
    thread = Thread(
        target=start_grpc_server,
        args=(
            DeployController(knowledge, app_db, scenario_pln, app_judge),
            deploy_grpc.add_DeployControllerServicer_to_server,
            CTL_HOST,
            CTL_PORT
        ),
        kwargs={'block': True}
    )
    thread.start()
