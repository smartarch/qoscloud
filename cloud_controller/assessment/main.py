#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Starts assessment process
"""
import argparse
import logging
import time
from threading import Thread
from typing import Optional

from cloud_controller.analysis.predictor_interface.predictor_scenario_planner import PredictorScenarioPlanner
from cloud_controller.assessment import deploy_controller
from cloud_controller.assessment.depenedency_solver import MasterSlaveSolver
from cloud_controller.assessment.deploy_controller import AppJudge
from cloud_controller.assessment.mapek_wrapper import MapekWrapper
from cloud_controller.assessment.model import AppDatabase
from cloud_controller.assessment.scenario_executor import ScenarioExecutor, FakeScenarioExecutor
from cloud_controller.assessment.scenario_planner import ScenarioPlanner, FakeScenarioPlanner, \
    SimpleScenarioPlanner
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import Node
from cloud_controller.middleware.helpers import setup_logging

if __name__ == "__main__":
    # Logging
    setup_logging()
    logging.info("Starting assessment process")

    # Parse input args
    parser = argparse.ArgumentParser(description="Avocado's assessment process")
    parser.add_argument('-p', '--fake_planner', action='store_true',
                        help='Creates single scenario per probe')
    parser.add_argument('-m', '--fake_mapek', action='store_true',
                        help='Does NOT use K8S for measurement (doesn\'t measure anything)')
    parser.add_argument('-t', '--multi_thread', action='store_true',
                        help='Uses multiple threads for measurement')
    args = parser.parse_args()

    # Apps database
    app_db = AppDatabase()

    # MAPE-K wrapper
    mapek_wrapper: Optional[MapekWrapper] = None
    if not args.fake_mapek:
        mapek_wrapper = MapekWrapper()
        knowledge = mapek_wrapper.get_knowledge()
    else:
        # Adds some static nodes
        FAKE_HW_CONFIG = "hw0"
        knowledge = Knowledge()
        knowledge.nodes["node0"] = Node("node0", FAKE_HW_CONFIG, "", [])
        knowledge.nodes["node1"] = Node("node1", FAKE_HW_CONFIG, "", [])

    # Planner
    scenario_pln: ScenarioPlanner
    if not args.fake_planner:
        scenario_pln = PredictorScenarioPlanner(knowledge)
    else:
        scenario_pln = FakeScenarioPlanner(knowledge.nodes[0].hardware_id)

    dependency_solver = MasterSlaveSolver()

    app_judge = AppJudge(app_db, scenario_pln)

    # Executor
    scenario_executor: ScenarioExecutor
    if not args.fake_mapek:
        assert mapek_wrapper is not None
        scenario_executor = ScenarioExecutor(knowledge, scenario_pln, dependency_solver, mapek_wrapper, app_judge,
                                             multi_thread=args.multi_thread)
    else:
        assert mapek_wrapper is None
        scenario_executor = FakeScenarioExecutor(knowledge, scenario_pln, dependency_solver, app_judge)

    # Deploy controller server
    deploy_ctl_thread = Thread(target=deploy_controller.start_servers,
                               args=(knowledge, app_db, scenario_pln, app_judge),
                               name="DC-Thread")
    deploy_ctl_thread.start()

    # Start benchmark controller thread
    scenario_executor_thread = Thread(target=scenario_executor.run, name="SE-Thread")
    scenario_executor_thread.start()

    # Sleep and wait for exit
    logging.info("Assessment process started")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info('^C received, ending')
