#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from cloud_controller.middleware import AGENT_HOST, AGENT_PORT
from cloud_controller.middleware.helpers import setup_logging, start_grpc_server
from cloud_controller.middleware.middleware_agent import MiddlewareAgent
from cloud_controller.middleware.middleware_pb2_grpc import add_MiddlewareAgentServicer_to_server

if __name__ == "__main__":
    setup_logging()
    agent = MiddlewareAgent(standalone=True)
    start_grpc_server(agent, add_MiddlewareAgentServicer_to_server, AGENT_HOST, AGENT_PORT, block=True)
