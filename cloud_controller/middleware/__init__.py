"""
This package contains the middleware library that has to be integrated into any component (or client)
that is meant to be run with the framework.
"""

from cloud_controller.middleware.helpers import load_config_from_file

MIDDLEWARE_CONFIG_FILE_LOCATION = "config/middleware-config.yaml"  # This parameter is not configurable externally

AGENT_PORT = 7777
AGENT_HOST = "0.0.0.0"
CLIENT_CONTROLLER_EXTERNAL_PORT = 4444
CLIENT_CONTROLLER_EXTERNAL_HOST = "0.0.0.0"

MONGOS_PORT = 27017
SHARD_KEY = 'client_id'

load_config_from_file(__name__, MIDDLEWARE_CONFIG_FILE_LOCATION)
