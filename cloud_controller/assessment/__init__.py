from cloud_controller.middleware.helpers import load_config_from_file

ASSESSMENT_CONFIG_FILE_LOCATION = "config/assessment-config.yaml"  # This parameter is not configurable externally

CTL_PORT = 9932
CTL_HOST = "0.0.0.0"

PUBLISHER_PORT = 9938
PUBLISHER_HOST = "0.0.0.0"

RESULTS_PATH = "./probes"

load_config_from_file(__name__, ASSESSMENT_CONFIG_FILE_LOCATION)
