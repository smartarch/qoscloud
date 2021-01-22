from cloud_controller.middleware.helpers import load_config_from_file
from cloud_controller.knowledge.user_equipment import UEManagementPolicy

CONFIG_FILE_LOCATION = "config/main-config.yaml"  # This parameter is not configurable externally

CLIENT_CONTROLLER_HOST = "0.0.0.0"
CLIENT_CONTROLLER_PORT = 4217
DEFAULT_SECRET_NAME = "dockerhubsecret"
DEFAULT_HARDWARE_ID = "nodetype_1"
PRODUCTION_KUBECONFIG = "/root/.kube/config-production"
ASSESSMENT_KUBECONFIG = "/root/.kube/config-assessment"
DATACENTER_LABEL = "mlmec/DataCenter"
MONGO_SHARD_LABEL = "mlmec/MongoShard"
MONGOS_LABEL = "mlmec/MongosHost"
HARDWARE_ID_LABEL = "mlmec/HardwareID"
HOSTNAME_LABEL = "kubernetes.io/hostname"
DEFAULT_UE_MANAGEMENT_POLICY = UEManagementPolicy.FREE
MAX_CLIENTS = 200  # Basically, the number of clients that can be connected at the same time
PRODUCTION_MONGOS_SERVER_IP = '10.210.0.24'
ASSESSMENT_MONGOS_SERVER_IP = '10.190.0.24'
MAIN_MONGO_SHARD_NAME = 'master'
SYSTEM_DATABASE_NAME = 'default'
APPS_COLLECTION_NAME = 'apps'
RESERVED_NAMESPACES = ['default', 'kube-node-lease', 'kube-public', 'kube-system']
DEBUG = True
THREAD_COUNT = 32
DEFAULT_EUCLID_TOPOLOGY_CONFIG = "config/topology-config.yaml"
DEFAULT_PREDICTOR_CONFIG = "./config/predictor-config.yaml"
USE_VIRTUAL_NETWORK_CONTROLLER = False
DEFAULT_DOCKER_IMAGE = "dankhalev/ivis-job"

PREDICTOR_HOST = "0.0.0.0"
PREDICTOR_PORT = 4317
GLOBAL_PERCENTILE = 90.0

API_ENDPOINT_IP = "195.113.20.222"
API_ENDPOINT_PORT = 8082

DEFAULT_WARMUP_RUNS = 0
DEFAULT_MEASURED_RUNS = 100

THROUGHPUT_ENABLED = True
THROUGHPUT_PERCENTILES = [50.0, 90.0, 99.0]
REPORTED_PERCENTILES = [50, 80, 90, 95]

DEFAULT_WAIT_SIGNAL_FREQUENCY = 5  # Seconds
VIRTUAL_COUNT_CONSTANT = 3
VIRTUAL_COUNT_PERCENT = 0.1

DATAFILE_EXTENSION = ".csv"
HEADERFILE_EXTENSION = ".header"
RESULTS_PATH = "./measurements"
MAX_ARITY = 5
CSP_DEFAULT_TIME_LIMIT = 5
CSP_RUNNING_NODE_COST = 1
CSP_LATENCY_COST = 10
CSP_REDEPLOYMENT_COST = 2

load_config_from_file(__name__, CONFIG_FILE_LOCATION)
if not isinstance(DEFAULT_UE_MANAGEMENT_POLICY, UEManagementPolicy):
    assert isinstance(DEFAULT_UE_MANAGEMENT_POLICY, str)
    DEFAULT_UE_MANAGEMENT_POLICY = UEManagementPolicy[DEFAULT_UE_MANAGEMENT_POLICY.upper()]
