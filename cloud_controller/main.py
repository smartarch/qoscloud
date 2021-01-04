import threading

from cloud_controller import PRODUCTION_MONGOS_SERVER_IP
from cloud_controller.cleanup import ClusterCleaner
from cloud_controller.extension_manager import ExtensionManager
from cloud_controller.ivis.ivis_interface import IvisInterface
from cloud_controller.ivis.ivis_mock import IVIS_INTERFACE_HOST, IVIS_INTERFACE_PORT
from cloud_controller.ivis.ivis_pb2_grpc import add_IvisInterfaceServicer_to_server
from cloud_controller.middleware.helpers import setup_logging, start_grpc_server

if __name__ == "__main__":
    # TONOWDO: move ivis interface start elsewhere
    setup_logging()
    ClusterCleaner(PRODUCTION_MONGOS_SERVER_IP).cleanup()
    adaptation_ctl = ExtensionManager().get_adaptation_ctl()
    ivis_interface = IvisInterface(adaptation_ctl.knowledge)
    ivis_interface_thread = threading.Thread(
        target=start_grpc_server,
        args=(ivis_interface, add_IvisInterfaceServicer_to_server, IVIS_INTERFACE_HOST, IVIS_INTERFACE_PORT, 10, True))
    ivis_interface_thread.start()
    adaptation_ctl.run()
