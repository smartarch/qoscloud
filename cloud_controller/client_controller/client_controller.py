"""
Client controller module.

Consists of internal and external interfaces to the Client Controller, as well as ClientModel - the data structure
that is shared between these interfaces.

Both interfaces of the Client Controller may be started with start_client_controller function.
"""
import logging
import sys
import time

import argparse

import cloud_controller.knowledge.knowledge_pb2_grpc as servicers
import cloud_controller.middleware.middleware_pb2_grpc as mw_servicers
from cloud_controller import CLIENT_CONTROLLER_HOST, CLIENT_CONTROLLER_PORT, MAX_CLIENTS, DEFAULT_WAIT_SIGNAL_FREQUENCY
from cloud_controller.client_controller.client_model import ClientModel
from cloud_controller.client_controller.client import ClientStatus
from cloud_controller.client_controller.external import ClientControllerExternal
from cloud_controller.client_controller.internal import ClientControllerInternal

from cloud_controller.middleware import CLIENT_CONTROLLER_EXTERNAL_HOST, CLIENT_CONTROLLER_EXTERNAL_PORT
from cloud_controller.middleware.helpers import setup_logging, start_grpc_server


def start_client_controller(wait_signal_frequency=DEFAULT_WAIT_SIGNAL_FREQUENCY) -> None:
    """
    Starts both external and internal Client Controller interfaces. Creates the shared data structure for these
    interfaces. Runs the liveness check thread that checks whether any clients have been disconnected.
    This function does not return.
    May be invoked from a thread or as a separate process.
    """
    setup_logging()
    # A common data structure for external and internal interfaces:
    client_model = ClientModel(wait_signal_frequency)

    internal_server = start_grpc_server(
        servicer=ClientControllerInternal(client_model),
        adder=servicers.add_ClientControllerInternalServicer_to_server,
        host=CLIENT_CONTROLLER_HOST,
        port=CLIENT_CONTROLLER_PORT
    )

    external_server = start_grpc_server(
        servicer=ClientControllerExternal(client_model),
        adder=mw_servicers.add_ClientControllerExternalServicer_to_server,
        host=CLIENT_CONTROLLER_EXTERNAL_HOST,
        port=CLIENT_CONTROLLER_EXTERNAL_PORT,
        threads=MAX_CLIENTS
    )

    try:
        while True:
            time.sleep(client_model.liveness_check_frequency)
            client_model.update_distances()
            timestamp = time.perf_counter()
            clients_to_delete = []
            for application in client_model.clients:
                for id_ in client_model.clients[application]:
                    client = client_model.clients[application][id_]
                    if client.last_call < timestamp - client_model.liveness_check_frequency and \
                            client.status == ClientStatus.CONNECTED:
                        clients_to_delete.append((application, id_))
            for app, id_ in clients_to_delete:
                logging.info("Cancelling the call for client (%s:%s)" % (app, id_))
                client_model.disconnect_client(app, id_)
    except KeyboardInterrupt:
        print("ClientController: ^C received, ending")
        external_server.stop(0)
        internal_server.stop(0)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Client controller")
    arg_parser.add_argument("-f", "--wait-signal-freq", type=float, default=5,
                            help="Frequency in seconds after which the WAIT command is send to a connected client.")
    args = arg_parser.parse_args()
    if not 0 < args.wait_signal_freq <= 60:
        print(f"Wait signal frequency {args.wait_signal_freq} is out of bounds.", file=sys.stderr)
        exit(1)

    start_client_controller(args.wait_signal_freq)
