#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
from pathlib import Path
from time import perf_counter
from typing import List

import argparse
import grpc
import frpyc
import frpyc.rs_pb2 as protocols
import frpyc.rs_pb2_grpc as servicers
from cloud_controller.middleware.agent import ClientAgent
from cloud_controller.middleware.helpers import setup_logging

TIMEOUT = 6  # Seconds


class RecognizerClient:

    def __init__(self, agent_: ClientAgent, images_: List):
        self.images = images_
        self.agent = agent_
        self.recognizer_ip = None
        self.client_id = None
        self.recognizer = None

    def initialize(self):
        """
        Waits until client agent receives the client ID and an address of the recognizer dependency (and thus can start
        communicating with the recognizer)
        """
        while self.recognizer_ip is None or self.client_id is None:
            self.recognizer_ip = self.agent.get_dependency_address(frpyc.RECOGNIZER_TYPE)
            self.client_id = self.agent.get_id()
            time.sleep(1)
        channel = grpc.insecure_channel(self.recognizer_ip + ":" + str(frpyc.RS_PORT))
        self.recognizer = servicers.RecognizerServerStub(channel)

    def check_dependency_changes(self):
        """
        Checks whether the address of the recognizer has changed (since the time of the previos call). Has to be called
        each time before calling a recognizer method. In the future will be moved to the client agent (the agent will
        perform this check automatically before each call)
        """
        if self.recognizer_ip != self.agent.get_dependency_address(frpyc.RECOGNIZER_TYPE):
            logging.info('Handover: changing recognizer address from %s to %s' %
                         (self.recognizer_ip, self.agent.get_dependency_address(frpyc.RECOGNIZER_TYPE)))
            self.recognize_with_timeout(protocols.RecognitionData(imageId=-1), TIMEOUT)
            self.recognizer_ip = self.agent.get_dependency_address(frpyc.RECOGNIZER_TYPE)
            channel = grpc.insecure_channel(self.recognizer_ip + ":" + str(frpyc.RS_PORT))
            self.recognizer = servicers.RecognizerServerStub(channel)

    def recognize_with_timeout(self, image_pb, timeout):
        """
        This method is used only because setting gRPC timeout does not always work.
        :param image_pb: request to the recognizer.
        :param timeout: timeout for request (seconds).
        :return: Recognizer response or None if timeout had happened.
        """
        start = perf_counter()
        faces_future = self.recognizer.Recognize.future(image_pb)
        while perf_counter() - start < timeout:
            if faces_future.done():
                return faces_future.result()
        faces_future.cancel()
        return None

    def recognize(self, image):
        """
        Sends a single image to the assigned recognizer.
        :param image: an image to recognize
        :return: detected faces
        """
        self.check_dependency_changes()
        logging.info('Processing image %s using recognizer at %s' % (str(image), self.recognizer_ip))
        image_pb = protocols.RecognitionData()
        image_pb.clientId = self.client_id
        with image.open(mode='rb') as f:
            content = f.read()
            image_pb.bytes = content
        faces = self.recognize_with_timeout(image_pb, TIMEOUT)
        if faces is None:
            logging.info("Request to recognizer timeouted")
        elif faces.rc == protocols.RecognizerRC.Value("OK"):
            logging.info('Detected %d faces in %s' % (len(faces.faces), str(image)))
            logging.info(f"Images processed: {faces.total}. Unique images: {faces.unique}.")
        elif faces.rc == protocols.RecognizerRC.Value("FINISHED"):
            logging.info("Recognizer is transferring state to a new recognizer. Cannot get response.")

        return faces

    def run(self):
        """
        Runs the client. Sends the images from the provided list to the server for recognition.
        :return:
        """
        try:
            self.initialize()
            while True:
                try:
                    for i in range(0, len(self.images)):
                        start = time.perf_counter()
                        self.recognize(self.images[i])
                        logging.info(f"Recognition time: {(time.perf_counter() - start):.6f} seconds")
                        time.sleep(frpyc.CLIENT_PERIOD / 1000.0)
                except grpc._channel._Rendezvous:
                    logging.info(f"Could not connect to recognizer. Retrying")
                    time.sleep(frpyc.CLIENT_PERIOD / 1000.0)
        except KeyboardInterrupt:
            logging.info('^C received, ending')
            self.agent.stop()


def list_images():
    p = Path(frpyc.IMAGES_PATH)
    if p.is_dir():
        images_ = [x for x in p.iterdir()]
    elif p.exists():
        images_ = [p]
    else:
        logging.error("The given path (%s) does not exist." % frpyc.IMAGES_PATH)
        images_ = None
    return images_


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Frpyc client')
    parser.add_argument('-i', '--id', default=None, type=str, help='desired client ID')
    parser.add_argument('-f', '--force', action='store_true', help='exit if received ID does not match the real one')
    args = parser.parse_args()

    agent = ClientAgent(frpyc.APPLICATION_NAME, frpyc.CLIENT_TYPE, args.id)
    agent.start()
    setup_logging()

    while not agent.connected():
        time.sleep(1)
    if args.force:
        while agent.get_id() != args.id:
            logging.info(f"Received ID ({agent.get_id()} does not match the desired one. Restarting the agent.")
            agent.stop()
            agent = ClientAgent(frpyc.APPLICATION_NAME, frpyc.CLIENT_TYPE, args.id)
            agent.start()
            while not agent.connected():
                time.sleep(1)

    images = list_images()
    if images is not None:
        client = RecognizerClient(agent, images)
        client.run()
    else:
        agent.stop()

