#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import logging
import sys
import time
from concurrent import futures
from enum import Enum
from io import BytesIO
from pathlib import Path
from threading import RLock

import grpc
import numpy as np
import yaml
from PIL import Image

import frpyc.rs_pb2 as protocols
import frpyc.rs_pb2_grpc as servicers
from cloud_controller.middleware.agent import ServerAgent
from cloud_controller.middleware.helpers import setup_logging

from frpyc.detectors import Detectors
from frpyc.facerecognition import FaceRecognizer
from frpyc import RS_HOST, RS_PORT


class TransferableRecognizerData:

    def __init__(self):
        self.image_hashtable = {}
        self.total_processed_images = 0
        self.unique_images = 0

    def deserialize(self, data: str) -> None:
        data_dict = yaml.safe_load(data)
        self.total_processed_images = data_dict["total"]
        self.unique_images = data_dict["unique"]
        self.image_hashtable = data_dict["hashtable"]
        for hash_, faces in data_dict["hashtable"].items():
            self.image_hashtable[hash_] = []
            for face in faces:
                self.image_hashtable[hash_].append(((face["x"], face["y"], face["w"], face["h"]),
                                                    face["name"], face["conf"]))
        logging.info(f"Received the state. Total images: {self.total_processed_images}. Unique images: "
                     f"{self.unique_images}. Hashed images: {len(self.image_hashtable)}")

    def serialize(self) -> str:
        data_dict = {
            "total": self.total_processed_images,
            "unique": self.unique_images,
            "hashtable": {},
        }
        for hash_, faces in self.image_hashtable.items():
            data_dict["hashtable"][hash_] = []
            for face in faces:
                data_dict["hashtable"][hash_].append({
                    "x": face[0][0],
                    "y": face[0][1],
                    "w": face[0][2],
                    "h": face[0][3],
                    "name": face[1],
                    "conf": face[2]
                })

        logging.info(f"Sent the state. Total images: {self.total_processed_images}. Unique images: "
                     f"{self.unique_images}. Hashed images: {len(self.image_hashtable)}")
        return yaml.dump(data_dict)


class RecognizerPhase(Enum):
    INIT = 0
    WORKING = 1
    FINISHED = 2


class RecognizerGrpcServer(servicers.RecognizerServerServicer):

    def __init__(self, model, detector, faces_dir):
        """
        Initializes the server.
        """
        self.model = model
        self.detector = detector
        self.lock = RLock()
        self.trained_recognizer = FaceRecognizer(detector=detector, model=model)
        self.data = TransferableRecognizerData()
        self.phase: RecognizerPhase = RecognizerPhase.INIT
        if detector != Detectors.non:
            self.trained_recognizer.trainFromDir(str(faces_dir))

    @staticmethod
    def hash_image(image) -> int:
        return int(hashlib.md5(image).hexdigest(), 16)

    def finalize(self):
        with self.lock:
            self.phase = RecognizerPhase.FINISHED
        return self.data.serialize()

    def initialize(self, data):
        with self.lock:
            if self.phase == RecognizerPhase.INIT:
                self.data.deserialize(data)
                self.phase = RecognizerPhase.WORKING

    def Recognize(self, request: protocols.RecognitionData, context):
        """
        Processes a single image from the client. Uses a locally trained recognizer.
        :param request: RecognitionData, a protobuf containing an image sent from client
        :param context: gRPC servicer context
        :return: Recognized faces and their locations
        """
        with self.lock:
            if self.phase == RecognizerPhase.INIT:
                self.phase = RecognizerPhase.WORKING
            elif self.phase == RecognizerPhase.FINISHED:
                return protocols.RecognizedFaces(rc=protocols.RecognizerRC.Value("FINISHED"))
        logging.debug("Processing an image from client %s." % request.clientId)
        image_pil = Image.open(BytesIO(request.bytes)).convert('L')
        image = np.array(image_pil, 'uint8')
        hash_ = self.hash_image(request.bytes)
        if hash_ not in self.data.image_hashtable:
            faces_with_info = self.trained_recognizer.detectAndRecognize(image)
            self.data.image_hashtable[hash_] = faces_with_info
            self.data.unique_images += 1
        else:
            faces_with_info = self.data.image_hashtable[hash_]
        response = protocols.RecognizedFaces()
        for (face, nbr_predicted, conf) in faces_with_info:
            face_pb = response.faces.add()
            face_pb.name = nbr_predicted
            face_pb.confidence = conf
            face_pb.x, face_pb.y, face_pb.width, face_pb.height = face[0], face[1], face[2], face[3]
        logging.info("Image from client %s: detected %d faces." % (request.clientId, len(faces_with_info)))
        self.data.total_processed_images += 1
        response.total = self.data.total_processed_images
        response.unique = self.data.unique_images
        logging.info(f"Images processed: {self.data.total_processed_images}. Unique images: {self.data.unique_images}.")
        return response


def run():
    setup_logging()
    # Parse arguments:
    parser = argparse.ArgumentParser(description='A face recognizer server')
    parser.add_argument('-m', '--model', default='lbpcascade_frontalface.xml', help='path to the face detector model (default lbpcascade_frontalface.xml')
    parser.add_argument('-c', '--usecpu', action='store_true', help='use cpu (gpu is default)')
    parser.add_argument('-s', '--skip', action='store_true', help='immediately return zero detected faces')
    parser.add_argument('-f', '--faces', type=str, default=None, help='directory with faces to be learnt and recognized')

    args = parser.parse_args()
    # decide whether to use DB or not
    if args.faces:
        img_dir = Path(args.faces)
        if not img_dir.is_dir():
            print('%s is not directory' % args.faces)
            sys.exit(1)
    else:
        logging.error("This implementation of recognizer cannot work with database. A directory with faces to be "
                      "trained from must be specified (syntax: \"-f directory\").")
        return
    # setting appropriate face detector
    if args.skip:
        detector = Detectors.non
    elif args.usecpu:
        detector = Detectors.cpu
    else:
        detector = Detectors.gpu
    logging.info('Starting server on %s:%s, using %s.' % (RS_HOST, str(RS_PORT), str(detector)))

    # Instantiate the recognizer:
    recognizer = RecognizerGrpcServer(args.model, detector, img_dir)

    # Integrating and starting middleware agent:
    agent = ServerAgent({}, None, recognizer.finalize, recognizer.initialize)
    agent.start()
    # Start the recognizer:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicers.add_RecognizerServerServicer_to_server(recognizer, server)
    server.add_insecure_port(RS_HOST + ":" + str(RS_PORT))
    server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info('^C received, ending')
        server.stop(0)


if __name__ == "__main__":
    run()
