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
from time import perf_counter

import grpc
import numpy as np
from PIL import Image
from pymongo import ReturnDocument

import frpyc.rs_pb2 as protocols
import frpyc.rs_pb2_grpc as servicers
from cloud_controller.middleware.user_agents import Phase, ComponentAgent
from cloud_controller.middleware.helpers import setup_logging
from cloud_controller.middleware.mongo_agent import MongoAgent

from frpyc.detectors import Detectors
from frpyc.facerecognition import FaceRecognizer
from frpyc import RS_HOST, RS_PORT


def convert_faces_to_doc(faces, hash_):
    faces_doc = {
        'hash': hash_,
        'faces': []
    }
    for face in faces:
        faces_doc['faces'].append({
            'x': face[0][0],
            'y': face[0][1],
            'w': face[0][2],
            'h': face[0][3],
            'name': face[1],
            'conf': face[2]
        })
    return faces_doc


class RecognizerPhase(Enum):
    INIT = 0
    WORKING = 1
    FINISHED = 2


class RecognizerGrpcServer(servicers.RecognizerServerServicer):

    def __init__(self, model, detector, faces_dir, agent):
        """
        Initializes the server.
        """
        self.model = model
        self.detector = detector
        self.lock = RLock()
        self.trained_recognizer = FaceRecognizer(detector=detector, model=model)
        self.phase: RecognizerPhase = RecognizerPhase.INIT
        if detector != Detectors.non:
            self.trained_recognizer.trainFromDir(str(faces_dir))
        # Prepare data
        self.agent: ComponentAgent = agent
        self.mongo_agent: MongoAgent = agent.get_mongo_agent()
        doc = self.mongo_agent.find_one({'role': 'counter'})
        if doc is None:
            self.mongo_agent.insert_one({
                'role': 'counter',
                'total': 0,
                'unique': 0
            })

    @staticmethod
    def hash_image(image) -> int:
        return int(hashlib.md5(image).hexdigest(), 16)

    def Recognize(self, request: protocols.RecognitionData, context):
        """
        Processes a single image from the client. Uses a locally trained recognizer.
        :param request: RecognitionData, a protobuf containing an image sent from client
        :param context: gRPC servicer context
        :return: Recognized faces and their locations
        """
        if self.agent.get_phase() == Phase.FINISHED:
            return protocols.RecognizedFaces(rc=protocols.RecognizerRC.Value("FINISHED"))
        if request.imageId == -1:
            self.agent.set_finished()
            return protocols.RecognizedFaces(rc=protocols.RecognizerRC.Value("FINISHED"))
        logging.debug("Processing an image from client %s." % request.clientId)
        image_pil = Image.open(BytesIO(request.bytes)).convert('L')
        image = np.array(image_pil, 'uint8')
        hash_ = str(self.hash_image(request.bytes))
        doc = self.mongo_agent.find_one({'hash': hash_})
        if doc is None:
            faces_with_info = self.trained_recognizer.detectAndRecognize(image)
            doc = convert_faces_to_doc(faces_with_info, hash_)
            self.mongo_agent.insert_one(doc)
            self.mongo_agent.update_one({'role': 'counter'}, {'$inc': {'unique': 1}})
        response = protocols.RecognizedFaces()
        for face in doc['faces']:
            face_pb = response.faces.add()
            face_pb.name = face['name']
            face_pb.confidence = face['conf']
            face_pb.x, face_pb.y, face_pb.width, face_pb.height = face['x'], face['y'], face['w'], face['h']
        logging.info("Image from client %s: detected %d faces." % (request.clientId, len(doc['faces'])))
        doc = self.mongo_agent.find_one_and_update({'role': 'counter'}, {'$inc': {'total': 1}},
                                                   return_document=ReturnDocument.AFTER)
        response.total = doc['total']
        response.unique = doc['unique']
        logging.info(f"Images processed: {doc['total']}. Unique images: {doc['unique']}.")
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

    # Integrating and starting middleware agent:
    agent = ComponentAgent({})
    agent.start()
    mongo_agent = None
    while not mongo_agent:
        mongo_agent = agent.get_mongo_agent()

    # Start the recognizer:
    recognizer = RecognizerGrpcServer(args.model, detector, img_dir, agent)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicers.add_RecognizerServerServicer_to_server(recognizer, server)
    server.add_insecure_port(RS_HOST + ":" + str(RS_PORT))
    server.start()
    agent.set_ready()

    try:
        finalization_time = None
        while True:
            if not finalization_time and agent.get_phase() == Phase.FINALIZING:
                finalization_time = perf_counter()
            if finalization_time and perf_counter() - finalization_time > 30.0:
                agent.set_finished()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info('^C received, ending')
        server.stop(0)


if __name__ == "__main__":
    run()
