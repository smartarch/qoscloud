#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import tempfile

import cv2.face as cv2face
import numpy as np
from PIL import Image
from pymongo import MongoClient

from frpyc.detectors import get_face_detector, Detectors


class FaceRecognizer(object):
    """
    A wrapper around the face detector and recognizer.
    """
    def __init__(self, detector=Detectors.gpu, model='lbpcascade_frontalface.xml', mongoHost='localhost', mongoPort=27017):
        """
        Initializes the recognizer. 
        
        *model* is a path to the model file.
        *mongoHost* is the host name of a computer with mongoDB to be used for storing the trained recognizer
        *mongoPort* is the port where mongoDB accepts connections
        """
        self.model = model
        self.mongoHost = mongoHost
        self.mongoPort = mongoPort
        self.images = []
        self.labels = []
        self.names = []
        self.recognizer = cv2face.createLBPHFaceRecognizer()
        self.detector = get_face_detector(model, detector)

    def detectAndRecognize(self, image):
        """
        Detects and recognizes faces in the given image.
        
        *image* is the numpy array with the image 
        *Returns* a list of triples ``(rectangle, name, confidence)``
        """
        faces = self.detector.detectMultiScale(image)
        facesWithInfo = []
        for (x, y, w, h) in faces:
            (nbr_predicted, conf) = self.recognizer.predict(image[y: y + h, x: x + w])
            facesWithInfo.append(((x, y, w, h), self.names[nbr_predicted], conf))
        return facesWithInfo
        
    def trainFromDir(self, facesDirPath):
        """
        Trains the recognizer with the faces taken from the *facesDirPath*
        directory. Images containing the same person must have the same name,
        i.e., the file name pattern is as follows ``person_name.number.jpg``,
        e.g., ``Petr.1.jpg``, ``Petr.2.jpg``, ``Petr.3.jpg``, ``Tomas.1.jpg``,
        ``Tomas.2.jpg``, ``Tomas.3.jpg``.
        """
        image_paths = [os.path.join(facesDirPath, f) for f in os.listdir(facesDirPath)]
        logging.info('Loading images...')
        for image_path in image_paths:
            logging.info('Loading image %s',  image_path)
            image_pil = Image.open(image_path).convert('L')
            image = np.array(image_pil, 'uint8')
            name = os.path.split(image_path)[1].split(".")[0]
            if name not in self.names:
                self.names.append(name)
            lbl = self.names.index(name)
            faces = self.detector.detectMultiScale(image)
            if not faces:
                logging.warning('No faces detected in image %s', image_path)
            else:
                for (x, y, w, h) in faces:
                    logging.info('face detected: %d %d %d %d' % (x, y, w, h))
                    self.images.append(image[y: y + h, x: x + w])
                    self.labels.append(lbl)
        logging.info('Loading images finished')
        logging.info('Training images...')
        self.recognizer.train(self.images, np.array(self.labels))
        logging.info('Training images finished')
        
    def saveTrainedRecognizerToFile(self, filename):
        """
        Saves the trained recognizer to the given *file*.
        """
        self.recognizer.save(filename)
        
    def loadTrainedRecognizerFromFile(self, filename):
        """
        Loads the previously saved recognizer from the given *file*.
        """
        self.recognizer.load(filename)

    def saveTrainedRecognizerToDB(self, id):
        """
        Saves the trained recognizer to mongoDB.
        
        *id* is the identifier under which the recognizer is stored in DB
        """
        mClient = MongoClient(self.mongoHost, self.mongoPort)
        savedRecCollection = mClient.facesDB.savedRecognizers
        fn = tempfile.mkstemp(suffix = '.sav', prefix = 'facerec')
        os.close(fn[0])
        self.saveTrainedRecognizerToFile(fn[1])
        with open(fn[1], 'r') as content_file:
          savedRec = content_file.read()
          mongoDoc = {'id' : id, 'labels': self.labels, 'names' : self.names, 'recognizer' : savedRec }
          savedRecCollection.replace_one({'id' : id}, mongoDoc, True)
          logging.info('Recognizer saved')        
        os.remove(fn[1])

    def loadTrainedRecognizerFromDB(self, id):
        """
        Loads the trained recognizer from mongoDB.
        
        *id* is the identifier under which the recognizer is stored in DB
        """
        mClient = MongoClient(self.mongoHost, self.mongoPort)
        savedRecCollection = mClient.facesDB.savedRecognizers
        savedRec = savedRecCollection.find_one({'id' : id})
        if not savedRec:
            logging.error("no saved recognizer with the given id")
            raise RuntimeError("no saved recognizer with the given id")
        fn = tempfile.mkstemp(suffix = '.sav', prefix = 'facerec')
        os.close(fn[0])
        with open(fn[1], 'w') as content_file:
            content_file.write(savedRec['recognizer'])
        self.loadTrainedRecognizerFromFile(fn[1])
        self.labels = savedRec['labels']
        self.names = savedRec['names']
        os.remove(fn[1])
