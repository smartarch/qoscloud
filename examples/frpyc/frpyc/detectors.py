#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from enum import Enum

import cv2


class NoDetector(object):
    def __init__(self, model):
        self.model = model

    def detectMultiScale(self, image):
        return []


class CpuDetectorWrapper(object):
    def __init__(self, model):
        self.dctor = cv2.CascadeClassifier(model)

    def detectMultiScale(self, image):
        faces = self.dctor.detectMultiScale(image)
        ret_faces = []
        for (x, y, w, h) in faces:
            ret_faces.append((x.item(), y.item(), w.item(), h.item()))
        return ret_faces


try:
    from gpufacedetector import Recognizer as gpurecognizer
except ImportError:
    # logging.debug('"gpufacedetector" module not found. Using cpu even when gpu is requested.')

    class gpurecognizer(CpuDetectorWrapper):
        pass


class Detectors(Enum):
    non = (NoDetector, 'skipping detection')
    cpu = (CpuDetectorWrapper, 'using cpu')
    gpu = (gpurecognizer, 'using gpu')

    def __init__(self, detector, msg):
        self.dctor = detector
        self.msg = msg

    def detector(self, model):
        return self.dctor(model)

    def __str__(self):
        return self.msg


def get_face_detector(model, version=Detectors.gpu):
    return version.detector(model)
