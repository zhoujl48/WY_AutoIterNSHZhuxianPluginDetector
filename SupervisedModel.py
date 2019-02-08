#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "离线训练模块: 监督模型基类"
__usage1__ = "供调用"

# 仅供其他监督模型类继承


from abc import abstractmethod, ABCMeta
import tensorflow.keras.backend as K


class SupervisedModel(object):
    __metaclass__ = ABCMeta

    def __init__(self, epoch=30, batch_size=128, regular=0.001):
        self._epoch = epoch
        self._batch_size = batch_size
        self._regular = regular

    '''metrics f1_score'''
    @staticmethod
    def f1_score(y_true, y_pred):
        c1 = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        c2 = K.sum(K.round(K.clip(y_pred, 0, 1)))
        c3 = K.sum(K.round(K.clip(y_true, 0, 1)))
        precision = c1 / (c2 + K.epsilon())
        recall = c1 / (c3 + K.epsilon())
        f1_score = (2 * precision * recall) / (precision + recall + K.epsilon())
        return f1_score

    @staticmethod
    def precision(y_true, y_pred):
        c1 = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        c2 = K.sum(K.round(K.clip(y_pred, 0, 1)))
        precision = c1 / (c2 + K.epsilon())
        return precision

    @staticmethod
    def recall(y_true, y_pred):
        print(y_true, y_pred)
        c1 = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        c3 = K.sum(K.round(K.clip(y_true, 0, 1)))
        recall = c1 / (c3 + K.epsilon())
        return recall

    @abstractmethod
    def model(self):
        raise NotImplemented

    @abstractmethod
    def run(self):
        raise NotImplemented


