#!/usr/bin/python
# -*- coding:utf-8 -*-

__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "离线训练模块: MLP模型"
__usage1__ = "python MLPModel.py --ds_start 20181212 --ds_num 28 ..."

import argparse
import numpy as np
from datetime import datetime, timedelta
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import load_model, Sequential
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras import regularizers
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from SupervisedModel import SupervisedModel
from FeatureEngineering import *
from config import SAVE_DIR_BASE


class MLPModel(SupervisedModel):
    def __init__(self, train_data, test_data, feature_type, save_path='base', epoch=30, batch_size=128, dropout_size=0.2,
                 regular=0.002, dense_size_1=128, dense_size_2=128, ids=None):
        SupervisedModel.__init__(self, epoch=epoch, batch_size=batch_size, regular=regular)
        '''Data'''
        assert len(train_data) == 2 and len(test_data) == 2
        self._feature_train, self._label_train = train_data
        self._feature_test, self._label_test = test_data
        self._feature_test = np.array(self._feature_test)
        self._feature_train = np.array(self._feature_train)
        self._feature_type = feature_type
        self._ids = ids

        '''Network'''
        self._embedding_dropout_size = dropout_size
        self._dense_size_1 = dense_size_1
        self._dense_size_2 = dense_size_2
        self._dense_dropout_size = dropout_size
        self._model_file = os.path.join(save_path, 'mlp_feature_{feature}_dense1_{dense_size1}_dense2_{dense_size2}'.format(
                                        feature=self._feature_type,
                                        dense_size1=self._dense_size_1,
                                        dense_size2=self._dense_size_2))

    # Model定义及训练
    def model(self):
        log('[{time}] Building model...'.format(time=get_time()))
        model = Sequential()
        model.add(Dense(self._dense_size_1, input_dim=len(self._feature_train[0]), activation='relu',
                        kernel_regularizer=regularizers.l1(self._regular)))
        model.add(Dense(self._dense_size_2, activation='relu', kernel_regularizer=regularizers.l1(self._regular)))
        model.add(Dense(1, activation='sigmoid'))
        # if os.path.exists(self.model_file):
        #     model.load_weights(self.model_file)
        model.compile(loss='binary_crossentropy',
                      optimizer='adam',
                      metrics=['accuracy', self.precision, self.recall, self.f1_score])
        log(model.summary())
        # checkpoint
        checkpoint = ModelCheckpoint(self._model_file + '.{epoch:03d}-{val_f1_score:.4f}.hdf5', monitor='val_f1_score',
                                     verbose=1, save_best_only=True, mode='max')
        callbacks_list = [checkpoint]
        log('[{time}] Training...'.format(time=get_time()))
        model.fit(self._feature_train,
                  self._label_train,
                  epochs=self._epoch,
                  callbacks=callbacks_list,
                  validation_data=(self._feature_test, self._label_test))

    # 离线训练调用接口
    def run(self):
        self.model()

    # 预测调用接口(离线评估用)
    def run_predict(self, model_path, ts_pred_start):

        # 加载模型
        model = load_model(model_path, compile=False)

        # 预测
        suspect_scores = model.predict(self._feature_train)
        print(len(self._ids), len(suspect_scores))

        # 保存文件，返回预测结果
        result_file = 'mlp_{ts_pred_start}'.format(ts_pred_start=ts_pred_start)
        pred_dir = os.path.join(SAVE_DIR_BASE, 'classification')
        if not os.path.exists(pred_dir):
            os.mkdir(pred_dir)
        results = list()
        with open(os.path.join(pred_dir, result_file), 'w') as f:
            for i in range(len(self._ids)):
                role_id = str(self._ids[i])
                suspect_score = str(suspect_scores[i][0])
                f.write(role_id + ',' + suspect_score + '\n')
                results.append([role_id, suspect_score])
        return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser('MLP Model Train, feature generation and model train. \n'
                                     'Usage: python MLPModel.py ds_range_list ... ..')
    parser.add_argument('--ds_start', type=str)
    parser.add_argument('--ds_num', type=int)
    parser.add_argument('--feature', help='set specified feature generated for training. available: '
                                          '\'freq\', \'freqg\', \'seq\', \'tseq\', \'time\', \'timeg\'', default='freq')
    parser.add_argument('--epoch', help='set the training epochs', default=30, type=int)
    parser.add_argument('--batch_size', help='set the training batch size', default=128, type=int)
    parser.add_argument('--dropout_size', help='set the dropout size for fc layer or lstm cells', default=0.2, type=float)
    parser.add_argument('--regular', help='set regularization', default=0.0, type=float)
    parser.add_argument('--dense_size_1', help='set dense size 1', default=64, type=int)
    parser.add_argument('--dense_size_2', help='set dense size 2', default=32, type=int)
    parser.add_argument('--test_size', help='set test ratio when splitting data sets into train and test', default=0.2, type=float)
    parser.add_argument('--sampling_type', help='set sampling type, \'up\' or \'down\'', default='up')
    parser.add_argument('--max_num', help='max num of data of each label', default=0, type=int)
    args = parser.parse_args()
    ds_start = args.ds_start
    ds_num = args.ds_num
    cal_ds = lambda ds_str, ds_delta: (datetime.strptime(ds_str, '%Y%m%d') + timedelta(days=ds_delta)).strftime('%Y%m%d')
    ds_list = [cal_ds(ds_start, ds_delta) for ds_delta in range(ds_num)]

    # 数据路径
    data_path_list = [os.path.join(SAVE_DIR_BASE, 'data', ds_range) for ds_range in ds_list]
    logid_path = os.path.join('/home/zhoujialiang/nsh_zhuxian_sl_auto/logid', '41')

    # 模型保存路径
    PATH_MODEL_SAVE = os.path.join(SAVE_DIR_BASE, 'model', '{}_{}'.format(ds_start, ds_num))
    if not os.path.exists(PATH_MODEL_SAVE):
        os.mkdir(PATH_MODEL_SAVE)

    # 导入数据，训练
    data = eval('Ev{feature}Loader_hbase(data_path_list, logid_path=logid_path, sampling_type=args.sampling_type, '
                'test_size=args.test_size, max_num=args.max_num)'.format(feature=args.feature))
    data.run()
    model = MLPModel(train_data=data.train_data, test_data=data.test_data, feature_type=args.feature, save_path=PATH_MODEL_SAVE,
                     epoch=args.epoch, batch_size=args.batch_size, dropout_size=args.dropout_size,
                     regular=args.regular, dense_size_1=args.dense_size_1, dense_size_2=args.dense_size_2)
    model.run()
